# Tender Watch

面向公路养护类招标信息的定向采集器，当前固定支持 4 个 profile。

当前系统支持：
- 全量快照 `snapshot`
- 增量采集 `incremental`
- 问题源补采 `retry` / `retry_high_value` / `retry_long_tail`
- Profile 切换
- 站点差异化策略
- 反爬处理
- API 直连
- 问题日志与运行汇总

## 目录结构

- [collector.py](/Users/openclaw/Documents/Playground/tender-watch/collector.py): 主程序
- [config/sources.csv](/Users/openclaw/Documents/Playground/tender-watch/config/sources.csv): 信息源配置
- [config/rules.json](/Users/openclaw/Documents/Playground/tender-watch/config/rules.json): 过滤规则配置
- `config/profiles/`: 可切换监控口径
- [data/last_incremental_run.txt](/Users/openclaw/Documents/Playground/tender-watch/data/last_incremental_run.txt): 历史兼容的全局增量时间文件
- `data/last_incremental_run_<profile_id>.txt`: 当前实际使用的 profile 级增量时间文件
- [data/fetch_strategy.json](/Users/openclaw/Documents/Playground/tender-watch/data/fetch_strategy.json): 抓取策略缓存
- `logs/`: 汇总与问题日志
- `output/`: 结果文件
- [run_monitor.sh](/Users/openclaw/Documents/Playground/tender-watch/run_monitor.sh): 一键运行脚本
- [dashboard_server.py](/Users/openclaw/Documents/Playground/tender-watch/dashboard_server.py): 本地 Web 控制页服务
- [open_dashboard.sh](/Users/openclaw/Documents/Playground/tender-watch/open_dashboard.sh): 启动控制页并打开浏览器
- [ops/com.openclaw.tender-watch.incremental.plist](/Users/openclaw/Documents/Playground/tender-watch/ops/com.openclaw.tender-watch.incremental.plist): `launchd` 定时模板

## 环境准备

```bash
cd /Users/openclaw/Documents/Playground/tender-watch
python3 -m venv .venv
source .venv/bin/activate
pip install requests beautifulsoup4 lxml cloudscraper playwright
```

如果 Playwright 未安装浏览器，可执行：

```bash
python -m playwright install chromium
```

当前程序会优先使用本机 Chrome；若无本机 Chrome，再回退到 Playwright 自带浏览器。

## 运行方式

### 1. 全量快照

只保留 `2026-01-01 00:00:00` 之后的命中。

```bash
python collector.py snapshot
```

输出：
- `output/hits_full_<profile_id>_*.json`
- `logs/issues_full_<profile_id>_*.json`
- `logs/summary_full_<profile_id>.json`
- `logs/summary_full.json`：最近一次快照的通用指针

### 2. 增量采集

只保留“上次成功增量运行时间”之后的新公告。

```bash
python collector.py incremental
```

运行成功后会更新：
- `data/last_incremental_run_<profile_id>.txt`

说明：
- 4 个 profile 现在分别维护自己的增量时间窗
- 不同 profile 连续运行时，不会再互相覆盖“上次成功增量时间”

输出：
- `output/hits_<profile_id>_*.json`
- `logs/issues_<profile_id>_*.json`
- `logs/summary_incremental_<profile_id>.json`
- `logs/summary_incremental.json`：最近一次增量的通用指针

### 3. 问题源补采

```bash
python collector.py retry
python collector.py retry_high_value
python collector.py retry_long_tail
```

## Profile 切换

当前固定 4 个 profile：

- `hunan_operating_bot_concession_expressway_maintenance_design`
  - 湖南省内
  - 仅搜索“已确认项目清单”内的经营性 / BOT / 特许经营高速项目
  - 只保留 `养护 + 设计` 类项目
  - 直接剔除建设期项目，如 `新建`、`建设`、`扩容`、`改扩建`、`投资人`、`特许经营者`
  - 当前项目清单见 [profile1_hunan_concession_project_list_20260311.md](/Users/openclaw/Documents/Playground/tender-watch/output/profile1_hunan_concession_project_list_20260311.md)

- `non_hunan_expressway_maintenance_design`
  - 湖南省外
  - 高速公路
  - 养护 + 设计
  - 当前默认

- `non_hunan_local_road_maintenance_design`
  - 湖南省外
  - 地方道路 / 普通公路 / 国省道
  - 养护 + 设计

- `hunan_local_road_maintenance_construction`
  - 湖南省内
  - 地方道路 / 普通公路 / 国省道
  - 养护类词
  - 当前按“地方道路 + 养护类词”匹配，不再强制要求标题出现“施工”或“设计”
  - 当前已覆盖湖南省平台 + 14 个市州平台 + 湖南交通厅 + 湖南高速集团，共 `17` 个源

使用示例：

```bash
python collector.py snapshot --profile non_hunan_expressway_maintenance_design
python collector.py incremental --profile hunan_operating_bot_concession_expressway_maintenance_design
python collector.py snapshot --profile non_hunan_local_road_maintenance_design
python collector.py incremental --profile hunan_local_road_maintenance_construction
```

你也可以直接新增一个新的 profile 文件到 `config/profiles/`，然后通过 `--profile` 指定文件名。

## 推荐使用方式

### 日常监控

平时只跑增量：

```bash
python collector.py incremental
```

默认会使用 `3` 个 worker 跑非浏览器站点，减少省外 profile 的日常扫描耗时。

你主要看：
- [logs/summary_incremental.json](/Users/openclaw/Documents/Playground/tender-watch/logs/summary_incremental.json)
- 最新 `output/hits_*.json`

### 周期验收

每天或每周跑一次全量快照：

```bash
python collector.py snapshot
```

你主要看：
- [logs/summary_full.json](/Users/openclaw/Documents/Playground/tender-watch/logs/summary_full.json)
- 最新 `output/hits_full_*.json`

## 输出文件怎么看

### 结果文件

字段含义：
- `title`: 公告标题
- `url`: 公告链接
- `source`: 来源站点
- `province`: 省份
- `category`: 分类
- `published_at`: 发布时间
- `fetched_at`: 抓取时间
- `id`: 去重 ID

### 汇总文件

重点字段：
- `count`: 本轮结果数
- `cutoff`: 时间窗口起点
- `wall_time_sec`: 本轮总耗时
- `sources[].matched`: 每个站命中数
- `sources[].elapsed_sec`: 每个站耗时
- `issues_count`: 聚合问题数

### 问题日志

重点字段：
- `source`: 哪个站
- `stage`: 出问题阶段
- `problem`: 问题类型
- `sample_urls`: 样本链接
- `action`: 程序采取的跳过动作

## 一键运行脚本

常用方式：

```bash
./run_monitor.sh incremental
./run_monitor.sh snapshot
./run_monitor.sh retry_high_value
./run_monitor.sh incremental hunan_operating_bot_concession_expressway_maintenance_design
./run_monitor.sh snapshot non_hunan_local_road_maintenance_design
```

脚本会自动：
- 激活虚拟环境
- 创建日志目录
- 将终端输出写到 `logs/run_*.log`

## 本地控制页

启动方式：

```bash
cd /Users/openclaw/Documents/Playground/tender-watch
./open_dashboard.sh
```

默认地址：

```text
http://127.0.0.1:8765/
```

控制页功能：
- 4 个 profile x 2 种模式的操作卡片
- `刷新状态`：重新读取所有 profile 的最新汇总与结果路径
- `一键全部增量采集`：在 Terminal 中顺序启动 4 个 profile 的增量任务
- `弹出终端运行`：直接打开 macOS Terminal 并执行采集
- `查看最新命中`：单独预览当前结果文件前几条命中标题、来源和链接
- `查看最新结果`：在浏览器里打开最新 `hits_*.json`
- `查看问题日志`：在浏览器里打开最新 `issues_*.json`
- `打开结果文件`：用系统默认程序打开当前结果文件
- 状态标签会直接显示 `正常 / 有问题`
- 标红时会直接显示问题摘要，例如 `湖北省交通运输厅: detail_limit_exceeded -> skip_page_details`
- 当某个模式当前没有对应文件时，查看/打开按钮会自动禁用
- 顶部快捷按钮可直接打开 `output`、`logs`、4 个 Profile 源清单
- `Profile 1` 的项目清单已并入 `4 个 Profile 源清单` 文档中的 `hunan_operating_bot_concession_expressway_maintenance_design` 小节

## 定时运行

### 方案 A: launchd

模板文件：
- [ops/com.openclaw.tender-watch.incremental.plist](/Users/openclaw/Documents/Playground/tender-watch/ops/com.openclaw.tender-watch.incremental.plist)

安装方法：

```bash
mkdir -p ~/Library/LaunchAgents
cp /Users/openclaw/Documents/Playground/tender-watch/ops/com.openclaw.tender-watch.incremental.plist ~/Library/LaunchAgents/
launchctl unload ~/Library/LaunchAgents/com.openclaw.tender-watch.incremental.plist 2>/dev/null || true
launchctl load ~/Library/LaunchAgents/com.openclaw.tender-watch.incremental.plist
```

### 方案 B: crontab

示例：

```cron
0 9,12,15,18 * * * cd /Users/openclaw/Documents/Playground/tender-watch && ./run_monitor.sh incremental >> logs/cron_incremental.log 2>&1
30 18 * * * cd /Users/openclaw/Documents/Playground/tender-watch && ./run_monitor.sh snapshot >> logs/cron_snapshot.log 2>&1
```

## 当前建议流程

1. 日常运行：
```bash
./run_monitor.sh incremental
```

2. 查看结果：
```bash
cat logs/summary_incremental.json
ls -t output/hits_*.json | head
```

3. 周期验收：
```bash
./run_monitor.sh snapshot
cat logs/summary_full.json
```

4. 如果发现某站异常：
```bash
./run_monitor.sh retry_high_value
```

## 当前已知情况

- 4 个 profile 的增量时间窗已经完全独立，互不覆盖
- `profile 1` 当前快照结果为 `0`
  - 原因是当前口径已收紧为“确定项目清单内的养护设计类项目”
- `profile 4` 当前快照结果为 `28`
  - 省内市州源已经补齐到 `17` 个
- `profile 2` / `profile 3` 当前增量问题日志已清零
- 湖北交通厅分页和增量噪声问题已修复
