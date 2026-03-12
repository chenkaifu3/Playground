# OpenClaw VPS 一键安装工具

这个目录现在面向 Linux VPS。

控制端输入最少信息后，工具会通过 SSH 把 Bash 安装脚本传到 VPS，自动完成：

- 安装基础依赖：`curl`、`ca-certificates`、`gnupg`、`python3`
- 安装 Node.js 22
- `npm -g install openclaw@latest`
- 初始化 `~/.openclaw`
- 配置 Gateway 端口、bind、token、workspace
- 安装并启动 OpenClaw Gateway 服务
- 尝试开放 VPS 防火墙端口
- 尝试启用 `loginctl enable-linger`，让 systemd 用户服务在退出 SSH 后保持运行

## 文件

- `deploy.py`：控制端入口
- `remote_install.sh`：远端 Linux 安装脚本
- `inventory.example.yaml`：批量部署模板
- `inventory.yaml`：当前清单
- `requirements.txt`：控制端依赖

## 前提

控制端：

- macOS / Linux
- 已安装系统 `ssh`
- Python 3.10+
- 能通过 SSH 密钥登录 VPS

远端 VPS：

- Linux
- 推荐 Ubuntu / Debian
- SSH 已可登录
- 当前 SSH 用户最好是 `root`，或者至少具备 `sudo` 权限

## 最快使用方式

不写 inventory，直接交互输入：

```bash
cd /Users/openclaw/Documents/Playground/openclaw-remote-deploy
python3 -m pip install -r requirements.txt
python3 deploy.py
```

你会被依次询问：

- VPS 地址/IP
- SSH 用户名
- SSH 私钥路径
- Gateway 端口
- Gateway token
- OpenClaw 工作目录
- 是否自动开放防火墙
- 是否启用 linger

## 批量方式

```bash
python3 deploy.py inventory.yaml --report report.json
```

## inventory 关键字段

- `hosts[].address`：VPS 地址
- `hosts[].username`：SSH 用户
- `ssh.key_filename`：控制端私钥路径
- `install.workspace`：OpenClaw 工作目录
- `install.gateway.port`：Gateway 端口
- `install.gateway.token`：Gateway token

## 说明

- 当前版本默认按 Linux VPS 场景设计，不再处理 Windows。
- 远端执行依赖系统 `ssh` 客户端，不走 WinRM。
- 若远端不是 `root`，则需要 `sudo` 可用。
- `openclaw gateway install` 在 Linux 上安装的是 systemd 用户服务；工具会尽量自动启用 linger。
