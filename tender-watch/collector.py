import atexit
import argparse
import csv
import datetime
import hashlib
import json
import random
import re
import shutil
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import cloudscraper
except Exception:
    cloudscraper = None

try:
    from playwright.sync_api import sync_playwright
except Exception:
    sync_playwright = None

BASE = Path(__file__).parent
CFG = BASE / "config"
PROFILE_DIR = CFG / "profiles"
OUT = BASE / "output"
DATA = BASE / "data"
LOGS = BASE / "logs"
OUT.mkdir(exist_ok=True)
DATA.mkdir(exist_ok=True)
LOGS.mkdir(exist_ok=True)
SNAPSHOT_CUTOFF = datetime.datetime(2026, 1, 1, 0, 0, 0)
LEGACY_LAST_INCREMENTAL_RUN_FILE = DATA / "last_incremental_run.txt"
DEFAULT_PROFILE = "non_hunan_expressway_maintenance_design"


def load_profile(profile_name=None):
    name = profile_name or DEFAULT_PROFILE
    candidate = Path(name)
    if candidate.exists():
        path = candidate
    else:
        if not name.endswith(".json"):
            name = f"{name}.json"
        path = PROFILE_DIR / name
    profile = json.loads(path.read_text(encoding="utf-8"))
    profile["_path"] = str(path)
    profile["_name"] = path.stem
    return profile


ACTIVE_PROFILE = load_profile()
rules = ACTIVE_PROFILE["rules"]


def get_last_incremental_run_file(profile=None):
    active = profile or ACTIVE_PROFILE
    return DATA / f"last_incremental_run_{active.get('_name', DEFAULT_PROFILE)}.txt"

UA_LIST = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
]
PLAYWRIGHT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
PLAYWRIGHT_EXECUTABLE = next(
    (
        p
        for p in [
            shutil.which("google-chrome"),
            shutil.which("chromium"),
            shutil.which("chromium-browser"),
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        ]
        if p and Path(p).exists()
    ),
    None,
)

BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

COMMON_CHANNEL_PATHS = [
    "/bmdt/ztbxx/zbgg/",
    "/zwgk/zdlyxxgk_5948535/zbtb_5948548/zbgg_5948549/",
    "/jyxx/001001/001001003/001001003001/",
    "/article/category/zbzqgczb",
    "/ztbxx/zbgg/",
    "/zfcg/",
]

LIST_HINTS = [
    "招标",
    "采购",
    "交易信息",
    "工程建设",
    "公示公告",
    "中标",
    "候选人",
    "通知公告",
    "公共资源",
    "信息公开",
]

FALLBACK_CHANNEL_HINTS = [
    "notice",
    "article",
    "list",
    "tzgg",
    "zb",
    "zfcg",
    "gg",
    "news",
    "xxgk",
]

MAX_CHANNEL_PAGES = 18
MAX_LIST_PAGES_PER_CHANNEL = 6
MAX_LINKS_PER_PAGE = 500
MAX_DETAIL_CHECKS_PER_SOURCE = 50
REQUEST_TIMEOUT = 5
MAX_SOURCE_SECONDS = 35
BLOCKED_CODES = {403, 412, 418, 429}
RUN_ISSUES = []
RUN_ISSUE_KEYS = set()
ISSUE_COUNTS = {}
ISSUE_SAMPLES = {}
MAX_ISSUE_SAMPLES = 3
FETCH_STRATEGY_FILE = DATA / "fetch_strategy.json"

SITE_PROFILES = {
    "ggzy.hunan.gov.cn": {
        "fetch_mode": "requests",
        "source_budget": 8,
        "channel_limit": 2,
        "page_limit": 2,
        "detail_limit": 4,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "https://www.hnsggzy.com/#/jygk",
            "https://www.hnsggzy.com/trade/index.jhtml",
        ],
        "api_search": {
            "url": "https://www.hnsggzy.com/tradeApi/constructionTender/listByFile",
            "root": "https://www.hnsggzy.com/#/jygk",
            "request_mode": "get",
            "response_kind": "page_records",
            "tenderProjectType": "TRANSPORATATION",
            "pageSize": 20,
            "max_pages": 5,
            "link_prefix": "https://www.hnsggzy.com",
            "detail_api_prefix": "https://www.hnsggzy.com/tradeApi/constructionTender/getBySectionId?sectionId=",
            "detail_notice_api_prefix": "https://www.hnsggzy.com/tradeApi/constructionNotice/getBySectionId?sectionId=",
        },
    },
    "jtt.hunan.gov.cn": {
        "fetch_mode": "requests",
        "source_budget": 18,
        "channel_limit": 3,
        "page_limit": 3,
        "detail_limit": 80,
        "seed_only": True,
        "channel_paths": [
            "/jtt/xxgk/zdlyxxgk/zbzb/zbgg/index.html",
            "/jtt/xxgk/zwgg/index.html",
        ],
    },
    "www.hngs.net": {
        "fetch_mode": "requests",
        "source_budget": 10,
        "channel_limit": 3,
        "page_limit": 1,
        "detail_limit": 0,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "/biddingInformation",
            "/bidwinPublicity",
            "/yanghu",
        ],
        "api_search": [
            {
                "url": "https://www.hngs.net/content/page",
                "root": "https://www.hngs.net/biddingInformation.jhtml",
                "request_mode": "get",
                "response_kind": "content_page",
                "channelIds": "2356",
                "pageSize": 20,
                "max_pages": 5,
                "link_prefix": "https://www.hngs.net",
            },
            {
                "url": "https://www.hngs.net/content/page",
                "root": "https://www.hngs.net/bidwinPublicity.jhtml",
                "request_mode": "get",
                "response_kind": "content_page",
                "channelIds": "2611",
                "pageSize": 20,
                "max_pages": 5,
                "link_prefix": "https://www.hngs.net",
            },
            {
                "url": "https://www.hngs.net/content/page",
                "root": "https://www.hngs.net/yanghu.jhtml",
                "request_mode": "get",
                "response_kind": "content_page",
                "channelIds": "2621",
                "pageSize": 20,
                "max_pages": 5,
                "link_prefix": "https://www.hngs.net",
            },
        ],
    },
    "csggzy.changsha.gov.cn": {
        "fetch_mode": "requests",
        "preferred_scheme": "http",
        "source_budget": 8,
        "channel_limit": 2,
        "page_limit": 2,
        "detail_limit": 4,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "/xxgk/xxgkml/qt/tzgg/",
            "https://changsha.hnsggzy.com/#/jygk",
        ],
        "api_search": {
            "url": "https://changsha.hnsggzy.com/tradeApi/constructionTender/listByFile",
            "root": "https://changsha.hnsggzy.com/#/jygk",
            "request_mode": "get",
            "response_kind": "page_records",
            "tenderProjectType": "TRANSPORATATION",
            "regionCode": "430100",
            "pageSize": 20,
            "max_pages": 5,
            "link_prefix": "https://changsha.hnsggzy.com",
            "detail_api_prefix": "https://changsha.hnsggzy.com/tradeApi/constructionTender/getBySectionId?sectionId=",
            "detail_notice_api_prefix": "https://changsha.hnsggzy.com/tradeApi/constructionNotice/getBySectionId?sectionId=",
        },
    },
    "ggzy.xiangtan.gov.cn": {
        "fetch_mode": "requests",
        "preferred_scheme": "http",
        "source_budget": 8,
        "channel_limit": 2,
        "page_limit": 2,
        "detail_limit": 4,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "/2451/2464/2466/index.htm",
            "https://xiangtan.hnsggzy.com/trade/index.jhtml",
        ],
        "api_search": {
            "url": "https://xiangtan.hnsggzy.com/tradeApi/constructionTender/listByFile",
            "root": "https://xiangtan.hnsggzy.com/#/jygk",
            "request_mode": "get",
            "response_kind": "page_records",
            "tenderProjectType": "TRANSPORATATION",
            "regionCode": "430300",
            "pageSize": 20,
            "max_pages": 5,
            "link_prefix": "https://xiangtan.hnsggzy.com",
            "detail_api_prefix": "https://xiangtan.hnsggzy.com/tradeApi/constructionTender/getBySectionId?sectionId=",
            "detail_notice_api_prefix": "https://xiangtan.hnsggzy.com/tradeApi/constructionNotice/getBySectionId?sectionId=",
        },
    },
    "ggzy.yueyang.gov.cn": {
        "fetch_mode": "requests",
        "source_budget": 8,
        "channel_limit": 2,
        "page_limit": 2,
        "detail_limit": 4,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "https://yueyang.hnsggzy.com/#/jygk",
            "https://yueyang.hnsggzy.com/trade/index.jhtml",
        ],
        "api_search": {
            "url": "https://yueyang.hnsggzy.com/tradeApi/constructionTender/listByFile",
            "root": "https://yueyang.hnsggzy.com/#/jygk",
            "request_mode": "get",
            "response_kind": "page_records",
            "tenderProjectType": "TRANSPORATATION",
            "regionCode": "430600",
            "pageSize": 20,
            "max_pages": 5,
            "link_prefix": "https://yueyang.hnsggzy.com",
            "detail_api_prefix": "https://yueyang.hnsggzy.com/tradeApi/constructionTender/getBySectionId?sectionId=",
            "detail_notice_api_prefix": "https://yueyang.hnsggzy.com/tradeApi/constructionNotice/getBySectionId?sectionId=",
        },
    },
    "ggzy.hengyang.gov.cn": {
        "fetch_mode": "requests",
        "source_budget": 8,
        "channel_limit": 3,
        "page_limit": 2,
        "detail_limit": 4,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "/xwzx/tzgg/index.html",
            "https://hengyang.hnsggzy.com/index.jhtml#/jygk?type=ENGINEERING&menu=CONSTRUCTION",
        ],
        "api_search": {
            "url": "https://hengyang.hnsggzy.com/tradeApi/constructionTender/listByFile",
            "root": "https://hengyang.hnsggzy.com/#/jygk",
            "request_mode": "get",
            "response_kind": "page_records",
            "tenderProjectType": "TRANSPORATATION",
            "regionCode": "430400",
            "pageSize": 20,
            "max_pages": 5,
            "link_prefix": "https://hengyang.hnsggzy.com",
            "detail_api_prefix": "https://hengyang.hnsggzy.com/tradeApi/constructionTender/getBySectionId?sectionId=",
            "detail_notice_api_prefix": "https://hengyang.hnsggzy.com/tradeApi/constructionNotice/getBySectionId?sectionId=",
        },
    },
    "zhuzhou.hnsggzy.com": {
        "fetch_mode": "requests",
        "source_budget": 8,
        "channel_limit": 2,
        "page_limit": 2,
        "detail_limit": 4,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "/#/jygk",
        ],
        "api_search": {
            "url": "https://zhuzhou.hnsggzy.com/tradeApi/constructionTender/listByFile",
            "root": "https://zhuzhou.hnsggzy.com/#/jygk",
            "request_mode": "get",
            "response_kind": "page_records",
            "tenderProjectType": "TRANSPORATATION",
            "regionCode": "430200",
            "pageSize": 20,
            "max_pages": 5,
            "link_prefix": "https://zhuzhou.hnsggzy.com",
            "detail_api_prefix": "https://zhuzhou.hnsggzy.com/tradeApi/constructionTender/getBySectionId?sectionId=",
            "detail_notice_api_prefix": "https://zhuzhou.hnsggzy.com/tradeApi/constructionNotice/getBySectionId?sectionId=",
        },
    },
    "shaoyang.hnsggzy.com": {
        "fetch_mode": "requests",
        "source_budget": 8,
        "channel_limit": 2,
        "page_limit": 2,
        "detail_limit": 4,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "/#/jygk",
        ],
        "api_search": {
            "url": "https://shaoyang.hnsggzy.com/tradeApi/constructionTender/listByFile",
            "root": "https://shaoyang.hnsggzy.com/#/jygk",
            "request_mode": "get",
            "response_kind": "page_records",
            "tenderProjectType": "TRANSPORATATION",
            "regionCode": "430500",
            "pageSize": 20,
            "max_pages": 5,
            "link_prefix": "https://shaoyang.hnsggzy.com",
            "detail_api_prefix": "https://shaoyang.hnsggzy.com/tradeApi/constructionTender/getBySectionId?sectionId=",
            "detail_notice_api_prefix": "https://shaoyang.hnsggzy.com/tradeApi/constructionNotice/getBySectionId?sectionId=",
        },
    },
    "changde.hnsggzy.com": {
        "fetch_mode": "requests",
        "source_budget": 8,
        "channel_limit": 2,
        "page_limit": 2,
        "detail_limit": 4,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "/#/jygk",
        ],
        "api_search": {
            "url": "https://changde.hnsggzy.com/tradeApi/constructionTender/listByFile",
            "root": "https://changde.hnsggzy.com/#/jygk",
            "request_mode": "get",
            "response_kind": "page_records",
            "tenderProjectType": "TRANSPORATATION",
            "regionCode": "430700",
            "pageSize": 20,
            "max_pages": 5,
            "link_prefix": "https://changde.hnsggzy.com",
            "detail_api_prefix": "https://changde.hnsggzy.com/tradeApi/constructionTender/getBySectionId?sectionId=",
            "detail_notice_api_prefix": "https://changde.hnsggzy.com/tradeApi/constructionNotice/getBySectionId?sectionId=",
        },
    },
    "zhangjiajie.hnsggzy.com": {
        "fetch_mode": "requests",
        "source_budget": 8,
        "channel_limit": 2,
        "page_limit": 2,
        "detail_limit": 4,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "/#/jygk",
        ],
        "api_search": {
            "url": "https://zhangjiajie.hnsggzy.com/tradeApi/constructionTender/listByFile",
            "root": "https://zhangjiajie.hnsggzy.com/#/jygk",
            "request_mode": "get",
            "response_kind": "page_records",
            "tenderProjectType": "TRANSPORATATION",
            "regionCode": "430800",
            "pageSize": 20,
            "max_pages": 5,
            "link_prefix": "https://zhangjiajie.hnsggzy.com",
            "detail_api_prefix": "https://zhangjiajie.hnsggzy.com/tradeApi/constructionTender/getBySectionId?sectionId=",
            "detail_notice_api_prefix": "https://zhangjiajie.hnsggzy.com/tradeApi/constructionNotice/getBySectionId?sectionId=",
        },
    },
    "yiyang.hnsggzy.com": {
        "fetch_mode": "requests",
        "source_budget": 8,
        "channel_limit": 2,
        "page_limit": 2,
        "detail_limit": 4,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "/#/jygk",
        ],
        "api_search": {
            "url": "https://yiyang.hnsggzy.com/tradeApi/constructionTender/listByFile",
            "root": "https://yiyang.hnsggzy.com/#/jygk",
            "request_mode": "get",
            "response_kind": "page_records",
            "tenderProjectType": "TRANSPORATATION",
            "regionCode": "430900",
            "pageSize": 20,
            "max_pages": 5,
            "link_prefix": "https://yiyang.hnsggzy.com",
            "detail_api_prefix": "https://yiyang.hnsggzy.com/tradeApi/constructionTender/getBySectionId?sectionId=",
            "detail_notice_api_prefix": "https://yiyang.hnsggzy.com/tradeApi/constructionNotice/getBySectionId?sectionId=",
        },
    },
    "chenzhou.hnsggzy.com": {
        "fetch_mode": "requests",
        "source_budget": 8,
        "channel_limit": 2,
        "page_limit": 2,
        "detail_limit": 4,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "/#/jygk",
        ],
        "api_search": {
            "url": "https://chenzhou.hnsggzy.com/tradeApi/constructionTender/listByFile",
            "root": "https://chenzhou.hnsggzy.com/#/jygk",
            "request_mode": "get",
            "response_kind": "page_records",
            "tenderProjectType": "TRANSPORATATION",
            "regionCode": "431000",
            "pageSize": 20,
            "max_pages": 5,
            "link_prefix": "https://chenzhou.hnsggzy.com",
            "detail_api_prefix": "https://chenzhou.hnsggzy.com/tradeApi/constructionTender/getBySectionId?sectionId=",
            "detail_notice_api_prefix": "https://chenzhou.hnsggzy.com/tradeApi/constructionNotice/getBySectionId?sectionId=",
        },
    },
    "yongzhou.hnsggzy.com": {
        "fetch_mode": "requests",
        "source_budget": 8,
        "channel_limit": 2,
        "page_limit": 2,
        "detail_limit": 4,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "/#/jygk",
        ],
        "api_search": {
            "url": "https://yongzhou.hnsggzy.com/tradeApi/constructionTender/listByFile",
            "root": "https://yongzhou.hnsggzy.com/#/jygk",
            "request_mode": "get",
            "response_kind": "page_records",
            "tenderProjectType": "TRANSPORATATION",
            "regionCode": "431100",
            "pageSize": 20,
            "max_pages": 5,
            "link_prefix": "https://yongzhou.hnsggzy.com",
            "detail_api_prefix": "https://yongzhou.hnsggzy.com/tradeApi/constructionTender/getBySectionId?sectionId=",
            "detail_notice_api_prefix": "https://yongzhou.hnsggzy.com/tradeApi/constructionNotice/getBySectionId?sectionId=",
        },
    },
    "huaihua.hnsggzy.com": {
        "fetch_mode": "requests",
        "source_budget": 8,
        "channel_limit": 2,
        "page_limit": 2,
        "detail_limit": 4,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "/#/jygk",
        ],
        "api_search": {
            "url": "https://huaihua.hnsggzy.com/tradeApi/constructionTender/listByFile",
            "root": "https://huaihua.hnsggzy.com/#/jygk",
            "request_mode": "get",
            "response_kind": "page_records",
            "tenderProjectType": "TRANSPORATATION",
            "regionCode": "431200",
            "pageSize": 20,
            "max_pages": 5,
            "link_prefix": "https://huaihua.hnsggzy.com",
            "detail_api_prefix": "https://huaihua.hnsggzy.com/tradeApi/constructionTender/getBySectionId?sectionId=",
            "detail_notice_api_prefix": "https://huaihua.hnsggzy.com/tradeApi/constructionNotice/getBySectionId?sectionId=",
        },
    },
    "loudi.hnsggzy.com": {
        "fetch_mode": "requests",
        "source_budget": 8,
        "channel_limit": 2,
        "page_limit": 2,
        "detail_limit": 4,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "/#/jygk",
        ],
        "api_search": {
            "url": "https://loudi.hnsggzy.com/tradeApi/constructionTender/listByFile",
            "root": "https://loudi.hnsggzy.com/#/jygk",
            "request_mode": "get",
            "response_kind": "page_records",
            "tenderProjectType": "TRANSPORATATION",
            "regionCode": "431300",
            "pageSize": 20,
            "max_pages": 5,
            "link_prefix": "https://loudi.hnsggzy.com",
            "detail_api_prefix": "https://loudi.hnsggzy.com/tradeApi/constructionTender/getBySectionId?sectionId=",
            "detail_notice_api_prefix": "https://loudi.hnsggzy.com/tradeApi/constructionNotice/getBySectionId?sectionId=",
        },
    },
    "xiangxi.hnsggzy.com": {
        "fetch_mode": "requests",
        "source_budget": 8,
        "channel_limit": 2,
        "page_limit": 2,
        "detail_limit": 4,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "/#/jygk",
        ],
        "api_search": {
            "url": "https://xiangxi.hnsggzy.com/tradeApi/constructionTender/listByFile",
            "root": "https://xiangxi.hnsggzy.com/#/jygk",
            "request_mode": "get",
            "response_kind": "page_records",
            "tenderProjectType": "TRANSPORATATION",
            "regionCode": "433100",
            "pageSize": 20,
            "max_pages": 5,
            "link_prefix": "https://xiangxi.hnsggzy.com",
            "detail_api_prefix": "https://xiangxi.hnsggzy.com/tradeApi/constructionTender/getBySectionId?sectionId=",
            "detail_notice_api_prefix": "https://xiangxi.hnsggzy.com/tradeApi/constructionNotice/getBySectionId?sectionId=",
        },
    },
    "bulletin.cebpubservice.com": {
        "fetch_mode": "requests",
        "source_budget": 6,
        "channel_limit": 1,
        "page_limit": 1,
        "detail_limit": 0,
        "seed_only": True,
    },
    "ggzy.yn.gov.cn": {
        "fetch_mode": "plain_requests",
        "source_budget": 8,
        "channel_limit": 1,
        "page_limit": 1,
        "detail_limit": 0,
        "extract_attempts": 1,
        "seed_only": True,
    },
    "jtt.hubei.gov.cn": {
        "fetch_mode": "playwright",
        "source_budget": 150,
        "incremental_source_budget": 12,
        "channel_limit": 1,
        "page_limit": 5,
        "detail_limit": 16,
        "incremental_detail_limit": 4,
        "candidate_limit": 6,
        "seed_only": True,
        "include_source_url": False,
        "channel_paths": [
            "/bmdt/ztbxx/zbgg/",
        ],
    },
    "ygp.gdzwfw.gov.cn": {
        "fetch_mode": "requests",
        "source_budget": 2,
        "channel_limit": 1,
        "page_limit": 1,
        "detail_limit": 0,
        "seed_only": True,
        "incremental_source_budget": 2,
        "incremental_channel_limit": 1,
        "incremental_page_limit": 1,
        "incremental_detail_limit": 0,
    },
    "jtt.gxzf.gov.cn": {
        "fetch_mode": "requests",
        "source_budget": 6,
        "channel_limit": 2,
        "page_limit": 1,
        "detail_limit": 0,
        "seed_only": True,
        "include_source_url": False,
        "channel_paths": [
            "/zfxxgk/fdzdgk/ggzyjy/zbxx_1/",
            "/zfxxgk/fdzdgk/ggzyjy/zbxx_217/",
        ],
    },
    "ggzyfw.beijing.gov.cn": {
        "fetch_mode": "requests",
        "source_budget": 5,
        "channel_limit": 3,
        "page_limit": 1,
        "detail_limit": 0,
        "seed_only": True,
        "include_source_url": False,
        "channel_paths": [
            "/jyxxgcjsgzgg/index.html",
            "/jyxxzbhxrgs/index.html",
            "/jyxxgcjszsgs/index.html",
        ],
    },
    "ebidding.hebtig.com": {
        "fetch_mode": "requests",
        "source_budget": 18,
        "channel_limit": 3,
        "incremental_channel_limit": 1,
        "page_limit": 3,
        "detail_limit": 12,
        "seed_only": True,
        "include_source_url": False,
        "exclude_channel_patterns": [
            "/TPFrame",
            "/TPBidder",
            "/001001003/001001003001/",
        ],
        "channel_paths": [
            "/jyxx/001001/001001003/trade.html",
            "/jyxx/001001/trade_jyxx.html",
            "/jyxx/001001/001001003/001001003001/",
        ],
        "page_templates": [
            {"suffix": "/trade.html", "template": "{base}{n}.html", "start": 2},
            {"suffix": "/trade_jyxx.html", "template": "{base}{n}.html", "start": 2},
        ],
    },
    "www.sdhsg.com": {
        "fetch_mode": "requests",
        "source_budget": 15,
        "channel_limit": 3,
        "page_limit": 3,
        "detail_limit": 10,
        "seed_only": True,
        "channel_paths": [
            "/article/category/zbzqgczb",
            "/article/category/zbgs",
        ],
    },
    "jsggzy.jszwfw.gov.cn": {
        "fetch_mode": "requests",
        "preferred_scheme": "http",
        "source_budget": 20,
        "channel_limit": 1,
        "page_limit": 3,
        "detail_limit": 10,
        "api_only": True,
        "seed_only": True,
        "exclude_channel_patterns": [
            "/search/",
            "/systemIndex.html",
            "/webportal/",
            "/xwdt/",
        ],
        "channel_paths": [
            "/jyxx/tradeInfonew.html?type=jtgc",
        ],
        "api_search": {
            "url": "http://jsggzy.jszwfw.gov.cn/inteligentsearch/rest/esinteligentsearch/getFullTextDataNew",
            "root": "http://jsggzy.jszwfw.gov.cn/jyxx/tradeInfonew.html?type=jtgc",
            "wd": "养护 设计",
            "rn": 100,
            "max_pages": 5,
            "cnum": "001",
            "sort": '{"infodatepx":"0"}',
            "condition": [
                {"fieldName": "categorynum", "isLike": True, "likeType": 2, "equal": "003002"},
            ],
            "time": [
                {
                    "fieldName": "infodatepx",
                    "startTime": "2026-01-01 00:00:00",
                    "endTime": "2026-12-31 23:59:59",
                }
            ],
            "content_type": "application/json;charset=utf-8",
        },
    },
    "ggzy.zwfwb.tj.gov.cn": {
        "fetch_mode": "requests",
        "preferred_scheme": "http",
        "source_budget": 18,
        "channel_limit": 1,
        "page_limit": 3,
        "detail_limit": 10,
        "seed_only": True,
        "exclude_channel_patterns": [
            "/xwzx/",
            "/zcfg/",
            "/fwzn/",
            "/topic.jspx",
            "/jyxxtj.jspx",
        ],
        "channel_paths": [
            "/jyxxzbgg/index.jhtml",
            "/jyxxhxgs/index.jhtml",
            "/jyxxzbjb/index.jhtml",
        ],
    },
    "ggzy.ln.gov.cn": {
        "fetch_mode": "plain_requests",
        "preferred_scheme": "http",
        "source_budget": 6,
        "channel_limit": 1,
        "page_limit": 1,
        "detail_limit": 0,
        "extract_attempts": 1,
        "seed_only": True,
        "channel_paths": [
            "/jyxx/",
        ],
    },
    "www.hebpr.cn": {
        "fetch_mode": "requests",
        "source_budget": 18,
        "channel_limit": 1,
        "page_limit": 3,
        "detail_limit": 10,
        "api_only": True,
        "validate_api_links": True,
        "seed_only": True,
        "channel_paths": [
            "https://szj.hebei.gov.cn/hbggfwpt/",
        ],
        "api_search": {
            "url": "https://szj.hebei.gov.cn/inteligentsearchnew/rest/esinteligentsearch/getFullTextDataNew",
            "root": "https://szj.hebei.gov.cn/hbggfwpt/",
            "link_prefix": "https://szj.hebei.gov.cn/hbggfwpt",
            "wd": "养护 设计",
            "rn": 100,
            "max_pages": 5,
            "condition": '[{\"fieldName\":\"categorynum\",\"equal\":\"003\",\"notEqual\":null,\"equalList\":null,\"notEqualList\":null,\"isLike\":true,\"likeType\":2}]',
        },
    },
    "hnsggzyjy.henan.gov.cn": {
        "fetch_mode": "requests",
        "preferred_scheme": "http",
        "source_budget": 20,
        "channel_limit": 2,
        "page_limit": 3,
        "detail_limit": 10,
        "api_only": True,
        "seed_only": True,
        "channel_paths": [
            "/jyxx/002001/transaction_notice.html",
            "/jyxx/002001/002001001/transaction_notice.html",
            "/jyxx/002001/002001003/transaction_notice.html",
            "/jyxx/002001/002001006/transaction_notice.html",
        ],
        "api_search": {
            "url": "http://hnsggzyjy.henan.gov.cn/EpointWebBuilder/rest/frontAppCustomAction/getPageInfoListNewYzm",
            "root": "http://hnsggzyjy.henan.gov.cn/jyxx/002001/transaction_notice.html",
            "request_mode": "form",
            "response_kind": "custom_infodata",
            "siteGuid": "7eb5f7f1-9041-43ad-8e13-8fcb82ea831a",
            "categoryNum": "002001",
            "wd": "高速公路",
            "startDate": "2026-01-01",
            "endDate": "2026-12-31",
            "pageSize": 20,
            "max_pages": 5,
            "xiaqucode": "4100",
        },
    },
    "jtyst.fujian.gov.cn": {
        "fetch_mode": "requests",
        "source_budget": 8,
        "channel_limit": 2,
        "page_limit": 1,
        "detail_limit": 2,
        "seed_only": True,
        "channel_paths": [
            "/zwgk/tzgg/",
            "/zwgk/zfxxgkzl/zfxxgkml/?ztfl=1",
        ],
    },
    "jtt.jiangsu.gov.cn": {
        "fetch_mode": "plain_requests",
        "preferred_scheme": "http",
        "source_budget": 4,
        "channel_limit": 1,
        "page_limit": 1,
        "detail_limit": 0,
        "extract_attempts": 1,
        "seed_only": True,
    },
    "jtt.ln.gov.cn": {
        "fetch_mode": "plain_requests",
        "source_budget": 6,
        "channel_limit": 2,
        "page_limit": 1,
        "detail_limit": 0,
        "extract_attempts": 1,
        "seed_only": True,
        "channel_paths": [
            "/jtt/cx/sbjztbxx/index.shtml",
            "/jtt/cx/gdsztbxx/index.shtml",
        ],
    },
    "www.jchc.cn": {
        "fetch_mode": "requests",
        "source_budget": 4,
        "channel_limit": 1,
        "page_limit": 1,
        "detail_limit": 0,
        "seed_only": True,
    },
    "www.cncico.com": {
        "fetch_mode": "plain_requests",
        "source_budget": 6,
        "channel_limit": 2,
        "page_limit": 1,
        "detail_limit": 2,
        "extract_attempts": 1,
        "seed_only": True,
        "channel_paths": [
            "/tender.html",
            "/information.html?nid=1&category=7214",
            "https://cncico.zjgzzc.com/cms/default/webfile/index.html",
        ],
    },
}

TYPE_PROFILES = {
    "portal": {
        "fetch_mode": "requests",
        "source_budget": 10,
        "channel_limit": 3,
        "page_limit": 2,
        "detail_limit": 8,
        "incremental_source_budget": 6,
        "incremental_channel_limit": 1,
        "incremental_page_limit": 1,
        "incremental_detail_limit": 4,
    },
    "provincial": {
        "fetch_mode": "requests",
        "source_budget": 12,
        "channel_limit": 3,
        "page_limit": 3,
        "detail_limit": 8,
        "incremental_source_budget": 8,
        "incremental_channel_limit": 2,
        "incremental_page_limit": 2,
        "incremental_detail_limit": 4,
    },
    "transport_dept": {
        "fetch_mode": "requests",
        "source_budget": 12,
        "channel_limit": 3,
        "page_limit": 3,
        "detail_limit": 10,
        "incremental_source_budget": 10,
        "incremental_channel_limit": 2,
        "incremental_page_limit": 2,
        "incremental_detail_limit": 6,
    },
    "operator": {
        "fetch_mode": "requests",
        "source_budget": 10,
        "channel_limit": 3,
        "page_limit": 2,
        "detail_limit": 10,
        "incremental_source_budget": 8,
        "incremental_channel_limit": 1,
        "incremental_page_limit": 1,
        "incremental_detail_limit": 4,
    },
}


def record_issue(source, stage, problem, url="", detail="", action="skip"):
    key = (source.get("name", ""), stage, problem, action)
    ISSUE_COUNTS[key] = ISSUE_COUNTS.get(key, 0) + 1
    samples = ISSUE_SAMPLES.setdefault(key, [])
    sample = {
        "url": url,
        "detail": detail[:180],
    }
    if len(samples) < MAX_ISSUE_SAMPLES and sample not in samples:
        samples.append(sample)
    if key in RUN_ISSUE_KEYS:
        return
    RUN_ISSUE_KEYS.add(key)
    RUN_ISSUES.append(
        {
            "source": source.get("name", ""),
            "province": source.get("province", ""),
            "type": source.get("type", ""),
            "stage": stage,
            "problem": problem,
            "count": ISSUE_COUNTS[key],
            "sample_urls": [x["url"] for x in samples if x["url"]],
            "sample_details": [x["detail"] for x in samples if x["detail"]],
            "action": action,
            "logged_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )


def source_allowed(source):
    scope = ACTIVE_PROFILE.get("source_scope", {})
    province = source.get("province", "")
    source_type = source.get("type", "")
    name = source.get("name", "")

    include_provinces = set(scope.get("include_provinces", []))
    exclude_provinces = set(scope.get("exclude_provinces", []))
    include_types = set(scope.get("include_types", []))
    exclude_types = set(scope.get("exclude_types", []))
    include_names = set(scope.get("include_names", []))
    exclude_names = set(scope.get("exclude_names", []))
    include_national = scope.get("include_national", True)

    if include_names and name not in include_names:
        return False
    if exclude_names and name in exclude_names:
        return False
    if include_types and source_type not in include_types:
        return False
    if exclude_types and source_type in exclude_types:
        return False
    if province in exclude_provinces:
        return False
    if province == "全国":
        return include_national
    if include_provinces and province not in include_provinces:
        return False
    return True


def get_source_profile(source):
    domain = urlparse(source["url"]).netloc.lower()
    profile = {
        "fetch_mode": "requests",
        "source_budget": MAX_SOURCE_SECONDS,
        "channel_limit": 5,
        "page_limit": MAX_LIST_PAGES_PER_CHANNEL,
        "detail_limit": MAX_DETAIL_CHECKS_PER_SOURCE,
    }
    profile.update(TYPE_PROFILES.get(source.get("type", ""), {}))
    profile.update(SITE_PROFILES.get(domain, {}))
    return profile


def apply_mode_profile(profile, snapshot):
    if snapshot:
        return profile
    profile = dict(profile)
    inc_budget = profile.get("incremental_source_budget")
    if inc_budget:
        profile["source_budget"] = min(profile["source_budget"], inc_budget)
    inc_detail = profile.get("incremental_detail_limit")
    if inc_detail is not None:
        profile["detail_limit"] = min(profile["detail_limit"], inc_detail)
    inc_channel = profile.get("incremental_channel_limit")
    if inc_channel:
        profile["channel_limit"] = min(profile["channel_limit"], inc_channel)
    inc_page = profile.get("incremental_page_limit")
    if inc_page:
        profile["page_limit"] = min(profile["page_limit"], inc_page)
    return profile


def apply_retry_profile(profile, source, retry_issue_map):
    problems = retry_issue_map.get(source.get("name", ""), set())
    if not problems:
        return profile
    profile = dict(profile)
    source_type = source.get("type", "")
    if "source_timeout" in problems:
        if source_type in {"transport_dept", "operator"}:
            profile["source_budget"] = min(profile["source_budget"] + 20, 90)
        else:
            profile["source_budget"] = min(profile["source_budget"] + 8, 45)
            profile["channel_limit"] = min(profile["channel_limit"], 2)
            profile["page_limit"] = min(profile["page_limit"], 3)
    if "detail_limit_exceeded" in problems:
        if source_type in {"transport_dept", "operator"}:
            profile["detail_limit"] = min(profile["detail_limit"] + 20, 80)
        else:
            profile["detail_limit"] = min(profile["detail_limit"] + 8, 24)
    if {"empty_candidate_links", "channel_root_unavailable", "channel_page_unavailable"} & problems:
        profile["fetch_mode"] = "playwright"
        profile["source_budget"] = min(profile["source_budget"] + 12, 90)
    return profile


def load_fetch_strategy():
    if not FETCH_STRATEGY_FILE.exists():
        return {}
    try:
        return json.loads(FETCH_STRATEGY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_fetch_strategy(strategy):
    FETCH_STRATEGY_FILE.write_text(json.dumps(strategy, ensure_ascii=False, indent=2), encoding="utf-8")


class AntiBotFetcher:
    def __init__(self):
        self.session = requests.Session()
        retry = Retry(
            total=2,
            connect=2,
            read=2,
            backoff_factor=0.4,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET"]),
        )
        self.session.mount("http://", HTTPAdapter(max_retries=retry))
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

        self.scraper = None
        if cloudscraper:
            try:
                self.scraper = cloudscraper.create_scraper()
            except Exception:
                self.scraper = None

        self.pw = None
        self.browser = None
        self.context = None

        self.last_hit = {}
        self.domain_mode = load_fetch_strategy()
        self.cache = {}
        atexit.register(self.close)

    def close(self):
        try:
            if self.context:
                self.context.close()
        except Exception:
            pass
        try:
            if self.browser:
                self.browser.close()
        except Exception:
            pass
        try:
            if self.pw:
                self.pw.stop()
        except Exception:
            pass
        self.context = None
        self.browser = None
        self.pw = None

    def _throttle(self, url):
        domain = urlparse(url).netloc
        now = time.perf_counter()
        last = self.last_hit.get(domain, 0)
        gap = 0.35
        if now - last < gap:
            time.sleep(gap - (now - last))
        self.last_hit[domain] = time.perf_counter()

    def _headers(self, referer=None):
        headers = dict(BASE_HEADERS)
        headers["User-Agent"] = random.choice(UA_LIST)
        headers["Upgrade-Insecure-Requests"] = "1"
        if referer:
            headers["Referer"] = referer
        return headers

    def _is_html_response(self, resp):
        ctype = (resp.headers.get("Content-Type") or "").lower()
        if "text/html" in ctype or "application/xhtml+xml" in ctype:
            return True
        return "<html" in resp.text[:500].lower()

    def _is_block_shell(self, text):
        t = (text or "").strip().lower()
        if not t:
            return True
        if "403 forbidden" in t or "access denied" in t:
            return True
        if t in ("<html><head></head><body></body></html>", "<html><head></head><body> </body></html>"):
            return True
        if len(t) < 120 and "<html" in t and "</html>" in t and "<a " not in t and "<script" not in t:
            return True
        return False

    def _has_domain_cookie(self, domain):
        for cookie in self.session.cookies:
            if domain == cookie.domain or domain.endswith(cookie.domain.lstrip(".")) or cookie.domain.lstrip(".").endswith(domain):
                return True
        return False

    def _sync_context_cookies(self):
        if not self.context:
            return
        try:
            for cookie in self.context.cookies():
                name = cookie.get("name")
                if not name:
                    continue
                self.session.cookies.set(
                    name,
                    cookie.get("value", ""),
                    domain=cookie.get("domain"),
                    path=cookie.get("path", "/"),
                )
        except Exception:
            pass

    def _try_requests(self, url, referer=None):
        self._throttle(url)
        try:
            r = self.session.get(url, timeout=REQUEST_TIMEOUT, headers=self._headers(referer))
            if r.status_code in BLOCKED_CODES:
                return None
            r.raise_for_status()
            r.encoding = r.apparent_encoding or r.encoding
            if not self._is_html_response(r):
                return None
            if self._is_block_shell(r.text):
                return None
            return r.text
        except Exception:
            return None

    def _try_cloudscraper(self, url, referer=None):
        if not self.scraper:
            return None
        self._throttle(url)
        try:
            r = self.scraper.get(url, timeout=REQUEST_TIMEOUT, headers=self._headers(referer))
            if r.status_code in BLOCKED_CODES:
                return None
            r.raise_for_status()
            r.encoding = r.apparent_encoding or r.encoding
            if not self._is_html_response(r):
                return None
            if self._is_block_shell(r.text):
                return None
            return r.text
        except Exception:
            return None

    def _ensure_playwright(self):
        if self.context:
            return True
        if not sync_playwright:
            return False
        try:
            self.pw = sync_playwright().start()
            launch_kwargs = {
                "headless": True,
                "args": [
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
            }
            if PLAYWRIGHT_EXECUTABLE:
                launch_kwargs["executable_path"] = PLAYWRIGHT_EXECUTABLE
            self.browser = self.pw.chromium.launch(**launch_kwargs)
            self.context = self.browser.new_context(
                user_agent=PLAYWRIGHT_UA,
                locale="zh-CN",
                viewport={"width": 1440, "height": 920},
                java_script_enabled=True,
                extra_http_headers={
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Cache-Control": "no-cache",
                },
            )
            return True
        except Exception:
            self.close()
            return False

    def _try_playwright(self, url, referer=None):
        if not self._ensure_playwright():
            return None
        self._throttle(url)
        page = None
        try:
            page = self.context.new_page()
            warmup = referer
            if not warmup or warmup == url:
                warmup = root_url(url)
            if warmup and warmup != url:
                try:
                    page.goto(warmup, wait_until="domcontentloaded", timeout=20000)
                    page.wait_for_timeout(3000)
                except Exception:
                    pass
            resp = page.goto(url, wait_until="domcontentloaded", timeout=20000)
            page.wait_for_timeout(3000)
            html = page.content()
            blocked = (resp and resp.status in BLOCKED_CODES) or len(html or "") < 120
            # Some anti-bot pages (e.g., 412 challenge) set cookies first, then pass on reload.
            if blocked:
                for _ in range(3):
                    try:
                        page.reload(wait_until="domcontentloaded", timeout=20000)
                        page.wait_for_timeout(3000)
                        html = page.content()
                        if len(html or "") >= 120 and not self._is_block_shell(html):
                            break
                    except Exception:
                        break
            if resp and resp.status in BLOCKED_CODES:
                return None
            if not html or "<html" not in html[:500].lower() or self._is_block_shell(html):
                return None
            self._sync_context_cookies()
            return html
        except Exception:
            return None
        finally:
            if page:
                try:
                    page.close()
                except Exception:
                    pass

    def fetch_html(self, url, referer=None, mode="auto"):
        cache_key = url
        if cache_key in self.cache:
            return self.cache[cache_key]

        domain = urlparse(url).netloc
        learned_mode = self.domain_mode.get(domain, "auto")
        mode = learned_mode if mode == "auto" else mode
        html = None

        if mode == "playwright" and self._has_domain_cookie(domain):
            html = self._try_requests(url, referer) or self._try_cloudscraper(url, referer)
            if html:
                self.cache[cache_key] = html
                return html

        if mode in ("auto", "requests", "plain_requests"):
            html = self._try_requests(url, referer)
            if not html and mode != "plain_requests":
                html = self._try_cloudscraper(url, referer)
            if html:
                self.domain_mode.setdefault(domain, "requests")

        if not html and mode in ("auto", "playwright"):
            html = self._try_playwright(url, referer)
            if html:
                self.domain_mode[domain] = "playwright"

        if not html and mode == "playwright":
            # Fallback once: some domains become passable after browser-warmed cookies.
            html = self._try_requests(url, referer) or self._try_cloudscraper(url, referer)

        if html:
            self.cache[cache_key] = html
        return html


FETCHER = AntiBotFetcher()


def hit_include(text):
    return any(k in text for k in rules["include_keywords"])


def hit_exclude(text):
    return any(k in text for k in rules["exclude_keywords"])


def hit_road(text):
    return any(k in text for k in rules.get("must_road_keywords", []))


def hit_non_road(text):
    return any(k in text for k in rules.get("exclude_project_keywords", []))


def hit_notice(text):
    return any(k in text for k in rules.get("must_notice_keywords", []))


def hit_non_notice(text):
    exclude_hit = any(k in text for k in rules.get("exclude_notice_keywords", []))
    if not exclude_hit:
        return False
    if hit_notice(text):
        return False
    return True


def hit_highway_signal(text):
    if any(k in text for k in rules.get("non_highway_keywords", [])):
        return False
    groups = rules.get("target_keyword_groups", {})
    highway_words = groups.get("highway", [])
    if any(k in text for k in highway_words):
        return True
    if any(k in text for k in rules.get("highway_infer_keywords", [])):
        return True
    local_profile = any(
        k in highway_words for k in ["国道", "省道", "国省道", "县道", "乡道", "农村公路", "村道"]
    )
    if local_profile and re.search(r"(?:^|[^A-Za-z0-9])[GSXYC]\d{1,4}(?:[^A-Za-z0-9]|$)", text, flags=re.IGNORECASE):
        return True
    route_hit = re.search(r"(?:^|[^A-Za-z0-9])[GS]\d{2,4}(?:[^A-Za-z0-9]|$)", text, flags=re.IGNORECASE)
    facility_hit = re.search(r"高速|互通|枢纽|收费站|服务区", text)
    if route_hit and facility_hit:
        return True
    return False


def hit_strict_target(text):
    groups = rules.get("target_keyword_groups", {})
    branches = rules.get("strict_rule_branches", [])
    if branches:
        for branch in branches:
            ok = True
            for group_name in branch:
                if group_name == "highway":
                    if not hit_highway_signal(text):
                        ok = False
                        break
                    continue
                words = groups.get(group_name, [])
                if not any(k in text for k in words):
                    ok = False
                    break
            if ok:
                return True
        return False
    maint_words = groups.get("maintenance", [])
    design_words = groups.get("design", [])
    require_design = rules.get("strict_requires_design", True)
    has_highway = hit_highway_signal(text)
    has_maint = any(k in text for k in maint_words)
    has_design = True if not require_design else any(k in text for k in design_words)
    return has_highway and has_maint and has_design


def hit_maint_design(text):
    groups = rules.get("target_keyword_groups", {})
    branches = rules.get("strict_rule_branches", [])
    if branches:
        return hit_strict_target(text)
    maint_words = groups.get("maintenance", [])
    design_words = groups.get("design", [])
    require_design = rules.get("strict_requires_design", True)
    has_maint = any(k in text for k in maint_words)
    has_design = True if not require_design else any(k in text for k in design_words)
    return has_maint and has_design


def classify(text):
    for cat, kws in rules["category_rules"].items():
        if any(k in text for k in kws):
            return cat
    return "其他"


def normalize(s):
    return re.sub(r"\s+", " ", s or "").strip()


def strip_tags(s):
    return re.sub(r"<[^>]+>", "", s or "")


def is_http_link(href):
    return href.startswith("http://") or href.startswith("https://")


def prefer_https(url):
    p = urlparse(url)
    if p.scheme == "http":
        return p._replace(scheme="https").geturl()
    return url


def normalize_link(url, profile=None):
    p = urlparse(url)
    preferred = (profile or {}).get("preferred_scheme")
    if preferred in {"http", "https"} and p.scheme in {"http", "https"} and p.netloc:
        return p._replace(scheme=preferred).geturl()
    return prefer_https(url)


def same_domain(u1, u2):
    n1 = urlparse(u1).netloc
    n2 = urlparse(u2).netloc
    return n1 == n2 or n1.endswith("." + n2) or n2.endswith("." + n1)


def root_url(url):
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}/"


URL_STATUS_CACHE = {}


def is_detail_page(url):
    u = url.lower()
    if not (u.endswith(".html") or u.endswith(".shtml")):
        return False
    if re.search(r"/20\d{2}(?:0[1-9]|1[0-2])(?:[0-3]\d)?/", u):
        return True
    if re.search(r"/t20\d{6}_\d+\.(?:shtml|html)$", u):
        return True
    if re.search(r"/[0-9a-f]{8}-[0-9a-f-]{27,}\.(?:shtml|html)$", u):
        return True
    return False


def request_html(url, referer=None, mode="auto"):
    return FETCHER.fetch_html(url, referer=referer, mode=mode)


def is_live_url(url, referer=None):
    cache_key = (url, referer or "")
    if cache_key in URL_STATUS_CACHE:
        return URL_STATUS_CACHE[cache_key]
    try:
        resp = FETCHER.session.get(
            url,
            timeout=REQUEST_TIMEOUT,
            headers={
                "User-Agent": random.choice(UA_LIST),
                "Referer": referer or root_url(url),
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            },
        )
        ok = resp.status_code < 400
    except Exception:
        ok = False
    URL_STATUS_CACHE[cache_key] = ok
    return ok


def discover_channel_pages(source, profile):
    source_url = source["url"]
    root = f"{urlparse(source_url).scheme}://{urlparse(source_url).netloc}"
    seeded_pages = [source_url] if profile.get("include_source_url", True) else []
    seen = set(seeded_pages)
    for path in profile.get("channel_paths", []):
        guess = normalize_link(urljoin(root, path), profile)
        if guess in seen:
            continue
        seeded_pages.append(guess)
        seen.add(guess)
    excludes = profile.get("exclude_channel_patterns", [])

    def blocked(url):
        return any(pat in url for pat in excludes)

    seeded_pages = [u for u in seeded_pages if not blocked(u)]
    seen = set(seeded_pages)

    if profile.get("seed_only"):
        prioritized = [u for u in seeded_pages if u != source_url]
        if source_url in seeded_pages:
            prioritized.append(source_url)
        return prioritized or [source_url]

    html = request_html(source_url, referer=root_url(source_url), mode=profile["fetch_mode"])
    if not html:
        valid_seeded = []
        for guess in seeded_pages:
            if request_html(guess, referer=root_url(source_url), mode=profile["fetch_mode"]):
                valid_seeded.append(guess)
        if valid_seeded:
            record_issue(source, "channel_discovery", "channel_root_unavailable", source_url, action="use_seed_channels")
            return valid_seeded
        record_issue(source, "channel_discovery", "channel_root_unavailable", source_url, action="fallback_root")
        return [source_url]

    soup = BeautifulSoup(html, "html.parser")
    pages = list(seeded_pages)
    fallback = []

    for a in soup.select("a[href]"):
        title = normalize(a.get_text(" ", strip=True))
        href = normalize(a.get("href", ""))
        if not href or href.lower().startswith("javascript:"):
            continue
        full = normalize_link(urljoin(source_url, href), profile)
        if not is_http_link(full) or not same_domain(full, source_url):
            continue
        if blocked(full):
            continue
        if is_detail_page(full):
            continue

        text = f"{title} {full}"
        if full in seen:
            continue
        if any(k in text for k in LIST_HINTS):
            seen.add(full)
            pages.append(full)
            if len(pages) >= MAX_CHANNEL_PAGES:
                break
            continue
        if any(k in full.lower() for k in FALLBACK_CHANNEL_HINTS):
            fallback.append(full)

    for full in fallback:
        if full in seen:
            continue
        seen.add(full)
        pages.append(full)
        if len(pages) >= MAX_CHANNEL_PAGES:
            break

    for path in COMMON_CHANNEL_PATHS:
        guess = normalize_link(urljoin(root, path), profile)
        if guess in seen:
            continue
        if blocked(guess):
            continue
        if request_html(guess, referer=root_url(source_url), mode=profile["fetch_mode"]):
            seen.add(guess)
            pages.append(guess)
            if len(pages) >= MAX_CHANNEL_PAGES:
                break

    return pages


def extract_embedded_links(html, page_url, profile):
    out = []
    for m in re.finditer(r'https?://[^\s"\'>]+(?:html|shtml)', html, flags=re.IGNORECASE):
        full = normalize_link(m.group(0), profile)
        slug = full.rstrip("/").split("/")[-1]
        title = normalize(re.sub(r"[_-]+", " ", slug))
        out.append({"title": title, "url": full, "context": "", "published_at": ""})

    for m in re.finditer(r'["\'](/[^"\']+(?:html|shtml))["\']', html, flags=re.IGNORECASE):
        full = normalize_link(urljoin(page_url, m.group(1)), profile)
        slug = full.rstrip("/").split("/")[-1]
        title = normalize(re.sub(r"[_-]+", " ", slug))
        out.append({"title": title, "url": full, "context": "", "published_at": ""})

    return out


def extract_candidate_links(page_url, fetch_mode, profile):
    max_attempts = max(1, int(profile.get("extract_attempts", 2)))
    for attempt in range(max_attempts):
        if attempt:
            FETCHER.cache.pop(page_url, None)
            if fetch_mode == "playwright":
                FETCHER.close()
        html = request_html(page_url, referer=root_url(page_url), mode=fetch_mode)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        out = []
        seen = set()

        for a in soup.select("a[href]")[:MAX_LINKS_PER_PAGE]:
            title = normalize(a.get_text(" ", strip=True))
            href = normalize(a.get("href", ""))
            if not href or href.lower().startswith("javascript:"):
                continue
            full = normalize_link(urljoin(page_url, href), profile)
            if not is_http_link(full):
                continue
            if len(title) < 6:
                continue
            container = a.find_parent(["li", "tr", "article", "section", "div", "td"])
            context = normalize(container.get_text(" ", strip=True)) if container else ""
            published_dt = extract_publish_dt(f"{title} {context}", full)
            key = (title, full)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "title": title,
                    "url": full,
                    "context": context[:500],
                    "published_at": published_dt.strftime("%Y-%m-%d %H:%M:%S") if published_dt else "",
                }
            )

        if len(out) < 5:
            for item in extract_embedded_links(html, page_url, profile):
                title = item["title"]
                full = item["url"]
                if not is_http_link(full):
                    continue
                key = (title, full)
                if key in seen:
                    continue
                seen.add(key)
                out.append(item)
                if len(out) >= MAX_LINKS_PER_PAGE:
                    break

        if out or attempt == max_attempts - 1:
            return out

    return []


def discover_paginated_pages(channel_url, profile):
    fetch_mode = profile["fetch_mode"]
    page_limit = profile["page_limit"]
    pages = [channel_url]
    seen = {channel_url}
    cu = channel_url.lower()

    def add_page(u):
        if u in seen:
            return
        seen.add(u)
        pages.append(u)

    for tpl in profile.get("page_templates", []):
        suffix = tpl.get("suffix", "").lower()
        if suffix and cu.endswith(suffix):
            base = channel_url[: -len(suffix.lstrip("/"))]
            for i in range(int(tpl.get("start", 2)), page_limit + int(tpl.get("start", 2))):
                add_page(tpl.get("template", "{base}{n}.html").format(base=base, n=i))

    html = request_html(channel_url, referer=root_url(channel_url), mode=fetch_mode)
    if not html:
        return pages[: page_limit + 1]

    m = re.search(r"createPageHTML\(\s*\d+\s*,\s*\d+\s*,\s*\"([^\"]+)\"\s*,\s*\"([^\"]+)\"\s*,", html)
    if m:
        total_pages_match = re.search(r"createPageHTML\(\s*(\d+)\s*,", html)
        total_pages = int(total_pages_match.group(1)) if total_pages_match else 0
        prefix = m.group(1)
        ext = m.group(2)
        base = ""
        if channel_url.endswith(f"{prefix}.{ext}"):
            base = channel_url[: -len(f"{prefix}.{ext}")]
        elif channel_url.endswith("/"):
            base = channel_url
        if base:
            for i in range(1, min(total_pages - 1, page_limit) + 1):
                add_page(f"{base}{prefix}_{i}.{ext}")

    if any(k in cu for k in ["zbgg", "zbtb", "jyxx", "tzgg", "notice", "gg"]) and len(pages) == 1:
        if channel_url.endswith("/"):
            prefix = channel_url + "index"
            ext = "html"
        elif channel_url.endswith("index.shtml"):
            prefix = channel_url[: -len("index.shtml")] + "index"
            ext = "shtml"
        elif channel_url.endswith("index.html"):
            prefix = channel_url[: -len("index.html")] + "index"
            ext = "html"
        else:
            prefix = ""
            ext = ""
        if prefix:
            for i in range(2, page_limit + 2):
                add_page(f"{prefix}_{i}.{ext}")

    soup = BeautifulSoup(html, "html.parser")
    for a in soup.select("a[href]"):
        text = normalize(a.get_text(" ", strip=True))
        href = normalize(a.get("href", ""))
        if not href:
            continue
        full = normalize_link(urljoin(channel_url, href), profile)
        if not is_http_link(full) or not same_domain(full, channel_url):
            continue
        t = f"{text} {full.lower()}"
        if not (
            "下一页" in text
            or "下页" in text
            or "尾页" in text
            or "page=" in t
            or "index_" in t
        ):
            continue
        add_page(full)

    pages = pages[: page_limit + 1]
    return pages


def fetch_detail_text(url, fetch_mode):
    html = request_html(url, referer=root_url(url), mode=fetch_mode)
    if not html:
        return {"title": "", "text": ""}
    soup = BeautifulSoup(html, "html.parser")
    t1 = normalize(soup.title.get_text(" ", strip=True)) if soup.title else ""
    h1 = normalize(soup.find("h1").get_text(" ", strip=True)) if soup.find("h1") else ""
    h2 = normalize(soup.find("h2").get_text(" ", strip=True)) if soup.find("h2") else ""
    title_text = normalize(f"{t1} {h1} {h2}")
    body = normalize(soup.get_text(" ", strip=True))
    return {"title": title_text, "text": body[:5000]}


def make_id(title, url):
    raw = f"{title}|{url}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def load_seen():
    p = DATA / "seen_ids.txt"
    if not p.exists():
        return set()
    return set(p.read_text(encoding="utf-8").splitlines())


def save_seen(seen):
    (DATA / "seen_ids.txt").write_text("\n".join(sorted(seen)), encoding="utf-8")


def load_last_incremental_run(profile=None):
    run_file = get_last_incremental_run_file(profile)
    if run_file.exists():
        text = run_file.read_text(encoding="utf-8").strip()
    elif LEGACY_LAST_INCREMENTAL_RUN_FILE.exists():
        text = LEGACY_LAST_INCREMENTAL_RUN_FILE.read_text(encoding="utf-8").strip()
    else:
        return SNAPSHOT_CUTOFF
    if not text:
        return SNAPSHOT_CUTOFF
    try:
        return datetime.datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return SNAPSHOT_CUTOFF


def save_last_incremental_run(run_started_at, profile=None):
    run_file = get_last_incremental_run_file(profile)
    run_file.write_text(run_started_at.strftime("%Y-%m-%d %H:%M:%S"), encoding="utf-8")


def latest_issue_file():
    files = sorted(LOGS.glob("issues_full_*.json"))
    return files[-1] if files else None


def load_retry_issue_map():
    issue_file = latest_issue_file()
    if not issue_file:
        return {}
    try:
        issues = json.loads(issue_file.read_text(encoding="utf-8"))
    except Exception:
        return {}
    by_source = {}
    for issue in issues:
        by_source.setdefault(issue.get("source", ""), set()).add(issue.get("problem", ""))
    return by_source


def _parse_dt(value, fmts):
    for fmt in fmts:
        try:
            return datetime.datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def extract_publish_dt(text, url):
    joined = normalize(f"{text} {url}")

    for m in re.finditer(r"(20\d{2})[-/\.](\d{1,2})[-/\.](\d{1,2})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?", joined):
        year, month, day = map(int, m.group(1, 2, 3))
        hour = int(m.group(4) or 0)
        minute = int(m.group(5) or 0)
        second = int(m.group(6) or 0)
        try:
            return datetime.datetime(year, month, day, hour, minute, second)
        except ValueError:
            continue

    for m in re.finditer(r"(20\d{2})年(\d{1,2})月(\d{1,2})日(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?", joined):
        year, month, day = map(int, m.group(1, 2, 3))
        hour = int(m.group(4) or 0)
        minute = int(m.group(5) or 0)
        second = int(m.group(6) or 0)
        try:
            return datetime.datetime(year, month, day, hour, minute, second)
        except ValueError:
            continue

    for m in re.finditer(r"/(20\d{2})(\d{2})(\d{2})/", url):
        year, month, day = map(int, m.group(1, 2, 3))
        try:
            return datetime.datetime(year, month, day, 0, 0, 0)
        except ValueError:
            continue

    for m in re.finditer(r"/t(20\d{2})(\d{2})(\d{2})_\d+\.(?:shtml|html)", url, flags=re.IGNORECASE):
        year, month, day = map(int, m.group(1, 2, 3))
        try:
            return datetime.datetime(year, month, day, 0, 0, 0)
        except ValueError:
            continue

    for m in re.finditer(r"\b(20\d{2})(\d{2})(\d{2})\b", joined):
        year, month, day = map(int, m.group(1, 2, 3))
        try:
            return datetime.datetime(year, month, day, 0, 0, 0)
        except ValueError:
            continue

    return None


def score_candidate(candidate, source, cutoff):
    text = normalize(
        f"{candidate['title']} {candidate.get('context', '')} {candidate['url']} "
        f"{source.get('province', '')} {source.get('name', '')} {source.get('type', '')}"
    )
    score = 0
    if hit_maint_design(text):
        score += 20
    if hit_highway_signal(text):
        score += 12
    if hit_include(text):
        score += 8
    if "招标公告" in text:
        score += 5
    for kw in rules.get("priority_keywords", []):
        if kw in text:
            score += 9
    published_at = candidate.get("published_at", "")
    if published_at:
        try:
            published_dt = datetime.datetime.strptime(published_at, "%Y-%m-%d %H:%M:%S")
            if published_dt >= cutoff:
                score += 6
            else:
                score -= 10
        except ValueError:
            pass
    return score


def page_priority(url, snapshot):
    match = re.search(r"index_(\d+)\.s?html?$", url, flags=re.IGNORECASE)
    if match:
        idx = int(match.group(1))
        return idx if snapshot else -idx
    return -1 if snapshot else 1


def fetch_api_items(source, profile):
    api_cfg = profile.get("api_search")
    if not api_cfg:
        return []
    api_list = api_cfg if isinstance(api_cfg, list) else [api_cfg]
    items = []
    seen = set()
    for api in api_list:
        detail_cache = {}
        notice_cache = {}
        headers = {
            "User-Agent": random.choice(UA_LIST),
            "Referer": api["root"],
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
        if api.get("content_type"):
            headers["Content-Type"] = api["content_type"]
        request_mode = api.get("request_mode", "json_string")
        response_kind = api.get("response_kind", "result_records")
        try:
            FETCHER.session.get(api["root"], timeout=REQUEST_TIMEOUT, headers=headers)
        except Exception:
            pass
        for page in range(api.get("max_pages", 1)):
            if request_mode == "form":
                payload = {
                    "siteGuid": api.get("siteGuid"),
                    "categoryNum": api.get("categoryNum"),
                    "kw": api.get("wd", ""),
                    "startDate": api.get("startDate", ""),
                    "endDate": api.get("endDate", ""),
                    "pageIndex": page,
                    "pageSize": api.get("pageSize", 20),
                    "jytype": api.get("jytype", ""),
                    "xiaqucode": api.get("xiaqucode", "4100"),
                }
            elif request_mode == "get":
                payload = {
                    "contentId": api.get("contentId", ""),
                    "channelIds": api.get("channelIds", ""),
                    "channelOption": api.get("channelOption", 1),
                    "tagIds": api.get("tagIds", ""),
                    "channelPaths": api.get("channelPaths", ""),
                    "siteIds": api.get("siteIds", ""),
                    "typeIds": api.get("typeIds", ""),
                    "title": api.get("title", ""),
                    "isNew": api.get("isNew", ""),
                    "isTop": api.get("isTop", ""),
                    "timeBegin": api.get("timeBegin", ""),
                    "timeEnd": api.get("timeEnd", ""),
                    "excludeId": api.get("excludeId", ""),
                    "orderBy": api.get("orderBy", "29"),
                    "page": page + 1,
                    "size": api.get("pageSize", 20),
                    "releaseTarget": api.get("releaseTarget", ""),
                    "pageNo": page + 1,
                    "pageNum": page + 1,
                    "pageSize": api.get("pageSize", 20),
                    "tenderProjectType": api.get("tenderProjectType", ""),
                    "regionCode": api.get("regionCode", ""),
                    "notice": api.get("notice", ""),
                    "status": api.get("status", ""),
                    "tenderMode": api.get("tenderMode", ""),
                }
            else:
                payload = {
                    "token": "",
                    "pn": page * int(api.get("rn", 100)),
                    "rn": str(api.get("rn", 100)),
                    "sdt": api.get("sdt", ""),
                    "edt": api.get("edt", ""),
                    "wd": api.get("wd", " "),
                    "inc_wd": "",
                    "exc_wd": "",
                    "fields": "title",
                    "cnum": api.get("cnum", "001"),
                    "sort": api.get("sort", '{"webdate":0}'),
                    "ssort": "title",
                    "cl": 200,
                    "terminal": "",
                    "condition": api.get("condition"),
                    "time": api.get("time"),
                    "highlights": "title",
                    "statistics": None,
                    "unionCondition": None,
                    "accuracy": "",
                    "noParticiple": "0",
                    "searchRange": None,
                    "isBusiness": "1",
                }
            try:
                if request_mode == "form":
                    resp = FETCHER.session.post(api["url"], data=payload, timeout=REQUEST_TIMEOUT, headers=headers)
                elif request_mode == "get":
                    resp = FETCHER.session.get(api["url"], params=payload, timeout=REQUEST_TIMEOUT, headers=headers)
                else:
                    resp = FETCHER.session.post(
                        api["url"],
                        data=json.dumps(payload, ensure_ascii=False),
                        timeout=REQUEST_TIMEOUT,
                        headers=headers,
                    )
                resp.raise_for_status()
                data = resp.json()
            except Exception:
                break
            if response_kind == "custom_infodata":
                records = (((data or {}).get("custom") or {}).get("infodata")) or []
            elif response_kind == "content_page":
                records = (((data or {}).get("data") or {}).get("content")) or []
            elif response_kind == "page_records":
                records = (((data or {}).get("data") or {}).get("records")) or []
            else:
                records = (((data or {}).get("result") or {}).get("records")) or []
            if not records:
                break
            for rec in records:
                title = normalize(
                    strip_tags(
                        rec.get("title")
                        or rec.get("titlenew")
                        or rec.get("customtitle")
                        or rec.get("msgPlace")
                        or rec.get("tenderProjectName")
                        or rec.get("bidSectionName")
                        or rec.get("noticeName")
                        or rec.get("name")
                        or ""
                    )
                )
                raw_href = rec.get("linkurl", "") or rec.get("infourl", "") or rec.get("url", "") or rec.get("urlWhole", "")
                if not raw_href and api.get("detail_api_prefix") and rec.get("bidSectionId"):
                    raw_href = api["detail_api_prefix"] + rec.get("bidSectionId")
                if api.get("link_prefix") and isinstance(raw_href, str) and raw_href.startswith("/"):
                    href = normalize_link(api["link_prefix"].rstrip("/") + raw_href, profile)
                else:
                    href = normalize_link(urljoin(api["root"], raw_href), profile)
                if not title or not href or (title, href) in seen:
                    continue
                seen.add((title, href))
                if profile.get("validate_api_links") and not is_live_url(href, referer=api["root"]):
                    record_issue(source, "api_validate", "detail_url_unavailable", href, title, action="skip_item")
                    continue
                body = normalize(
                    f"{title} {strip_tags(rec.get('content', ''))} "
                    f"{strip_tags(rec.get('categoryname', ''))} "
                    f"{strip_tags(rec.get('catename', ''))} "
                    f"{strip_tags(rec.get('infod', ''))} "
                    f"{strip_tags(rec.get('customtitle', ''))} "
                    f"{strip_tags(rec.get('description', ''))} "
                    f"{strip_tags(rec.get('channelName', ''))} "
                    f"{strip_tags(rec.get('tenderProjectName', ''))} "
                    f"{strip_tags(rec.get('bidSectionName', ''))} "
                    f"{strip_tags(rec.get('name', ''))} "
                    f"{strip_tags(rec.get('noticeType', ''))}"
                )
                section_id = rec.get("bidSectionId", "")
                if response_kind == "page_records" and section_id:
                    coarse_hit = any(
                        k in body
                        for k in [
                            "养护",
                            "施工",
                            "设计",
                            "公路",
                            "国道",
                            "省道",
                            "县道",
                            "乡道",
                            "农村公路",
                            "桥梁",
                            "路面",
                            "提质改造",
                            "大中修",
                            "路面改善",
                        ]
                    )
                    if coarse_hit:
                        if api.get("detail_api_prefix") and section_id not in detail_cache:
                            try:
                                detail_resp = FETCHER.session.get(
                                    api["detail_api_prefix"] + section_id,
                                    timeout=REQUEST_TIMEOUT,
                                    headers=headers,
                                )
                                detail_resp.raise_for_status()
                                detail_cache[section_id] = detail_resp.json()
                            except Exception:
                                detail_cache[section_id] = {}
                        detail_data = ((detail_cache.get(section_id) or {}).get("data")) or {}
                        construction_tender = detail_data.get("constructionTender") or {}
                        construction_project = detail_data.get("constructionProject") or {}
                        section_list = detail_data.get("constructionSectionList") or []
                        section_data = section_list[0] if section_list else {}

                        if api.get("detail_notice_api_prefix") and section_id not in notice_cache:
                            try:
                                notice_resp = FETCHER.session.get(
                                    api["detail_notice_api_prefix"] + section_id,
                                    timeout=REQUEST_TIMEOUT,
                                    headers=headers,
                                )
                                notice_resp.raise_for_status()
                                notice_cache[section_id] = notice_resp.json()
                            except Exception:
                                notice_cache[section_id] = {}
                        notice_list = (((notice_cache.get(section_id) or {}).get("data")) or {}).get("noticeList") or []
                        notice_blob = " ".join(
                            normalize(
                                f"{strip_tags(x.get('noticeName', ''))} "
                                f"{strip_tags(x.get('bulletinName', ''))} "
                                f"{strip_tags(x.get('noticeContent', ''))[:1200]}"
                            )
                            for x in notice_list[:3]
                        )
                        detail_blob = normalize(
                            f"{strip_tags(construction_tender.get('tenderProjectName', ''))} "
                            f"{strip_tags(construction_tender.get('ownerName', ''))} "
                            f"{strip_tags(construction_tender.get('tendererName', ''))} "
                            f"{strip_tags(construction_tender.get('tenderContent', ''))} "
                            f"{strip_tags(construction_project.get('projectName', ''))} "
                            f"{strip_tags(construction_project.get('approvalName', ''))} "
                            f"{strip_tags(section_data.get('bidSectionName', ''))} "
                            f"{strip_tags(section_data.get('bidSectionContent', ''))}"
                        )
                        body = normalize(f"{body} {detail_blob} {notice_blob}")
                if hit_exclude(body) or hit_non_notice(body) or hit_non_road(body):
                    continue
                if rules.get("strict_target_mode"):
                    if not hit_maint_design(body):
                        continue
                    if not hit_strict_target(body):
                        continue
                elif not hit_include(body):
                    continue
                published_at = normalize(
                    rec.get("webdate")
                    or rec.get("infodate")
                    or rec.get("releaseTime")
                    or rec.get("createTime")
                    or rec.get("noticeSendTime")
                    or ""
                )
                items.append(
                    {
                        "title": title,
                        "url": href,
                        "source": source["name"],
                        "province": source["province"],
                        "category": classify(body),
                        "published_at": published_at.replace("T", " ")[:19],
                        "fetched_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
    return items


def retry_source_priority(source, retry_issue_map):
    problems = retry_issue_map.get(source.get("name", ""), set())
    score = 0
    if source.get("type") == "operator":
        score += 30
    elif source.get("type") == "transport_dept":
        score += 25
    elif source.get("type") == "provincial":
        score += 10
    if "detail_limit_exceeded" in problems:
        score += 10
    if "source_timeout" in problems:
        score += 6
    if {"empty_candidate_links", "channel_root_unavailable", "channel_page_unavailable"} & problems:
        score += 4
    return score


def reset_runtime_state():
    RUN_ISSUES.clear()
    RUN_ISSUE_KEYS.clear()
    ISSUE_COUNTS.clear()
    ISSUE_SAMPLES.clear()
    FETCHER.cache.clear()


def suppress_successful_timeout_noise(source_name, items):
    if not items:
        return
    keep_issues = []
    keep_keys = set()
    for issue in RUN_ISSUES:
        key = (issue["source"], issue["stage"], issue["problem"], issue["action"])
        if issue["source"] == source_name and issue["stage"] == "source_budget" and issue["problem"] == "source_timeout":
            ISSUE_COUNTS.pop(key, None)
            ISSUE_SAMPLES.pop(key, None)
            continue
        keep_issues.append(issue)
        keep_keys.add(key)
    RUN_ISSUES[:] = keep_issues
    RUN_ISSUE_KEYS.clear()
    RUN_ISSUE_KEYS.update(keep_keys)


def suppress_nonfatal_detail_budget_noise(source_name, items):
    if not items or source_name != "湖北省交通运输厅":
        return
    keep_issues = []
    keep_keys = set()
    for issue in RUN_ISSUES:
        key = (issue["source"], issue["stage"], issue["problem"], issue["action"])
        if issue["source"] == source_name and issue["stage"] == "detail_budget" and issue["problem"] == "detail_limit_exceeded":
            ISSUE_COUNTS.pop(key, None)
            ISSUE_SAMPLES.pop(key, None)
            continue
        keep_issues.append(issue)
        keep_keys.add(key)
    RUN_ISSUES[:] = keep_issues
    RUN_ISSUE_KEYS.clear()
    RUN_ISSUE_KEYS.update(keep_keys)


def collect_source_result(source, cutoff, snapshot, retry_issue_map, active_profile=None):
    global ACTIVE_PROFILE, rules
    if active_profile:
        ACTIVE_PROFILE = active_profile
        rules = ACTIVE_PROFILE["rules"]
    reset_runtime_state()
    started = time.perf_counter()
    items = fetch_items(source, cutoff=cutoff, snapshot=snapshot, retry_issue_map=retry_issue_map)
    suppress_successful_timeout_noise(source.get("name", ""), items)
    suppress_nonfatal_detail_budget_noise(source.get("name", ""), items)
    result = {
        "source": source,
        "items": items,
        "elapsed_sec": round(time.perf_counter() - started, 1),
        "issues": list(RUN_ISSUES),
        "issue_counts": dict(ISSUE_COUNTS),
        "issue_samples": dict(ISSUE_SAMPLES),
        "domain_mode": dict(FETCHER.domain_mode),
    }
    FETCHER.close()
    return result


def merge_source_result(result):
    RUN_ISSUES.extend(result["issues"])
    for key, value in result["issue_counts"].items():
        ISSUE_COUNTS[key] = ISSUE_COUNTS.get(key, 0) + value
    for key, samples in result["issue_samples"].items():
        dst = ISSUE_SAMPLES.setdefault(key, [])
        for sample in samples:
            if sample not in dst and len(dst) < MAX_ISSUE_SAMPLES:
                dst.append(sample)
    FETCHER.domain_mode.update(result["domain_mode"])


def keep_items_for_source(source, source_items, cutoff, seen, items):
    kept = 0
    for it in source_items:
        published_at = it.get("published_at", "")
        if not published_at:
            record_issue(source, "time_filter", "missing_publish_time", it.get("url", ""), action="skip_item")
            continue
        try:
            published_dt = datetime.datetime.strptime(published_at, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            record_issue(source, "time_filter", "invalid_publish_time", it.get("url", ""), published_at, action="skip_item")
            continue
        if published_dt < cutoff:
            continue
        _id = make_id(it["title"], it["url"])
        if _id in seen:
            continue
        seen.add(_id)
        it["id"] = _id
        items.append(it)
        kept += 1
    return kept


def run_collect(snapshot=False, retry_only=False, retry_scope="all", workers=1):
    RUN_ISSUES.clear()
    RUN_ISSUE_KEYS.clear()
    ISSUE_COUNTS.clear()
    ISSUE_SAMPLES.clear()
    FETCHER.cache.clear()
    source_file = ACTIVE_PROFILE.get("sources_file", "sources.csv")
    sources = list(csv.DictReader((CFG / source_file).read_text(encoding="utf-8").splitlines()))
    sources = [s for s in sources if source_allowed(s)]
    retry_issue_map = load_retry_issue_map() if retry_only else {}
    if retry_only and retry_issue_map:
        sources = [s for s in sources if s["name"] in retry_issue_map]
        sources = sorted(sources, key=lambda s: retry_source_priority(s, retry_issue_map), reverse=True)
        if retry_scope == "high_value":
            sources = [s for s in sources if s.get("type") in {"operator", "transport_dept"}]
        elif retry_scope == "long_tail":
            sources = [s for s in sources if s.get("type") not in {"operator", "transport_dept"}]
    seen = set() if snapshot or retry_only else load_seen()
    run_started_at = datetime.datetime.now()
    run_started_perf = time.perf_counter()
    cutoff = SNAPSHOT_CUTOFF if (snapshot or retry_only) else load_last_incremental_run(ACTIVE_PROFILE)
    items = []
    source_stats = []

    parallel_sources = []
    serial_sources = []
    for s in sources:
        profile = apply_retry_profile(get_source_profile(s), s, retry_issue_map)
        if workers > 1 and profile.get("fetch_mode") != "playwright":
            parallel_sources.append(s)
        else:
            serial_sources.append(s)

    if parallel_sources:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_map = {
                executor.submit(collect_source_result, s, cutoff, (snapshot or retry_only), retry_issue_map, ACTIVE_PROFILE): s
                for s in parallel_sources
            }
            for future in as_completed(future_map):
                result = future.result()
                s = result["source"]
                merge_source_result(result)
                kept = keep_items_for_source(s, result["items"], cutoff, seen, items)
                source_stats.append(
                    {
                        "source": s["name"],
                        "province": s["province"],
                        "type": s["type"],
                        "matched": kept,
                        "elapsed_sec": result["elapsed_sec"],
                    }
                )
                status_label = "retry" if retry_only else ("full" if snapshot else "new")
                print(f"[{s['name']}] {status_label}={kept}")

    for s in serial_sources:
        result = collect_source_result(s, cutoff, (snapshot or retry_only), retry_issue_map, ACTIVE_PROFILE)
        merge_source_result(result)
        kept = keep_items_for_source(s, result["items"], cutoff, seen, items)
        source_stats.append(
            {
                "source": s["name"],
                "province": s["province"],
                "type": s["type"],
                "matched": kept,
                "elapsed_sec": result["elapsed_sec"],
            }
        )
        status_label = "retry" if retry_only else ("full" if snapshot else "new")
        print(f"[{s['name']}] {status_label}={kept}")

    if not snapshot:
        save_seen(seen)
        save_last_incremental_run(run_started_at, ACTIVE_PROFILE)
    save_fetch_strategy(FETCHER.domain_mode)

    aggregated_issues = []
    seen_issue_keys = set()
    for issue in RUN_ISSUES:
        key = (issue["source"], issue["stage"], issue["problem"], issue["action"])
        if key in seen_issue_keys:
            continue
        seen_issue_keys.add(key)
        issue["count"] = ISSUE_COUNTS.get(key, 1)
        samples = ISSUE_SAMPLES.get(key, [])
        issue["sample_urls"] = [x["url"] for x in samples if x["url"]]
        issue["sample_details"] = [x["detail"] for x in samples if x["detail"]]
        aggregated_issues.append(issue)

    profile_id = ACTIVE_PROFILE.get("_name", DEFAULT_PROFILE)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    if retry_only:
        out_name = f"hits_retry_{profile_id}_{ts}.json"
    else:
        out_name = f"hits_full_{profile_id}_{ts}.json" if snapshot else f"hits_{profile_id}_{ts}.json"
    out_file = OUT / out_name
    out_file.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")

    if retry_only:
        issue_name = f"issues_retry_{profile_id}_{ts}.json"
    else:
        issue_name = f"issues_full_{profile_id}_{ts}.json" if snapshot else f"issues_{profile_id}_{ts}.json"
    issue_file = LOGS / issue_name
    issue_file.write_text(json.dumps(aggregated_issues, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = {
        "mode": "retry" if retry_only else ("snapshot" if snapshot else "incremental"),
        "profile": ACTIVE_PROFILE.get("name", DEFAULT_PROFILE),
        "profile_id": ACTIVE_PROFILE.get("_name", DEFAULT_PROFILE),
        "count": len(items),
        "cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
        "wall_time_sec": round(time.perf_counter() - run_started_perf, 1),
        "sources": source_stats,
        "issues_count": len(aggregated_issues),
        "issue_event_count": int(sum(ISSUE_COUNTS.values())),
        "output_file": str(out_file),
        "issues_file": str(issue_file),
    }
    if retry_only:
        summary_file = LOGS / f"summary_retry_{profile_id}.json"
    else:
        summary_file = LOGS / (f"summary_full_{profile_id}.json" if snapshot else f"summary_incremental_{profile_id}.json")
    summary_file.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    if retry_only:
        legacy_summary_file = LOGS / "summary_retry.json"
    else:
        legacy_summary_file = LOGS / ("summary_full.json" if snapshot else "summary_incremental.json")
    legacy_summary_file.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    label = "total_retry" if retry_only else ("total_full" if snapshot else "new_items")
    print(f"{label}={len(items)}")
    print(out_file)
    print(issue_file)
    return out_file, issue_file, summary_file


def fetch_items(source, cutoff, snapshot, retry_issue_map=None):
    items = []

    profile = get_source_profile(source)
    profile = apply_mode_profile(profile, snapshot)
    profile = apply_retry_profile(profile, source, retry_issue_map or {})
    api_items = fetch_api_items(source, profile)
    if api_items:
        items.extend(api_items)
    if profile.get("api_only"):
        return items
    checked_detail = 0
    detail_cache = {}
    seen_local = set()
    seen_pages = set()
    source_start = time.perf_counter()

    channels = discover_channel_pages(source, profile)

    def channel_score(url):
        u = url.lower()
        score = 0
        if any(urlparse(url).path.startswith(p.rstrip("/")) for p in profile.get("channel_paths", [])):
            score += 10
        if "zbgg" in u or "zbtb" in u:
            score += 8
        if "jyxx" in u:
            score += 6
        if "ggzy" in u:
            score += 4
        if "notice" in u or "tzgg" in u:
            score += 3
        return score

    channels = sorted(channels, key=channel_score, reverse=True)
    channel_limit = profile["channel_limit"] if rules.get("strict_target_mode") else MAX_CHANNEL_PAGES
    if not snapshot:
        incremental_channel_limit = int(profile.get("incremental_channel_limit", 0) or 0)
        if incremental_channel_limit > 0:
            channel_limit = min(channel_limit, incremental_channel_limit)
    if rules.get("strict_target_mode"):
        strong = [c for c in channels if channel_score(c) >= 6]
        if strong:
            channels = strong[:channel_limit]
            if source["url"] not in channels and len(channels) < channel_limit:
                channels.append(source["url"])
        else:
            channels = channels[:channel_limit]
    else:
        channels = channels[:channel_limit]

    for channel_url in channels:
        if time.perf_counter() - source_start > profile["source_budget"]:
            record_issue(source, "source_budget", "source_timeout", channel_url, action="skip_source")
            break
        page_urls = discover_paginated_pages(channel_url, profile)
        page_urls = sorted(page_urls, key=lambda url: page_priority(url, snapshot), reverse=True)
        if not snapshot:
            incremental_page_limit = int(profile.get("incremental_page_limit", 0) or 0)
            if incremental_page_limit > 0:
                page_urls = page_urls[:incremental_page_limit]
        if len(page_urls) == 1 and page_urls[0] == channel_url and channel_score(channel_url) >= 6:
            record_issue(source, "pagination", "channel_page_unavailable", channel_url, action="skip_channel")
        for page_url in page_urls:
            if time.perf_counter() - source_start > profile["source_budget"]:
                record_issue(source, "source_budget", "source_timeout", page_url, action="skip_source")
                break
            if page_url in seen_pages:
                continue
            seen_pages.add(page_url)

            links = extract_candidate_links(page_url, profile["fetch_mode"], profile)
            if not links and channel_score(page_url) >= 6:
                record_issue(source, "list_extract", "empty_candidate_links", page_url, action="skip_page")
            links = sorted(links, key=lambda item: score_candidate(item, source, cutoff), reverse=True)
            candidate_limit = int(profile.get("candidate_limit", 0) or 0)
            if candidate_limit > 0:
                links = links[:candidate_limit]
            for candidate in links:
                title = candidate["title"]
                href = candidate["url"]
                if time.perf_counter() - source_start > profile["source_budget"]:
                    record_issue(source, "source_budget", "source_timeout", href, action="skip_source")
                    break
                if (title, href) in seen_local:
                    continue
                seen_local.add((title, href))

                context = candidate.get("context", "")
                txt = f"{title} {context} {href} {source.get('province', '')} {source.get('name', '')} {source.get('type', '')}"
                list_publish_dt = None
                if candidate.get("published_at"):
                    try:
                        list_publish_dt = datetime.datetime.strptime(candidate["published_at"], "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        list_publish_dt = None
                if list_publish_dt and list_publish_dt < cutoff:
                    continue
                if hit_exclude(txt):
                    continue
                if hit_non_notice(txt):
                    continue
                if not hit_notice(txt):
                    continue
                if hit_non_road(txt):
                    continue

                detail_seed = None
                prehit = hit_include(txt) and hit_highway_signal(txt)
                if not prehit:
                    if not rules.get("strict_target_mode"):
                        continue
                    if not (hit_maint_design(txt) or hit_include(txt) or hit_highway_signal(txt)):
                        continue
                    if checked_detail >= profile["detail_limit"]:
                        record_issue(source, "detail_budget", "detail_limit_exceeded", page_url, action="skip_page_details")
                        continue
                    if href not in detail_cache:
                        detail_cache[href] = fetch_detail_text(href, profile["fetch_mode"])
                    checked_detail += 1
                    d = detail_cache[href]
                    detail_seed = f"{txt} {d['title']}"
                    if not (hit_include(detail_seed) and hit_highway_signal(detail_seed)):
                        continue

                if rules.get("strict_target_mode"):
                    judge_text = detail_seed or txt
                    need_detail = False
                    if not hit_maint_design(judge_text):
                        need_detail = True

                    if rules.get("strict_title_only"):
                        if not (hit_maint_design(title) and hit_strict_target(txt)):
                            continue
                    else:
                        if need_detail:
                            if checked_detail >= profile["detail_limit"]:
                                record_issue(source, "detail_budget", "detail_limit_exceeded", page_url, action="skip_page_details")
                                continue
                            if href not in detail_cache:
                                detail_cache[href] = fetch_detail_text(href, profile["fetch_mode"])
                            checked_detail += 1
                            d = detail_cache[href]
                            judge_text = f"{txt} {d['title']}"
                        if hit_non_road(judge_text):
                            continue
                        if not hit_maint_design(judge_text):
                            continue
                        if hit_strict_target(judge_text):
                            pass
                        else:
                            if checked_detail >= profile["detail_limit"]:
                                record_issue(source, "detail_budget", "detail_limit_exceeded", page_url, action="skip_page_details")
                                continue
                            if href not in detail_cache:
                                detail_cache[href] = fetch_detail_text(href, profile["fetch_mode"])
                            checked_detail += 1
                            d = detail_cache[href]
                            judge_text = f"{txt} {d['title']} {d['text']}"
                        if hit_non_road(judge_text):
                            continue
                        if not hit_strict_target(judge_text):
                            continue

                publish_dt = list_publish_dt or extract_publish_dt(f"{title} {context}", href)
                if not publish_dt and href in detail_cache:
                    d = detail_cache[href]
                    publish_dt = extract_publish_dt(f"{title} {d['title']} {d['text']}", href)
                if not publish_dt and rules.get("strict_target_mode"):
                    if checked_detail < profile["detail_limit"] and href not in detail_cache:
                        detail_cache[href] = fetch_detail_text(href, profile["fetch_mode"])
                        checked_detail += 1
                    if href in detail_cache:
                        d = detail_cache[href]
                        publish_dt = extract_publish_dt(f"{title} {d['title']} {d['text']}", href)
                if not publish_dt:
                    record_issue(source, "publish_time", "publish_time_unresolved", href, title, action="skip_item")
                    continue

                items.append(
                    {
                        "title": title,
                        "url": href,
                        "source": source["name"],
                        "province": source["province"],
                        "category": classify(txt),
                        "published_at": publish_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "fetched_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )

    return items


def main():
    global ACTIVE_PROFILE, rules
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        nargs="?",
        choices=["incremental", "snapshot", "retry", "retry_high_value", "retry_long_tail"],
        default="incremental",
    )
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--workers", type=int, default=None)
    args = parser.parse_args()
    ACTIVE_PROFILE = load_profile(args.profile)
    rules = ACTIVE_PROFILE["rules"]
    retry_only = args.mode in {"retry", "retry_high_value", "retry_long_tail"}
    retry_scope = "all"
    if args.mode == "retry_high_value":
        retry_scope = "high_value"
    elif args.mode == "retry_long_tail":
        retry_scope = "long_tail"
    workers = args.workers if args.workers is not None else (4 if args.mode == "snapshot" else (3 if retry_only else 3))
    run_collect(
        snapshot=args.mode == "snapshot",
        retry_only=retry_only,
        retry_scope=retry_scope,
        workers=max(1, workers),
    )


if __name__ == "__main__":
    main()
