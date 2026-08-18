# -*- coding: utf-8 -*-
"""
影刀定时任务脚本：电视直播源筛选实测与同步
=============================================
移植自 tvtouyin 的 TVSourceManager 逻辑：
  - 多上游聚合（国内源）
  - 广告/测试/广播/竖屏过滤
  - 频道名规范化（上海卫视→东方卫视）
  - 分类：央视台 / 卫视台 / 地方台
  - 同名合并 + URL 去重 + 单频道 ≤16 源
  - HTTP / ffprobe 实测
  - 结果推送到 GitHub

本机（家用/办公 IP）探测不会被轻易误判，适合每日定时跑。
"""

import os
import re
import sys
import json
import time
import datetime
import subprocess
import concurrent.futures
import urllib.request
import urllib.error
import urllib.parse
import socket

from typing import *
try:
    from xbot.app.logging import trace as print
except Exception:
    pass


DEFAULT_REPO_URL = "https://github.com/eoow123/juying-subscriptions.git"


def sync_cn_tv_sources(repo_url=DEFAULT_REPO_URL,
                       local_workspace="D:/juying_visual_workspace",
                       github_token="",
                       probe_mode="http",
                       output_file="iptv_cn_filtered.txt",
                       enable_legacy_sources=0,
                       proxy="",
                       probe_timeout=8,
                       probe_workers=16,
                       github_actions=False,
                       repo_path=None):
    """
    title: 电视直播源筛选实测与同步
    description: 聚合多路电视直播源，进行广告过滤、分类去重，并通过 HTTP 或 ffprobe 进行实测，最后同步至 GitHub 仓库。
    inputs:
        - repo_url (str): GitHub 仓库地址，eg: "https://github.com/eoow123/juying-subscriptions.git"
        - local_workspace (folder): 本地工作目录，eg: "D:/juying_workspace"
        - github_token (str): GitHub PAT，用于 clone/push 鉴权，eg: "ghp_xxxxxxxxxxxx"
        - probe_mode (str): 探测模式，可选 "http", "ffprobe", "none"，eg: "http"
        - output_file (str): 生成的文件名，eg: "iptv_cn_filtered.txt"
        - enable_legacy_sources (int): 是否同时合并 hkbiang/sdyby2006 两个旧源 (1为是, 0为否)，eg: 1
        - proxy (str): 可选 HTTP 代理，eg: "http://127.0.0.1:7890"
        - probe_timeout (int): 单源探测超时（秒），默认 8
        - probe_workers (int): 探测并发数，默认 16
    outputs:
        - status_msg (str): 运行结果摘要，eg: "筛选实测完成并推送 GitHub：央视台 50 / 卫视台 40 / 地方台 100，共 190 源。"
    """

    # --- 内部常量与配置 ---
    CN_TV_SOURCES = [
        {"label": "vbskycn国内", "urls": ["https://ghproxy.net/https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.m3u", "https://cdn.jsdelivr.net/gh/vbskycn/iptv@master/tv/iptv4.m3u"]},
        {"label": "hujingguang国内", "urls": ["https://ghproxy.net/https://raw.githubusercontent.com/hujingguang/ChinaIPTV/main/cnTV2_GuoNei.m3u8", "https://cdn.jsdelivr.net/gh/hujingguang/ChinaIPTV@main/cnTV2_GuoNei.m3u8"]},
        {"label": "best-fan国内全量", "urls": ["https://ghproxy.net/https://raw.githubusercontent.com/best-fan/iptv-sources/master/cn_all.m3u8", "https://cdn.jsdelivr.net/gh/best-fan/iptv-sources@master/cn_all.m3u8"]},
        {"label": "CCSH国内(含地方台)", "urls": ["https://ghproxy.net/https://raw.githubusercontent.com/CCSH/IPTV/refs/heads/main/live.m3u", "https://cdn.jsdelivr.net/gh/CCSH/IPTV@main/live.m3u"]},
        {"label": "YueChan国内", "urls": ["https://ghproxy.net/https://raw.githubusercontent.com/YueChan/Live/main/IPTV.m3u", "https://cdn.jsdelivr.net/gh/YueChan/Live@main/IPTV.m3u"]},
        {"label": "APTV国内", "urls": ["https://ghproxy.net/https://raw.githubusercontent.com/Kimentanm/aptv/master/m3u/iptv.m3u", "https://cdn.jsdelivr.net/gh/Kimentanm/aptv@master/m3u/iptv.m3u"]},
        {"label": "YanG国内", "urls": ["https://ghproxy.net/https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u", "https://cdn.jsdelivr.net/gh/YanG-1989/m3u@main/Gather.m3u"]},
        {"label": "zbefine国内", "urls": ["https://ghproxy.net/https://raw.githubusercontent.com/zbefine/iptv/main/iptv.m3u", "https://cdn.jsdelivr.net/gh/zbefine/iptv@main/iptv.m3u"]},
        {"label": "fanmingming国内(高频)", "urls": ["https://ghproxy.net/https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv6.m3u", "https://cdn.jsdelivr.net/gh/fanmingming/live@main/tv/m3u/ipv6.m3u"]},
        {"label": "iptv-org中国", "urls": ["https://ghproxy.net/https://raw.githubusercontent.com/iptv-org/iptv/master/channels/cn.m3u", "https://cdn.jsdelivr.net/gh/iptv-org/iptv@master/channels/cn.m3u"]},
    ]
    LEGACY_TXT_SOURCES = [
        {"label": "hkbiang", "urls": ["https://raw.githubusercontent.com/hkbiang/TV/master/result.txt"]},
        {"label": "sdyby2006", "urls": ["https://raw.githubusercontent.com/sdyby2006/TV/master/result.txt"]},
    ]
    PROBE_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    HTTP_PROBE_TIMEOUT = 6          # 单源 HTTP 探测超时（秒）；m3u8 首包通常 1-3s，6s 足够筛死链
    HTTP_PROBE_WORKERS = 32          # HTTP 探测并发；默认家用宽带可承受 32
    FFPROBE_TIMEOUT = 12             # ffprobe 单源超时（秒）；比 HTTP 严但更可靠
    FFPROBE_WORKERS = 6              # ffprobe 并发不宜过高，避免本机 CPU/IO 爆掉
    FETCH_TIMEOUT = 60
    FETCH_WORKERS = 8
    MAX_SOURCES_PER_CHANNEL = 16
    PROBE_CACHE_TTL_DAYS = 7         # 探测结果缓存 7 天，避免每日全量重测
    NAME_ALIASES = {"上海卫视": "东方卫视", "上海东方卫视": "东方卫视", "内蒙卫视": "内蒙古卫视"}
    AD_KEYWORDS = ["广告", "购物", "推广", "商城", "彩票", "测试", "带货", "广播", "广播电台", "test", "test.", "undefined", "radio", "xxx", "av", "成人", "福利", "诈"]
    PROVINCES = ["北京", "天津", "河北", "山西", "内蒙古", "辽宁", "吉林", "黑龙江", "上海", "江苏", "浙江", "安徽", "福建", "江西", "山东", "河南", "湖北", "湖南", "广东", "广西", "海南", "重庆", "四川", "贵州", "云南", "西藏", "陕西", "甘肃", "青海", "宁夏", "新疆", "香港", "澳门", "台湾", "深圳", "厦门", "青岛", "大连", "宁波", "成都", "杭州", "南京", "武汉", "广州", "西安"]
    CLARITY_RE = re.compile(r"\s*[\(（]\s*((?:\d{3,4}\s*[pP])|高清|超清|标清|原画|蓝光|流畅|画质|清晰度|HD|SD|FHD|UHD|4K|2K|1080|720|540|360)\s*[\)）]\s*$", re.IGNORECASE)
    RES_RE_XY = re.compile(r"(\d{3,4})\s*[xX*×]\s*(\d{3,4})")
    RES_RE_P = re.compile(r"(\d{3,4})\s*[pP]")

    # 允许影刀参数覆盖
    HTTP_PROBE_TIMEOUT = int(probe_timeout or 8)
    HTTP_PROBE_WORKERS = int(probe_workers or 16)

    # --- 内部工具函数 ---
    def _log(msg):
        print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}")

    def _fetch_text(url, timeout=FETCH_TIMEOUT, proxy_url=None):
        headers = {"User-Agent": PROBE_UA}
        # github 系域名带 Bearer 鉴权：避免 Actions 等共享 IP 被 GitHub API 限流
        # （无 token 也能直连 raw.githubusercontent，只是有 60/h 频率上限；单文件拉取通常无碍）
        gh_token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or ""
        if gh_token and ("github.com" in url or "raw.githubusercontent.com" in url or "api.github.com" in url):
            headers["Authorization"] = f"Bearer {gh_token}"
        req = urllib.request.Request(url, headers=headers)
        handlers = []
        if proxy_url:
            handlers.append(urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url}))
        opener = urllib.request.build_opener(*handlers) if handlers else urllib.request
        with opener.urlopen(req, timeout=timeout) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            return resp.read().decode(charset, errors="replace")

    def _fetch_first(urls, timeout=FETCH_TIMEOUT, proxy_url=None):
        expanded = []
        for u in urls:
            expanded.append(u)
            # ghproxy.net 包装的 URL 自动展开为直连，作为 Actions 等环境的回退：
            # ghproxy 在部分数据中心 IP 上不可达，而 raw.githubusercontent 属 GitHub 自家 CDN，Actions 必定可达。
            m = re.match(r"https?://ghproxy\.net/(https?://.+)", u)
            if m:
                expanded.append(m.group(1))
        for url in expanded:
            try:
                return _fetch_text(url, timeout=timeout, proxy_url=proxy_url)
            except Exception:
                continue
        return None

    def _parse_m3u(text):
        out, cur_name, cur_group, cur_attrs = [], "", "", {}
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            if line.startswith("#EXTINF"):
                cur_attrs = {m.group(1).lower(): m.group(2) for m in re.finditer(r'([\w-]+)="([^"]*)"', line)}
                cur_group = cur_attrs.get("group-title", "")
                cur_name = line.split(",", 1)[-1].strip()
            elif line.startswith(("http://", "https://", "rtmp://", "rtsp://")):
                if cur_name:
                    out.append((cur_group, cur_name, line, cur_attrs))
                cur_name, cur_group, cur_attrs = "", "", {}
        return out

    def _parse_txt(text):
        out, group = [], "默认"
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            if line.lower().startswith("#genre#"):
                head = line[:line.lower().index("#genre#")].strip()
                if head:
                    group = head
                continue
            parts = line.split(",", 1)
            if len(parts) == 2 and parts[1].strip().startswith(("http", "rtmp", "rtsp")):
                name = parts[0].strip()
                for u in parts[1].strip().split("#"):
                    if u.strip():
                        out.append((group, name, u.strip(), {}))
        return out

    def _clean_name(raw):
        s = raw.strip()
        for _ in range(3):
            s = CLARITY_RE.sub("", s)
        return s.strip()

    def _parse_resolution(s):
        if not s:
            return 0, False
        m = RES_RE_XY.search(s)
        if m:
            w, h = int(m.group(1)), int(m.group(2))
            return (h, False) if h <= w else (0, True)
        m = RES_RE_P.search(s)
        return (int(m.group(1)), False) if m else (0, False)

    def _category_of(name):
        if any(x in name for x in ["CCTV", "央视", "中央"]):
            return "央视台"
        if "卫视" in name:
            return "卫视台"
        if any(p in name for p in PROVINCES):
            return "地方台"
        return "其他"

    def _is_ad(name, group=""):
        hay = f"{name} {group}".lower()
        return any(k.lower() in hay for k in AD_KEYWORDS)

    def _normalize_key(name):
        s = re.sub(r"[\s\-_·•\.]+", "", name.lower())
        return re.sub(r"[^\w\d]", "", s)

    def _parse_source(label, text):
        rows = _parse_m3u(text) if "#EXTM3U" in text[:1000] else _parse_txt(text)
        out = []
        for group, raw_name, url, attrs in rows:
            name = NAME_ALIASES.get(_clean_name(raw_name), _clean_name(raw_name))
            if not name or _is_ad(name, group):
                continue
            height, portrait_invalid = _parse_resolution(attrs.get("tvg-resolution", ""))
            if not portrait_invalid:
                out.append((group, name, url, height))
        return out

    def _merge_and_filter(rows, max_sources):
        by_key, seen_urls = {}, set()
        for group, name, url, height in rows:
            cat = _category_of(name)
            if cat == "其他":
                continue
            key = _normalize_key(name)
            if key not in by_key:
                by_key[key] = {"name": name, "category": cat, "sources": []}
            if url.strip().lower() not in seen_urls:
                seen_urls.add(url.strip().lower())
                by_key[key]["sources"].append((url, height))
        by_cat = {"央视台": [], "卫视台": [], "地方台": []}
        for item in by_key.values():
            by_cat[item["category"]].append((item["name"], item["sources"][:max_sources]))

        def _nsort(item):
            m = re.search(r"(\d+)", item[0])
            return (0, int(m.group(1)), item[0]) if m else (1, 0, item[0])

        for c in by_cat:
            by_cat[c].sort(key=_nsort)
        return by_cat

    # 探测缓存：避免每日全量重测；TTL 内直接复用上次结果
    def _load_probe_cache() -> dict:
        cache_path = os.path.join(abs_repo_path, "generated", ".rt_probe_cache.json")
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_probe_cache(cache: dict):
        cache_path = os.path.join(abs_repo_path, "generated", ".rt_probe_cache.json")
        try:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False)
        except Exception as e:
            _log(f"  [WARN] 写探测缓存失败: {e}")

    def _http_probe(url, cache=None):
        if not url.startswith("http"):
            return "alive", True
        if cache is not None:
            c = cache.get(url)
            if c and (time.time() - c.get("t", 0)) < PROBE_CACHE_TTL_DAYS * 86400:
                return c.get("s", "unknown"), True  # 缓存命中，直接返回状态
        try:
            req = urllib.request.Request(url, headers={"User-Agent": PROBE_UA, "Range": "bytes=0-0", "Referer": url}, method="GET")
            with urllib.request.urlopen(req, timeout=HTTP_PROBE_TIMEOUT) as resp:
                code = resp.getcode()
                if code in (404, 410):
                    return "dead", True
                return "alive", True
        except urllib.error.HTTPError as e:
            if e.code in (404, 410):
                return "dead", True
            return "unknown", True  # 403/5xx 保留（fail-open）
        except (urllib.error.URLError, socket.timeout, ConnectionError, OSError):
            return "dead", True  # DNS 失败/连接拒绝/网络不可达
        except Exception:
            return "unknown", True

    # ffprobe 存在性只检查一次
    _FFPROBE_AVAILABLE = None

    def _ffprobe_available() -> bool:
        nonlocal _FFPROBE_AVAILABLE
        if _FFPROBE_AVAILABLE is not None:
            return _FFPROBE_AVAILABLE
        try:
            res = subprocess.run(["ffprobe", "-version"], capture_output=True, timeout=5)
            _FFPROBE_AVAILABLE = res.returncode == 0
        except Exception:
            _FFPROBE_AVAILABLE = False
        return _FFPROBE_AVAILABLE

    def _ffprobe_check(url, cache=None):
        if not url.startswith("http"):
            return "alive", True
        if cache is not None:
            c = cache.get(url)
            if c and (time.time() - c.get("t", 0)) < PROBE_CACHE_TTL_DAYS * 86400:
                return c.get("s", "unknown"), True
        if not _ffprobe_available():
            return "unknown", False  # False 表示 ffprobe 不可用，需外层报错
        headers = f"User-Agent: {PROBE_UA}\r\nReferer: {url}\r\n"
        cmd = [
            "ffprobe", "-hide_banner", "-v", "error",
            "-analyzeduration", "1s",
            "-probesize", "512",
            "-headers", headers,
            "-timeout", str(FFPROBE_TIMEOUT * 1000000),
            "-show_entries", "format=format_name",
            "-show_entries", "stream=codec_type",
            "-of", "default=noprint_wrappers=1:nokey=1",
            url
        ]
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=FFPROBE_TIMEOUT + 5)
            out = res.stdout.strip().lower()
            err = res.stderr.strip().lower()
            # 只要有视频或音频流，且没致命错误，就判 alive
            has_stream = "video" in out or "audio" in out
            if res.returncode == 0 and has_stream:
                return "alive", True
            # 明确的死链/不可播信号
            fatal = ("404 not found", "forbidden", "connection refused", "could not resolve",
                     "invalid data", "unable to open resource", "http error 4", "http error 5",
                     "end of file", "operation not permitted", "no route to host",
                     "input/output error", "failed to resolve")
            if any(s in err for s in fatal):
                return "dead", True
            # 其他情况保留（fail-open）
            return "unknown", True
        except subprocess.TimeoutExpired:
            return "unknown", True
        except Exception:
            return "unknown", True

    def _run_git(args, cwd=None, check=True, timeout=60, **_kwargs):
        if cwd and os.path.exists(os.path.join(cwd, ".git", "index.lock")):
            try:
                os.remove(os.path.join(cwd, ".git", "index.lock"))
            except Exception:
                pass
        return subprocess.run(["git"] + args, cwd=cwd, check=check,
                              capture_output=True, text=True, encoding="utf-8",
                              timeout=timeout)

    # --- 主逻辑 ---
    _log("=== 电视直播源筛选实测任务开始 ===")
    abs_workspace = os.path.abspath(local_workspace)
    repo_name = repo_url.rstrip("/").split("/")[-1].replace(".git", "")
    abs_repo_path = os.path.join(abs_workspace, repo_name)
    auth_url = f"https://{github_token}@{repo_url.replace('https://', '')}" if github_token else repo_url

    # 1. 仓库准备
    if github_actions:
        # Actions 环境已通过 actions/checkout 拿到仓库，直接用工作区根目录，不再 clone
        abs_repo_path = os.path.abspath(repo_path or os.environ.get("GITHUB_WORKSPACE") or os.getcwd())
        os.makedirs(abs_repo_path, exist_ok=True)
        _log(f"[Actions] 使用已 checkout 仓库: {abs_repo_path}")
    else:
        if not os.path.exists(os.path.join(abs_repo_path, ".git")):
            _log(f"正在克隆仓库...")
            os.makedirs(abs_workspace, exist_ok=True)
            _run_git(["clone", auth_url, abs_repo_path])

    # 2. 网络检查与同步
    github_ok = False
    if github_actions:
        _log("[Actions] 跳过 git ls-remote/fetch/reset/push，由 workflow 负责提交 generated/")
    else:
        try:
            subprocess.run(["git", "ls-remote", auth_url, "HEAD"], capture_output=True, timeout=15, check=True)
            github_ok = True
        except Exception:
            _log("GitHub 当前不可达，将仅在本地处理")

        if github_ok:
            _run_git(["remote", "set-url", "origin", auth_url], cwd=abs_repo_path)
            _run_git(["fetch", "origin"], cwd=abs_repo_path)
            _run_git(["reset", "--hard", "origin/main"], cwd=abs_repo_path)

    # 3. 拉取上游
    sources = CN_TV_SOURCES[:]
    if int(enable_legacy_sources or 0) == 1:
        sources.extend(LEGACY_TXT_SOURCES)
    all_rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futures = {ex.submit(_fetch_first, s["urls"], proxy_url=proxy): s for s in sources}
        for f in concurrent.futures.as_completed(futures):
            label, text = futures[f]["label"], f.result()
            if text:
                rows = _parse_source(label, text)
                all_rows.extend(rows)
                _log(f"  [成功] {label}: 解析 {len(rows)} 条")
            else:
                _log(f"  [失败] {label}: 拉取失败")

    if not all_rows:
        return "无数据可处理"

    # 4. 预处理与探测
    by_cat = _merge_and_filter(all_rows, MAX_SOURCES_PER_CHANNEL)
    if probe_mode != "none":
        flat_sources = [(c, n, u, h) for c, chs in by_cat.items() for n, srcs in chs for u, h in srcs]
        total = len(flat_sources)
        _log(f"开始探测 {total} 个源...（并发={HTTP_PROBE_WORKERS if probe_mode == 'http' else FFPROBE_WORKERS}, 超时={HTTP_PROBE_TIMEOUT if probe_mode == 'http' else FFPROBE_TIMEOUT}s）")

        cache = _load_probe_cache()
        cache_hit = 0
        ok_set = set()
        workers = HTTP_PROBE_WORKERS if probe_mode == "http" else FFPROBE_WORKERS
        probe_fn = _http_probe if probe_mode == "http" else _ffprobe_check

        completed = 0
        dead = 0
        unknown = 0
        progress_interval = max(1, total // 20)  # 每 5% 报一次
        t0 = time.time()
        cache_updated = {}

        # 先检查 ffprobe 是否可用
        if probe_mode == "ffprobe" and not _ffprobe_available():
            return "错误：本机未检测到 ffprobe。请安装 ffmpeg 并确保 ffprobe 在 PATH 中，或改用 probe_mode='http'/'none'。"

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            fs = {ex.submit(probe_fn, u, cache): (c, n, u, h) for c, n, u, h in flat_sources}
            for f in concurrent.futures.as_completed(fs):
                item = fs[f]
                completed += 1
                try:
                    status, ok = f.result()
                except Exception:
                    status, ok = "unknown", True

                # 如果 ffprobe 返回 ok=False，说明 ffprobe 不可用（前面已预检，理论上不会触发）
                if not ok:
                    return "错误：ffprobe 不可用。请安装 ffmpeg 或改用 http/none 模式。"

                cache_updated[item[2]] = {"s": status, "t": int(time.time())}
                if status == "dead":
                    dead += 1
                elif status == "unknown":
                    unknown += 1
                    ok_set.add(item)  # fail-open 保留
                else:
                    ok_set.add(item)
                    if item[2] in cache:
                        cache_hit += 1

                if completed % progress_interval == 0 or completed == total:
                    pct = completed * 100 // total
                    elapsed = time.time() - t0
                    eta = (elapsed / completed) * (total - completed) if completed else 0
                    _log(f"  探测进度 {completed}/{total} ({pct}%) | 死链 {dead} | 保留(含未知) {len(ok_set)} | 已用 {elapsed:.0f}s | 预计剩余 {eta:.0f}s")

        cache.update(cache_updated)
        _save_probe_cache(cache)
        _log(f"  [缓存] 本次探测 {len(cache_updated)} 条，缓存命中约 {cache_hit} 条")

        # 重建结果
        new_by_cat = {"央视台": [], "卫视台": [], "地方台": []}
        for c, n, u, h in ok_set:
            found = next((x for x in new_by_cat[c] if x[0] == n), None)
            if found:
                found[1].append((u, h))
            else:
                new_by_cat[c].append([n, [(u, h)]])
        by_cat = new_by_cat

    # 5. 写文件
    gen_dir = os.path.join(abs_repo_path, "generated")
    os.makedirs(gen_dir, exist_ok=True)
    out_path = os.path.join(gen_dir, output_file)

    lines = []
    for cat in ["央视台", "卫视台", "地方台"]:
        if by_cat[cat]:
            lines.append(f"{cat},#genre#")
            for name, srcs in by_cat[cat]:
                for u, _ in srcs:
                    lines.append(f"{name},{u}")
            lines.append("")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")

    total = sum(len(s) for ch in by_cat.values() for _, s in ch)
    _log(f"已生成 {out_path}：央视台 {len(by_cat['央视台'])} / 卫视台 {len(by_cat['卫视台'])} / 地方台 {len(by_cat['地方台'])}，共 {total} 源")

    if github_actions:
        return f"[Actions] 生成完成（未推送，交由 workflow 提交）：央视台 {len(by_cat['央视台'])} / 卫视台 {len(by_cat['卫视台'])} / 地方台 {len(by_cat['地方台'])}，共 {total} 源。"

    if not github_ok:
        return f"完成：共 {total} 源。GitHub 不可达，结果保留本地。"

    # 6. 推送
    _run_git(["config", "user.name", "juying-bot"], cwd=abs_repo_path)
    _run_git(["config", "user.email", "bot@local"], cwd=abs_repo_path)
    _run_git(["add", "generated/"], cwd=abs_repo_path)

    diff = _run_git(["diff", "--cached", "--quiet"], cwd=abs_repo_path, check=False)
    if diff.returncode == 0:
        return f"完成：共 {total} 源，内容无变化。"

    _run_git(["commit", "-m", f"chore: 自动更新直播源 {datetime.datetime.now():%Y-%m-%d %H:%M}"], cwd=abs_repo_path)
    _run_git(["push", "origin", "main"], cwd=abs_repo_path, timeout=120)
    return f"完成并推送：央视台 {len(by_cat['央视台'])} / 卫视台 {len(by_cat['卫视台'])} / 地方台 {len(by_cat['地方台'])}，共 {total} 源。"


def sync_live_sources_to_github(repo_url=DEFAULT_REPO_URL,
                                 local_workspace="D:/juying_visual_workspace",
                                 github_token="",
                                 probe_mode="http",
                                 probe_timeout=8,
                                 probe_workers=16):
    """
    title: 直播源本地探测与同步（旧源 hkbiang/sdyby2006，已弃用）
    description: 兼容旧影刀调用入口。实际已合并到 sync_cn_tv_sources，默认不再处理旧源。
    """
    return sync_cn_tv_sources(
        repo_url=repo_url,
        local_workspace=local_workspace,
        github_token=github_token,
        probe_mode=probe_mode,
        output_file="iptv_cn_filtered.txt",
        enable_legacy_sources=1,
        proxy="",
        probe_timeout=probe_timeout,
        probe_workers=probe_workers,
    )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="影刀直播源本地探测同步（本地调试）")
    parser.add_argument("--repo-url", default=DEFAULT_REPO_URL, help="GitHub 仓库地址")
    parser.add_argument("--workspace", default="D:/juying_visual_workspace", help="本地工作目录")
    parser.add_argument("--token", default="", help="GitHub PAT")
    parser.add_argument("--probe", default="http", choices=["http", "ffprobe", "none"], help="探测模式")
    parser.add_argument("--output", default="iptv_cn_filtered.txt", help="输出文件名")
    parser.add_argument("--legacy", type=int, default=0, help="是否合并旧源 hkbiang/sdyby2006")
    parser.add_argument("--proxy", default="", help="HTTP 代理")
    parser.add_argument("--probe-timeout", type=int, default=8, help="单源探测超时秒")
    parser.add_argument("--probe-workers", type=int, default=16, help="探测并发数")
    parser.add_argument("--github-actions", action="store_true", help="GitHub Actions 模式：跳过 clone/push，直接写 generated/")
    parser.add_argument("--repo-path", default=None, help="Actions 模式仓库根目录（默认取 GITHUB_WORKSPACE 或 cwd）")
    args = parser.parse_args()
    msg = sync_cn_tv_sources(
        args.repo_url, args.workspace, args.token, args.probe,
        args.output, args.legacy, args.proxy, args.probe_timeout, args.probe_workers,
        github_actions=args.github_actions, repo_path=args.repo_path
    )
    print("RESULT:", msg)
