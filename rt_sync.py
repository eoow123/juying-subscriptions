# -*- coding: utf-8 -*-
"""
影刀定时任务脚本：拉取直播订阅 → 本地死链/ playable 探测 → 推送 GitHub
=====================================================================
配合「聚影TV 订阅仓库」(eoow123/juying-subscriptions) 使用。

与 GitHub Actions 版 build.py 的区别：
  - build.py：在 Actions(数据中心 IP) 跑，方案二(HTTP 探测)默认关闭，怕被上游当爬虫。
  - 本脚本：在本机(家用/办公 普通 IP)跑，探测不会被轻易误判，所以**默认开启死链探测**。
  - 本脚本只处理「单 txt 直播源」(hkbiang / sdyby2006)，复用 build.py 的解析/过滤函数，
    不重造轮子，也不会和 Actions 产物分叉。
  - **不动 iptv-org.txt**：iptv-org 有 1.4 万+ 频道，本地逐个探测不现实，仍由 Actions 生成。
  - 只更新 generated/hkbiang.txt、generated/sdyby2006.txt、generated/.probe_cache.json，
    不碰 dist/repo.json / dist/repo.b64（manifest 里 live 条目已指向这些生成文件，无需改）。

探测模式（probe_mode）：
  - "http"（默认）：用 build.py 的 HTTP Range 请求探测，速度快、误判低，适合每日定时跑。
  - "ffprobe"：调用本机 ffmpeg 的 ffprobe 读前几帧/检查流信息，属于"播放器级"实测，
    比 OpenCV 稳得多（OpenCV 不是播放器，大量 m3u8/Referer/加密流解不了会误判）。
    但 ffprobe 也慢，400+ 源 × 10 秒也要跑很久，不建议对 iptv-org 开启。
  - "none"：只做方案一（丢组播/内网代理/黑名单），不做 HTTP/播放器探测。

GitHub 网络不通时的行为：
  - 脚本会先检测 GitHub 是否可达。若不可达，仍然完成本地拉取、探测、写文件，
    但会跳过 git push，并返回明确提示 "GitHub 不可达，结果已保留在本地..."。
  - 这样不会因为网络抽风导致整段逻辑白跑，等网络恢复后再跑一次即可 push。

安全设计：
  - 生成前先 `git fetch + reset --hard origin/main`，从上游干净状态重新生成，
    既避免与 Actions 每日重建冲突（non-fast-forward 推送被拒），也保证绝不会把旧的/坏的
    本地状态推上去覆盖 Actions 正确的 iptv-org.txt / repo.json。
  - 只 `git add` 三个指定文件，绝不 `git add -A`。
  - 拉取失败 / 过滤后为空 → 跳过、保留上一次产物，绝不写空文件。
  - commit 带 `user.email/name` 兜底。
  - token 会写进该仓库 `.git/config` 的 origin URL；用完建议去 GitHub 撤销/轮换该 PAT。
"""

import os
import sys
import json
import subprocess
import datetime
import concurrent.futures
import time

# 影刀内 print 走 xbot 日志；影刀外（本地调试）回退内置 print，避免导入崩溃。
try:
    from xbot.app.logging import trace as print
except Exception:
    pass


DEFAULT_REPO_URL = "https://github.com/eoow123/juying-subscriptions.git"


def sync_live_sources_to_github(repo_url=DEFAULT_REPO_URL,
                                 local_workspace="D:/juying_visual_workspace",
                                 github_token="",
                                 probe_mode="http"):
    """
    title: 直播源本地探测与同步（支持播放器级 ffprobe 检测）
    description: 拉取直播订阅源，在本地进行死链/playable 探测，并将过滤后的结果更新并推送到 GitHub 仓库。GitHub 不可达时仍保留本地结果。
    inputs:
        - repo_url (str): GitHub 仓库地址，默认已填好，eg: "https://github.com/eoow123/juying-subscriptions.git"
        - local_workspace (folder): 本地工作目录，eg: "D:/juying_visual_workspace"
        - github_token (str): GitHub 访问令牌 (PAT)，用于克隆/推送鉴权，若本地已配置凭据可为空，eg: "ghp_xxxxxxxxxxxx"
        - probe_mode (str): 探测模式，"http"（默认，推荐）/ "ffprobe"（播放器级，慢）/ "none"（只丢组播），eg: "http"
    outputs:
        - status_msg (str): 运行结果摘要，eg: "成功处理 2/2 个源，已推送到 GitHub"
    """

    probe_mode = (probe_mode or "http").strip().lower()
    if probe_mode not in ("http", "ffprobe", "none"):
        return f"参数错误: probe_mode 必须是 http / ffprobe / none 之一，当前为 '{probe_mode}'"

    # 1. 路径与 URL 准备
    repo_name = (repo_url.rstrip("/").split("/")[-1] or "juying-subscriptions").replace(".git", "")
    abs_workspace = os.path.abspath(local_workspace)
    abs_repo_path = os.path.join(abs_workspace, repo_name)
    build_py_path = os.path.join(abs_repo_path, "build.py")

    # 带鉴权的 URL
    clean_url = repo_url.replace("https://", "")
    auth_url = f"https://{github_token}@{clean_url}" if github_token else repo_url

    def _log(msg: str):
        print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}")

    def _safe_err(e) -> str:
        s = getattr(e, "stderr", None) or getattr(e, "stdout", None) or str(e)
        return str(s).strip()[-400:]

    def _run_git(args, cwd=None, check=True, timeout=60):
        """运行 git 命令；失败时抛 subprocess.CalledProcessError。"""
        # 清理可能的 git 锁
        if cwd and os.path.exists(os.path.join(cwd, ".git", "index.lock")):
            try:
                os.remove(os.path.join(cwd, ".git", "index.lock"))
            except Exception:
                pass
        return subprocess.run(["git"] + args, cwd=cwd, check=check,
                              capture_output=True, text=True, encoding="utf-8",
                              timeout=timeout)

    def _github_reachable() -> bool:
        """快速检测 GitHub 是否可达。不用重试，避免网络不通时卡死。"""
        try:
            # 用 git ls-remote 测，同时验证鉴权是否有效
            _run_git(["ls-remote", auth_url, "HEAD"], cwd=None, timeout=15)
            return True
        except Exception as e:
            _log(f"GitHub 连通性检测失败: {_safe_err(e)[:120]}")
            return False

    def _ensure_repo() -> str:
        """确保本地有仓库。不存在/空目录 → clone；已存在且含 build.py → 复用；非空非仓库 → 报错。"""
        if os.path.exists(build_py_path):
            return "ok"
        if os.path.exists(abs_repo_path) and os.listdir(abs_repo_path):
            return f"路径 {abs_repo_path} 已存在且非空但不是仓库（缺 build.py），请换个空文件夹"
        os.makedirs(abs_workspace, exist_ok=True)
        _log(f"本地无仓库，正在 clone 到 {abs_repo_path} ...")
        try:
            _run_git(["clone", auth_url, abs_repo_path], cwd=None, timeout=120)
        except subprocess.CalledProcessError as e:
            return f"Git 克隆失败: {_safe_err(e)}"
        return "ok"

    def _preflight() -> str:
        """生成前先同步到上游最新干净状态。"""
        try:
            _run_git(["remote", "set-url", "origin", auth_url], cwd=abs_repo_path, timeout=15)
        except Exception as e:
            return f"设置远程地址失败: {_safe_err(e)}"
        try:
            _run_git(["fetch", "origin"], cwd=abs_repo_path, timeout=60)
        except subprocess.CalledProcessError as e:
            return f"Git fetch 失败（可能离线）: {_safe_err(e)}"
        try:
            _run_git(["reset", "--hard", "origin/main"], cwd=abs_repo_path, timeout=30)
        except subprocess.CalledProcessError as e:
            return f"Git reset 失败: {_safe_err(e)}"
        return "ok"

    # 2. 导入 build.py（复用解析/过滤）
    if abs_repo_path not in sys.path:
        sys.path.insert(0, abs_repo_path)
    if "build" in sys.modules:
        del sys.modules["build"]
    try:
        import build as B
    except ImportError as e:
        return f"导入仓库中的 build.py 失败: {e}"

    os.environ["LIVE_PROBE"] = "1" if probe_mode != "none" else "0"

    LIVE_SOURCES = [
        {
            "id": "hkbiang",
            "name": "hkbiang/TV 直播源（每日校验）",
            "url": "https://raw.githubusercontent.com/hkbiang/TV/master/result.txt",
            "generated_name": "hkbiang.txt",
        },
        {
            "id": "sdyby2006",
            "name": "sdyby2006/TV 直播源（每 12h 校验）",
            "url": "https://raw.githubusercontent.com/sdyby2006/TV/master/result.txt",
            "generated_name": "sdyby2006.txt",
        },
    ]

    GENERATED_DIR = getattr(B, "GENERATED_DIR", os.path.join(abs_repo_path, "generated"))
    PROBE_CACHE_FILE = getattr(B, "PROBE_CACHE_FILE", os.path.join(GENERATED_DIR, ".probe_cache.json"))

    STAGE_PATHS = [
        os.path.join(GENERATED_DIR, "hkbiang.txt"),
        os.path.join(GENERATED_DIR, "sdyby2006.txt"),
        PROBE_CACHE_FILE,
    ]

    # 3. 探测函数
    def _ffprobe_check(channel_tuple) -> tuple:
        """用 ffprobe 检测单个频道是否可播放。channel_tuple: (group, name, url)"""
        try:
            grp, name, url = channel_tuple
        except Exception:
            return None
        if not url or not url.startswith(("http", "rtmp", "rtsp")):
            return None
        try:
            cmd = [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_name",
                "-of", "default=noprint_wrappers=1",
                "-timeout", "10000000",  # 10s，微秒
                url,
            ]
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=12)
            if res.returncode == 0 and res.stdout.strip():
                return channel_tuple
        except Exception:
            pass
        return None

    def _sync_one(src: dict) -> int:
        _log(f"==> 正在拉取源 [{src['id']}]...")
        try:
            text = B.fetch_text(src["url"], timeout=120)
        except Exception as e:
            _log(f"  [错误] 拉取失败: {e}")
            return 0

        channels = B.parse_live_txt(text)
        if not channels:
            _log(f"  [警告] 解析结果为空，跳过处理")
            return 0

        before_count = len(channels)
        filtered = []

        if probe_mode == "ffprobe":
            # 先过方案一（丢组播/黑名单），再 ffprobe 实测
            pre, _ = B.filter_live_channels(channels, enable_probe=False)
            _log(f"  [方案一] {before_count} -> {len(pre)} 条（丢组播/黑名单）")
            valid = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(_ffprobe_check, ch) for ch in pre]
                for future in concurrent.futures.as_completed(futures):
                    res = future.result()
                    if res:
                        valid.append(res)
            filtered = valid
            # ffprobe 不写入 probe_cache（它不是 HTTP 探测缓存）
        else:
            # http / none：走 build.py 统一过滤（enable_probe 由 probe_mode 控制）
            filtered, cache = B.filter_live_channels(channels, enable_probe=(probe_mode == "http"))
            B.save_probe_cache(cache)

        if not filtered:
            _log(f"  [警告] 过滤后无有效频道，跳过更新")
            return 0

        out_path = os.path.join(GENERATED_DIR, src["generated_name"])
        os.makedirs(GENERATED_DIR, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(B.emit_live_txt(filtered))

        dropped = before_count - len(filtered)
        _log(f"  [成功] {before_count} -> {len(filtered)} 条（过滤掉 {dropped} 条），已保存至 {src['generated_name']}")
        return len(filtered)

    # 4. Git 提交推送
    def _commit_push(total_valid: int, github_ok: bool) -> str:
        if not github_ok:
            return "GitHub 当前不可达，已跳过 push，本地结果保留在 generated/ 目录"

        try:
            _run_git(["config", "user.name", "juying-bot"], cwd=abs_repo_path)
            _run_git(["config", "user.email", "juying-bot@local"], cwd=abs_repo_path)

            rel_paths = [os.path.relpath(p, abs_repo_path) for p in STAGE_PATHS]
            _run_git(["add", "--"] + rel_paths, cwd=abs_repo_path)

            diff = subprocess.run(["git", "-C", abs_repo_path, "diff", "--cached", "--quiet"],
                                  capture_output=True, text=True)
            if diff.returncode == 0:
                return "文件内容无变化，无需推送 GitHub"

            commit_msg = f"chore: 影刀本地探测更新直播源 {datetime.datetime.now():%Y-%m-%d %H:%M} (有效:{total_valid})"
            _run_git(["commit", "-m", commit_msg], cwd=abs_repo_path)

            if github_token:
                push_url = auth_url
            else:
                push_url = "origin"
            _run_git(["push", push_url, "main"], cwd=abs_repo_path, timeout=120)
            return "已成功推送到 GitHub"
        except subprocess.CalledProcessError as e:
            return f"Git 操作失败: {_safe_err(e)}"

    # 主流程
    _log("=== 直播源本地探测同步任务开始 ===")
    _log(f"探测模式: {probe_mode}")

    ensure = _ensure_repo()
    if ensure != "ok":
        _log(f"=== {ensure} ===")
        return ensure

    github_ok = _github_reachable()
    if github_ok:
        pre = _preflight()
        if pre != "ok":
            _log(f"=== {pre} ===")
            return pre
    else:
        _log("GitHub 不可达，将仅生成本地文件，跳过 push")

    total_valid = 0
    for src in LIVE_SOURCES:
        try:
            n = _sync_one(src)
            total_valid += n
        except Exception as e:
            _log(f"  [严重错误] 处理 {src['id']} 时发生异常: {e}")

    git_result = _commit_push(total_valid, github_ok)
    final_msg = f"处理完成: 有效源 {total_valid} 条。Git状态: {git_result}"
    _log(f"=== {final_msg} ===")
    return final_msg


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="影刀直播源本地探测同步（本地调试）")
    parser.add_argument("--repo-url", default=DEFAULT_REPO_URL, help="GitHub 仓库地址")
    parser.add_argument("--workspace", default="D:/juying_visual_workspace", help="本地工作目录")
    parser.add_argument("--token", default="", help="GitHub PAT")
    parser.add_argument("--probe", default="http", choices=["http", "ffprobe", "none"], help="探测模式")
    args = parser.parse_args()
    msg = sync_live_sources_to_github(args.repo_url, args.workspace, args.token, args.probe)
    print("RESULT:", msg)
