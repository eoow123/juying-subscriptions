#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
聚影TV 采集状态邮件通知（纯标准库，SMTP over SSL）
====================================================
职责：读取 build.py / build_sources.py 生成的统计文件（generated/stats_*.json），
汇总「采集前 → 过滤后」的数字，发到用户邮箱，让用户直观看到软件工作状态。

触发：GitHub Actions 每日构建末尾调用（build.py / build_sources.py 之后）。

环境变量（在 GitHub 仓库 Settings→Secrets 配置）：
    MAIL_TO        收件邮箱（默认 1361916124@qq.com）
    MAIL_SMTP_HOST SMTP 服务器（默认 smtp.qq.com）
    MAIL_SMTP_PORT SSL 端口（默认 465）
    MAIL_USER      发件邮箱账号（如 xxx@qq.com）—— 必填
    MAIL_PASS      发件邮箱 SMTP 授权码（QQ邮箱在 设置→账户 生成）—— 必填
    MAIL_FROM      发件显示地址（默认 = MAIL_USER）

未配置 MAIL_USER/MAIL_PASS 时：只打印统计、跳过发送（不报错，不阻断构建）。
"""

import json
import os
import smtplib
import ssl
import sys
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
GEN_DIR = os.path.join(REPO_ROOT, "generated")


def _load(name):
    try:
        with open(os.path.join(GEN_DIR, name), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def build_report():
    b = _load("stats_build.json")     # 订阅仓库总构建
    s = _load("stats_sources.json")   # 采集站汇总过滤
    p = _load("stats_parsers.json")   # 解析端口采集

    bj = datetime.now(timezone.utc) + timedelta(hours=8)
    date_str = bj.strftime("%Y-%m-%d %H:%M")

    lines = []
    lines.append("聚影TV 服务端采集状态报告")
    lines.append(f"生成时间（北京）：{date_str}")
    lines.append("")
    lines.append("【采集站汇总过滤】（build_sources.py）")
    if s:
        lines.append(f"  · 采集前(ziyuanzu)：{s.get('zyz', '-')} 个")
        lines.append(f"  · 采集前(上游GitHub)：{s.get('upstream', '-')} 个")
        lines.append(f"  · 合并去重后：{s.get('merged', '-')} 个")
        lines.append(f"  · NSFW过滤后：{s.get('after_nsfw_filter', '-')} 个")
        lines.append(f"  · 搜索关键词命中(存活)：{s.get('alive', '-')} 个")
        lines.append(f"  · 最终发布采集站：{s.get('published_sites', '-')} 个")
    else:
        lines.append("  （无 stats_sources.json，采集站脚本可能未运行）")
    lines.append("")
    lines.append("【解析端口采集】（build_parsers.py）")
    if p:
        lines.append(f"  · 种子站点：{p.get('seeds', '-')} 个")
        lines.append(f"  · Bing 国际版发现：{p.get('engine_bing_found', '-')} 个候选站")
        lines.append(f"  · 搜狗发现（服务器海外IP可能被反爬拦截为0）：{p.get('engine_sogou_found', '-')} 个候选站")
        lines.append(f"  · Google 国际版发现（服务器可达时为补充）：{p.get('engine_google_found', '-')} 个候选站")
        lines.append(f"  · 实际抓取站点：{p.get('sites_scraped', '-')} 个")
        lines.append(f"  · 合并去重后端口：{p.get('merged_ports', '-')} 个")
        lines.append(f"  · 去掉死链：{p.get('dead_removed', '-')} 个")
        lines.append(f"  · 最终发布端口：{p.get('published_ports', '-')} 个（= 去死链后）")
        per = p.get("per_site_counts") or {}
        if per:
            lines.append("  · 各网站端口数（按数量降序）：")
            for site, cnt in sorted(per.items(), key=lambda kv: -kv[1]):
                tag = site if len(site) <= 64 else site[:61] + "..."
                lines.append(f"      - {tag}: {cnt}")
    else:
        lines.append("  （无 stats_parsers.json，解析端口脚本可能未运行）")
    lines.append("")
    lines.append("【订阅仓库构建】（build.py）")
    if b:
        lines.append(f"  · 总条目：{b.get('total_items', '-')}")
        tc = b.get("type_count", {})
        if isinstance(tc, dict) and tc:
            lines.append("  · 分类型：" + "，".join(f"{k}={v}" for k, v in tc.items()))
        ps = b.get("per_source", {})
        if isinstance(ps, dict) and ps:
            lines.append("  · 各源条目数：")
            for k, v in ps.items():
                lines.append(f"      - {k}: {v}")
    else:
        lines.append("  （无 stats_build.json，构建脚本可能未运行）")
    lines.append("")
    lines.append("—— 此邮件由 GitHub Actions 自动发送，用于监控软件采集工作状态。")
    return date_str, "\n".join(lines)


def write_status_file(date_str, body):
    """把报告写入仓库根目录 STATUS.md（GitHub 每日构建后 push，用户可直接看网址）。
    即使邮件未配置也会写，保证「GitHub 网址每天刷新更新情况」这条通道始终可用。"""
    try:
        md = []
        md.append("# 聚影TV 服务端采集状态")
        md.append("")
        md.append(f"> 最近更新（北京时间）：**{date_str}**  ")
        md.append("> 本页由 GitHub Actions 每日自动重建后刷新。")
        md.append("")
        md.append("```")
        md.append(body)
        md.append("```")
        with open(os.path.join(REPO_ROOT, "STATUS.md"), "w", encoding="utf-8") as f:
            f.write("\n".join(md) + "\n")
        print("\n[OK] 已写入 STATUS.md（GitHub 网址每日刷新此页）")
    except Exception as e:  # noqa: BLE001
        print(f"\n[WARN] 写 STATUS.md 失败：{e}", file=sys.stderr)


def main():
    date_str, body = build_report()
    print(body)

    # 无论邮件是否配置，都先落地 STATUS.md（GitHub 网址通道）
    write_status_file(date_str, body)

    to_addr = (os.environ.get("MAIL_TO") or "1361916124@qq.com").strip()
    host = (os.environ.get("MAIL_SMTP_HOST") or "smtp.qq.com").strip()
    port = int((os.environ.get("MAIL_SMTP_PORT") or "465").strip() or "465")
    user = (os.environ.get("MAIL_USER") or "").strip()
    passwd = (os.environ.get("MAIL_PASS") or "").strip()
    from_addr = (os.environ.get("MAIL_FROM") or user).strip()

    if not user or not passwd:
        print("\n[SKIP] 未配置 MAIL_USER / MAIL_PASS，跳过邮件发送（不阻断构建）。")
        return 0

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = Header(f"聚影TV 采集状态 {date_str}", "utf-8")
    msg["From"] = formataddr((str(Header("聚影TV 采集监控", "utf-8")), from_addr))
    msg["To"] = to_addr

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(host, port, context=ctx, timeout=30) as smtp:
            smtp.login(user, passwd)
            smtp.sendmail(from_addr, [to_addr], msg.as_string())
        print(f"\n[OK] 邮件已发送到 {to_addr}")
        return 0
    except Exception as e:
        print(f"\n[ERROR] 邮件发送失败（不阻断构建）：{e}", file=sys.stderr)
        return 0  # 不因邮件失败让整个 Actions 失败


if __name__ == "__main__":
    sys.exit(main())
