# -*- coding: utf-8 -*-
"""
spider250.py —— 豆瓣电影 Top250 并发爬虫

用法：
    python spider250.py                       完整跑：列表 10 页 → 详情 250 页 → 入库
    python spider250.py --list-only           只抓列表页（不出网详情）
    python spider250.py --workers 32 --delay 0 调并发与抖动
    python spider250.py --limit 5             只处理前 5 条（调试）
    python spider250.py --bad-url             注入一个坏 URL，验证容错
    python spider250.py --backfill-details    只补库里缺详情的记录（断点续抓）
    python spider250.py --selftest-faults     离线自测重试与容错（不访问豆瓣）

工程要点：
    1. 并发：列表页 ThreadPoolExecutor(10)，详情页 ThreadPoolExecutor(workers)
    2. 去重：url 建 UNIQUE 索引 + INSERT OR IGNORE / INSERT IGNORE，重复跑恒 250 行
    3. 健壮：fake_useragent 随机 UA、超时、最多重试 3 次（指数退避）、429 单独加长退避、
             单条失败被隔离不影响整体、预检被限流则跳过详情保住基础数据
"""

import argparse
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

import douban_db

# =====================================================================
# 【板块 1】配置常量
# =====================================================================
LIST_URL = "https://movie.douban.com/top250?start={start}"
PAGE_SIZE = 25
TOTAL_PAGES = 10
TOTAL_ITEMS = PAGE_SIZE * TOTAL_PAGES          # 250

WORKERS_LIST = 10                              # 列表页：10 页各一个线程
WORKERS_DETAIL = 32                            # 详情页默认并发（可 --workers 覆盖）
TIMEOUT = 8                                    # 单请求超时（秒）
MAX_RETRY = 3                                  # 重试次数（首次之外再试 3 次）
BACKOFF = (0.6, 1.2, 2.4)                      # 普通失败退避
RATE_LIMIT_BACKOFF = (3.0, 6.0, 9.0)           # 被 429/403 时的退避（更长）

BASE_HEADERS = {
    "Referer": "https://movie.douban.com/top250",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}
FALLBACK_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

try:
    from fake_useragent import UserAgent
    _UA = UserAgent(fallback=FALLBACK_UA)
    HAS_FAKE_UA = True
except Exception:
    _UA = None
    HAS_FAKE_UA = False

_lock = threading.Lock()
STATS = {"requests": 0, "retries": 0, "failed": 0, "rate_limited": 0}


def pick_ua():
    if HAS_FAKE_UA:
        try:
            return _UA.random
        except Exception:
            pass
    return FALLBACK_UA


# =====================================================================
# 【板块 2】带重试的抓取
# =====================================================================
class FetchError(Exception):
    pass


def fetch(url, label=""):
    """取一页 HTML。失败重试 MAX_RETRY 次；429/403 用更长退避；最终失败抛 FetchError。"""
    last = None
    for attempt in range(MAX_RETRY + 1):
        headers = dict(BASE_HEADERS)
        headers["User-Agent"] = pick_ua()
        with _lock:
            STATS["requests"] += 1
        try:
            resp = requests.get(url, headers=headers, timeout=TIMEOUT)
            if resp.status_code in (403, 418, 429):
                with _lock:
                    STATS["rate_limited"] += 1
                if attempt < MAX_RETRY:
                    time.sleep(RATE_LIMIT_BACKOFF[min(attempt, len(RATE_LIMIT_BACKOFF) - 1)])
                last = FetchError("被限流 HTTP %d" % resp.status_code)
                continue
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp.text
        except FetchError:
            raise
        except Exception as exc:                       # 网络/超时/HTTP 错误
            last = exc
            if attempt < MAX_RETRY:
                with _lock:
                    STATS["retries"] += 1
                time.sleep(BACKOFF[min(attempt, len(BACKOFF) - 1)])
    with _lock:
        STATS["failed"] += 1
    raise FetchError("%s 抓取失败：%s" % (label or url, last))


# =====================================================================
# 【板块 3】阶段 A：列表页解析
# =====================================================================
def parse_list(html, page_index=0):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for item in soup.select("div.item"):
        a = item.select_one("div.hd a")
        title_tag = item.select_one("span.title")
        if not a or not title_tag:
            continue
        rating = item.select_one("span.rating_num")
        votes_tag = re.search(r"(\d+)\s*人评价", item.get_text(" "))
        quote = item.select_one("span.inq") or item.select_one("p.quote span")
        cover = item.select_one("div.pic img")
        rank_tag = item.select_one("div.pic em")
        rank = None
        if rank_tag and rank_tag.text.strip().isdigit():
            rank = int(rank_tag.text.strip())
        out.append({
            "rank_no": rank if rank else page_index * PAGE_SIZE + len(out) + 1,
            "title": title_tag.text.strip(),
            "alias": None,
            "rating": float(rating.text) if rating else None,
            "votes": int(votes_tag.group(1)) if votes_tag else None,
            "quote": quote.text.strip() if quote else None,
            "url": a["href"].strip(),
            "cover": cover["src"].strip() if cover else None,
            "directors": None, "actors": None, "genre": None, "country": None,
            "year": None, "duration": None, "summary": None,
            "crawled_at": douban_db.now_str(),
        })
    return out


def crawl_list():
    """10 页并发；某页失败只少几条，不中断。"""
    tasks = [(i, LIST_URL.format(start=i * PAGE_SIZE)) for i in range(TOTAL_PAGES)]
    movies, errors = [], []

    def work(pair):
        idx, url = pair
        try:
            return parse_list(fetch(url, "第 %d 页" % (idx + 1)), idx), None
        except Exception as exc:
            return [], "第 %d 页：%s" % (idx + 1, exc)

    with ThreadPoolExecutor(max_workers=WORKERS_LIST) as pool:
        for fut in as_completed([pool.submit(work, t) for t in tasks]):
            rows, err = fut.result()
            movies.extend(rows)
            if err:
                errors.append(err)
    movies.sort(key=lambda m: m["rank_no"])
    return movies, errors


# =====================================================================
# 【板块 4】阶段 B：详情页解析
# =====================================================================
INFO_KEYS = {
    "导演": "directors", "编剧": "writers", "主演": "actors",
    "类型": "genre", "制片国家/地区": "country",
    "语言": "language", "上映日期": "year", "片长": "duration",
    "又名": "alias",
}


def parse_detail(html):
    soup = BeautifulSoup(html, "html.parser")
    data = {}
    info = soup.select_one("#info")
    if info:
        text = info.get_text("\n")
        for line in text.split("\n"):
            line = line.strip()
            if ":" not in line:
                continue
            key, val = line.split(":", 1)
            field = INFO_KEYS.get(key.strip())
            if field:
                data[field] = re.sub(r"\s+", " ", val).strip()
        if data.get("year"):
            m = re.search(r"(19|20)\d{2}", data["year"])
            data["year"] = m.group(0) if m else data["year"]

    summary = soup.select_one('span[property="v:summary"]') or soup.select_one("#link-report span")
    if summary:
        text = re.sub(r"\s+", " ", summary.get_text(" ")).strip()
        data["summary"] = text[:300] if text else None
    return data


def crawl_details(movies, bad_url=False, workers=WORKERS_DETAIL, delay=0.0, limit=0):
    """详情页并发；单条失败只丢详情字段，基础数据仍在。"""
    targets = list(movies)
    if limit and limit < len(targets):
        targets = targets[:limit]
    if bad_url:
        targets = targets[:1] + [{"url": "https://movie.douban.com/subject/9999999999/",
                                  "title": "__故意写错的URL__"}]

    def work(movie):
        try:
            if delay > 0:
                time.sleep(random.uniform(0, delay))
            return movie["url"], parse_detail(fetch(movie["url"], "详情 %s" % movie.get("title", ""))), None
        except Exception as exc:
            return movie["url"], {}, str(exc)

    detail_map, errors = {}, []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for fut in as_completed([pool.submit(work, m) for m in targets]):
            url, data, err = fut.result()
            if data:
                detail_map[url] = data
            if err:
                errors.append(err)

    for m in movies:
        extra = detail_map.get(m["url"])
        if extra:
            for k, v in extra.items():
                if v and k in m:
                    m[k] = v
    return errors


# =====================================================================
# 【板块 5】离线容错自测
# =====================================================================
def selftest_faults():
    print("=" * 62)
    print("离线容错自测（不访问豆瓣）")
    print("=" * 62)
    ok = True

    print("\n[1] 抓不可达地址（预期重试 %d 次后抛 FetchError）" % MAX_RETRY)
    t0 = time.time()
    before = dict(STATS)
    try:
        fetch("http://127.0.0.1:9/", "不可达地址")
        print("    ✘ 竟然成功")
        ok = False
    except FetchError as exc:
        cost = time.time() - t0
        retried = STATS["retries"] - before["retries"]
        print("    ✔ 抛出 FetchError（%.1f 秒，重试 %d 次）：%s" % (cost, retried, str(exc)[-50:]))
        if retried != MAX_RETRY:
            print("    ✘ 重试次数不符")
            ok = False

    print("\n[2] 解析空页面（不应抛异常）")
    try:
        n = len(parse_list("<html></html>", 0))
        d = parse_detail("<html></html>")
        print("    ✔ 列表 %d 条，详情字段 %d 个，未崩溃" % (n, len(d)))
        if n != 0:
            ok = False
    except Exception as exc:
        print("    ✘ 异常：%s" % exc)
        ok = False

    print("\n[3] 3 个坏任务混批（单条失败被隔离，主流程继续）")
    caught = 0
    for u in ["http://127.0.0.1:9/a", "http://127.0.0.1:9/b", "http://127.0.0.1:9/c"]:
        try:
            fetch(u, "批量")
        except FetchError:
            caught += 1
    print("    ✔ 捕获 %d/3，未中断" % caught)
    if caught != 3:
        ok = False

    print("\n" + "-" * 62)
    print("自测结果：%s" % ("全部通过 ✔" if ok else "有失败项 ✘"))
    print("-" * 62)
    return 0 if ok else 1


# =====================================================================
# 【板块 6】主流程
# =====================================================================
def main():
    ap = argparse.ArgumentParser(description="豆瓣 Top250 并发爬虫")
    ap.add_argument("--list-only", action="store_true", help="只抓列表页")
    ap.add_argument("--no-detail", action="store_true", help="跳过详情页")
    ap.add_argument("--bad-url", action="store_true", help="注入坏 URL 验证容错")
    ap.add_argument("--backend", default="sqlite", choices=["sqlite", "mysql"])
    ap.add_argument("--workers", type=int, default=WORKERS_DETAIL, help="详情页并发数")
    ap.add_argument("--delay", type=float, default=0.0, help="详情请求随机抖动上限（秒）")
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 条")
    ap.add_argument("--backfill-details", action="store_true", help="只补库里缺详情的记录")
    ap.add_argument("--selftest-faults", action="store_true", help="离线自测重试与容错")
    args = ap.parse_args()

    if args.selftest_faults:
        return selftest_faults()

    # ---------------- 断点续抓 ----------------
    if args.backfill_details:
        store = douban_db.Store(backend=args.backend).connect().init_schema()
        rows = store.pending_details(limit=args.limit)
        print("\n[补详情] 库里还有 %d 条缺详情" % len(rows))
        if not rows:
            print("    无需补全")
            store.close()
            return 0
        jobs = [{"url": u, "title": t, "rank_no": r} for u, t, r in rows]
        t0 = time.time()
        errs = crawl_details(jobs, workers=args.workers, delay=args.delay)
        t_detail = time.time() - t0
        ok = sum(1 for m in jobs if m.get("summary"))
        payload = [(m.get("summary"), m.get("directors"), m.get("actors"), m.get("genre"),
                    m.get("country"), m.get("year"), m.get("duration"), m.get("alias"), m["url"])
                   for m in jobs if m.get("summary")]
        store.update_details(payload)
        left = len(store.pending_details())
        total = store.count()
        store.close()
        print("    成功 %d / %d，失败 %d，耗时 %.2f 秒" % (ok, len(jobs), len(errs), t_detail))
        print("    仍缺详情 %d 条；库内共 %d 行" % (left, total))
        return 0 if left == 0 else 1

    t_all = time.time()
    print("=" * 62)
    print("豆瓣 Top250 并发爬虫　UA随机=%s　详情并发=%d"
          % ("开" if HAS_FAKE_UA else "关", args.workers))
    print("=" * 62)

    # ---------------- 阶段 A ----------------
    t0 = time.time()
    print("\n[阶段 A] 列表页并发（10 页 / 并发 %d）..." % WORKERS_LIST)
    movies, list_errors = crawl_list()
    t_list = time.time() - t0
    print("    拿到 %d 条，耗时 %.2f 秒，失败页 %d" % (len(movies), t_list, len(list_errors)))
    for e in list_errors:
        print("    ! " + e)
    if not movies:
        print("\n列表页全部失败，退出（数据库未改动）")
        return 2

    # ---------------- 阶段 B ----------------
    t_detail, detail_errors = 0.0, []
    if not (args.list_only or args.no_detail):
        skip_detail = False
        try:
            parse_detail(fetch(movies[0]["url"], "预检"))
            print("\n[阶段 B] 预检通过")
        except Exception as exc:
            msg = str(exc)
            if any(code in msg for code in ("429", "403", "418")):
                print("\n[阶段 B] 预检被限流（%s），跳过详情页" % msg)
                print("    基础字段（排名/片名/评分/评价数/引言/封面/链接）阶段 A 已取全")
                print("    稍后续抓：python spider250.py --backfill-details --workers 6 --delay 1.0")
                skip_detail = True
            else:
                print("\n[阶段 B] 预检失败（%s），仍继续尝试" % msg)

        if not skip_detail:
            t0 = time.time()
            print("\n[阶段 B] 详情页并发（%d 个 / 并发 %d / 抖动 %.2fs）..."
                  % (len(movies), args.workers, args.delay))
            detail_errors = crawl_details(movies, bad_url=args.bad_url,
                                          workers=args.workers, delay=args.delay, limit=args.limit)
            t_detail = time.time() - t0
            ok = sum(1 for m in movies if m.get("summary"))
            print("    详情补全 %d/%d，耗时 %.2f 秒，失败 %d"
                  % (ok, len(movies), t_detail, len(detail_errors)))
            for e in detail_errors[:5]:
                print("    ! " + e)

    # ---------------- 阶段 C ----------------
    t0 = time.time()
    print("\n[阶段 C] 批量入库（executemany + 唯一索引 + INSERT OR IGNORE）...")
    store = douban_db.Store(backend=args.backend).connect().init_schema()
    exec_rows, new_rows = store.save_movies(movies)
    total = store.count()
    st = store.stats()
    t_db = time.time() - t0
    print("    提交 %d 行，新增 %d 行，库内共 %d 行，耗时 %.3f 秒"
          % (exec_rows, new_rows, total, t_db))
    print("    排名 %s ~ %s，平均分 %s" % (st["rank_min"], st["rank_max"], st["avg_rating"]))
    for row in store.top(3):
        print("    第 %d 名  %s  %.1f" % (row[0], row[1], row[2] or 0))
    store.close()

    # ---------------- 汇总 ----------------
    total_time = time.time() - t_all
    print("\n" + "-" * 62)
    print("总耗时 %.2f 秒（列表 %.2f + 详情 %.2f + 入库 %.2f）"
          % (total_time, t_list, t_detail, t_db))
    print("请求 %d 次，重试 %d 次，被限流 %d 次，最终失败 %d"
          % (STATS["requests"], STATS["retries"], STATS["rate_limited"], STATS["failed"]))
    print("库内行数 %d（目标 %d）%s" % (total, TOTAL_ITEMS, "✔" if total == TOTAL_ITEMS else "✘"))
    print("-" * 62)
    return 0 if total == TOTAL_ITEMS else 1


if __name__ == "__main__":
    sys.exit(main())
