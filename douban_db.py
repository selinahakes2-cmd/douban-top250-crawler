# -*- coding: utf-8 -*-
"""
douban_db.py —— 存储层（SQLite 默认 / MySQL 可选）

# =====================================================================
# 【板块 1】设计要点
#   1. 唯一索引：url 上建 UNIQUE，保证同一部电影只可能有一条记录
#   2. 幂等写：SQLite 用 INSERT OR IGNORE，MySQL 用 INSERT IGNORE
#      —— 重复运行不会报错、不会重复插入，count 恒为 250
#   3. 批量写：executemany 一次提交，不做 for 循环单条 INSERT
# =====================================================================
"""

import os
import sqlite3
import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "douban_top250.db")

# 电影表的字段顺序（入库元组必须按这个顺序排）
COLUMNS = [
    "rank_no", "title", "alias", "rating", "votes", "quote", "url", "cover",
    "directors", "actors", "genre", "country", "year", "duration", "summary",
    "crawled_at",
]

SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS movies250 (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    rank_no     INTEGER NOT NULL,
    title       TEXT    NOT NULL,
    alias       TEXT,
    rating      REAL,
    votes       INTEGER,
    quote       TEXT,
    url         TEXT    NOT NULL,
    cover       TEXT,
    directors   TEXT,
    actors      TEXT,
    genre       TEXT,
    country     TEXT,
    year        TEXT,
    duration    TEXT,
    summary     TEXT,
    crawled_at  TEXT    NOT NULL
);
"""

SQLITE_INDEXES = [
    "CREATE UNIQUE INDEX IF NOT EXISTS uk_movies250_url  ON movies250(url);",
    "CREATE INDEX        IF NOT EXISTS idx_movies250_rank ON movies250(rank_no);",
    "CREATE INDEX        IF NOT EXISTS idx_movies250_rating ON movies250(rating);",
]

MYSQL_DDL = """
CREATE TABLE IF NOT EXISTS movies250 (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    rank_no     INT          NOT NULL,
    title       VARCHAR(200) NOT NULL,
    alias       VARCHAR(255),
    rating      DECIMAL(3,1),
    votes       INT,
    quote       VARCHAR(255),
    url         VARCHAR(255) NOT NULL,
    cover       VARCHAR(255),
    directors   VARCHAR(255),
    actors      VARCHAR(255),
    genre       VARCHAR(120),
    country     VARCHAR(120),
    year        VARCHAR(40),
    duration    VARCHAR(60),
    summary     TEXT,
    crawled_at  DATETIME     NOT NULL,
    UNIQUE KEY uk_movies250_url (url),
    KEY idx_movies250_rank (rank_no),
    KEY idx_movies250_rating (rating)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


class Store:
    """统一入口：Store(backend='sqlite' | 'mysql')。"""

    def __init__(self, backend="sqlite", mysql_cfg=None):
        self.backend = backend
        self.mysql_cfg = mysql_cfg or {}
        self.conn = None

    # ---------------- 连接 ----------------
    def connect(self):
        if self.backend == "sqlite":
            self.conn = sqlite3.connect(DB_PATH)
            self.conn.execute("PRAGMA journal_mode=WAL;")
        else:
            import pymysql
            cfg = dict(host="127.0.0.1", port=3306, user="douban",
                       password=os.environ.get("DOUBAN_DB_PASS", "douban123456"),
                       database="douban_top100", charset="utf8mb4")
            cfg.update(self.mysql_cfg)
            self.conn = pymysql.connect(**cfg)
        return self

    def close(self):
        if self.conn:
            self.conn.close()

    # ---------------- 建表 ----------------
    def init_schema(self):
        cur = self.conn.cursor()
        if self.backend == "sqlite":
            cur.executescript(SQLITE_DDL)
            for stmt in SQLITE_INDEXES:
                cur.execute(stmt)
        else:
            cur.execute(MYSQL_DDL)
        self.conn.commit()
        return self

    # ---------------- 批量写入 ----------------
    def save_movies(self, movies, batch_size=250):
        """movies: list[dict]（键见 COLUMNS）。返回 (执行行数, 实际新增行数)。"""
        if not movies:
            return 0, 0

        before = self.count()

        # 按 url 去重（列表页理论上不重复，这里兜底）
        seen, uniq = set(), []
        for m in movies:
            if m["url"] in seen:
                continue
            seen.add(m["url"])
            uniq.append(m)

        ph = "?" if self.backend == "sqlite" else "%s"
        verb = "INSERT OR IGNORE" if self.backend == "sqlite" else "INSERT IGNORE"
        sql = "%s INTO movies250 (%s) VALUES (%s)" % (
            verb,
            ", ".join(COLUMNS),
            ", ".join([ph] * len(COLUMNS)),
        )

        rows = [tuple(m.get(c) for c in COLUMNS) for m in uniq]
        cur = self.conn.cursor()
        total = 0
        try:
            for i in range(0, len(rows), batch_size):
                chunk = rows[i:i + batch_size]
                cur.executemany(sql, chunk)
                self.conn.commit()
                total += len(chunk)
        except Exception:
            self.conn.rollback()
            raise
        rows_now = self.count()
        return total, rows_now - before


    # ---------------- 详情补全（断点续抓用） ----------------
    def pending_details(self, limit=0):
        """返回 (url, title, rank_no) 还没补全详情的记录。"""
        cur = self.conn.cursor()
        ph = "?" if self.backend == "sqlite" else "%s"
        sql = ("SELECT url, title, rank_no FROM movies250 "
               "WHERE summary IS NULL OR summary = '' ORDER BY rank_no")
        if limit:
            sql += " LIMIT " + str(int(limit))
        cur.execute(sql)
        return cur.fetchall()

    def missing_detail_urls(self):
        """返回还没有简介的 url 列表（用于断点续抓）。"""
        cur = self.conn.cursor()
        cur.execute("SELECT url FROM movies250 WHERE summary IS NULL OR summary = '' ORDER BY rank_no")
        return [r[0] for r in cur.fetchall()]

    def update_details(self, rows, batch_size=100):
        """rows: list[(summary, directors, actors, genre, country, year, duration, alias, url)]"""
        if not rows:
            return 0
        ph = "?" if self.backend == "sqlite" else "%s"
        sql = ("UPDATE movies250 SET summary=%s, directors=%s, actors=%s, genre=%s, "
               "country=%s, year=%s, duration=%s, alias=%s WHERE url=%s") % ((ph,) * 9)
        cur = self.conn.cursor()
        try:
            for i in range(0, len(rows), batch_size):
                cur.executemany(sql, rows[i:i + batch_size])
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return len(rows)
    # ---------------- 查询 ----------------
    def count(self):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM movies250")
        return cur.fetchone()[0]

    def stats(self):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*), MIN(rank_no), MAX(rank_no), ROUND(AVG(rating),2) FROM movies250")
        n, lo, hi, avg = cur.fetchone()
        return {"count": n, "rank_min": lo, "rank_max": hi, "avg_rating": avg}

    def top(self, n=5):
        cur = self.conn.cursor()
        cur.execute("SELECT rank_no, title, rating FROM movies250 ORDER BY rank_no LIMIT %s" %
                    ("?" if self.backend == "sqlite" else "%s"), (n,))
        return cur.fetchall()


def now_str():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
