-- ============================================================
-- schema.sql —— 豆瓣 Top250 建表脚本（两套：SQLite / MySQL）
-- 关键点：url 上建 UNIQUE 索引，配合 INSERT IGNORE 实现幂等写入
-- ============================================================

-- ------------------------------------------------------------
-- 一、SQLite（当前默认后端，零部署）
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS movies250 (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    rank_no     INTEGER NOT NULL,          -- 排名 1~250
    title       TEXT    NOT NULL,          -- 片名
    alias       TEXT,                      -- 又名
    rating      REAL,                      -- 评分
    votes       INTEGER,                   -- 评价人数
    quote       TEXT,                      -- 一句话推荐
    url         TEXT    NOT NULL,          -- 详情页地址（唯一键）
    cover       TEXT,                      -- 封面图
    directors   TEXT,
    actors      TEXT,
    genre       TEXT,
    country     TEXT,
    year        TEXT,
    duration    TEXT,
    summary     TEXT,
    crawled_at  TEXT    NOT NULL
);

-- 唯一索引：同一部电影（同一 URL）只能存在一条
CREATE UNIQUE INDEX IF NOT EXISTS uk_movies250_url    ON movies250(url);
CREATE INDEX        IF NOT EXISTS idx_movies250_rank   ON movies250(rank_no);
CREATE INDEX        IF NOT EXISTS idx_movies250_rating ON movies250(rating);

-- 幂等写入示例（代码里用 executemany 批量执行）
-- INSERT OR IGNORE INTO movies250 (rank_no, title, ...) VALUES (?, ?, ...);


-- ------------------------------------------------------------
-- 二、MySQL 8.0（可选后端，--backend mysql）
-- ------------------------------------------------------------
-- CREATE DATABASE IF NOT EXISTS douban_top100 DEFAULT CHARSET utf8mb4;
-- USE douban_top100;
--
-- CREATE TABLE IF NOT EXISTS movies250 (
--     id          BIGINT AUTO_INCREMENT PRIMARY KEY,
--     rank_no     INT          NOT NULL,
--     title       VARCHAR(200) NOT NULL,
--     alias       VARCHAR(255),
--     rating      DECIMAL(3,1),
--     votes       INT,
--     quote       VARCHAR(255),
--     url         VARCHAR(255) NOT NULL,
--     cover       VARCHAR(255),
--     directors   VARCHAR(255),
--     actors      VARCHAR(255),
--     genre       VARCHAR(120),
--     country     VARCHAR(120),
--     year        VARCHAR(40),
--     duration    VARCHAR(60),
--     summary     TEXT,
--     crawled_at  DATETIME     NOT NULL,
--     UNIQUE KEY uk_movies250_url (url),
--     KEY idx_movies250_rank (rank_no),
--     KEY idx_movies250_rating (rating)
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
--
-- 幂等写入示例：
-- INSERT IGNORE INTO movies250 (rank_no, title, ...) VALUES (%s, %s, ...);
