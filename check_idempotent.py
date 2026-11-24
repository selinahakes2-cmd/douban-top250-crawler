# -*- coding: utf-8 -*-
"""离线验证：重复入库幂等性 + 唯一索引是否真实生效（不访问豆瓣）。"""
import time
import douban_db

s = douban_db.Store().connect().init_schema()
print("初始行数:", s.count())

cur = s.conn.cursor()
cur.execute("SELECT " + ",".join(douban_db.COLUMNS) + " FROM movies250 ORDER BY rank_no")
rows = [dict(zip(douban_db.COLUMNS, r)) for r in cur.fetchall()]
print("从库里读回 %d 条，模拟重复运行 3 次：" % len(rows))

for i in range(3):
    t0 = time.time()
    exec_rows, new_rows = s.save_movies(rows)
    print("  第%d次: 提交 %d 行 | 新增 %d 行 | 库内 %d 行 | %.4f 秒"
          % (i + 1, exec_rows, new_rows, s.count(), time.time() - t0))

cur.execute("SELECT url FROM movies250 LIMIT 1")
dup = cur.fetchone()[0]
cur.execute("INSERT OR IGNORE INTO movies250 (rank_no,title,url,crawled_at) VALUES (?,?,?,?)",
            (999, "重复测试", dup, douban_db.now_str()))
s.conn.commit()
print("手工插入重复 URL 后行数:", s.count())

cur.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='movies250'")
for name, sql in cur.fetchall():
    print("  索引:", name, "|", (sql or "").replace("\n", " ")[:90])

print("统计:", s.stats())
s.close()
