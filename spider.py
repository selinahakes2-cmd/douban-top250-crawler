import requests
from bs4 import BeautifulSoup
import pandas as pd
import time  # 新增：用于时间控制，防止被封


def get_douban_top100():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    all_movies = []

    # 优化点1：循环翻页（抓前4页，共100部电影）
    for i in range(4):
        start = i * 25
        url = f'https://movie.douban.com/top250?start={start}'
        print(f"正在抓取第 {i + 1} 页...")

        try:
            # 优化点2：加入 timeout=10，防止网络卡死时程序一直等待
            response = requests.get(url, headers=headers, timeout=10)
            response.encoding = 'utf-8'
            response.raise_for_status()  # 如果状态码不是200，会主动抛出异常

            soup = BeautifulSoup(response.text, 'html.parser')
            items = soup.find_all('div', class_='item')

            for item in items:
                # 优化点3：安全提取数据，防止网页结构改变导致程序崩溃
                title_tag = item.find('span', class_='title')
                rating_tag = item.find('span', class_='rating_num')

                if title_tag and rating_tag:
                    all_movies.append({
                        '电影名称': title_tag.text,
                        '评分': rating_tag.text
                    })

        except Exception as e:
            print(f"抓取第 {i + 1} 页时出错: {e}")

        # 优化点4：每页处理完都休眠 2 秒（成功或失败都等），防止请求过快被豆瓣封 IP
        time.sleep(2)

    # 优化点6：抓完先校验条数，抓漏了直接报错，避免用残缺数据覆盖旧表
    expected = 100
    if len(all_movies) != expected:
        raise RuntimeError(f"抓取不完整：预期 {expected} 条，实际 {len(all_movies)} 条，已跳过保存。")

    # 优化点5：保存数据（带错误处理）
    try:
        df = pd.DataFrame(all_movies)
        df.to_excel('douban_top100.xlsx', index=False)
        print(f"\n抓取完成！共抓取 {len(all_movies)} 部电影。")
        print("数据已成功保存为 douban_top100.xlsx")
    except Exception as e:
        print(f"保存文件时出错: {e}")


if __name__ == '__main__':
    get_douban_top100()