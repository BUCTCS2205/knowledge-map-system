import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'
}

base_url = "https://www.metmuseum.org"

# 通用的获取页面HTML的函数，如果网页请求失败，自动尝试最多5次，提升稳定性。
def fetch_html(url, max_retries=5, timeout=20):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"请求失败（第{attempt}次）: {e}")
            if attempt == max_retries:
                print(f"多次失败，跳过该URL: {url}")
                return None
            time.sleep(2)  # 每次失败后休息2秒

# 解析搜索结果页面
def parse_search_results(html):
    soup = BeautifulSoup(html, 'html.parser')
    artworks = []

    # 找到所有单个文物元素
    items = soup.find_all('figure', class_='collection-object_collectionObject__SuPct')

    for item in items:
        title_tag = item.find('a', class_='collection-object_link__qM3YR')
        title = title_tag.get_text(strip=True) if title_tag else "无标题"

        artist_tag = item.find('div', class_='collection-object_culture__BaSXn')
        artist = artist_tag.get_text(strip=True) if artist_tag else "未知"

        relative_url = title_tag['href'] if title_tag and 'href' in title_tag.attrs else ""
        full_url = urljoin(base_url, relative_url)

        img_tag = item.find('img', class_='collection-object_image__XVQPm')
        img_url = img_tag['src'] if img_tag and 'src' in img_tag.attrs else ""

        artworks.append({
            '标题': title,
            '艺术家': artist,
            '链接': full_url,
            '图片URL': img_url
        })

    return artworks

# 解析文物详情页面
def parse_artwork_details(html):
    soup = BeautifulSoup(html, 'html.parser')

    details = {
        '文化背景': '未知文化',
        '年代': '未知年代',
        '材质': '未知材质',
        '尺寸': '未知尺寸',
        '分类': '未知分类',
        '描述': '无描述'
    }

    desc_div = soup.find('div', class_='artwork__intro__desc js-artwork__intro__desc')
    if desc_div:
        paragraphs = desc_div.find_all('p')
        description = ' '.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        details['描述'] = ' '.join(description.split())

    overview_section = soup.find('section', id='overview')
    if overview_section:
        info_paragraphs = overview_section.find_all('p')

        for p in info_paragraphs:
            text = p.get_text(strip=True)

            if 'Date:' in text:
                details['年代'] = text.replace('Date:', '').strip()
            elif 'Culture:' in text:
                details['文化背景'] = text.replace('Culture:', '').strip()
            elif 'Medium:' in text:
                details['材质'] = text.replace('Medium:', '').strip()
            elif 'Dimensions:' in text:
                details['尺寸'] = text.replace('Dimensions:', '').strip()
            elif 'Classification:' in text:
                details['分类'] = text.replace('Classification:', '').strip()

    return details

# 单个文物处理函数
def process_artwork(artwork):
    detail_html = fetch_html(artwork['链接'])
    if not detail_html:
        print(f"跳过文物：{artwork['标题']}")
        return None

    artwork_details = parse_artwork_details(detail_html)

    full_info = {
        '标题': artwork['标题'],
        '艺术家': artwork['艺术家'],
        '文化背景': artwork_details['文化背景'],
        '年代': artwork_details['年代'],
        '材质': artwork_details['材质'],
        '尺寸': artwork_details['尺寸'],
        '分类': artwork_details['分类'],
        '描述': artwork_details['描述'],
        '图片URL': artwork['图片URL'],
        '链接': artwork['链接']
    }
    return full_info

# 主程序
def main():
    all_artworks = []
    offset = 0
    page_size = 40
    max_workers = 8

    while True:
        search_url = f"https://www.metmuseum.org/art/collection/search?q=chinese&geolocation=China&offset={offset}"
        print(f"\n正在抓取 offset={offset} 的搜索结果页面...")
        search_html = fetch_html(search_url)
        if not search_html:
            print("无法获取搜索结果，停止。")
            break

        artworks = parse_search_results(search_html)
        print(f"本页找到 {len(artworks)} 个文物。")

        if not artworks:
            print("没有更多文物，抓取完成。")
            break

        # 多线程处理当前页所有文物
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_artwork = {executor.submit(process_artwork, artwork): artwork for artwork in artworks}
            for future in as_completed(future_to_artwork):
                try:
                    result = future.result()
                    if result:
                        all_artworks.append(result)
                except Exception as exc:
                    print(f"处理文物时出错: {exc}")

        offset += page_size
        time.sleep(2)

    if all_artworks:
        columns = ['标题', '艺术家', '文化背景', '年代', '材质', '尺寸', '分类', '描述', '图片URL', '链接']
        df = pd.DataFrame(all_artworks)

        for col in columns:
            if col not in df.columns:
                df[col] = ''

        df = df[columns]

        df.to_csv('met_chinese_artworks1.csv', index=False, encoding='utf-8-sig')
        print("\n全部数据已保存为 met_chinese_artworks1.csv")
        print("\n示例数据:")
        print(df.head())
    else:
        print("没有获取到有效数据。")

if __name__ == "__main__":
    main()
