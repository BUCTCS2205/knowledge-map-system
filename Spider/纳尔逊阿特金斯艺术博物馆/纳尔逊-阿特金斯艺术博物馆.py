import requests
from lxml import etree
import pandas as pd
from openai import OpenAI
import os
import time

MAX_RETRIES = 3
TIMEOUT = 10

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}


def model(url):
    for retry in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=TIMEOUT)
            response.raise_for_status()
            page_content = response.text
            client = OpenAI(
                base_url="https://ark.cn-beijing.volces.com/api/v3/",
                api_key="xxxxxxxxxxxxxxxx"
            )
            completion = client.chat.completions.create(
                model="xxxxxxxxxxxx",
                messages=[
                    {"role": "system",
                     "content": "作为文物领域的权威专家，请依据给定的网页内容，精准判断其中提及的文物是否属于中国文物。仅需用 “yes” 或 “no” 作答，无需阐述依据。"},
                    {"role": "user", "content": page_content},
                ],
            )
            return completion.choices[0].message.content
        except requests.RequestException as e:
            print(f"请求 {url} 失败，重试次数: {retry + 1}/{MAX_RETRIES}，错误信息: {e}")
            if retry < MAX_RETRIES - 1:
                time.sleep(2)
    return None


def get_information(url):
    for retry in range(MAX_RETRIES):
        try:
            detail_response = requests.get(url, headers=headers, timeout=TIMEOUT)
            if detail_response.status_code == 200:
                is_china = model(url)
                if is_china and 'yes' in is_china:
                    print(is_china)
                    detail_html = etree.HTML(detail_response.text)
                    name = detail_html.xpath('//div[@class="detailField titleField"]/h1/text()')
                    name = name[0] if name else ''

                    date = detail_html.xpath('//div[@class="detailField displayDateField"]/span[contains(@class, "detailFieldValue")]/text()')
                    date = date[0] if date else ''

                    medium = detail_html.xpath('//div[@class="detailField mediumField"]/span[contains(@class, "detailFieldValue")]/text()')
                    medium = medium[0] if medium else ''

                    dimensions = detail_html.xpath('//div[@class="detailField dimensionsField"]/span[contains(@class, "detailFieldValue")]/div/text()')
                    dimensions = dimensions[0] if dimensions else ''

                    credit_line = detail_html.xpath('//div[@class="detailField creditlineField"]/span[contains(@class, "detailFieldValue")]/text()')
                    credit_line = credit_line[0] if credit_line else ''

                    object_number = detail_html.xpath('//div[@class="detailField invnoField"]/span[contains(@class, "detailFieldValue")]/text()')
                    object_number = object_number[0] if object_number else ''

                    on_view = detail_html.xpath('//div[@class="detailField onviewField"]/div/text()')
                    on_view = on_view[0] if on_view else ''

                    part_url = detail_html.xpath('//div[contains(@class, "emuseum-img-wrap")]/img/@src')
                    part_url = part_url[0] if part_url else ''
                    img_url = base_url + part_url if part_url else ''

                    artifacts_info.append({
                        '文物名字': name,
                        '年代': date,
                        '材质': medium,
                        '尺寸': dimensions,
                        '入藏信息': credit_line,
                        '藏品编号': object_number,
                        '当前是否展出': on_view,
                        '详情页面URL': url,
                        '文物图片链接': img_url
                    })
            else:
                print(f"页面 {url} 访问失败，状态码: {detail_response.status_code}")
            break
        except requests.RequestException as e:
            print(f"请求 {url} 失败，重试次数: {retry + 1}/{MAX_RETRIES}，错误信息: {e}")
            if retry < MAX_RETRIES - 1:
                time.sleep(2)


base_url = 'https://art.nelson-atkins.org'
artifacts_info = []

# 遍历多页
for page in range(1, 13):
    start_url = f'https://art.nelson-atkins.org/advancedsearch/objects/provenance%3Achina?page={page}'
    for retry in range(MAX_RETRIES):
        try:
            response = requests.get(start_url, headers=headers, timeout=TIMEOUT)
            if response.status_code == 200:
                html = etree.HTML(response.text)
                # 提取文物列表项
                artifact_items = html.xpath('//div[contains(@class, "result item grid-item")]')
                for item in artifact_items:
                    name_elements = item.xpath('.//h3/a/text()')
                    name_href = item.xpath('.//h3/a/@href')
                    for i in range(len(name_href)):
                        print(i)
                        detail_url = base_url + name_href[i]
                        print(detail_url)
                        get_information(detail_url)
            else:
                print(f"页面 {start_url} 访问失败，状态码: {response.status_code}")
            break
        except requests.RequestException as e:
            print(f"请求 {start_url} 失败，重试次数: {retry + 1}/{MAX_RETRIES}，错误信息: {e}")
            if retry < MAX_RETRIES - 1:
                time.sleep(2)

df = pd.DataFrame(artifacts_info)
df.to_csv('chinese_artifacts.csv', index=False, encoding='utf-8-sig')