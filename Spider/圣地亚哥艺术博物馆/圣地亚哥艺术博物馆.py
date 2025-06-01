import time
import csv
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from bs4 import BeautifulSoup
from requests.exceptions import ReadTimeout
from selenium.common.exceptions import WebDriverException
import logging
import chardet

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def scroll_page_to_bottom(driver):
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


def crawl_single_page(driver, url, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            driver.get(url)
            time.sleep(5)

            # 滚动页面到底部
            scroll_page_to_bottom(driver)

            page_source = driver.page_source
            # 检测编码
            encoding = chardet.detect(page_source.encode())['encoding']
            soup = BeautifulSoup(page_source.encode().decode(encoding), 'html.parser')

            links = []
            for link in soup.find_all('a', title="Open object description"):
                href = link.get('href')
                logging.info(f"找到链接: {href}")
                if href:
                    links.append(href)
            return links
        except (ReadTimeout, WebDriverException) as e:
            logging.warning(f"请求页面 {url} 时出现错误: {e}，正在重试第 {retries + 1} 次...")
            retries += 1
            time.sleep(5)
    logging.error(f"请求页面 {url} 失败，已达到最大重试次数。")
    return []


def crawl_museum():
    base_url = 'https://collection.sdmart.org/objects-1/portfolio?records=50&query=Creation_Place2%20has%20words%20%22china%22&sort=9&page='
    service = Service('D:\Desktop\msedgedriver.exe')
    driver = webdriver.Edge(service=service)

    all_links = []
    try:
        for page in range(0, 44):
            logging.info(f"正在处理第 {page} 页")
            time.sleep(1)
            url = base_url + str(page)
            page_links = crawl_single_page(driver, url)
            all_links.extend(page_links)
    except Exception as e:
        logging.error(f"发生未知错误: {e}")
    finally:
        driver.quit()

    with open('museum_13_links.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Link'])
        for link in all_links:
            writer.writerow([link])

    return all_links


def get_element_text(soup, element_type, element_text, default=""):
    element = soup.find(element_type, string=element_text)
    if element:
        return element.next_sibling.strip()
    return default


def crawl_artifact_details(base_url, driver, url, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            driver.get(url)
            time.sleep(5)

            # 滚动页面到底部
            scroll_page_to_bottom(driver)

            page_source = driver.page_source
            # 检测编码
            encoding = chardet.detect(page_source.encode())['encoding']
            soup = BeautifulSoup(page_source.encode().decode(encoding), 'html.parser')
            time.sleep(5)
            name_text = ""
            if "objectName=" in url:
                name_text = url.split("objectName=")[1]

            logging.info(f"文物名称: {name_text}")

            creation_date = get_element_text(soup, 'strong', 'Creation date:')
            logging.info(f"创作日期: {creation_date}")

            creation_place = get_element_text(soup, 'strong', 'Creation place:')
            logging.info(f"创作地点: {creation_place}")

            parent_element1 = soup.find('a', class_='highslide')
            img_src = ""
            if parent_element1:
                target_img = parent_element1.find('img')
                if target_img:
                    img_src = base_url + target_img.get('src')
                else:
                    logging.warning("父元素中未找到 img 标签。")
            else:
                logging.warning("未找到符合条件的父元素。")
            logging.info(f"图片链接: {img_src}")
            time.sleep(5)
            parent_element2 = soup.find('div',
                                        class_='content col-md-12')
            type_ = ""
            medium = ""
            Credit_Line = ""
            Accession_Number = ""
            Dimensions = ""
            if parent_element2:
                divs = parent_element2.find_all('div')
                for div in divs:
                    type_strong = div.find('strong', string='Type:')
                    if type_strong:
                        type_ = type_strong.next_sibling.strip()
                    medium_strong = div.find('strong', string='Medium and Support:')
                    if medium_strong:
                        medium = medium_strong.next_sibling.strip()
                    Credit_Line_strong = div.find('strong', string='Credit Line:')
                    if Credit_Line_strong:
                        Credit_Line = Credit_Line_strong.next_sibling.strip()
                    Accession_Number_strong = div.find('strong', string='Accession Number:')
                    if Accession_Number_strong:
                        Accession_Number = Accession_Number_strong.next_sibling.strip()
                    Dimensions_strong = div.find('strong', string='Dimensions:')
                    if Dimensions_strong:
                        Dimensions = Dimensions_strong.next_sibling.strip()


            target_div = soup.find('div', class_='embarkInfoNotes ui-accordion-content ui-corner-bottom ui-helper-reset ui-widget-content ui-accordion-content-active')
            combined_text = ""
            if target_div:
                text_pieces = [part.strip() for part in target_div.stripped_strings]
                combined_text = " ".join(text_pieces)
            logging.info(f"详细描述: {combined_text}")
            return name_text, creation_date, creation_place, img_src, type_, medium, Credit_Line, Accession_Number, Dimensions, combined_text
        except (ReadTimeout, WebDriverException) as e:
            logging.warning(f"请求页面 {url} 时出现错误: {e}，正在重试第 {retries + 1} 次...")
            retries += 1
            time.sleep(5)
    logging.error(f"请求页面 {url} 失败，已达到最大重试次数。")
    return "", "", "", "", "", "", "", "", "", ""


def crawl_all_artifacts(start_index=0):
    base_url = 'https://collection.sdmart.org'
    service = Service('D:\Desktop\msedgedriver.exe')
    driver = webdriver.Edge(service=service)
    all_details = []
    try:
        encodings = ['utf-8', 'gbk', 'latin-1']
        for encoding in encodings:
            try:
                with open('./museum_13_links.csv', 'r', encoding=encoding) as infile:
                    reader = csv.reader(infile)
                    next(reader)  # 跳过标题行
                    links = list(reader)
                    break
            except UnicodeDecodeError:
                continue
        else:
            logging.error("无法使用任何编码方式读取文件。")
            return all_details

        for row in links[start_index:]:
            link = row[0]
            full_url = base_url + link
            retries = 0
            while retries < 3:  # 重试 3 次
                try:
                    logging.info(f"正在处理链接: {full_url}")
                    logging.info('===========================================================')
                    name_text, creation_date, creation_place, img_src, type_, medium, Credit_Line, Accession_Number, Dimensions, combined_text = crawl_artifact_details(
                        base_url, driver, full_url)
                    logging.info('===========================================================')
                    all_details.append(
                        [name_text, creation_date, creation_place, img_src, type_, medium, Credit_Line,
                         Accession_Number,
                         Dimensions, combined_text])
                    break  # 如果成功，跳出重试循环
                except (ReadTimeout, WebDriverException) as e:
                    logging.warning(f"请求页面 {full_url} 时出现错误: {e}，正在重试第 {retries + 1} 次...")
                    retries += 1
                    time.sleep(5)
            if retries == 3:
                logging.error(f"请求页面 {full_url} 失败，已达到最大重试次数。")

    except Exception as e:
        logging.error(f"发生未知错误: {e}")
    finally:
        driver.quit()

    # 追加数据到 CSV 文件
    with open('./museum_artifact_details.csv', 'a', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        for details in all_details:
            writer.writerow(details)

    return all_details


if __name__ == "__main__":
    results = crawl_all_artifacts()
    logging.info(f"共爬取到 {len(results)} 个文物的详细信息，已追加到 museum_artifact_details.csv 文件中。")
