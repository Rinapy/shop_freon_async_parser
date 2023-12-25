import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
import lxml

TOTAL = 0
URL_CATEGORY = 'https://shop.feron.ru/catalog'
URL = 'https://shop.feron.ru'
PAGEN = '?PAGEN_2='
HEADERS = {'Accept':
           'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
           'Accept-Encoding':
           'gzip, deflate, br',
           'Accept-Language':
           'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
           'Cache-Control':
           'no-cache',
           'Cookie':
           'PHPSESSID=o3hxD8MkORbhpiQFWK5F4HfJHF46eYUq; BITRIX_SM_GUEST_ID=3164624; BITRIX_SM_SALE_UID=597537c5c1964134cc44f57d4bc6dd94; BITRIX_CONVERSION_CONTEXT_s1=%7B%22ID%22%3A1%2C%22EXPIRE%22%3A1703451540%2C%22UNIQUE%22%3A%5B%22conversion_visit_day%22%5D%7D; BX_USER_ID=1e975bde5ff29d3c0cc8166b11c50611; _gid=GA1.2.277887451.1703437916; _ym_uid=1703437916567236872; _ym_d=1703437916; _ym_isad=1; _ymab_param=yniu28toswLf0GYnd9qCp_IY83Xy6iTrvkD1QKvwBEX_WG32E48IcOriVlDzJFkqEBTXVnxLGJAIsH1Nkh6PG5MrfkI; _ym_visorc=w; _ga_SQ28P9VEEJ=GS1.1.1703437916.1.1.1703438194.59.0.0; _ga=GA1.1.338064580.1703437916; BITRIX_SM_LAST_VISIT=24.12.2023%2020%3A17%3A34',
           'Pragma':
           'no-cache',
           'Referer':
           'https://shop.feron.ru/catalog/lampy/lampa_svetodiodnaya_saffit_sba6015_shar_e27_15w_4000k/',
           'Sec-Ch-Ua':
           'Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120',
           'Sec-Ch-Ua-Mobile':
           '?0',
           'Sec-Ch-Ua-Platform':
           'Windows',
           'Sec-Fetch-Dest':
           'document',
           'Sec-Fetch-Mode':
           'navigate',
           'Sec-Fetch-Site':
           'same-origin',
           'Sec-Fetch-User':
           '?1',
           'Upgrade-Insecure-Requests':
           '1',
           'User-Agent':
           'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
           }


class Parser:
    def __init__(self) -> None:
        self.task_list = []
        self.result_dict = {}

    async def get_items_info(self, url, session, total_request: int = 0) -> dict[str, None] | dict[str, dict[str, str]]:
        global TOTAL
        total_request = total_request
        func_name = 'get_items_info'
        try:
            response = await session.get(url)
            html = await response.text()
        except Exception:
            return {func_name: None}
        soup = BeautifulSoup(html, 'lxml')
        try:
            article = soup.find(
                'p', class_='article').text.replace(' ', '').replace('\n', '').split('л')[-1]
        except AttributeError:
            return {func_name: None}
        item_name = soup.find(
            'div', attrs={'class': ['col-lg-10', 'order-2.order-lg-1']}).text.replace('\n', '')

        items_descript = soup.find(
            'div', attrs={'class': ['col-lg-10 order-3 order-lg-1']})
        item_descript = []

        if items_descript is not None:
            p_list = items_descript.find_all('p')
            for p in p_list:
                item_descript.append(p.text.replace('\\r\\n', ''))
        TOTAL += 1
        return {'article': {article: f'{item_name} -- {item_descript}'}}

    async def get_items_urls(self, url, session,  total_request: int = 0):
        global TOTAL
        func_name = 'get_items_urls'
        total_request = total_request
        try:
            response = await session.get(url)
            html = await response.text()
        except Exception:
            total_request += 1
            if total_request > 10:
                return {func_name: None}
            self.task_list.append(asyncio.create_task(
                self.get_items_urls(url, session, total_request)))
            return {func_name: None}
        soup = BeautifulSoup(html, 'lxml')
        all_p = soup.find_all(
            'p', class_='name-product')
        items_urls = []
        for p in all_p:
            a = p.find('a', href=True)['href']
            items_urls.append(f'{URL}{a}')
        TOTAL += 1
        return {func_name: items_urls}

    async def get_page_urls(self, url, session):
        global TOTAL
        func_name = 'get_page_urls'
        response = await session.get(url)
        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')

        page_count = soup.find_all('a', class_='page',
                                   href=True)[-1].text
        TOTAL += 1
        pagen_urls = []
        for page_number in range(int(page_count) + 1):
            pagen_urls.append(f'{url}{PAGEN}{page_number}')
        return {func_name: pagen_urls}

    async def get_category_url(self, url, session):
        global TOTAL
        func_name = 'get_category_url'
        response = await session.get(url)
        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')
        all_div = soup.find_all('div', class_='catalog-content__item-title')
        catalog_urls = []
        for div in all_div:
            try:
                a = div.find('a', href=True)['href']
            except Exception:
                pass
            if a != '':
                catalog_urls.append(URL + a)
        TOTAL += 1
        return {func_name: catalog_urls}

    async def main(self):
        self.map_task = {
            'get_category_url': self.get_page_urls,
            'get_page_urls': self.get_items_urls,
            'get_items_urls': self.get_items_info,
        }
        conn = aiohttp.TCPConnector(limit=20)
        async with aiohttp.ClientSession(connector=conn, headers=HEADERS) as session:
            self.task_list.append(asyncio.create_task(
                self.get_category_url(URL_CATEGORY, session)))
            while self.task_list:
                done, pending = await asyncio.wait(self.task_list, return_when=asyncio.FIRST_COMPLETED)
                print(len(pending))
                for done_task in done:
                    dict = done_task.result()
                    for func_name, result_task in dict.items():
                        if result_task is None:
                            print(f'Error {done_task}')
                            # self.task_list.remove(done_task)
                            continue
                        if func_name == 'article':
                            for article, info in result_task.items():
                                print('Добавил в словарь')
                                self.result_dict[article] = info
                                # self.task_list.remove(done_task)
                                continue
                        with open('tasks.txt', 'a') as file:
                            file.write(f'{func_name} -- {result_task}\n')
                            file.close()
                        next_func = self.map_task.get(func_name)
                        for url in result_task:
                            try:
                                self.task_list.append(
                                    asyncio.create_task(next_func(url, session)))
                            except TypeError:
                                print(
                                    f'Ошибку вызвала {func_name} c результатом {result_task}')
                    self.task_list.remove(done_task)

    def parsing(self):
        asyncio.run(self.main())
        df = pd.DataFrame(self.result_dict)
        df.to_excel('./result.xlsx')
        return self.result_dict


if __name__ == '__main__':
    parser = Parser()
    print(parser.parsing())
