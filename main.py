import csv
from datetime import datetime
import aiohttp
import asyncio
import query_params

class Parser:
    session: aiohttp.ClientSession

    def __init__(self, size: int):
        self.url = 'https://api.metro-cc.ru/products-api/graph'
        self.size = size
        self.payload = {
            'query': query_params.query,
            'variables':query_params.variables
        }
        self.items = []

    async def _send_post_request(self, payload: dict):
        async with self.session.post(url=self.url,json=payload) as resp:
            resp_json = await resp.json()
        return resp_json

    async def _get_total_count(self) -> int:
        self.payload['variables']['size'] = 0
        response = await self._send_post_request(self.payload)
        self.payload['variables']['size'] = self.size
        return response['data']['category']['total']
    
    async def _get_page_data(self,start: int):
        self.payload['variables']['from'] = start
        response = await self._send_post_request(self.payload)
        result = Parser.brush_res(response['data']['category']['products'])
        self.items.extend(result)
        print(f'Обработал страницу [{int(start/self.size+1)}]')

    async def _gather_data(self,total:int):
        tasks = []
        for i in range(0,total,self.size):
            task = asyncio.create_task(self._get_page_data(i))
            tasks.append(task)

        await asyncio.gather(*tasks)

    async def run(self):
        self.session = aiohttp.ClientSession()

        try:
            total_count = await self._get_total_count()
            await self._gather_data(total_count)
        except Exception as err:
            print(f"Connection error: {err}")
        finally:
            await self.session.close()

        self._save_to_csv()
        
    def _save_to_csv(self):
        head = ['id','name','link','regular_price','promo_price','brand']
        dt = '{:%Y-%m-%d_%H-%M-%S}'.format(datetime.now())
        with open(file=f'csv/metro_{dt}.csv',mode='w',encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(head)
            for item in self.items:
                writer.writerow(item)

    @staticmethod
    def brush_res(items: dict) -> list:
        res = []
        domen = 'https://online.metro-cc.ru'
        for item in items:
            if item['stocks'][0]['prices']['old_price'] is None:
                regular_price = item['stocks'][0]['prices']['price']
                promo_price = 'Нет акции'
            else:
                regular_price = item['stocks'][0]['prices']['old_price']
                promo_price = item['stocks'][0]['prices']['price']

            temp = [
                item['article'],
                item['name'],
                domen+item['url'],
                regular_price,
                promo_price,
                item['manufacturer']['name']
            ]
            res.append(temp)
        return res


if __name__ == "__main__":
    parser = Parser(size=30)
    asyncio.run(parser.run())