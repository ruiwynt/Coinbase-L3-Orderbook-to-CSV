import asyncio
import websockets
import aiohttp
import json
import time
import datetime

from normaliser import NormaliseCoinbase
from csv_writer import write_csv
from orderbook import L3OrderBookManager

url = 'wss://ws-feed.pro.coinbase.com'

normaliser = NormaliseCoinbase()
order_book_manager = L3OrderBookManager()

async def recv_msg(ws):
    with open("coinbase_btcusd.csv", "w") as csv:
        write_csv(csv, "quote_no,event_no,order_id,original_order_id,side,price,size,lob_action,event_timestamp,send_timestamp,receive_timestamp,order_type,is_implied,order_executed,execution_price,executed_size,aggressor_side,matching_order_id,old_order_id,trade_id,size_ahead orders_ahead")
        async with aiohttp.ClientSession() as session:
            rest_url = 'https://api.exchange.coinbase.com/products/BTC-USD/book?level=3'
            async with session.get(rest_url) as resp:
                pokemon = await resp.json()
        snapshot = normaliser.normalise(pokemon)
        for event in snapshot["lob_events"]:
            # order_book_manager.handle_event(event)
            # event["size_ahead"], event["orders_ahead"] = order_book_manager.get_ahead(event)
            write_csv(csv, event)
        async for msg in ws:
            msg = json.loads(msg)
            msg["receive_timestamp"] = int(time.time()*10**3)
            normalised_msgs = normaliser.normalise(msg)
            for event in normalised_msgs['lob_events']:
                # order_book_manager.handle_event(event)
                # event["size_ahead"], event["orders_ahead"] = order_book_manager.get_ahead(event)
                write_csv(csv, event)

async def connect():
    async for ws in websockets.connect(url):
        try:
            msg = {"type": "subscribe",
                    "channels": [{"name": "full", "product_ids": ["BTC-USD"]}]
                }
            await ws.send(json.dumps(msg))
            await recv_msg(ws)
        except websockets.ConnectionClosed:
            print(f"Connection Closed at {datetime.datetime.now()}")
            continue


if __name__ == "__main__":
    asyncio.run(connect())