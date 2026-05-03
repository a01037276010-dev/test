from __future__ import annotations

import warnings
warnings.filterwarnings('ignore')

import os
import sys
import time
import json
import uuid
import asyncio
from collections import deque

import requests
import websockets
import pyupbit
from dotenv import load_dotenv

from decimal import Decimal

from upbit import Upbit, DefaultAioHttpClient, AsyncUpbit
from upbit.types.connect_public_server_event import Ticker, Orderbook, WsCandleResponse

class AutoTrading :
    def __init__(self):
        self.target_tickers = []
        self.market_data = {}
        self.MIN_PRICE = 10
        self.MAX_PRICE = 1000000
        self.NON_TARGET = []

    def init_target_tickers(self):
            print(f"🔍 [스캔 시작] 조건에 맞는 타겟 종목 검색 중...")
            
            # 업비트 원화 마켓 전 종목의 현재가를 한 번에 불러옵니다.
            prices = pyupbit.get_current_price(pyupbit.get_tickers("KRW"))
            
            # 가격 조건에 맞고, 예외 리스트(NON_TARGET)에 없는 코인만 골라냅니다.
            self.target_tickers = [
                t for t, p in prices.items() 
                if p is not None and self.MIN_PRICE <= p <= self.MAX_PRICE and t not in self.NON_TARGET
            ]
            
            # 선정된 코인들에게 각자의 데이터를 담을 전용 서랍(Dictionary)을 만들어줍니다.
            current_minute_id = int(time.time() // 60)
            for ticker in self.target_tickers:
                self.market_data[ticker] = {
                    "vol_24h": deque(maxlen=1440),      # 과거 24시간 1분봉 거래대금 바구니
                    "trade_24h": deque(maxlen=1440),     # 과거 24시간 1분봉 종가 바구니
                    "high_24h": deque(maxlen=1440),      # 과거 24시간 1분봉 고가 바구니
                    "low_24h": deque(maxlen=1440),       # 과거 24시간 1분봉 저가 바구니
                    "open_24h": deque(maxlen=1440),      # 과거 24시간 1분봉 시가 바구니

                    "tick_price": 0.0,                  # 방금 터진 체결 가격
                    "tick_vol": 0.0,                    # 방금 터진 체결 거래대금(원화)
                    "ask_bid": "",                      # 방금 터진 체결 종류 (매수/매도)
                }
                
            print(f"✅ 타겟 코인 총 {len(self.target_tickers)}개 선정 및 서랍장 생성 완료!\n")

    async def async_ws_connect_public_candle_3m(self) -> None:
        async with AsyncUpbit() as client:
            async with client.ws_stream.candle(self.target_tickers, interval="1m") as stream:
                async for event in stream:
                    print(event)


    async def main(self) -> None:
        # 윈도우 환경 비동기 에러 방지 패치
        if sys.platform.startswith('win'):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        self.init_target_tickers()
        await self.async_ws_connect_public_candle_3m()

if __name__ == "__main__":
    auto_trading = AutoTrading()

    asyncio.run(auto_trading.main())
