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

# =====================================================================
# [환경 설정] 보안 키 및 외부 연동 설정
# =====================================================================
load_dotenv('000_key_code.env')
ACCESS_KEY = os.getenv("access_key")
SECRET_KEY = os.getenv("secret_key")
DISCORD_URL = os.getenv("discode")


class QuantEngine:
    def __init__(self):
        # --------------------------------------------------
        # 1. 종목 필터링 및 보호 구역 설정
        # --------------------------------------------------
        self.MIN_PRICE = 10         # 최소 가격 (10원 이상)
        self.MAX_PRICE = 100000     # 최대 가격 (10만 원 이하)
        
        self.NON_TARGET = []                  # 아예 감시조차 하지 않을 예외 코인
        self.NOT_SELL = ['KRW', 'APENFT']     # 들고 있어도 절대 팔지 않을 코인 (현금, 에어드랍 등)
        
        # --------------------------------------------------
        # 2. 시스템 데이터 저장소
        # --------------------------------------------------
        self.target_tickers = []    # 최종적으로 감시할 코인 목록
        self.market_data = {}       # 코인별 실시간 데이터 및 과거 큐(Queue) 저장소
        self.portfolio = {}         # 현재 봇이 매수해서 보유 중인 코인 목록
        
        # --------------------------------------------------
        # 3. 가상 매매 자산 및 리스크 관리 설정
        # --------------------------------------------------
        self.virtual_balance = 10_000_000                         # 내 가상 지갑 잔고 (1,000만 원)
        self.MAX_SLOTS = 5                                        # 최대 동시 보유 가능 종목 수
        self.UNIT_AMOUNT = self.virtual_balance / self.MAX_SLOTS  # 1종목당 투입할 진입 금액 (200만 원)


    # =====================================================================
    # [유틸리티] 디스코드 비동기 알림 시스템
    # =====================================================================
    async def send_discord(self, msg):
        print(msg) 
        if DISCORD_URL:
            def post():
                try:
                    requests.post(DISCORD_URL, json={'content': msg})
                except Exception:
                    pass
            await asyncio.to_thread(post)


    # =====================================================================
    # [초기화 1] 타겟 코인 스캔 및 데이터 서랍장 생성
    # =====================================================================
    def scan_target_tickers(self):
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
                "vol_1m": 0.0,                      # 이번 1분 동안 쌓이고 있는 거래대금 누적치
                "last_minute_id": current_minute_id # 정각(1분) 갱신 확인용 타이머
            }
            
        print(f"✅ 타겟 코인 총 {len(self.target_tickers)}개 선정 및 서랍장 생성 완료!\n")


    # =====================================================================
    # [초기화 2] 과거 24시간 거래대금 데이터 로딩
    # =====================================================================
    async def init_api_data(self) -> None:
        print(f"⏳ 과거 24시간(1440분) 데이터 로딩 시작 (비동기 병렬 모드)...")
        
        async with AsyncUpbit(
            http_client=DefaultAioHttpClient()
        ) as client:
            semaphore = asyncio.Semaphore(3)
            async def fetch_ticker_data(ticker):
                async with semaphore:
                    try:
                        candles = await client.candles.list_minutes(
                            unit=1,
                            market=ticker,
                            count=1440,
                        )
                        
                        # 과거 데이터부터 채우기 위해 뒤집기
                        for c in reversed(candles):
                            # Decimal 변환 시 str() 사용은 정밀도를 위한 필수 습관입니다.
                            self.market_data[ticker]["vol_24h"].append(Decimal(c.candle_acc_trade_price))
                            self.market_data[ticker]["trade_24h"].append(Decimal(c.trade_price))
                            self.market_data[ticker]["high_24h"].append(Decimal(c.high_price))
                            self.market_data[ticker]["low_24h"].append(Decimal(c.low_price))
                            self.market_data[ticker]["open_24h"].append(Decimal(c.opening_price))
                        
                        print(f"✅ {ticker} 로딩 완료")
                        await asyncio.sleep(0.2)
                        return True
                    except Exception as e:
                        print(f"⚠️ {ticker} 로딩 실패: {e}")
                        if "429" in str(e) or "too many" in str(e):
                            print(f"\n🚨 [긴급 경고]")
                            sys.exit(1)
                        return False

            # 3. 모든 타겟 코인에 대해 작업 생성
            tasks = [fetch_ticker_data(ticker) for ticker in self.target_tickers]
            
            # 4. 병렬 실행 및 결과 수집
            results = await asyncio.gather(*tasks)
            
            # 실패한 코인 개수 확인
            fail_count = results.count(False)
            if fail_count >= 10:
                print(f"❌ 실패가 너무 많습니다 ({fail_count}개). 중단 후 종료합니다.")
                sys.exit(1)

        print(f"✅ 24시간 과거 데이터베이스 세팅 완료! (실패: {fail_count}개)\n")
    # =====================================================================
    # [행동 로직] 가상 매수 실행부
    # =====================================================================
    async def execute_buy(self, ticker, price, reason):
        """조건이 맞았을 때 가상 지갑에서 돈을 빼고 코인을 기록하는 매수 함수입니다."""
        
        # 방어막: 이미 보유 중이거나, 절대 사면 안 되는 코인이면 취소
        if ticker in self.portfolio or ticker in self.NON_TARGET: 
            return

        # 방어막: 지갑에 남은 돈이 1슬롯(200만 원)보다 적으면 취소
        if self.virtual_balance < self.UNIT_AMOUNT:
            return 

        # 업비트 기본 수수료(0.05%)를 떼고 실제로 내 지갑에 들어올 코인 수량 계산
        buy_amount = (self.UNIT_AMOUNT * 0.9995) / price
        
        # 지갑에서 현금이 차감됨
        self.virtual_balance -= self.UNIT_AMOUNT
        
        # 포트폴리오(내 보유 종목 리스트)에 기록
        self.portfolio[ticker] = {
            "buy_price": price,
            "amount": buy_amount,
            "buy_time": time.time()
        }
        
        msg = f"🔵 **[가상 매수] {ticker}**\n• 매수가: `{price:,.2f}원`\n• 투자금액: `{self.UNIT_AMOUNT:,.0f}원`\n• 잔고: `{self.virtual_balance:,.0f}원`\n• 사유: {reason}"
        await self.send_discord(msg)

    # =====================================================================
    # [행동 로직] 가상 매도 실행부
    # =====================================================================
    async def execute_sell(self, ticker, reason="조건 달성"):
        """조건이 맞았을 때 코인을 팔고 가상 지갑에 수익금을 넣는 매도 함수입니다."""
        
        # 방어막: 절대 팔면 안 되는 코인(현금 등)이거나, 내가 안 들고 있는 코인이면 취소
        if ticker in self.NOT_SELL or ticker not in self.portfolio:
            return

        # 내가 샀을 때의 기록을 꺼내옵니다.
        buy_info = self.portfolio[ticker]
        buy_price = buy_info["buy_price"]
        amount = buy_info["amount"]
        
        # 실시간 웹소켓에서 갱신되고 있는 현재가를 가져옵니다. (안전장치: 0원이면 매수가로 대체)
        current_price = self.market_data[ticker]["tick_price"]
        if current_price == 0: 
            current_price = buy_price 

        # 수수료(0.05%)를 떼고 실제로 내 지갑에 들어올 원화(KRW) 금액 계산
        sell_krw_amount = (current_price * amount) * 0.9995
        
        # 순수 투자 원금 대비 수익금과 수익률 계산
        pnl = sell_krw_amount - self.UNIT_AMOUNT
        pnl_rate = (pnl / self.UNIT_AMOUNT) * 100

        # 지갑에 현금이 들어오고, 포트폴리오에서 코인을 지웁니다.
        self.virtual_balance += sell_krw_amount
        del self.portfolio[ticker]

        icon = "🟢" if pnl > 0 else "🔴"
        msg = f"{icon} **[가상 매도] {ticker}**\n• 매도가: `{current_price:,.2f}원`\n• 손익: `{pnl:,.0f}원` ({pnl_rate:.2f}%)\n• 잔고: `{self.virtual_balance:,.0f}원`\n• 사유: {reason}"
        await self.send_discord(msg)


    # =====================================================================
    # [두뇌 로직] 타점 감시 구역 (매수)
    # =====================================================================
    async def check_buy_logic(self, ticker):
        """포트폴리오에 없는 코인들을 감시하며 진입각(Z-Score 등)을 잽니다."""
        if ticker in self.portfolio: 
            return
        
        data = self.market_data[ticker]
        # [작업 예정 공간] 매수 알고리즘이 들어갈 자리입니다.
        pass

    # =====================================================================
    # [두뇌 로직] 타점 감시 구역 (매도)
    # =====================================================================
    async def check_sell_logic(self, ticker):
        """포트폴리오에 있는 코인들을 감시하며 탈출각(익절/손절)을 잽니다."""
        if ticker not in self.portfolio: 
            return
            
        data = self.market_data[ticker]
        buy_info = self.portfolio[ticker]
        # [작업 예정 공간] 매도 알고리즘이 들어갈 자리입니다.
        pass


    # =====================================================================
    # [심장 로직] 웹소켓 실시간 데이터 수신 및 1분 롤링
    # =====================================================================
    async def websocket_loop(self):
        URI = "wss://api.upbit.com/websocket/v1"
        subscribe_data = [
            {"ticket": str(uuid.uuid4())},
            {"type": "trade", "codes": self.target_tickers},
            {"format": "SIMPLE"}
        ]

        async with websockets.connect(URI, ping_interval=None, ping_timeout=None) as ws:
            await ws.send(json.dumps(subscribe_data))
            await self.send_discord("🚀 **[시스템] 가상 매매 엔진 통신 개방!** 실시간 감시 시작...")

            while True:
                # 1. 데이터 수신 및 안전한 파싱
                raw_data = await ws.recv()
                if isinstance(raw_data, bytes): 
                    raw_data = raw_data.decode("utf-8")
                data = json.loads(raw_data)

                # 2. 타겟 코인 확인
                ticker = data.get('cd') or data.get('mk') or data.get('code')
                if ticker not in self.market_data: 
                    continue

                # 3. 체결 가격 및 수량 확인
                price = data.get('tp') or data.get('trade_price')
                volume = data.get('tv') or data.get('trade_volume')
                if price is None or volume is None: 
                    continue

                # 4. 해당 코인 서랍장에 방금 터진 실시간 데이터 즉시 업데이트
                target_data = self.market_data[ticker]
                target_data["tick_price"] = price
                target_data["tick_vol"] = price * volume

                # 5. [시간 롤링] 1분 정각이 지났는지 확인
                current_minute_id = int(time.time() // 60)
                
                if current_minute_id > target_data["last_minute_id"]:
                    # 정각이 지났으면 1분 누적액을 12시간 큐에 넣고 누적기를 0으로 초기화
                    target_data["vol_12h"].append(target_data["vol_1m"])
                    target_data["vol_1m"] = 0.0
                    target_data["last_minute_id"] = current_minute_id

                # 6. 새로운 1분 누적기에 체결 대금을 계속 더해줌
                target_data["vol_1m"] += target_data["tick_vol"]

                # 7. 데이터가 갱신되었으므로 매도/매수 로직 즉시 감시
                await self.check_sell_logic(ticker) 
                await self.check_buy_logic(ticker)


    # =====================================================================
    # [메인 관리] 봇 라이프사이클 및 비상 안전망
    # =====================================================================
    async def run(self):
        # 1. 봇 초기화 세팅
        self.scan_target_tickers()
        await self.init_api_data()
        
        # 2. 메인 무한 루프
        try:
            while True: 
                try:
                    await self.websocket_loop()
                    
                # 통신이 일시적으로 끊기면 재접속
                except websockets.ConnectionClosed:
                    print("🔌 웹소켓 끊김. 3초 후 재접속...")
                    await asyncio.sleep(3)
                    
                # 관리자가 강제로 종료(Ctrl+C)하면 루프 탈출
                except asyncio.CancelledError:
                    print("🛑 외부 종료 시그널 감지. 메인 루프를 탈출합니다.")
                    break 
                    
        # 3. 봇 종료 직전 비상 안전망 (Finally)
        finally:
            await self.send_discord("🚨 **[시스템 종료]** 비상 안전망 가동. 보유 종목 시장가 청산을 시도합니다.")
            
            # 내가 팔아야 할 진짜 코인들만 걸러냅니다. (현금 등 예외 코인 제외)
            held_tickers = list(self.portfolio.keys())
            coins_to_sell = [t for t in held_tickers if t not in self.NOT_SELL]
            
            if not coins_to_sell:
                await self.send_discord("✅ 청산할 보유 종목이 없어 안전하게 종료합니다.")
            else:
                for ticker in coins_to_sell:
                    await self.execute_sell(ticker, reason="프로그램 종료 비상 매도")


# =====================================================================
# 파이썬 실행 진입점 (Entry Point)
# =====================================================================
if __name__ == "__main__":
    # 윈도우 환경 비동기 에러 방지 패치
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    engine = QuantEngine()
    
    # 봇 가동
    try:
        asyncio.run(engine.run())
        
    # 사용자의 Ctrl+C 입력 무시 (run 내부의 CancelledError로 자연스럽게 넘김)
    except KeyboardInterrupt:
        pass

    # 루프가 완전히 죽은 후 최종 사망 선고 알림
    finally:
        asyncio.run(engine.send_discord("🛑 모든 프로세스가 완전히 종료되었습니다."))
