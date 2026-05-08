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
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import requests
import websockets
import pyupbit
from dotenv import load_dotenv
from upbit import Upbit, AsyncUpbit

# =====================================================================
# [환경 설정] 보안 키 및 외부 연동
# =====================================================================
load_dotenv('000_key_code.env')
ACCESS_KEY = os.getenv("access_key")
SECRET_KEY = os.getenv("secret_key")
DISCORD_URL = os.getenv("discode")


class QuantEngine:
    def __init__(self):
        """시스템 전반의 환경 변수 및 저장소를 초기화합니다."""
        
        # 1. 종목 필터링 설정
        self.MIN_PRICE = 10         # 감시 최소 가격 (10원 이상)
        self.MAX_PRICE = 100000     # 감시 최대 가격 (10만 원 이하)
        self.NON_TARGET = []        # 감시 제외 코인 리스트
        self.NOT_SELL = ['KRW', 'APENFT'] # 절대 매도하지 않을 보유 자산
        
        # 2. 데이터 저장소
        self.target_tickers = []    # 최종 감시 대상 코인 리스트
        self.market_data = {}       # 코인별 시계열 데이터 및 실시간 상태 큐
        self.portfolio = {}         # 현재 보유 중인 가상 매수 포트폴리오
        
        # 3. 자산 및 리스크 관리
        self.virtual_balance = 10_000_000                          # 가상 초기 자본금 (1,000만 원)
        self.MAX_SLOTS = 5                                         # 최대 동시 보유 종목 수
        self.UNIT_AMOUNT = self.virtual_balance / self.MAX_SLOTS   # 1슬롯당 투입 금액 (200만 원)


    # =====================================================================
    # [유틸리티] 디스코드 알림
    # =====================================================================
    async def send_discord(self, msg: str):
        """비동기 통신을 방해하지 않고 디스코드로 메시지를 전송합니다."""
        print(msg) 
        if DISCORD_URL:
            def post():
                try:
                    requests.post(DISCORD_URL, json={'content': msg})
                except Exception:
                    pass
            await asyncio.to_thread(post)


    # =====================================================================
    # [초기화 1] 타겟 종목 스캔 및 저장소 할당
    # =====================================================================
    def init_target_tickers(self):
        """조건에 맞는 코인을 필터링하고, 각각의 데이터를 담을 큐(Deque)를 생성합니다."""
        print(f"🔍 [스캔 시작] 조건에 맞는 타겟 종목 검색 중...")
        
        prices = pyupbit.get_current_price(pyupbit.get_tickers("KRW"))
        
        self.target_tickers = [
            t for t, p in prices.items() 
            if p is not None and self.MIN_PRICE <= p <= self.MAX_PRICE and t not in self.NON_TARGET
        ]
        
        for ticker in self.target_tickers:
            self.market_data[ticker] = {
                # --- 과거 200분 확정 캔들 저장소 ---
                "open_24h": deque(maxlen=200),
                "high_24h": deque(maxlen=200),
                "low_24h": deque(maxlen=200),
                "trade_24h": deque(maxlen=200),
                "vol_24h": deque(maxlen=200),

                # --- 실시간 조립 중인 현재 1분 캔들 상태 ---
                "current_min_id": 0,    # 현재 조립 중인 분(Minute)의 고유 ID
                "cur_open": 0.0,        
                "cur_high": 0.0,        
                "cur_low": 0.0,         
                "cur_close": 0.0,       
                "cur_vol": 0.0          
            }
            
        print(f"✅ 타겟 코인 총 {len(self.target_tickers)}개 선정 및 서랍장 생성 완료!\n")


    # =====================================================================
    # [초기화 2] 과거 200분 시계열 데이터 로딩 및 동기화
    # =====================================================================
    def load_data(self) -> None:
        """업비트 REST API를 호출하여 과거 200분 데이터를 적재하고 빈 시간을 보정(Gap Filling)합니다."""
        print(f"⏳ 과거 200분 데이터 로딩 및 [KST 절대 시간축 동기화] 시작...")

        fail_count = 0
        KST = timezone(timedelta(hours=9))
        current_min_id = int(time.time()) // 60

        for ticker in self.target_tickers:
            try:
                candles = Upbit().candles.list_minutes(unit=1, market=ticker, count=200)
                target_data = self.market_data[ticker]
                last_close = Decimal('0')
                
                # 과거(가장 오래된 데이터)부터 현재 방향으로 순차 적재
                for c in reversed(candles):
                    # 1. 45ms 오차 방지를 위한 KST 문자열 파싱
                    dt_kst = datetime.strptime(c.candle_date_time_kst, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=KST)
                    c_min_id = int(dt_kst.timestamp()) // 60

                    # 2. 캔들 사이의 빈 공간(Gap) 감지 및 도지 캔들 보정
                    if target_data["current_min_id"] != 0:
                        gap = c_min_id - target_data["current_min_id"]
                        if gap > 1:
                            for _ in range(gap - 1):
                                target_data["open_24h"].append(last_close)
                                target_data["high_24h"].append(last_close)
                                target_data["low_24h"].append(last_close)
                                target_data["trade_24h"].append(last_close)
                                target_data["vol_24h"].append(Decimal('0'))

                    # 3. 실제 캔들 데이터 삽입
                    target_data["open_24h"].append(Decimal(str(c.opening_price)))
                    target_data["high_24h"].append(Decimal(str(c.high_price)))
                    target_data["low_24h"].append(Decimal(str(c.low_price)))
                    target_data["trade_24h"].append(Decimal(str(c.trade_price)))
                    target_data["vol_24h"].append(Decimal(str(c.candle_acc_trade_price)))

                    target_data["current_min_id"] = c_min_id
                    last_close = Decimal(str(c.trade_price))

                # 4. 꼬리 공백(Tail Gap) 보정: 마지막 체결 캔들 ~ 봇 가동 시점
                tail_gap = current_min_id - target_data["current_min_id"]
                if tail_gap > 0:
                    for _ in range(tail_gap):
                        target_data["open_24h"].append(last_close)
                        target_data["high_24h"].append(last_close)
                        target_data["low_24h"].append(last_close)
                        target_data["trade_24h"].append(last_close)
                        target_data["vol_24h"].append(Decimal('0'))

                # 5. 실시간 웹소켓이 이어받을 수 있도록 현재 상태 셋팅
                target_data["cur_open"] = last_close
                target_data["cur_high"] = last_close
                target_data["cur_low"] = last_close
                target_data["cur_close"] = last_close
                target_data["cur_vol"] = Decimal('0')
                target_data["current_min_id"] = current_min_id
                        
                print(f"✅ {ticker} 로딩 및 보정 완료")
                time.sleep(0.1) # API Rate Limit(429) 방어

            except Exception as e:
                error_msg = str(e).lower()
                print(f"⚠️ {ticker} 로딩 실패: {e}")
                if "429" in error_msg or "too many" in error_msg:
                    print(f"\n🚨 [긴급 경고] 업비트 IP 차단 감지. 즉시 가동 중단.")
                    sys.exit(1)
                
                fail_count += 1
                if fail_count >= 10:
                    print(f"❌ 데이터 로딩 실패 누적({fail_count}회). 시스템 안전 종료.")
                    sys.exit(1)

        print("✅ 초기 200분 데이터베이스 세팅(Gap 자동 보정) 완료!\n")


    # =====================================================================
    # [행동 엔진] 가상 매수/매도 실행
    # =====================================================================
    async def execute_buy(self, ticker: str, price: Decimal, reason: str):
        """조건 충족 시 가상 잔고를 차감하고 포트폴리오에 코인을 편입합니다."""
        if ticker in self.portfolio or ticker in self.NON_TARGET: return
        if self.virtual_balance < self.UNIT_AMOUNT: return 

        buy_amount = (self.UNIT_AMOUNT * 0.9995) / float(price) # 0.05% 수수료 차감
        self.virtual_balance -= self.UNIT_AMOUNT
        
        self.portfolio[ticker] = {
            "buy_price": float(price),
            "amount": buy_amount,
            "buy_time": time.time()
        }
        
        msg = f"🔵 **[매수] {ticker}**\n• 체결가: `{price:,.2f}원`\n• 잔고: `{self.virtual_balance:,.0f}원`\n• 사유: {reason}"
        await self.send_discord(msg)

    async def execute_sell(self, ticker: str, reason: str = "조건 달성"):
        """조건 충족 시 코인을 처분하고 가상 잔고에 수익금을 합산합니다."""
        if ticker in self.NOT_SELL or ticker not in self.portfolio: return

        buy_info = self.portfolio[ticker]
        buy_price = buy_info["buy_price"]
        amount = buy_info["amount"]
        
        current_price = float(self.market_data[ticker]["tick_price"])
        if current_price == 0: current_price = buy_price 

        sell_krw_amount = (current_price * amount) * 0.9995 # 0.05% 수수료 차감
        pnl = sell_krw_amount - self.UNIT_AMOUNT
        pnl_rate = (pnl / self.UNIT_AMOUNT) * 100

        self.virtual_balance += sell_krw_amount
        del self.portfolio[ticker]

        icon = "🟢" if pnl > 0 else "🔴"
        msg = f"{icon} **[매도] {ticker}**\n• 체결가: `{current_price:,.2f}원`\n• 손익: `{pnl:,.0f}원` ({pnl_rate:.2f}%)\n• 사유: {reason}"
        await self.send_discord(msg)


    # =====================================================================
    # [두뇌 로직] 타점 감시 알고리즘 (추후 구현)
    # =====================================================================
    async def check_buy_logic(self, ticker: str):
        """미보유 종목의 매수 타점을 감시합니다."""
        if ticker in self.portfolio: return
        data = self.market_data[ticker]
        pass # 매수 알고리즘 작성 공간

    async def check_sell_logic(self, ticker: str):
        """보유 종목의 익절/손절 타점을 감시합니다."""
        if ticker not in self.portfolio: return
        data = self.market_data[ticker]
        pass # 매도 알고리즘 작성 공간


    # =====================================================================
    # [심장 로직] 실시간 틱 데이터 수신 및 1분 캔들 조립
    # =====================================================================
    async def live_data(self) -> None:
        """웹소켓을 통해 실시간 체결 데이터를 받아 1분 단위 캔들을 자체 조립합니다."""
        async with AsyncUpbit() as client:
            await self.send_discord("🚀 **[시스템] 자체 캔들 조립 엔진 가동! 실시간 감시 시작...**")
            
            async with client.ws_stream.trade(self.target_tickers) as stream:
                async for event in stream:
                    ticker = event.code
                    price = Decimal(str(event.trade_price))
                    volume = Decimal(str(event.trade_volume))
                    tick_vol = price * volume
                    ask_bid = event.ask_bid  

                    # 1. 절대 시간 파싱 (UTC 문자열 -> KST 변환 -> 분 ID 산출)
                    dt_utc_str = f"{event.trade_date}T{event.trade_time}"
                    dt_utc = datetime.strptime(dt_utc_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                    minute_id = int(dt_utc.timestamp()) // 60 

                    target_data = self.market_data[ticker]
                    
                    # 2. 새로운 분(Minute)이 시작되었을 때의 처리
                    if target_data["current_min_id"] != minute_id:
                        
                        # 직전에 조립 중이던 캔들이 존재한다면 큐에 확정 저장
                        if target_data["current_min_id"] != 0:
                            target_data["open_24h"].append(target_data["cur_open"])
                            target_data["high_24h"].append(target_data["cur_high"])
                            target_data["low_24h"].append(target_data["cur_low"])
                            target_data["trade_24h"].append(target_data["cur_close"])
                            target_data["vol_24h"].append(target_data["cur_vol"])

                            # 체결 지연으로 인한 빈 시간(Gap) 도지 보정
                            gap = minute_id - target_data["current_min_id"]
                            if gap > 1:
                                for _ in range(gap - 1):
                                    target_data["open_24h"].append(target_data["cur_close"])
                                    target_data["high_24h"].append(target_data["cur_close"])
                                    target_data["low_24h"].append(target_data["cur_close"])
                                    target_data["trade_24h"].append(target_data["cur_close"])
                                    target_data["vol_24h"].append(Decimal('0'))

                        # 새로운 1분 캔들 초기화 (시/고/저/종가를 현재가로 셋팅)
                        target_data["current_min_id"] = minute_id
                        target_data["cur_open"] = price
                        target_data["cur_high"] = price
                        target_data["cur_low"] = price
                        target_data["cur_close"] = price
                        target_data["cur_vol"] = tick_vol
                        
                    # 3. 같은 분(Minute) 내에서 체결 시: 고가/저가 배틀 및 누적 거래대금 합산
                    else:
                        target_data["cur_high"] = max(target_data["cur_high"], price)
                        target_data["cur_low"] = min(target_data["cur_low"], price)
                        target_data["cur_close"] = price
                        target_data["cur_vol"] += tick_vol

                    # 4. 틱(Tick) 단위 최신화 및 매매 로직 트리거
                    target_data["tick_price"] = price
                    target_data["tick_vol"] = tick_vol
                    target_data["ask_bid"] = ask_bid

                    await self.check_sell_logic(ticker) 
                    await self.check_buy_logic(ticker)


    # =====================================================================
    # [메인 관리] 봇 라이프사이클 및 비상 안전망
    # =====================================================================
    async def run(self):
        """시스템을 초기화하고 무한 재접속(Zombie) 루프를 돌며 봇을 가동합니다."""
        # 1. 시스템 초기화
        self.init_target_tickers()
        self.load_data()
        
        # 2. 메인 라이프사이클 (무한 재가동 루프)
        while True:
            try:
                await self.live_data()
                
            except websockets.exceptions.ConnectionClosed:
                print("🔌 웹소켓 통신 단절. 3초 후 엔진을 재가동합니다...")
                await asyncio.sleep(3)
                
            except asyncio.CancelledError:
                print("🛑 사용자 강제 종료 시그널 감지. 메인 루프를 탈출합니다.")
                break 
                
            except Exception as e:
                print(f"⚠️ 예상치 못한 에러 발생: {e}. 5초 후 재시도...")
                await asyncio.sleep(5)
                    
        # 3. 비상 안전망 (메인 루프 탈출 시 1회 실행)
        await self.send_discord("🚨 **[시스템 종료]** 비상 안전망 가동. 보유 종목 시장가 청산을 시도합니다.")
        
        held_tickers = list(self.portfolio.keys())
        coins_to_sell = [t for t in held_tickers if t not in self.NOT_SELL]
        
        if not coins_to_sell:
            await self.send_discord("✅ 청산할 보유 종목이 없어 엔진을 안전하게 종료합니다.")
        else:
            for ticker in coins_to_sell:
                await self.execute_sell(ticker, reason="프로그램 종료 비상 매도")


# =====================================================================
# 파이썬 실행 진입점 (Entry Point)
# =====================================================================
if __name__ == "__main__":
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    engine = QuantEngine()
    
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        pass # Ctrl+C 입력 시 안전망(finally)으로 자연스럽게 유도
    finally:
        asyncio.run(engine.send_discord("🛑 모든 퀀트 프로세스가 완전히 종료되었습니다."))
