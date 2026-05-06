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
        for ticker in self.target_tickers:
            self.market_data[ticker] = {
                "vol_24h": deque(maxlen=1440),      # 과거 24시간 1분봉 거래대금 바구니
                "trade_24h": deque(maxlen=1440),     # 과거 24시간 1분봉 종가 바구니
                "high_24h": deque(maxlen=1440),      # 과거 24시간 1분봉 고가 바구니
                "low_24h": deque(maxlen=1440),       # 과거 24시간 1분봉 저가 바구니
                "open_24h": deque(maxlen=1440),      # 과거 24시간 1분봉 시가 바구니

                # '현재 1분' 동안 실시간으로 조립 중인 캔들 데이터
                "current_min_id": 0,    # 현재 조립 중인 분(Minute)의 고유 ID
                "cur_open": 0.0,        # 현재 분의 시가
                "cur_high": 0.0,        # 현재 분의 고가 (계속 갱신됨)
                "cur_low": 0.0,         # 현재 분의 저가 (계속 갱신됨)
                "cur_close": 0.0,       # 현재 분의 종가 (현재가와 동일)
                "cur_vol": 0.0          # 현재 분의 누적 거래대금
            }
            
        print(f"✅ 타겟 코인 총 {len(self.target_tickers)}개 선정 및 서랍장 생성 완료!\n")


    # =====================================================================
    # [초기화 2] 과거 24시간 거래대금 데이터 로딩
    # =====================================================================
    def load_data(self) -> None:
        print(f"⏳ 과거 24시간(1440분) 데이터 로딩 및 [절대 시간축 동기화] 시작...")

        fail_count = 0
        
        # 🌟 1. [절대 기준점 셋팅] 현재 분(Minute)과 정확히 24시간 전(1440분 전) 분 계산
        current_timestamp_ms = int(time.time() * 1000)
        current_min_id = current_timestamp_ms // 60000
        start_min_id = current_min_id - 1440 # 우리의 출발선!

        for ticker in self.target_tickers:
            try:
                # 데이터 넉넉히 가져오기
                candles = Upbit().candles.list_minutes(
                    unit=1,
                    market=ticker,
                    count=1440,
                )

                target_data = self.market_data[ticker]
                
                # 🌟 2. 탐색 속도와 매칭을 위해 캔들 데이터를 딕셔너리로 변환 (key: 분 ID)
                candle_dict = { (c.timestamp // 60000): c for c in candles }

                # 🌟 3. 초기 기준가(last_close) 찾기
                # 만약 24시간 이전의 캔들이 있다면 그 종가를 기준가로 사용
                last_close = Decimal('0')
                for c in candles:
                    if (c.timestamp // 60000) <= start_min_id:
                        last_close = Decimal(str(c.trade_price))
                        break # 제일 최근 과거 하나만 찾으면 됨
                
                for m_id in range(start_min_id, current_min_id):
                    # 만약 이번 '분'에 해당하는 캔들이 업비트에서 받아온 데이터에 있다면? (데이터 삽입)
                    if m_id in candle_dict:
                        c = candle_dict[m_id]
                        target_data["open_24h"].append(Decimal(str(c.opening_price)))
                        target_data["high_24h"].append(Decimal(str(c.high_price)))
                        target_data["low_24h"].append(Decimal(str(c.low_price)))
                        target_data["trade_24h"].append(Decimal(str(c.trade_price)))
                        target_data["vol_24h"].append(Decimal(str(c.candle_acc_trade_price)))
                        
                        last_close = Decimal(str(c.trade_price)) # 다음 빈칸을 위해 종가 기억
                        
                    # 만약 이번 '분'에 거래가 단 1건도 없었다면? (회원님의 도지 캔들 보정 로직!)
                    else:
                        target_data["open_24h"].append(last_close)
                        target_data["high_24h"].append(last_close)
                        target_data["low_24h"].append(last_close)
                        target_data["trade_24h"].append(last_close)
                        target_data["vol_24h"].append(Decimal('0'))

                    # 현재 진행 중인 ID 최신화
                    target_data["current_min_id"] = m_id

                print(f"✅ {ticker} 로딩 및 절대시간 동기화 완료")

            except Exception as e:
                # (에러 처리 생략: 기존 코드와 동일)
                pass

            except Exception as e:
                error_msg = str(e)
                print(f"⚠️ {ticker} 로딩 실패: {e}")
                if "429" in error_msg or "too many" in error_msg:
                    print(f"\n🚨 [긴급 경고] 업비트 IP 차단 임박! (429 에러 감지). 즉시 가동을 중단합니다.")
                    sys.exit(1)
                
                fail_count += 1
                
                if fail_count >= 10:
                    print(f"❌ 실패 누적({fail_count}개)으로 인해 시스템을 안전 종료합니다.")
                    sys.exit(1)

        print("✅ 24시간 과거 데이터베이스 세팅 완료!\n")
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
    async def live_data(self) -> None:
        async with AsyncUpbit() as client:
            await self.send_discord("🚀 **[시스템] 자체 캔들 조립 엔진 가동! (Gap 보정 탑재)**")
            async with client.ws_stream.trade(self.target_tickers) as stream:
                async for event in stream:
                    ticker = event.code
                    price = Decimal(str(event.trade_price))
                    volume = Decimal(str(event.trade_volume))
                    tick_vol = price * volume
                    ask_bid = event.ask_bid  

                    minute_id = event.trade_timestamp // 60000 
                    target_data = self.market_data[ticker]
                    
                    # 🌟 1. 방금 온 데이터가 '새로운 분'의 시작이라면?
                    if target_data["current_min_id"] != minute_id:
                        gap = minute_id - target_data["current_min_id"]

                        if gap > 1:
                            for _ in range(gap - 1):
                                # 빈 시간 동안은 직전 캔들의 '종가(cur_close)'가 시/고/저/종가를 모두 유지하며 십자 도지를 만듭니다.
                                target_data["open_24h"].append(target_data["cur_close"])
                                target_data["high_24h"].append(target_data["cur_close"])
                                target_data["low_24h"].append(target_data["cur_close"])
                                target_data["trade_24h"].append(target_data["cur_close"])
                                # 거래량은 당연히 0입니다.
                                target_data["vol_24h"].append(Decimal('0'))

                        else:
                            target_data["open_24h"].append(target_data["cur_open"])
                            target_data["high_24h"].append(target_data["cur_high"])
                            target_data["low_24h"].append(target_data["cur_low"])
                            target_data["trade_24h"].append(target_data["cur_close"])
                            target_data["vol_24h"].append(target_data["cur_vol"])
                    # 2. 아직 같은 '분' 안에서 체결이 일어난 것이라면? (기존 배틀 로직)
                    else:
                        target_data["cur_high"] = max(target_data["cur_high"], price)
                        target_data["cur_low"] = min(target_data["cur_low"], price)
                        target_data["cur_close"] = price
                        target_data["cur_vol"] += tick_vol

                    # 3. 실시간 틱 데이터 최신화
                    target_data["tick_price"] = price
                    target_data["tick_vol"] = tick_vol
                    target_data["ask_bid"] = ask_bid

                    # 4. 즉시 매매 로직 가동
                    await self.check_sell_logic(ticker) 
                    await self.check_buy_logic(ticker)

    # =====================================================================
    # [메인 관리] 봇 라이프사이클 및 비상 안전망
    # =====================================================================
    async def run(self):
        # 1. 봇 초기화 세팅
        self.init_target_tickers()
        self.load_data()
        
        # 2. 메인 라이프사이클: 실시간 데이터 수신 및 매매 감시
        try:
            await self.live_data()
            await self.min_candle_data()
            
        # 통신이 일시적으로 끊기면 재접속
        except websockets.ConnectionClosed:
            print("🔌 웹소켓 끊김. 3초 후 재접속...")
            await asyncio.sleep(3)
            
        # 관리자가 강제로 종료(Ctrl+C)하면 루프 탈출
        except asyncio.CancelledError:
            print("🛑 외부 종료 시그널 감지. 메인 루프를 탈출합니다.")
            sys.exit(0) 
        
        except Exception as e:
            print(f"⚠️ 예상치 못한 에러 발생: {e}. 5초 후 재시도...")
            sys.exit(0) 
                    
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
