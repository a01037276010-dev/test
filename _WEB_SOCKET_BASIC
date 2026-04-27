"""시세 조회 시나리오.

업비트 Quotation API를 사용하여 시세 정보를 조회하는 예제입니다.
  - 페어 목록 조회
  - 현재가(Ticker) 조회 (개별/복수/마켓 단위)
  - 캔들(OHLCV) 조회 (분/일/주/월봉, to 파라미터)
  - 최근 체결 내역 조회 (days_ago 포함)
  - 호가(Orderbook) 조회
  - WebSocket 실시간 시세 구독 (현재가/호가/캔들)

Quotation API는 인증이 필요하지 않습니다.

실행:
    uv run examples/quotation_kr.py
"""

from __future__ import annotations

from decimal import Decimal

from upbit import Upbit
from upbit.types.connect_public_server_event import Ticker, Orderbook, WsCandleResponse

def _divider(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")

def get_candles(client: Upbit) -> None:
    _divider("3. 캔들 조회")

    # 5분봉
    candles = client.candles.list_minutes(
        unit=1,
        market="KRW-BTC",
        count=100,
    )
    print("  [1분봉]")
    print(f"  {'시각(KST)':<22} {'시가':>14} {'종가':>14} {'거래량':>14}")
    print(f"  {'-' * 68}")
    for c in candles:
        print(
            f"{c}"
            f"  {c.candle_date_time_kst:<22}"
            f" {Decimal(c.opening_price):>14,.0f}"
            f" {Decimal(c.trade_price):>14,.0f}"
            f" {Decimal(c.candle_acc_trade_volume):>14.4f}"
        )

    
if __name__ == "__main__":
    client = Upbit()
    get_candles(client)
