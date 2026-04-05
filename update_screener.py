#!/usr/bin/env python3
"""
KOSPI/KOSDAQ 스크리닝 봇 (경량 버전)
- FinanceDataReader(KRX 원본) + pykrx fallback
- 크롤링 없음 → FDR 데이터만으로 완결
- data.json 생성 → Git 자동 푸시
"""

import json
import os
import sys
import subprocess
import datetime
from pathlib import Path

# ─── 라이브러리 import ───
try:
    import FinanceDataReader as fdr
    HAS_FDR = True
except ImportError:
    HAS_FDR = False

try:
    from pykrx import stock
    HAS_PYKRX = True
except ImportError:
    HAS_PYKRX = False

if not HAS_FDR and not HAS_PYKRX:
    print("[ERROR] FinanceDataReader 또는 pykrx 중 하나 이상 설치 필요")
    print("  pip install finance-datareader pykrx")
    sys.exit(1)

# ─── 설정 ───
SCRIPT_DIR = Path(__file__).parent.resolve()
DATA_JSON = SCRIPT_DIR / "data.json"

# HTML 슬라이더 기본값 (프론트엔드에서 사용자 조절 가능)
DEFAULT_FILTERS = {
    "min_trading_value": 1000,       # 거래대금 최소 (억원)
    "min_change_rate": -30.0,        # 등락률 최소 (%)
    "max_change_rate": 30.0,         # 등락률 최대 (%)
    "max_market_cap": 20000000,      # 시총 최대 (억원 = 2,000조)
}


# ═══════════════════════════════════════════
#   1. 데이터 수집
# ═══════════════════════════════════════════

def get_trading_date():
    """최근 거래일 추정"""
    today = datetime.date.today()
    wd = today.weekday()
    if wd == 5:
        return today - datetime.timedelta(days=1)
    elif wd == 6:
        return today - datetime.timedelta(days=2)
    return today


def fetch_all_stocks_fdr():
    """FinanceDataReader로 KOSPI+KOSDAQ 전종목 수집"""
    print("\n[FDR] 데이터 수집 시작")
    all_stocks = []

    for market in ["KOSPI", "KOSDAQ"]:
        try:
            df = fdr.StockListing(market)
            count = 0
            for _, row in df.iterrows():
                code = str(row.get("Code", "")).strip()
                name = str(row.get("Name", "")).strip()
                close = row.get("Close", 0)
                change_rate = row.get("ChagesRatio", 0)
                volume = row.get("Volume", 0)
                amount = row.get("Amount", 0)
                marcap = row.get("Marcap", 0)

                if not code or not name or close == 0:
                    continue

                trading_value = amount / 1e8   # 원 → 억원
                market_cap = marcap / 1e8

                all_stocks.append({
                    "ticker": code,
                    "name": name,
                    "market": market,
                    "close": int(close),
                    "volume": int(volume),
                    "trading_value": round(trading_value, 0),
                    "market_cap": round(market_cap, 0),
                    "change_rate": round(float(change_rate), 2),
                })
                count += 1

            print(f"  [{market}] {count}개 종목 수집 완료")
        except Exception as e:
            print(f"  [{market}] FDR 오류: {e}")

    return all_stocks


def fetch_all_stocks_pykrx(date_str):
    """pykrx fallback"""
    print("\n[pykrx] 데이터 수집 시작")
    all_stocks = []

    for market in ["KOSPI", "KOSDAQ"]:
        try:
            df_ohlcv = stock.get_market_ohlcv_by_ticker(date_str, market=market)
            df_cap = stock.get_market_cap_by_ticker(date_str, market=market)
            df_change = stock.get_market_price_change(date_str, date_str, market=market)

            if df_ohlcv.empty:
                print(f"  [{market}] 데이터 없음")
                continue

            count = 0
            for ticker in df_ohlcv.index:
                try:
                    name = stock.get_market_ticker_name(ticker)
                    close = int(df_ohlcv.loc[ticker, "종가"])
                    volume = int(df_ohlcv.loc[ticker, "거래량"])
                    trading_value = int(df_ohlcv.loc[ticker, "거래대금"]) / 1e8
                    market_cap = int(df_cap.loc[ticker, "시가총액"]) / 1e8 if ticker in df_cap.index else 0
                    change_rate = float(df_change.loc[ticker, "등락률"]) if ticker in df_change.index else 0

                    if close == 0:
                        continue

                    all_stocks.append({
                        "ticker": ticker,
                        "name": name,
                        "market": market,
                        "close": close,
                        "volume": volume,
                        "trading_value": round(trading_value, 0),
                        "market_cap": round(market_cap, 0),
                        "change_rate": round(change_rate, 2),
                    })
                    count += 1
                except Exception:
                    continue

            print(f"  [{market}] {count}개 종목 수집 완료")
        except Exception as e:
            print(f"  [{market}] pykrx 오류: {e}")

    return all_stocks


def fetch_all_stocks(date_str):
    """FDR 우선 → pykrx fallback"""
    if HAS_FDR:
        all_stocks = fetch_all_stocks_fdr()
        if all_stocks:
            return all_stocks

    if HAS_PYKRX:
        print("[FDR 실패 → pykrx fallback]")
        return fetch_all_stocks_pykrx(date_str)

    return []


# ═══════════════════════════════════════════
#   2. 필터 & 하이라이트
# ═══════════════════════════════════════════

def apply_filters(all_stocks, filters=None):
    """거래대금 기준만으로 필터 (등락률/시총은 프론트엔드에서 조절)"""
    if filters is None:
        filters = DEFAULT_FILTERS
    filtered = [
        s for s in all_stocks
        if s["trading_value"] >= filters["min_trading_value"]
        and 0 < s["market_cap"] <= filters["max_market_cap"]
    ]
    filtered.sort(key=lambda x: x["trading_value"], reverse=True)
    return filtered


def generate_highlights(filtered_stocks):
    """주목 포인트 자동 생성"""
    highlights = []

    # 상한가 종목
    limit_up = [s for s in filtered_stocks if s["change_rate"] >= 29.5]
    for s in limit_up:
        highlights.append("🚀 상한가: " + s["name"] + "(+" + str(s["change_rate"]) + "%)")

    # 하한가 종목
    limit_down = [s for s in filtered_stocks if s["change_rate"] <= -29.5]
    for s in limit_down:
        highlights.append("💥 하한가: " + s["name"] + "(" + str(s["change_rate"]) + "%)")

    # 거래대금 TOP 3
    top3 = sorted(filtered_stocks, key=lambda x: x["trading_value"], reverse=True)[:3]
    if top3:
        names = ", ".join(s["name"] + "(" + str(int(s["trading_value"])) + "억)" for s in top3)
        highlights.append("💰 거래대금 TOP3: " + names)

    # 급등 (등락률 +15% 이상) 종목 수
    sharp_rise = [s for s in filtered_stocks if s["change_rate"] >= 15]
    if len(sharp_rise) >= 3:
        highlights.append("🔥 급등(+15%↑) " + str(len(sharp_rise)) + "개 종목 — 시장 과열 주의")

    # 급락 (등락률 -10% 이하) 종목 수
    sharp_fall = [s for s in filtered_stocks if s["change_rate"] <= -10]
    if len(sharp_fall) >= 3:
        highlights.append("⚠️ 급락(-10%↓) " + str(len(sharp_fall)) + "개 종목 — 투매 경계")

    return highlights


# ═══════════════════════════════════════════
#   3. 데이터 저장 & Git
# ═══════════════════════════════════════════

def build_data_json(date_str, filtered_stocks, all_stocks, highlights):
    """data.json 생성"""
    kospi_total = sum(1 for s in all_stocks if s["market"] == "KOSPI")
    kosdaq_total = sum(1 for s in all_stocks if s["market"] == "KOSDAQ")
    kospi_filtered = sum(1 for s in filtered_stocks if s["market"] == "KOSPI")
    kosdaq_filtered = sum(1 for s in filtered_stocks if s["market"] == "KOSDAQ")

    data = {
        "date": date_str,
        "generated_at": datetime.datetime.now().isoformat(),
        "filter_defaults": DEFAULT_FILTERS,
        "total_filtered": len(filtered_stocks),
        "stocks": filtered_stocks,
        "highlights": highlights,
        "all_stocks_summary": {
            "kospi_count": kospi_filtered,
            "kosdaq_count": kosdaq_filtered,
            "total_kospi": kospi_total,
            "total_kosdaq": kosdaq_total,
            "total": len(all_stocks),
        },
    }

    with open(DATA_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n[저장] {DATA_JSON} ({len(filtered_stocks)}개 종목)")
    return data


def git_commit_and_push(date_str):
    """Git 자동 커밋 & 푸시"""
    os.chdir(SCRIPT_DIR)

    # stale lock 자동 제거
    for lock in [".git/HEAD.lock", ".git/index.lock"]:
        lock_path = SCRIPT_DIR / lock
        if lock_path.exists():
            try:
                lock_path.unlink()
            except Exception:
                pass

    try:
        subprocess.run(["git", "add", "-A"], check=True, capture_output=True)

        commit_msg = "📊 스크리닝 업데이트: " + date_str
        result = subprocess.run(
            ["git", "commit", "-m", commit_msg],
            capture_output=True, text=True
        )

        if "nothing to commit" in result.stdout:
            print("[Git] 변경사항 없음")
            return True

        print("[Git] 커밋: " + commit_msg)

        result = subprocess.run(
            ["git", "push", "origin", "main"],
            capture_output=True, text=True, timeout=30
        )

        if result.returncode == 0:
            print("[Git] 푸시 완료!")
            return True
        else:
            print("[Git] 푸시 실패: " + result.stderr)
            return False

    except Exception as e:
        print("[Git] 오류: " + str(e))
        return False


# ═══════════════════════════════════════════
#   4. 메인
# ═══════════════════════════════════════════

def main():
    print("=" * 60)
    print("  KOSPI/KOSDAQ 스크리닝 봇 (경량)")
    print("  FDR(KRX 원본) | 크롤링 없음 | 빠른 실행")
    print("=" * 60)

    # 1) 거래일
    target = get_trading_date()
    date_str = target.strftime("%Y-%m-%d")
    date_str_compact = target.strftime("%Y%m%d")
    print(f"\n📅 대상일: {date_str}")

    # 2) 전 종목 수집
    all_stocks = fetch_all_stocks(date_str_compact)
    if not all_stocks:
        print("\n❌ 데이터 수집 실패")
        return None

    kospi_n = sum(1 for s in all_stocks if s["market"] == "KOSPI")
    kosdaq_n = sum(1 for s in all_stocks if s["market"] == "KOSDAQ")
    print(f"\n총 {len(all_stocks)}개 종목 (KOSPI {kospi_n} / KOSDAQ {kosdaq_n})")

    # 3) 필터 (거래대금 2000억↑, 시총 2000조↓)
    filtered = apply_filters(all_stocks)
    print(f"\n[필터] {len(filtered)}개 종목 통과 (거래대금 {DEFAULT_FILTERS['min_trading_value']:,}억↑)")

    # 4) 하이라이트
    highlights = generate_highlights(filtered)

    # 5) 저장
    data = build_data_json(date_str, filtered, all_stocks, highlights)

    # 6) Git 푸시
    git_commit_and_push(date_str)

    # 7) 요약
    print("\n" + "=" * 60)
    print("  📊 결과 요약")
    print("=" * 60)
    print(f"  대상일: {date_str}")
    print(f"  수집: KOSPI {kospi_n} / KOSDAQ {kosdaq_n}")
    print(f"  필터 통과: {len(filtered)}개")

    # 상승/하락 분포
    up = sum(1 for s in filtered if s["change_rate"] > 0)
    down = sum(1 for s in filtered if s["change_rate"] < 0)
    flat = len(filtered) - up - down
    print(f"  상승 {up} / 보합 {flat} / 하락 {down}")

    if filtered:
        print(f"\n  거래대금 TOP 10:")
        for s in filtered[:10]:
            sign = "+" if s["change_rate"] >= 0 else ""
            mc_str = str(int(s["market_cap"])) + "억"
            if s["market_cap"] >= 10000:
                mc_str = str(round(s["market_cap"] / 10000, 1)) + "조"
            print(f"    {s['name']} ({s['market']}): {sign}{s['change_rate']}% | "
                  f"TV {s['trading_value']:,.0f}억 | MC {mc_str}")

    if highlights:
        print(f"\n  🔍 주목:")
        for h in highlights:
            print(f"    {h}")

    print(f"\n  🌐 https://whysosary-dot.github.io/stock-screener/")
    print("\n✅ 완료!")
    return data


if __name__ == "__main__":
    main()
