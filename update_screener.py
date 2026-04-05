#!/usr/bin/env python3
"""
KOSPI/KOSDAQ 급등 스크리닝 봇
- FinanceDataReader(KRX 원본 데이터) + pykrx fallback
- 네이버 금융에서 섹터/뉴스 보강
- data.json 생성 → Git 자동 푸시
"""

import json
import os
import sys
import subprocess
import datetime
import time
import re
import requests
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
OUTPUT_HTML = SCRIPT_DIR / "index.html"

DEFAULT_FILTERS = {
    "min_trading_value": 2000,   # 거래대금 최소 (억원)
    "min_change_rate": 10.0,     # 등락률 최소 (%)
    "max_market_cap": 300000,    # 시총 최대 (억원 = 30조)
}

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}


# ═══════════════════════════════════════════
#   1. 데이터 수집 (FDR 우선, pykrx fallback)
# ═══════════════════════════════════════════

def get_trading_date():
    """최근 거래일 추정"""
    today = datetime.date.today()
    weekday = today.weekday()
    if weekday == 5:       # 토
        return today - datetime.timedelta(days=1)
    elif weekday == 6:     # 일
        return today - datetime.timedelta(days=2)
    elif weekday == 0:
        # 월요일: 장 시작 전이면 금요일 데이터
        now = datetime.datetime.now()
        if now.hour < 16:  # 16시 이전이면 아직 장 마감 전
            # FDR은 현재 시점 데이터를 줌 → 장중이면 오늘, 아니면 전일
            pass
        return today
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
                amount = row.get("Amount", 0)       # 거래대금 (원)
                marcap = row.get("Marcap", 0)        # 시가총액 (원)

                if not code or not name or close == 0:
                    continue

                # 원 → 억원 변환
                trading_value = amount / 1e8
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
                    "three_month_return": None,
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
                        "three_month_return": None,
                    })
                    count += 1
                except Exception:
                    continue

            print(f"  [{market}] {count}개 종목 수집 완료")
        except Exception as e:
            print(f"  [{market}] pykrx 오류: {e}")

    return all_stocks


def fetch_all_stocks(date_str):
    """FDR 우선 시도 → 실패 시 pykrx fallback"""
    all_stocks = []

    if HAS_FDR:
        all_stocks = fetch_all_stocks_fdr()

    if not all_stocks and HAS_PYKRX:
        print("[FDR 데이터 없음 → pykrx fallback]")
        all_stocks = fetch_all_stocks_pykrx(date_str)

    if not all_stocks:
        print("[ERROR] 모든 데이터 소스 실패")

    return all_stocks


# ═══════════════════════════════════════════
#   2. 필터 & 섹터/뉴스 보강
# ═══════════════════════════════════════════

def apply_filters(all_stocks, filters=None):
    """필터 조건 적용"""
    if filters is None:
        filters = DEFAULT_FILTERS
    filtered = []
    for s in all_stocks:
        if (s["trading_value"] >= filters["min_trading_value"] and
            s["change_rate"] >= filters["min_change_rate"] and
            0 < s["market_cap"] <= filters["max_market_cap"]):
            filtered.append(s)
    filtered.sort(key=lambda x: x["trading_value"], reverse=True)
    return filtered


def fetch_naver_sector(ticker):
    """네이버 금융에서 업종(섹터) 정보"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        resp = requests.get(url, headers=HEADERS, timeout=5)
        # 패턴 1: 업종 <a> 태그
        for pattern in [
            r'업종[^<]*<a[^>]*>([^<]+)</a>',
            r'class="sub_tit"[^>]*>\s*<a[^>]*>([^<]+)</a>',
            r'업종\s*</th>\s*<td[^>]*>\s*<a[^>]*>([^<]+)</a>',
            r'sise_industry\.naver\?type_code=[^"]*"[^>]*>([^<]+)<',
        ]:
            m = re.search(pattern, resp.text, re.DOTALL)
            if m:
                sector = m.group(1).strip()
                if sector and len(sector) < 30:
                    return sector
    except Exception:
        pass
    return None


def fetch_naver_news(ticker):
    """네이버 금융 최신 뉴스 제목"""
    try:
        url = f"https://finance.naver.com/item/news.naver?code={ticker}"
        resp = requests.get(url, headers=HEADERS, timeout=5)
        matches = re.findall(r'class="tit">\s*<a[^>]*title="([^"]+)"', resp.text)
        if matches:
            return matches[0][:80]
    except Exception:
        pass
    return None


def enrich_filtered_stocks(filtered_stocks, all_stocks):
    """필터 통과 종목에 섹터/뉴스/동반상승 추가"""
    if not filtered_stocks:
        return filtered_stocks

    print("\n[섹터/뉴스 수집 중...]")

    # 1) 섹터 & 뉴스
    for s in filtered_stocks:
        time.sleep(0.3)
        sector = fetch_naver_sector(s["ticker"])
        s["sector"] = sector or "기타"
        news = fetch_naver_news(s["ticker"])
        s["news"] = news or ""
        status = "섹터=" + s["sector"]
        if news:
            status += ", 뉴스O"
        print(f"  {s['name']}: {status}")

    # 2) 동일섹터 동반상승
    print("\n[동일섹터 동반상승 검색 중...]")
    sectors = set(s["sector"] for s in filtered_stocks if s["sector"] != "기타")

    # 등락률 5% 이상 상승 종목만 후보로
    rising_candidates = [
        s for s in all_stocks
        if s["change_rate"] >= 5.0
        and s["ticker"] not in {fs["ticker"] for fs in filtered_stocks}
    ]
    rising_candidates.sort(key=lambda x: x["change_rate"], reverse=True)

    sector_companions = {}
    for sector in sectors:
        companions = []
        checked = 0
        for r in rising_candidates[:100]:
            if checked >= 15:
                break
            time.sleep(0.2)
            try:
                r_sector = fetch_naver_sector(r["ticker"])
                checked += 1
                if r_sector == sector:
                    companions.append({"name": r["name"], "change_rate": r["change_rate"]})
                    if len(companions) >= 4:
                        break
            except Exception:
                continue

        companions.sort(key=lambda x: x["change_rate"], reverse=True)
        sector_companions[sector] = companions[:4]
        if companions:
            names = ", ".join(c["name"] + "(+" + str(c["change_rate"]) + "%)" for c in companions)
            print(f"  {sector}: {names}")

    for s in filtered_stocks:
        s["companions"] = sector_companions.get(s["sector"], [])

    return filtered_stocks


# ═══════════════════════════════════════════
#   3. 하이라이트 & 데이터 저장
# ═══════════════════════════════════════════

def generate_highlights(filtered_stocks):
    """주목 포인트 생성"""
    highlights = []

    # 섹터별 그룹핑
    sg = {}
    for s in filtered_stocks:
        sec = s.get("sector", "기타")
        sg.setdefault(sec, []).append(s)

    for sec, stks in sg.items():
        if len(stks) >= 2 and sec != "기타":
            names = ", ".join(s["name"] for s in stks)
            highlights.append("🔥 " + sec + " 테마 집중 — " + str(len(stks)) + "개 (" + names + ")")

    # 상한가
    for s in filtered_stocks:
        if s["change_rate"] >= 29.5:
            highlights.append("🚀 상한가: " + s["name"] + "(+" + str(s["change_rate"]) + "%)")

    # 3개월 급등 지속
    momentum = [s for s in filtered_stocks if s.get("three_month_return") and s["three_month_return"] >= 100]
    if momentum:
        details = ", ".join(
            s["name"] + " (3M +" + str(s["three_month_return"]) + "%)"
            for s in momentum
        )
        highlights.append("⚠️ 단기 급등 지속 (추격 주의): " + details)

    # 테마 키워드 감지
    energy = [s for s in filtered_stocks if any(kw in s["name"] for kw in ["에너지", "이앤씨", "E&A", "솔라", "풍력", "태양"])]
    if len(energy) >= 2:
        names = ", ".join(s["name"] for s in energy)
        highlights.append("⚡ 에너지/건설 관련주 동반 급등 — " + names)

    return highlights


def build_data_json(date_str, filtered_stocks, all_stocks, highlights, near_miss):
    """data.json 생성"""
    kospi_filtered = sum(1 for s in filtered_stocks if s["market"] == "KOSPI")
    kosdaq_filtered = sum(1 for s in filtered_stocks if s["market"] == "KOSDAQ")
    kospi_total = sum(1 for s in all_stocks if s["market"] == "KOSPI")
    kosdaq_total = sum(1 for s in all_stocks if s["market"] == "KOSDAQ")

    data = {
        "date": date_str,
        "generated_at": datetime.datetime.now().isoformat(),
        "filter_defaults": DEFAULT_FILTERS,
        "total_filtered": len(filtered_stocks),
        "stocks": filtered_stocks,
        "near_miss": near_miss[:10],
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

    print(f"\n[저장] {DATA_JSON}")
    return data


# ═══════════════════════════════════════════
#   4. Git 커밋 & 푸시
# ═══════════════════════════════════════════

def git_commit_and_push(date_str):
    """Git 자동 커밋 & 푸시"""
    os.chdir(SCRIPT_DIR)

    # stale lock 자동 제거
    for lock in [".git/HEAD.lock", ".git/index.lock"]:
        lock_path = SCRIPT_DIR / lock
        if lock_path.exists():
            try:
                lock_path.unlink()
                print(f"[Git] stale lock 제거: {lock}")
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

        print("[Git] 커밋 완료: " + commit_msg)

        # Push
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
#   5. 메인
# ═══════════════════════════════════════════

def main():
    print("=" * 60)
    print("  KOSPI/KOSDAQ 급등 스크리닝 봇")
    print("  데이터: FinanceDataReader (KRX 원본) + pykrx fallback")
    print("=" * 60)

    # 1) 거래일
    target = get_trading_date()
    date_str = target.strftime("%Y-%m-%d")
    date_str_compact = target.strftime("%Y%m%d")
    print(f"\n📅 대상일: {date_str}")

    # 2) 전 종목 수집
    all_stocks = fetch_all_stocks(date_str_compact)
    if not all_stocks:
        print("\n❌ 데이터 수집 실패. 종료.")
        return None

    kospi_n = sum(1 for s in all_stocks if s["market"] == "KOSPI")
    kosdaq_n = sum(1 for s in all_stocks if s["market"] == "KOSDAQ")
    print(f"\n총 {len(all_stocks)}개 종목 수집 (KOSPI {kospi_n} / KOSDAQ {kosdaq_n})")

    # 3) 필터
    filtered = apply_filters(all_stocks)
    print(f"\n[필터 결과] {len(filtered)}개 종목 통과")
    print(f"  (거래대금 {DEFAULT_FILTERS['min_trading_value']:,}억↑, "
          f"등락률 +{DEFAULT_FILTERS['min_change_rate']}%↑, "
          f"시총 {DEFAULT_FILTERS['max_market_cap']:,}억↓)")
    print()
    for s in filtered:
        print(f"  {s['name']} ({s['ticker']}) | +{s['change_rate']}% | "
              f"거래대금 {s['trading_value']:,.0f}억 | 시총 {s['market_cap']:,.0f}억")

    # near-miss: 거래대금 500억+ 이지만 2000억 미만
    near_miss = [
        s for s in all_stocks
        if s["change_rate"] >= DEFAULT_FILTERS["min_change_rate"]
        and 500 <= s["trading_value"] < DEFAULT_FILTERS["min_trading_value"]
        and 0 < s["market_cap"] <= DEFAULT_FILTERS["max_market_cap"]
    ]
    near_miss.sort(key=lambda x: x["trading_value"], reverse=True)

    # 4) 섹터/뉴스 보강
    if filtered:
        filtered = enrich_filtered_stocks(filtered, all_stocks)

    # 5) 하이라이트
    highlights = generate_highlights(filtered)

    # 6) JSON 저장
    data = build_data_json(date_str, filtered, all_stocks, highlights, near_miss)

    # 7) Git 푸시
    git_commit_and_push(date_str)

    # 8) 결과 요약
    print("\n" + "=" * 60)
    print("  📊 결과 요약")
    print("=" * 60)
    print(f"  대상일: {date_str}")
    print(f"  수집: KOSPI {kospi_n}개, KOSDAQ {kosdaq_n}개")
    print(f"  필터 통과: {len(filtered)}개")
    if filtered:
        print()
        for s in filtered:
            sec = s.get("sector", "")
            news_str = ""
            if s.get("news"):
                news_str = " | " + s["news"]
            print(f"  • {s['name']} ({s['ticker']}): +{s['change_rate']}% | "
                  f"TV {s['trading_value']:,.0f}억 | MC {s['market_cap']:,.0f}억 | {sec}{news_str}")
    if near_miss:
        print(f"\n  📌 기준 근접 ({len(near_miss)}개):")
        for s in near_miss[:5]:
            print(f"    {s['name']}: +{s['change_rate']}% | TV {s['trading_value']:,.0f}억")
    if highlights:
        print(f"\n  🔍 주목:")
        for h in highlights:
            print(f"    {h}")
    print(f"\n  🌐 https://whysosary-dot.github.io/stock-screener/")
    print("\n✅ 완료!")
    return data


if __name__ == "__main__":
    main()
