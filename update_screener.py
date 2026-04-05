#!/usr/bin/env python3
"""
KOSPI/KOSDAQ 급등 스크리닝 봇
- pykrx로 일별 거래대금/등락률/시총 데이터 수집
- 조건 필터링 후 HTML 생성
- GitHub Pages 자동 커밋 & 푸시
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

# ─── pykrx import ───
try:
    from pykrx import stock
except ImportError:
    print("[ERROR] pykrx 미설치. pip install pykrx 실행 필요")
    sys.exit(1)


# ─── 설정 ───
SCRIPT_DIR = Path(__file__).parent.resolve()
OUTPUT_HTML = SCRIPT_DIR / "index.html"
DATA_JSON = SCRIPT_DIR / "data.json"

# 필터 기본값 (HTML에서 사용자 조절 가능)
DEFAULT_FILTERS = {
    "min_trading_value": 2000,   # 거래대금 최소 (억원)
    "min_change_rate": 10.0,     # 등락률 최소 (%)
    "max_market_cap": 300000,    # 시총 최대 (억원 = 30조)
}


# ─── 섹터 매핑 (KRX 업종코드) ───
def get_sector_map(date_str, market):
    """종목별 섹터 정보를 가져온다"""
    sector_map = {}
    try:
        tickers = stock.get_market_ticker_list(date_str, market=market)
        for ticker in tickers:
            try:
                name = stock.get_market_ticker_name(ticker)
                # 업종 정보는 get_market_sector_classifications 또는 개별 조회
                sector_map[ticker] = {"name": name}
            except:
                pass
    except:
        pass
    return sector_map


def get_trading_date():
    """최근 거래일 구하기 (오늘 또는 직전 영업일)"""
    today = datetime.date.today()

    # 주말이면 금요일로
    if today.weekday() == 5:  # 토요일
        today = today - datetime.timedelta(days=1)
    elif today.weekday() == 6:  # 일요일
        today = today - datetime.timedelta(days=2)

    date_str = today.strftime("%Y%m%d")

    # pykrx로 해당일 데이터 확인, 없으면 하루씩 뒤로
    for i in range(5):
        try:
            test_date = (today - datetime.timedelta(days=i)).strftime("%Y%m%d")
            df = stock.get_market_ohlcv_by_ticker(test_date, market="KOSPI")
            if len(df) > 0:
                return test_date
        except:
            continue

    return date_str


def fetch_market_data(date_str, market="KOSPI"):
    """
    특정 시장의 전 종목 데이터 수집
    Returns: list of dict
    """
    results = []

    try:
        # 1) OHLCV (시가/고가/저가/종가/거래량/거래대금)
        df_ohlcv = stock.get_market_ohlcv_by_ticker(date_str, market=market)
        if df_ohlcv.empty:
            print(f"  [{market}] OHLCV 데이터 없음 (휴장일?)")
            return results

        # 2) 시가총액
        df_cap = stock.get_market_cap_by_ticker(date_str, market=market)

        # 3) 등락률
        df_change = stock.get_market_price_change(date_str, date_str, market=market)

        # 4) 3개월 전 날짜 (수익률 계산용)
        target_date = datetime.datetime.strptime(date_str, "%Y%m%d")
        three_months_ago = (target_date - datetime.timedelta(days=90)).strftime("%Y%m%d")

        tickers = df_ohlcv.index.tolist()

        for ticker in tickers:
            try:
                name = stock.get_market_ticker_name(ticker)

                # OHLCV
                close = int(df_ohlcv.loc[ticker, "종가"]) if ticker in df_ohlcv.index else 0
                volume = int(df_ohlcv.loc[ticker, "거래량"]) if ticker in df_ohlcv.index else 0

                # 거래대금 (원 → 억원)
                trading_value_raw = int(df_ohlcv.loc[ticker, "거래대금"]) if ticker in df_ohlcv.index else 0
                trading_value = trading_value_raw / 100_000_000  # 억원

                # 시가총액 (원 → 억원)
                market_cap_raw = int(df_cap.loc[ticker, "시가총액"]) if ticker in df_cap.index else 0
                market_cap = market_cap_raw / 100_000_000  # 억원

                # 등락률
                change_rate = 0.0
                if ticker in df_change.index:
                    try:
                        change_rate = float(df_change.loc[ticker, "등락률"])
                    except:
                        # 전일 종가 대비 직접 계산
                        prev_close = int(df_ohlcv.loc[ticker, "시가"]) if ticker in df_ohlcv.index else 0
                        if prev_close > 0:
                            change_rate = ((close - prev_close) / prev_close) * 100

                if close == 0 or trading_value == 0:
                    continue

                # 3개월 수익률 계산
                three_month_return = None
                try:
                    df_hist = stock.get_market_ohlcv(three_months_ago, date_str, ticker)
                    if len(df_hist) >= 2:
                        old_close = int(df_hist.iloc[0]["종가"])
                        if old_close > 0:
                            three_month_return = round(((close - old_close) / old_close) * 100, 1)
                except:
                    pass

                results.append({
                    "ticker": ticker,
                    "name": name,
                    "market": market,
                    "close": close,
                    "volume": volume,
                    "trading_value": round(trading_value, 0),  # 억원
                    "market_cap": round(market_cap, 0),         # 억원
                    "change_rate": round(change_rate, 1),
                    "three_month_return": three_month_return,
                })

            except Exception as e:
                continue

        print(f"  [{market}] {len(results)}개 종목 수집 완료")

    except Exception as e:
        print(f"  [{market}] 데이터 수집 오류: {e}")

    return results


def get_sector_info(date_str, market="KOSPI"):
    """섹터별 종목 분류"""
    sector_data = {}
    try:
        # KRX 업종 분류 사용
        from pykrx.website.krx.market import wrap
        # 업종별 시세 조회
        tickers = stock.get_market_ticker_list(date_str, market=market)
        for ticker in tickers:
            try:
                # pykrx에서 업종 정보 가져오기
                sector = stock.get_market_ticker_name(ticker)  # fallback
                sector_data[ticker] = "기타"
            except:
                pass
    except:
        pass
    return sector_data


def fetch_naver_sector(ticker):
    """네이버 금융에서 섹터 정보 크롤링"""
    try:
        url = f"https://finance.naver.com/item/main.nhn?code={ticker}"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=5)

        # 업종 추출
        match = re.search(r'class="sub_tit">\s*<a[^>]*>([^<]+)</a>', resp.text)
        if match:
            return match.group(1).strip()

        # 대체 패턴
        match2 = re.search(r'업종\s*</th>\s*<td[^>]*>\s*<a[^>]*>([^<]+)</a>', resp.text)
        if match2:
            return match2.group(1).strip()
    except:
        pass
    return None


def fetch_naver_news(ticker):
    """네이버 금융에서 최신 뉴스 제목 크롤링"""
    try:
        url = f"https://finance.naver.com/item/news.nhn?code={ticker}"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=5)

        # 뉴스 제목 추출
        matches = re.findall(r'class="tit">\s*<a[^>]*title="([^"]+)"', resp.text)
        if matches:
            return matches[0][:80]  # 첫 번째 뉴스, 80자 제한
    except:
        pass
    return None


def enrich_with_sector_and_news(filtered_stocks, all_stocks):
    """필터 통과 종목에 섹터/뉴스/동일섹터 정보 추가"""
    print("\n[섹터/뉴스 정보 수집 중...]")

    # 섹터 정보 수집 (필터 통과 종목)
    for s in filtered_stocks:
        time.sleep(0.3)  # rate limit
        sector = fetch_naver_sector(s["ticker"])
        s["sector"] = sector or "기타"

        news = fetch_naver_news(s["ticker"])
        s["news"] = news or ""

        print(f"  {s['name']}: 섹터={s['sector']}, 뉴스={'있음' if news else '없음'}")

    # 동일섹터 동반상승 종목 찾기
    # 필터 통과 종목의 섹터에 해당하는 다른 종목들 조회
    sectors_to_check = set(s["sector"] for s in filtered_stocks if s["sector"] != "기타")

    sector_companions = {}
    for sector in sectors_to_check:
        companions = []
        for s in all_stocks:
            if s["ticker"] not in [fs["ticker"] for fs in filtered_stocks]:
                if s["change_rate"] > 0:
                    # 해당 섹터 종목인지 확인 (전체 종목의 섹터 조회는 너무 느려서,
                    # 등락률 상위 종목만 섹터 확인)
                    if s["change_rate"] >= 5:  # 5% 이상 상승 종목만 체크
                        time.sleep(0.2)
                        s_sector = fetch_naver_sector(s["ticker"])
                        if s_sector == sector:
                            companions.append({
                                "name": s["name"],
                                "change_rate": s["change_rate"]
                            })

        # 등락률 순 정렬, 상위 4개
        companions.sort(key=lambda x: x["change_rate"], reverse=True)
        sector_companions[sector] = companions[:4]

    for s in filtered_stocks:
        s["companions"] = sector_companions.get(s["sector"], [])

    return filtered_stocks


def apply_filters(all_stocks, filters=None):
    """필터 조건 적용"""
    if filters is None:
        filters = DEFAULT_FILTERS

    filtered = []
    for s in all_stocks:
        if (s["trading_value"] >= filters["min_trading_value"] and
            s["change_rate"] >= filters["min_change_rate"] and
            s["market_cap"] <= filters["max_market_cap"]):
            filtered.append(s)

    # 거래대금 순 정렬
    filtered.sort(key=lambda x: x["trading_value"], reverse=True)
    return filtered


def generate_highlights(filtered_stocks):
    """오늘의 주목 포인트 생성"""
    highlights = []

    # 섹터별 그룹핑
    sector_groups = {}
    for s in filtered_stocks:
        sec = s.get("sector", "기타")
        if sec not in sector_groups:
            sector_groups[sec] = []
        sector_groups[sec].append(s)

    # 2개 이상 종목이 있는 섹터
    for sec, stocks in sector_groups.items():
        if len(stocks) >= 2:
            names = ", ".join(s["name"] for s in stocks)
            highlights.append(f"🔥 {sec} 테마 집중 — 필터 통과 종목 {len(stocks)}개 ({names})")

    # 상한가 종목
    for s in filtered_stocks:
        if s["change_rate"] >= 29.5:
            highlights.append(f"🚀 상한가 종목: {s['name']}(+{s['change_rate']}%)")

    # 단기 급등 지속 종목
    momentum_stocks = []
    for s in filtered_stocks:
        if s.get("three_month_return") and s["three_month_return"] >= 100:
            momentum_stocks.append(s)

    if momentum_stocks:
        details = ", ".join(
            f"{s['name']} (3개월 +{s['three_month_return']}%, 오늘 +{s['change_rate']}%)"
            for s in momentum_stocks
        )
        highlights.append(f"⚠️ 단기 급등 지속 종목 (추격 주의): {details}")

    return highlights


def build_data_json(date_str, filtered_stocks, all_stocks, highlights):
    """JSON 데이터 파일 생성 (HTML에서 로드)"""
    data = {
        "date": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}",
        "generated_at": datetime.datetime.now().isoformat(),
        "filter_defaults": DEFAULT_FILTERS,
        "total_filtered": len(filtered_stocks),
        "stocks": filtered_stocks,
        "highlights": highlights,
        "all_stocks_summary": {
            "kospi_count": len([s for s in all_stocks if s["market"] == "KOSPI"]),
            "kosdaq_count": len([s for s in all_stocks if s["market"] == "KOSDAQ"]),
            "total": len(all_stocks),
        }
    }

    with open(DATA_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n[데이터 저장] {DATA_JSON}")
    return data


def git_commit_and_push(date_str):
    """Git 커밋 & 푸시"""
    os.chdir(SCRIPT_DIR)

    try:
        # Add all changes
        subprocess.run(["git", "add", "-A"], check=True, capture_output=True)

        # Commit
        formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        commit_msg = f"📊 스크리닝 업데이트: {formatted_date}"

        result = subprocess.run(
            ["git", "commit", "-m", commit_msg],
            capture_output=True, text=True
        )

        if "nothing to commit" in result.stdout:
            print("[Git] 변경사항 없음")
            return

        print(f"[Git] 커밋 완료: {commit_msg}")

        # Push
        result = subprocess.run(
            ["git", "push", "origin", "main"],
            capture_output=True, text=True, timeout=30
        )

        if result.returncode == 0:
            print("[Git] 푸시 완료!")
        else:
            # main이 아닌 경우 master 시도
            result2 = subprocess.run(
                ["git", "push", "origin", "master"],
                capture_output=True, text=True, timeout=30
            )
            if result2.returncode == 0:
                print("[Git] 푸시 완료! (master)")
            else:
                print(f"[Git] 푸시 실패: {result.stderr} {result2.stderr}")

    except Exception as e:
        print(f"[Git] 오류: {e}")


def main():
    print("=" * 60)
    print("  KOSPI/KOSDAQ 급등 스크리닝 봇")
    print("=" * 60)

    # 1) 거래일 확인
    date_str = get_trading_date()
    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    print(f"\n📅 대상일: {formatted_date}")

    # 2) 데이터 수집
    print("\n[데이터 수집 시작]")
    all_stocks = []

    kospi_data = fetch_market_data(date_str, "KOSPI")
    all_stocks.extend(kospi_data)

    time.sleep(1)

    kosdaq_data = fetch_market_data(date_str, "KOSDAQ")
    all_stocks.extend(kosdaq_data)

    print(f"\n총 {len(all_stocks)}개 종목 수집")

    # 3) 필터 적용
    filtered = apply_filters(all_stocks)
    print(f"\n[필터 결과] {len(filtered)}개 종목 통과")
    for s in filtered:
        print(f"  {s['name']} ({s['ticker']}) | 거래대금 {s['trading_value']:,.0f}억 | +{s['change_rate']}% | 시총 {s['market_cap']:,.0f}억")

    # 4) 섹터/뉴스 정보 추가
    if filtered:
        filtered = enrich_with_sector_and_news(filtered, all_stocks)

    # 5) 하이라이트 생성
    highlights = generate_highlights(filtered)

    # 6) JSON 데이터 저장
    data = build_data_json(date_str, filtered, all_stocks, highlights)

    # 7) HTML은 이미 존재 (data.json만 업데이트하면 됨)
    # HTML이 없으면 경고
    if not OUTPUT_HTML.exists():
        print(f"\n[경고] index.html이 없습니다! 먼저 HTML 파일을 배치해주세요.")

    # 8) Git 커밋 & 푸시
    git_commit_and_push(date_str)

    print("\n✅ 스크리닝 완료!")
    return data


if __name__ == "__main__":
    main()
