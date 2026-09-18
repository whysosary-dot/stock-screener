#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stock-screener 일일 스크리닝 파이프라인 (SKILL.md 전체 로직의 스크립트판 — 기능 동일)

사용법:
  python3 scripts/daily_screen.py            # 수집→필터→수익률→빌드→GitHub 푸시까지 전체 실행
  python3 scripts/daily_screen.py --no-push  # 푸시 없이 로컬 빌드만

- 수집: finance.naver.com sise_market_sum (KOSPI+KOSDAQ 전종목)
- 필터: 거래대금 1,000억↑
- 수익률: api.finance.naver.com siseJson (일별 OHLCV JSON) 30/60/90일 전 종가, ThreadPool 16
- 휴장 감지: 필터 통과 종목의 80%+ 등락률 0.00% → 휴장으로 판단, 푸시 스킵
- 푸시: data.json, daily/{date}.json, daily/index.json (GitHub Contents API)
출력: 요약 수치만 (하이라이트 / 상·하한가 / 커밋 결과)
"""
import json, os, re, sys, time, base64
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

GH_TOKEN = os.environ.get('GH_TOKEN') or ''
REPO = 'whysosary-dot/stock-screener'
HEADERS_GH = {'Authorization': f'token {GH_TOKEN}', 'Accept': 'application/vnd.github.v3+json'}
UA = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}

def fetch(url, retries=2):
    for i in range(retries + 1):
        try:
            r = requests.get(url, headers=UA, timeout=15)
            if r.ok:
                return r
        except Exception:
            pass
        time.sleep(0.3)
    return None

def parse_change_rate(col4):
    s = col4.text.strip().replace('%', '').replace(',', '').replace('+', '')
    try:
        return float(s)
    except Exception:
        return None

def collect_market(market_name):
    """market_name: 'KOSPI' or 'KOSDAQ'.
    구 finance.naver.com/sise/sise_market_sum.naver 는 stock.naver.com 신규 UI로
    리다이렉트되며 정적 HTML에 표가 없어 스크레이핑 불가 (2026-09 확인).
    대체: m.stock.naver.com 모바일 시가총액 API (JSON) 사용."""
    stocks, page, page_size, total = [], 1, 100, None
    while True:
        r = fetch(f'https://m.stock.naver.com/api/stocks/marketValue/{market_name}?page={page}&pageSize={page_size}')
        if not r:
            break
        try:
            d = r.json()
        except Exception:
            break
        items = d.get('stocks') or []
        if not items:
            break
        if total is None:
            total = d.get('totalCount', 0)
        for it in items:
            if it.get('stockEndType') != 'stock':
                continue
            try:
                ticker = it['itemCode']
                price = int(it['closePriceRaw'])
                change_rate = float(it['fluctuationsRatio'])
                mcap = round(int(it['marketValueRaw']) / 100000000)
                volume = int(it['accumulatedTradingVolumeRaw'])
                trading_value = round(int(it['accumulatedTradingValueRaw']) / 100000000, 1)
            except Exception:
                continue
            stocks.append({
                'ticker': ticker, 'name': it.get('stockName', ''), 'market': market_name,
                'price': price, 'change_rate': change_rate,
                'market_cap': mcap, 'volume': volume,
                'trading_value': trading_value,
            })
        page += 1
        if total is not None and (page - 1) * page_size >= total:
            break
        time.sleep(0.05)
    return stocks

def get_returns(ticker, price):
    """30/60/90일 전(달력일) 종가 → 1/2/3개월 수익률.
    finance.naver.com/item/sise_day 는 2026-09 부터 신규 UI 로 리다이렉트되어 표가 없다.
    api.finance.naver.com/siseJson.naver (일별 OHLCV JSON) 로 대체."""
    today = datetime.now()
    start = (today - timedelta(days=110)).strftime('%Y%m%d')
    end = today.strftime('%Y%m%d')
    r = fetch(f'https://api.finance.naver.com/siseJson.naver?symbol={ticker}&requestType=1'
              f'&startTime={start}&endTime={end}&timeframe=day')
    out = {'return_1m': None, 'return_2m': None, 'return_3m': None}
    if not r:
        return out
    try:
        rows = [json.loads(m) for m in re.findall(r'\["\d{8}".*?\]', r.text)]
    except Exception:
        return out
    closes = []
    for row in rows:
        try:
            closes.append((datetime.strptime(str(row[0]), '%Y%m%d'), float(row[4])))
        except Exception:
            continue
    if not closes:
        return out
    for k, target in (('1m', 30), ('2m', 60), ('3m', 90)):
        past = None
        for d, c in closes:                       # 오름차순 — target 일 이전의 마지막 거래일 종가
            if (today - d).days >= target and c > 0:
                past = c
            else:
                break
        if past:
            out[f'return_{k}'] = round((price - past) / past * 100, 1)
    return out

def build_highlights(stocks):
    h = []
    ups = [s for s in stocks if (s['change_rate'] or 0) >= 29.5]
    downs = [s for s in stocks if (s['change_rate'] or 0) <= -29.5 and abs(s['change_rate'] or 0) < 100]
    if ups:
        h.append('🚀 상한가: ' + ', '.join(s['name'] for s in ups[:10]))
    if downs:
        h.append('💥 하한가: ' + ', '.join(s['name'] for s in downs[:10]))
    top3 = sorted(stocks, key=lambda s: s['trading_value'], reverse=True)[:3]
    if top3:
        h.append('💰 거래대금 TOP3: ' + ', '.join(f"{s['name']}({s['trading_value']:,.0f}억)" for s in top3))
    surge = [s for s in stocks if (s['change_rate'] or 0) >= 15]
    if len(surge) >= 3:
        h.append(f'🔥 급등(+15%↑) {len(surge)}개 종목')
    plunge = [s for s in stocks if (s['change_rate'] or 0) <= -10]
    if len(plunge) >= 3:
        h.append(f'⚠️ 급락(-10%↓) {len(plunge)}개 종목')
    hot = [s for s in stocks if (s.get('return_3m') or 0) >= 100 and (s['change_rate'] or 0) >= 10]
    if hot:
        h.append('⚠️ 단기 급등 지속 (추격 주의): ' + ', '.join(s['name'] for s in hot[:8]))
    return h

def gh_get_sha(path):
    r = requests.get(f'https://api.github.com/repos/{REPO}/contents/{path}?ref=main', headers=HEADERS_GH)
    return r.json().get('sha') if r.status_code == 200 else None

def gh_put(path, content_str, message, sha=None):
    b64 = base64.b64encode(content_str.encode('utf-8')).decode('ascii')
    body = {'message': message, 'content': b64, 'branch': 'main',
            'committer': {'name': '리송', 'email': 'whysosary@naver.com'}}
    if sha:
        body['sha'] = sha
    r = requests.put(f'https://api.github.com/repos/{REPO}/contents/{path}', headers=HEADERS_GH, json=body)
    if not r.ok:
        print(f'PUT {path} FAILED:', r.status_code, r.text[:200])
    return r.ok

def main():
    push = '--no-push' not in sys.argv
    date = datetime.now().strftime('%Y-%m-%d')

    print('1/5 KOSPI 수집...', flush=True)
    kospi = collect_market('KOSPI')
    print(f'  KOSPI {len(kospi)}개', flush=True)
    print('2/5 KOSDAQ 수집...', flush=True)
    kosdaq = collect_market('KOSDAQ')
    print(f'  KOSDAQ {len(kosdaq)}개', flush=True)
    all_stocks = kospi + kosdaq
    if len(all_stocks) < 1000:
        print(f'❌ 수집 실패 의심 (총 {len(all_stocks)}개) — 중단'); sys.exit(1)

    filtered = [s for s in all_stocks if s['trading_value'] >= 0]
    print(f'3/5 필터 통과 {len(filtered)}개 — 수익률 계산...', flush=True)

    # 휴장 감지
    zero = sum(1 for s in filtered if not s['change_rate'])
    if filtered and zero / len(filtered) > 0.8:
        print('⏸ 공휴일/휴장일로 판단 (등락률 0 비중 80%↑) — 업데이트 없음'); return

    with ThreadPoolExecutor(max_workers=16) as ex:
        rets = list(ex.map(lambda s: get_returns(s['ticker'], s['price']), filtered))
    for s, r in zip(filtered, rets):
        s.update(r)

    print('4/5 데이터 빌드...', flush=True)
    filtered.sort(key=lambda s: s['change_rate'] if s['change_rate'] is not None else -999, reverse=True)
    limit_stocks = []
    for s in all_stocks:
        cr = s.get('change_rate')
        if cr is not None and (cr >= 27.0 or cr <= -27.0):
            limit_stocks.append({k: s[k] for k in
                ('ticker', 'name', 'market', 'price', 'change_rate', 'market_cap', 'volume', 'trading_value')})
    limit_stocks.sort(key=lambda x: x['change_rate'], reverse=True)

    data_out = {
        'date': date,
        'generated_at': datetime.now().isoformat(),
        'filter_defaults': {'min_trading_value': 0, 'min_change_rate': -30.0, 'max_change_rate': 30.0,
                            'max_market_cap': 20000000, 'sort_by': 'change_rate', 'sort_order': 'desc'},
        'total_filtered': len(filtered),
        'stocks': filtered,
        'highlights': build_highlights(filtered),
        'limit_stocks': limit_stocks,
        'all_stocks_summary': {'kospi_count': len([s for s in filtered if s['market'] == 'KOSPI']),
                               'kosdaq_count': len([s for s in filtered if s['market'] == 'KOSDAQ']),
                               'total_kospi': len(kospi), 'total_kosdaq': len(kosdaq),
                               'total': len(all_stocks)},
    }

    # daily/index.json 갱신
    r = requests.get(f'https://api.github.com/repos/{REPO}/contents/daily/index.json?ref=main', headers=HEADERS_GH)
    existing_idx, existing_sha = {'dates': []}, None
    if r.status_code == 200:
        j = r.json()
        existing_sha = j['sha']
        try:
            existing_idx = json.loads(base64.b64decode(j['content']).decode('utf-8'))
        except Exception:
            pass
    summary = data_out['all_stocks_summary']
    dt = datetime.strptime(date, '%Y-%m-%d')
    weekday = ['월', '화', '수', '목', '금', '토', '일'][dt.weekday()]
    lu = len([s for s in limit_stocks if s['change_rate'] >= 27])
    ld = len([s for s in limit_stocks if s['change_rate'] <= -27])
    new_entry = {
        'date': date,
        'label': f'{dt.year}년 {dt.month}월 {dt.day}일 ({weekday})',
        'sub': (f'필터통과 {len(filtered)}개 · KOSPI {summary["kospi_count"]}개 + KOSDAQ {summary["kosdaq_count"]}개'
                f' · 전체 수집 {summary["total_kospi"] + summary["total_kosdaq"]:,}개'),
        'total_filtered': len(filtered),
        'kospi_count': summary['kospi_count'], 'kosdaq_count': summary['kosdaq_count'],
        'total_kospi': summary['total_kospi'], 'total_kosdaq': summary['total_kosdaq'],
        'limit_up': lu, 'limit_down': ld,
        'generated_at': data_out['generated_at'],
    }
    dates = [d for d in existing_idx.get('dates', []) if d.get('date') != date]
    dates.append(new_entry)
    dates.sort(key=lambda x: x['date'], reverse=True)
    for i, d in enumerate(dates):
        d['new'] = (i == 0)
    idx_out = {'updated_at': datetime.now().isoformat(),
               'latest_date': dates[0]['date'] if dates else None,
               'count': len(dates), 'dates': dates}

    data_json_str = json.dumps(data_out, ensure_ascii=False, indent=2)
    idx_json_str = json.dumps(idx_out, ensure_ascii=False, indent=2)

    # BASE env가 있으면 로컬 사본에도 저장 (마운트된 워크스페이스 동기화 — 기존 동작 유지)
    base = os.environ.get('BASE')
    if base and os.path.isdir(base):
        try:
            os.makedirs(os.path.join(base, 'daily'), exist_ok=True)
            for p, s in ((os.path.join(base, 'data.json'), data_json_str),
                         (os.path.join(base, 'daily', f'{date}.json'), data_json_str),
                         (os.path.join(base, 'daily', 'index.json'), idx_json_str)):
                with open(p, 'w', encoding='utf-8') as f:
                    f.write(s)
            print(f'로컬 사본 저장: {base}')
        except Exception as e:
            print(f'로컬 사본 저장 실패(무시): {e}')

    if push:
        print('5/5 GitHub 푸시...', flush=True)
        msg = (f'📊 스크리닝 업데이트: {date} (KOSPI {summary["kospi_count"]}+KOSDAQ {summary["kosdaq_count"]}'
               f'={summary["kospi_count"] + summary["kosdaq_count"]} → 필터 {len(filtered)}개)')
        ok1 = gh_put('data.json', data_json_str, msg, gh_get_sha('data.json'))
        ok2 = gh_put(f'daily/{date}.json', data_json_str, msg, gh_get_sha(f'daily/{date}.json'))
        ok3 = gh_put('daily/index.json', idx_json_str, msg, existing_sha)
        print(f'푸시 결과: data.json={ok1}, daily/{date}.json={ok2}, daily/index.json={ok3}')
    else:
        with open('data_preview.json', 'w', encoding='utf-8') as f:
            f.write(data_json_str)
        print('(--no-push) data_preview.json 저장')

    # ── 요약 보고 (이것만 읽으면 됨) ──
    print('\n===== 요약 =====')
    print(f'날짜 {date} | 전체 {len(all_stocks):,}개 수집 (KOSPI {len(kospi)}+KOSDAQ {len(kosdaq)}) | 필터통과 {len(filtered)}개')
    print(f'상한가 {lu}개 / 하한가 {ld}개')
    for h in data_out['highlights']:
        print(' ·', h)
    top10 = sorted(filtered, key=lambda s: s['trading_value'], reverse=True)[:10]
    print('거래대금 TOP10: ' + ', '.join(f"{s['name']}({s['trading_value']:,.0f}억)" for s in top10))

if __name__ == '__main__':
    main()
