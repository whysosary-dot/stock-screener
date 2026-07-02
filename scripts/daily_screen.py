#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stock-screener 일일 스크리닝 파이프라인 (SKILL.md 전체 로직의 스크립트판 — 기능 동일)

사용법:
  python3 scripts/daily_screen.py            # 수집→필터→수익률→빌드→GitHub 푸시까지 전체 실행
  python3 scripts/daily_screen.py --no-push  # 푸시 없이 로컬 빌드만

- 수집: finance.naver.com sise_market_sum (KOSPI+KOSDAQ 전종목)
- 필터: 거래대금 1,000억↑
- 수익률: sise_day page 1~9, table.type2, 종가 cols[1], ThreadPool 16
- 휴장 감지: 필터 통과 종목의 80%+ 등락률 0.00% → 휴장으로 판단, 푸시 스킵
- 푸시: data.json, daily/{date}.json, daily/index.json (GitHub Contents API)
출력: 요약 수치만 (하이라이트 / 상·하한가 / 커밋 결과)
"""
import json, os, re, sys, time, base64
from datetime import datetime
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

def collect_market(sosok, market_name):
    stocks, seen, page = [], set(), 1
    while True:
        r = fetch(f'https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}')
        if not r:
            break
        soup = BeautifulSoup(r.content.decode('euc-kr', 'ignore'), 'html.parser')
        rows = soup.select('table.type_2 tr')
        got = 0
        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 10:
                continue
            a = cols[1].find('a')
            if not a or 'code=' not in (a.get('href') or ''):
                continue
            ticker = a['href'].split('code=')[1][:6]
            if ticker in seen:
                continue
            seen.add(ticker)
            try:
                price = int(cols[2].text.strip().replace(',', ''))
                volume = int(cols[9].text.strip().replace(',', '')) if cols[9].text.strip() else 0
                mcap = int(cols[6].text.strip().replace(',', '')) if cols[6].text.strip() else 0
            except Exception:
                continue
            stocks.append({
                'ticker': ticker, 'name': a.text.strip(), 'market': market_name,
                'price': price, 'change_rate': parse_change_rate(cols[4]),
                'market_cap': mcap, 'volume': volume,
                'trading_value': round(price * volume / 100000000, 1),
            })
            got += 1
        if got == 0:
            break
        page += 1
        time.sleep(0.05)
    return stocks

def get_returns(ticker, price):
    """30/60/90일 전 종가 → 1/2/3개월 수익률"""
    targets = {'1m': 30, '2m': 60, '3m': 90}
    found = {}
    today = datetime.now()
    for page in range(1, 10):
        r = fetch(f'https://finance.naver.com/item/sise_day.naver?code={ticker}&page={page}')
        if not r:
            break
        soup = BeautifulSoup(r.content.decode('euc-kr', 'ignore'), 'html.parser')
        rows = soup.select('table.type2 tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 7:
                continue
            date_text = cols[0].text.strip()
            if not date_text or '.' not in date_text:
                continue
            try:
                row_date = datetime.strptime(date_text, '%Y.%m.%d')
                p = int(cols[1].text.strip().replace(',', ''))
            except Exception:
                continue
            days = (today - row_date).days
            for k, target in targets.items():
                if k not in found and days >= target:
                    found[k] = p
        if len(found) == 3:
            break
    out = {}
    for k in ('1m', '2m', '3m'):
        past = found.get(k)
        out[f'return_{k}'] = round((price - past) / past * 100, 1) if past else None
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
    kospi = collect_market(0, 'KOSPI')
    print(f'  KOSPI {len(kospi)}개', flush=True)
    print('2/5 KOSDAQ 수집...', flush=True)
    kosdaq = collect_market(1, 'KOSDAQ')
    print(f'  KOSDAQ {len(kosdaq)}개', flush=True)
    all_stocks = kospi + kosdaq
    if len(all_stocks) < 1000:
        print(f'❌ 수집 실패 의심 (총 {len(all_stocks)}개) — 중단'); sys.exit(1)

    filtered = [s for s in all_stocks if s['trading_value'] >= 1000]
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
        'filter_defaults': {'min_trading_value': 1000, 'min_change_rate': -30.0, 'max_change_rate': 30.0,
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
