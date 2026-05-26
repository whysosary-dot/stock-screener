---
name: stock-screener-daily
description: 평일 장 마감 후 KOSPI/KOSDAQ 스크리닝 (네이버 금융 전체 스캔 → 거래대금 1,000억↑ 필터 → 1/2/3개월 수익률 병렬 계산 → 일자별 JSON + index.json + GitHub API 푸시)
---

## 목표
매일 장 마감 후 KOSPI/KOSDAQ 전체 종목을 스크리닝하여 GitHub Pages에 자동 배포한다.

## 사이트 구조 (★ 2026-05-26 변경)
메인 페이지(`index.html`)에서 날짜 카드 목록을 보여주고, 클릭 시 `daily.html?date=YYYY-MM-DD`로 이동해 해당 일자 데이터를 표시한다. 즐겨찾기는 localStorage 기반이며 종목 카드 옆 ☆ 버튼으로 추가/메모 가능.

**파일 구조:**
```
stock-screener/
├── index.html         # 메인 — 날짜 카드 목록 + 즐겨찾기 탭 (정적)
├── daily.html         # 상세 — ?date=YYYY-MM-DD 로 일자 데이터 로드 (정적)
├── data.json          # 최신 일자 데이터 (호환성 유지)
└── daily/
    ├── index.json     # 날짜 목록 + 일자별 요약 통계
    ├── 2026-05-26.json
    └── ...
```

**매일 스킬이 새로 만드는 파일:**
- `daily/{date}.json` — 그날 데이터 (data.json과 동일 내용)
- `daily/index.json` — 신규 날짜 항목 추가 + 최신 항목 `new:true` 갱신
- `data.json` — 최신 일자 데이터 덮어쓰기 (호환성)

**스킬이 건드리지 않는 파일:** `index.html`, `daily.html`, 아이콘들

## 중요: 실행 환경 제약
- 이 태스크는 샌드박스 환경에서 실행되므로 **pykrx, FinanceDataReader는 KRX API 접근 불가**
- **사용자 Mac 터미널 접근 불가** (자동 실행 시 승인 팝업 대응 불가)
- 따라서 **네이버 금융 웹 크롤링 + GitHub REST API**로 직접 처리한다

## ★ 핵심 주의사항 (버그 수정 이력)

### ★★ 수집 페이지: sise_market_sum (2026-05-14 변경)
- **sise_quant.naver 사용 금지** — 거래량 상위 100개/시장만 노출, 현대차·NAVER 등 주요 대형주 누락
- **sise_market_sum.naver 사용** — 전종목(KOSPI ~2330개, KOSDAQ ~1810개) 페이지네이션 정상 작동
- URL: `https://finance.naver.com/sise/sise_market_sum.naver?sosok={0|1}&page={n}`
- 컬럼 구조:
  - cols[1]: 종목명 (a 태그 href → code= → ticker)
  - cols[2]: 현재가
  - cols[4]: 등락률 (%) — **+/- 부호 이미 포함, parse_change_rate()로 직접 파싱**
  - cols[6]: 시가총액 (억원)
  - cols[9]: 거래량 (주수)
- **거래대금(억원) = cols[2] × cols[9] / 100,000,000** (직접 계산)

### ★★ 등락률 부호 버그 (2026-05-14 수정)
- **red02 클래스 = 상승(양수)** — 절대 하락 판단에 사용하지 말 것
- **올바른 파싱**: cols[4] 텍스트에 이미 +/- 부호 포함 → 그대로 파싱

```python
def parse_change_rate(col4):
    s = col4.text.strip().replace('%', '').replace(',', '').replace('+', '')
    try: return float(s)
    except: return None
```

### ★★ sise_day 수익률 파싱 (2026-05-15 수정) — 반드시 준수
- **셀렉터: `table.type2 tr`** ← `table.type1` 사용 시 rows=0, 수익률 전부 null 버그 발생
- **종가 컬럼: `cols[1]`** ← `cols[4]` 사용 금지 (cols[4]는 고가)
- 날짜: `cols[0]` (형식: `2026.05.15`)
- 올바른 파싱 코드:
```python
rows = soup.select('table.type2 tr')   # ← type2 필수
for row in rows:
    cols = row.find_all('td')
    if len(cols) < 7:
        continue
    date_text = cols[0].text.strip()
    if not date_text or '.' not in date_text:
        continue
    row_date = datetime.strptime(date_text, '%Y.%m.%d')
    p = int(cols[1].text.strip().replace(',', ''))  # ← cols[1] 종가
```

### ★★ sise_day 페이지 수 (2026-05-20 수정) — 반드시 준수
- **page 1~9 순회** (기존 1~6에서 변경)
- **page 1페이지 = 약 10 거래일** → 6페이지 = 약 60일 → **90일(3개월) 수익률은 최소 page 7 이상 필요**
- 실측: page 6 마지막 날짜 ≈ 70일 전, 3개월 타겟은 page 7에 존재
- **`for page in range(1, 10):`** — 절대 range(1, 7) 사용 금지

### ★★ data.json stocks 배열 필드명 (2026-05-15 확정)
- **`price`** : 현재가 (정수, 원) ← `current_price`/`close` 사용 금지
- **`change_rate`** : 등락률 (float, %)
- **`market_cap`** : 시가총액 (억원, 정수)
- **`volume`** : 거래량 (주수, 정수)
- **`trading_value`** : 거래대금 (억원, float)
- **`return_1m`** : 1개월 수익률 (float or null)
- **`return_2m`** : 2개월 수익률 (float or null)
- **`return_3m`** : 3개월 수익률 (float or null)

### ★ 기본 필터/정렬 설정 (사용자 지정)
- **거래대금 기본 필터: 1,000억 이상**
- **기본 정렬: 등락률순**

## 실행 단계

### 0단계: 패키지 설치
```
pip install requests beautifulsoup4 --break-system-packages -q
```

### 1단계: 전종목 수집 (sise_market_sum, KOSPI + KOSDAQ 분리 실행)
- KOSPI(sosok=0) / KOSDAQ(sosok=1) 각각 마지막 페이지까지 순회
- 중복 ticker(seen set) 감지 시 skip
- 수집 완료 후 outputs 폴더에 kospi_raw.json / kosdaq_raw.json 저장
- 예상 종목 수: KOSPI ~2330개, KOSDAQ ~1810개

### 2단계: 거래대금 1,000억↑ 필터
- 거래대금 = 현재가 × 거래량 / 100,000,000
- filtered_stocks.json으로 별도 저장

### 3단계: 1/2/3개월 수익률 계산 (배치 분리 + 병렬 처리)
- **타임아웃 방지**: 80개씩 배치 분리 (batch1: 0~79, batch2: 80~)
- `ThreadPoolExecutor(max_workers=16)`
- URL: `https://finance.naver.com/item/sise_day.naver?code={ticker}&page={n}`
- **셀렉터: `table.type2 tr`, 종가: `cols[1]`**
- **★★ page 1~9 순회** (`for page in range(1, 10):`)
- 30/60/90일 전 종가를 한 번에 수집 (목표 채워지면 조기 종료)
- 수익률 = (현재가 - 과거가) / 과거가 × 100, 소수점 1자리
- 결과: returns_batch1.json / returns_batch2.json → 병합

### 4단계: 하이라이트 생성
- 상한가 (등락률 ≥ 29.5%): "🚀 상한가: ..."
- 하한가 (등락률 ≤ -29.5% AND abs < 100): "💥 하한가: ..."
- 거래대금 TOP3: "💰 거래대금 TOP3: ..."
- 급등 15%↑ 3개↑: "🔥 급등(+15%↑) N개 종목"
- 급락 -10%↓ 3개↑: "⚠️ 급락(-10%↓) N개 종목"
- 3개월수익률≥100% && 당일≥10%: "⚠️ 단기 급등 지속 (추격 주의): ..."

### 4.5단계: limit_stocks 생성 (반드시 포함)
- **거래대금 무관**, 전체 수집 종목 중 등락률 ≥ +27% 또는 ≤ -27% 종목 추출
- 등락률 내림차순 정렬 (상한가 먼저, 하한가 마지막)
- 필드: ticker, name, market, price, change_rate, market_cap, volume, trading_value
- return_1m/2m/3m은 포함하지 않음

```python
limit_stocks = []
for s in kospi_raw + kosdaq_raw:
    cr = s.get('change_rate')
    if cr is not None and (cr >= 27.0 or cr <= -27.0):
        limit_stocks.append({
            'ticker': s['ticker'], 'name': s['name'], 'market': s['market'],
            'price': s['price'], 'change_rate': cr,
            'market_cap': s.get('market_cap'), 'volume': s.get('volume'),
            'trading_value': s.get('trading_value', 0),
        })
limit_stocks.sort(key=lambda x: x['change_rate'], reverse=True)
```

### 5단계: 데이터 JSON 빌드
`data_out` 변수에 아래 구조의 dict를 만든다:
```json
{
  "date": "YYYY-MM-DD",
  "generated_at": "ISO datetime",
  "filter_defaults": {"min_trading_value": 1000, "min_change_rate": -30.0, "max_change_rate": 30.0, "max_market_cap": 20000000, "sort_by": "change_rate", "sort_order": "desc"},
  "total_filtered": N,
  "stocks": [...],
  "highlights": [...],
  "limit_stocks": [...],
  "all_stocks_summary": {"kospi_count": ..., "kosdaq_count": ..., "total_kospi": ..., "total_kosdaq": ..., "total": ...}
}
```

### 5.5단계: 일자별 파일 + index.json 빌드 (★ 2026-05-26 추가 — 반드시 포함)

샌드박스 경로 베이스: `BASE = '/sessions/*/mnt/Claude/stock-screener'`

1. **data.json 저장 (호환성)**: `f'{BASE}/data.json'`에 `data_out` JSON 저장
2. **일자별 JSON 저장**: `f'{BASE}/daily/{date}.json'`에 동일한 `data_out` JSON 저장 (디렉터리는 `os.makedirs(..., exist_ok=True)`로 생성)
3. **GitHub의 daily/index.json 가져와서 갱신**:
   - GitHub REST API로 `daily/index.json` 현재 내용 다운로드 (404면 빈 구조 시작)
   - 동일 `date` 항목이 이미 있으면 갱신, 없으면 새로 추가
   - 전체 항목들 `new` 플래그 모두 false로 리셋 → 최신 항목만 `new:true`
   - `dates` 배열을 `date desc`로 정렬
   - `updated_at`, `latest_date`, `count` 필드 갱신
   - 신규 항목 빌드 코드:

```python
import json, os, requests, base64
from datetime import datetime

date = data_out['date']
total_filtered = data_out['total_filtered']
summary = data_out.get('all_stocks_summary', {})
limit_up_count = len([s for s in data_out.get('limit_stocks', []) if s.get('change_rate', 0) >= 27])
limit_down_count = len([s for s in data_out.get('limit_stocks', []) if s.get('change_rate', 0) <= -27])
dt = datetime.strptime(date, '%Y-%m-%d')
weekday = ['월','화','수','목','금','토','일'][dt.weekday()]

new_entry = {
    'date': date,
    'label': f'{dt.year}년 {dt.month}월 {dt.day}일 ({weekday})',
    'sub': f'필터통과 {total_filtered}개 · KOSPI {summary.get("kospi_count",0)}개 + KOSDAQ {summary.get("kosdaq_count",0)}개 · 전체 수집 {summary.get("total_kospi",0) + summary.get("total_kosdaq",0):,}개',
    'total_filtered': total_filtered,
    'kospi_count': summary.get('kospi_count', 0),
    'kosdaq_count': summary.get('kosdaq_count', 0),
    'total_kospi': summary.get('total_kospi', 0),
    'total_kosdaq': summary.get('total_kosdaq', 0),
    'limit_up': limit_up_count,
    'limit_down': limit_down_count,
    'generated_at': data_out['generated_at'],
}

# 기존 index.json 로드 (GitHub에서)
GH_TOKEN = '<YOUR_GITHUB_PAT>'  # 스케줄 태스크의 실제 토큰으로 교체
REPO = 'whysosary-dot/stock-screener'
HEADERS_GH = {'Authorization': f'token {GH_TOKEN}', 'Accept': 'application/vnd.github.v3+json'}
url_idx = f'https://api.github.com/repos/{REPO}/contents/daily/index.json?ref=main'
r = requests.get(url_idx, headers=HEADERS_GH)
existing_idx = {'dates': []}
existing_sha = None
if r.status_code == 200:
    j = r.json()
    existing_sha = j['sha']
    try:
        existing_idx = json.loads(base64.b64decode(j['content']).decode('utf-8'))
    except: pass

dates = [d for d in existing_idx.get('dates', []) if d.get('date') != date]
dates.append(new_entry)
dates.sort(key=lambda x: x['date'], reverse=True)
for i, d in enumerate(dates):
    d['new'] = (i == 0)

idx_out = {
    'updated_at': datetime.now().isoformat(),
    'latest_date': dates[0]['date'] if dates else None,
    'count': len(dates),
    'dates': dates,
}

# 로컬 저장도 함께
with open(f'{BASE}/daily/index.json', 'w', encoding='utf-8') as f:
    json.dump(idx_out, f, ensure_ascii=False, indent=2)
```

### 6단계: GitHub API로 커밋 & 푸시 (3개 파일)
- Token: `<YOUR_GITHUB_PAT>` (스케줄 태스크에 저장된 실제 토큰 사용)
- Repo: `whysosary-dot/stock-screener`
- committer: `{"name": "리송", "email": "whysosary@naver.com"}`
- **푸시 대상 3개 파일**:
  1. `data.json` (호환성)
  2. `daily/{date}.json` (일자별 데이터)
  3. `daily/index.json` (날짜 목록)
- 커밋 메시지: "📊 스크리닝 업데이트: YYYY-MM-DD (KOSPI X+KOSDAQ Y=합계 → 필터 N개)"
- **index.html, daily.html은 푸시 대상 아님** — 정적 파일

```python
def gh_put(path, content_str, message, sha=None):
    b64 = base64.b64encode(content_str.encode('utf-8')).decode('ascii')
    body = {'message': message, 'content': b64, 'branch': 'main',
            'committer': {'name': '리송', 'email': 'whysosary@naver.com'}}
    if sha: body['sha'] = sha
    url = f'https://api.github.com/repos/{REPO}/contents/{path}'
    r = requests.put(url, headers=HEADERS_GH, json=body)
    if not r.ok:
        print(f'PUT {path} FAILED:', r.status_code, r.text[:200])
    return r.ok

def gh_get_sha(path):
    url = f'https://api.github.com/repos/{REPO}/contents/{path}?ref=main'
    r = requests.get(url, headers=HEADERS_GH)
    return r.json().get('sha') if r.status_code == 200 else None

msg = f'📊 스크리닝 업데이트: {date} (KOSPI {summary.get("kospi_count",0)}+KOSDAQ {summary.get("kosdaq_count",0)}={summary.get("kospi_count",0)+summary.get("kosdaq_count",0)} → 필터 {total_filtered}개)'

data_json_str = json.dumps(data_out, ensure_ascii=False, indent=2)
idx_json_str = json.dumps(idx_out, ensure_ascii=False, indent=2)

gh_put('data.json', data_json_str, msg, gh_get_sha('data.json'))
gh_put(f'daily/{date}.json', data_json_str, msg, gh_get_sha(f'daily/{date}.json'))
gh_put('daily/index.json', idx_json_str, msg, existing_sha)
```

### 7단계: 결과 요약 보고
- 전체 수집 종목 수 / 필터 통과 / 거래대금 TOP10 / 하이라이트
- limit_stocks 개수 (상한가 X개, 하한가 Y개)
- 푸시한 파일 3개 확인

## 실행 구조 (bash 콜 4개로 분리)
- 1차 bash: KOSPI 전종목 수집 → kospi_raw.json
- 2차 bash: KOSDAQ 전종목 수집 → kosdaq_raw.json
- 3차 bash: 필터링 + 배치1 수익률
- 4차 bash: 배치2 수익률 + limit_stocks + data.json + daily/{date}.json + daily/index.json 생성 + GitHub 푸시 (3개 파일)

### 오류 처리
- 접속 실패 시: 2회 재시도 (0.3초 간격)
- 공휴일/휴장일: 이전 data.json 유지, "공휴일/휴장일로 업데이트 없음" 보고
- GitHub API 실패: 에러 메시지 보고

### 주의사항
- User-Agent 필수: `Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36`
- 크롤링 간격: time.sleep(0.05) 적용
- pip install: `pip install requests beautifulsoup4 --break-system-packages -q`
