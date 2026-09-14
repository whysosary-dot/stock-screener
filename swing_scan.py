#!/usr/bin/env python3
"""
1개월 추세 스캐너 (조용한 축적형) — "오르락내리락하면서 한 달 사이 오르는 종목" 포착.

당일 등락률 화면으로는 잡히지 않는 자리를 찾는다. 핵심 신호는 '거래대금 기준선 상향'이다.
거래대금 10일 중앙값 / 직전 50일 중앙값 = 축적배수(acc). 이 값이 3배 이상으로 올라오는데
주가는 아직 조용하면(20일 최대 일간 등락 < 8%) 누군가 모으는 중이라는 뜻이다.

백테스트(코스피·코스닥 663종목 × 2024.09~2026.09, 표본 24만 (종목,일)):
  기준(전체)                        20일 후 중앙 +0.20% · 승률 52%
  축적 3배 + 조용(mx<8)              +2.38% · 59%
  ★종합(축적3 + 조용8 + 3개월정체 + 저점상향)  +3.41% · 64%
  S(축적4 이상)                      +3.51% · 67%
  B(조용 기준 완화 mx<12)             +1.99% · 57%   ← 키다리스튜디오 26.08.20~21 여기서 포착
  (참고) 거래대금 10배 폭발일 추격      -5.09% · 39%   ← 추격은 통계적으로 손해
시총 상한은 두지 않는다. 유동성 하한(20일 평균 거래대금 0.5억)만 둔다.

출력: swing/<날짜>.json, swing/latest.json, swing/index.json  (whysosary-dot/stock-screener)
사용: python3 swing_scan.py [--dry-run] [--limit N]
"""
import sys, os, re, json, base64, time, datetime, statistics as st
import urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

BASE   = Path(__file__).parent.resolve()
REPO   = "whysosary-dot/stock-screener"
BRANCH = "main"
DRY    = "--dry-run" in sys.argv
LIMIT  = next((int(a.split("=")[1]) for a in sys.argv if a.startswith("--limit=")), 0)

NAVER_H = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/"}
RAW = f"https://raw.githubusercontent.com/{REPO}/{BRANCH}"

# ── 임계값 (백테스트로 결정) ───────────────────────────────
ACC_S, ACC_A, ACC_C = 4.0, 3.0, 2.0      # 축적배수
MX_TIGHT, MX_LOOSE = 8.0, 12.0           # 20일 최대 일간 등락(%)
R20_MIN, R20_MAX = 0.02, 0.35            # 20일 수익률 구간
R60_MAX = 0.15                           # 3개월 정체 (아직 재평가 안 된 자리)
HL_RATIO = 1.02                          # 저점 상향 (10일 저점 > 20일 저점 ×1.02)
VAL20_MIN = 0.5                          # 20일 평균 거래대금 하한(억)
CHASE_BURST, CHASE_R20 = 8.0, 0.30       # 추격주의: 최근 15일 폭발 8배 + 20일 +30%


def med(x):
    return st.median(x) if x else 0.0


def token():
    f = BASE / ".github_token"
    if f.exists():
        return f.read_text().strip()
    t = os.environ.get("GH_TOKEN", "")
    if t:
        return t
    raise SystemExit("깃허브 토큰 없음 (.github_token 또는 GH_TOKEN)")


def fetch_json(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "swing-scan"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def naver_hist(ticker, days=260):
    """[(날짜, 종가, 거래량), ...] 오름차순"""
    end = datetime.date.today()
    start = end - datetime.timedelta(days=int(days * 1.6))
    u = (f"https://api.finance.naver.com/siseJson.naver?symbol={ticker}&requestType=1"
         f"&startTime={start:%Y%m%d}&endTime={end:%Y%m%d}&timeframe=day")
    for attempt in range(2):
        try:
            s = urllib.request.urlopen(urllib.request.Request(u, headers=NAVER_H),
                                       timeout=20).read().decode("utf-8", "replace")
            rows = [json.loads(m) for m in re.findall(r'\["\d{8}".*?\]', s)]
            return [(r[0], float(r[4]), float(r[5])) for r in rows if r[4]]
        except Exception:
            if attempt == 0:
                time.sleep(0.5)
    return []


def analyse(rows):
    """신호 계산. 데이터 부족하면 None"""
    if len(rows) < 70:
        return None
    px = [r[1] for r in rows]
    vol = [r[2] for r in rows]
    val = [px[i] * vol[i] / 1e8 for i in range(len(px))]           # 억원
    chg = [0.0] + [(px[i] / px[i - 1] - 1) * 100 for i in range(1, len(px))]

    val20 = med(val[-20:])
    if val20 < VAL20_MIN:
        return None
    base = med(val[-60:-10]) or med(val[:-10])
    acc = (med(val[-10:]) / base) if base > 0 else 0.0
    r20 = px[-1] / px[-21] - 1
    r60 = px[-1] / px[-61] - 1 if len(px) > 61 else r20
    mx20 = max(abs(c) for c in chg[-20:])
    lo20, lo10, hi20 = min(px[-20:]), min(px[-10:]), max(px[-20:])
    hl = lo10 > lo20 * HL_RATIO
    up = sum(1 for c in chg[-20:] if c > 0) / 20
    burst15 = max((val[k] / (med(val[k - 20:k]) or 1e-9)) for k in range(len(val) - 15, len(val)))

    return dict(acc=round(acc, 2), r20=round(r20 * 100, 1), r60=round(r60 * 100, 1),
                mx20=round(mx20, 1), up=round(up * 100), val20=round(val20, 1),
                val10=round(med(val[-10:]), 1), burst15=round(burst15, 1),
                hl=hl, lo20=lo20, lo10=lo10, hi20=hi20, px=px[-1], chg=round(chg[-1], 2),
                pos=round((px[-1] / hi20 - 1) * 100, 1),
                spark=[round(p) for p in px[-40:]],
                vspark=[round(v, 1) for v in val[-40:]],
                date=rows[-1][0])


def grade(s):
    """S / A / B / C / None + 사유"""
    core = (R20_MIN <= s["r20"] / 100 <= R20_MAX) and s["hl"] and s["r60"] / 100 <= R60_MAX
    if core and s["acc"] >= ACC_S and s["mx20"] < MX_TIGHT:
        return "S", "축적 4배↑ · 조용 · 3개월 정체 · 저점 상향 (백테스트 승률 67%)"
    if core and s["acc"] >= ACC_A and s["mx20"] < MX_TIGHT:
        return "A", "축적 3배↑ · 조용 · 3개월 정체 · 저점 상향 (승률 64%)"
    if core and s["acc"] >= ACC_A and s["mx20"] < MX_LOOSE:
        return "B", "축적 3배↑ · 저점 상향 (등락 다소 큼 · 승률 57%) — 키다리형"
    if s["acc"] >= ACC_C and s["mx20"] < MX_LOOSE and R20_MIN <= s["r20"] / 100 <= R20_MAX:
        return "C", "축적 2배↑ — 관찰 단계 (아직 저점 상향·정체 조건 미충족)"
    return None, ""


def main():
    # 1) 최신 스크리너 스냅샷
    idx = fetch_json(f"{RAW}/daily/index.json")
    date = idx["latest_date"]
    day = fetch_json(f"{RAW}/daily/{date}.json")
    stocks = day["stocks"]

    # 2) 값싼 사전 필터 (네이버 호출 수를 줄이기 위한 것 — 시총 제한은 없음)
    cand = [s for s in stocks
            if (s.get("trading_value") or 0) >= VAL20_MIN
            and s.get("return_1m") is not None
            and -10 <= s["return_1m"] <= 40
            and (s.get("return_3m") or 0) <= 60]
    if LIMIT:
        cand = cand[:LIMIT]
    print(f"기준일 {date} · 전체 {len(stocks)} → 후보 {len(cand)}종목 이력 수집")

    # 3) 네이버 일별 이력
    def work(s):
        rows = naver_hist(s["ticker"])
        if not rows:
            return None
        a = analyse(rows)
        if not a:
            return None
        g, why = grade(a)
        chase = a["burst15"] >= CHASE_BURST and a["r20"] / 100 >= CHASE_R20
        if not g and not chase:
            return None
        a.update(ticker=s["ticker"], name=s["name"], market=s["market"],
                 mcap=s.get("market_cap"), grade=g, why=why, chase=chase)
        return a

    t0 = time.time()
    with ThreadPoolExecutor(6) as ex:
        res = [r for r in ex.map(work, cand) if r]
    print(f"  수집 {time.time()-t0:.0f}초 · 신호 {len(res)}건")

    order = {"S": 0, "A": 1, "B": 2, "C": 3, None: 9}
    items = sorted([r for r in res if r["grade"]],
                   key=lambda r: (order[r["grade"]], -r["acc"]))
    chase = sorted([r for r in res if r["chase"] and not r["grade"]],
                   key=lambda r: -r["burst15"])[:20]

    counts = {g: sum(1 for r in items if r["grade"] == g) for g in ("S", "A", "B", "C")}
    # 실제 시세 기준일 = 네이버 이력의 마지막 날 (스크리너 스냅샷보다 최신일 수 있음)
    px_dates = [r["date"] for r in res if r.get("date")]
    if px_dates:
        top = max(px_dates)
        date = f"{top[:4]}-{top[4:6]}-{top[6:]}"
    out = {
        "date": date, "screener_date": day.get("date"),
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "universe": len(stocks), "scanned": len(cand),
        "counts": counts, "chase_count": len(chase),
        "thresholds": {"acc_S": ACC_S, "acc_A": ACC_A, "acc_C": ACC_C,
                       "mx_tight": MX_TIGHT, "mx_loose": MX_LOOSE,
                       "r20": [R20_MIN, R20_MAX], "r60_max": R60_MAX,
                       "hl_ratio": HL_RATIO, "val20_min": VAL20_MIN},
        "backtest": {"sample": "663종목 × 2024.09~2026.09 · 24만 (종목,일)",
                     "base": {"med20": 0.20, "win": 52},
                     "S": {"med20": 3.51, "win": 67}, "A": {"med20": 3.41, "win": 64},
                     "B": {"med20": 1.99, "win": 57}, "C": {"med20": 2.38, "win": 59},
                     "chase": {"med20": -5.09, "win": 39}},
        "items": items, "chase": chase,
    }

    print(f"  S {counts['S']} · A {counts['A']} · B {counts['B']} · C {counts['C']} · 추격주의 {len(chase)}")
    for r in items[:12]:
        print(f"   [{r['grade']}] {r['name']}({r['ticker']}) 축적 {r['acc']}x · 20일 {r['r20']:+.1f}% "
              f"· 3개월 {r['r60']:+.1f}% · 최대등락 {r['mx20']}%")

    if DRY:
        Path("/tmp/swing_latest.json").write_text(json.dumps(out, ensure_ascii=False, indent=1))
        print("(dry-run — /tmp/swing_latest.json 에만 저장)")
        return 0

    tok = token()

    def gh(path, method="GET", body=None):
        url = f"https://api.github.com/repos/{REPO}/contents/{path}"
        if method == "GET":
            url += f"?ref={BRANCH}&t={time.time()}"
        req = urllib.request.Request(url, method=method,
                                     data=json.dumps(body).encode() if body else None,
                                     headers={"Authorization": f"token {tok}",
                                              "Accept": "application/vnd.github+json",
                                              "User-Agent": "swing-scan",
                                              "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 404 and method == "GET":
                return None
            raise SystemExit(f"GitHub {method} {path}: {e.code} {e.read()[:200]}")

    def put(path, obj, msg):
        cur = gh(path)
        body = {"message": msg, "branch": BRANCH,
                "content": base64.b64encode(
                    (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n").encode()).decode()}
        if cur:
            body["sha"] = cur["sha"]
        r = gh(path, "PUT", body)
        print(f"  ✓ {path}  {r['commit']['sha'][:7]}")

    put(f"swing/{date}.json", out, f"swing: {date} 1개월 추세 스캔")
    put("swing/latest.json", out, f"swing: latest → {date}")
    cur = gh("swing/index.json")
    dates = []
    if cur:
        dates = json.loads(base64.b64decode(cur["content"]).decode())
        dates = dates if isinstance(dates, list) else []
    if date not in dates:
        dates = sorted(set(dates + [date]), reverse=True)[:180]
        put("swing/index.json", dates, f"swing: index {date}")
    print("\n완료 → https://whysosary-dot.github.io/stock-screener/daily.html#swing")
    return 0


if __name__ == "__main__":
    sys.exit(main())
