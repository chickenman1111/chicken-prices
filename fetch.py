import asyncio, re, os, sys
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
from playwright.async_api import async_playwright

TARGET_URL = "http://www.chicken.or.kr/home/start.php"
KST = ZoneInfo("Asia/Seoul")

def to_num(x):
    s = re.sub(r"[^\d.]", "", str(x or ""))
    return int(float(s)) if s else None

def pick_prices_from_df(df, labels):
    out = {}
    df2 = df.copy()
    df2.columns = [str(c).strip().replace(" ", "") for c in df2.columns]
    col_today = next((c for c in df2.columns if "금일" in c), None)
    col_prev  = next((c for c in df2.columns if "전일" in c), None)
    if not (col_today and col_prev):
        return out
    for _, row in df2.iterrows():
        row_text = " ".join(map(str, row.values))
        for key, pattern in labels.items():
            if pattern.search(row_text):
                out[key] = {
                    "금일": to_num(row.get(col_today)),
                    "전일": to_num(row.get(col_prev)),
                }
    return out

async def fetch():
    async with async_playwright() as p:
        br = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await br.new_context(locale="ko-KR")
        pg = await ctx.new_page()
        await pg.goto(TARGET_URL, wait_until="domcontentloaded")
        try:
            await pg.wait_for_load_state("networkidle", timeout=8000)
        except:
            pass
        tables_html = await pg.locator("table").evaluate_all("els => els.map(e => e.outerHTML)")
        await br.close()

    dfs = []
    for html in tables_html:
        try:
            for t in pd.read_html(html):
                dfs.append(t)
        except:
            pass

    if not dfs:
        raise RuntimeError("표를 찾지 못했습니다.")

    broiler_labels = {
        "9~10호": re.compile(r"9\s*[~∼-]\s*10\s*호"),
        "11호":   re.compile(r"(^|[^0-9])11\s*호"),
        "12호":   re.compile(r"(^|[^0-9])12\s*호"),
    }
    live_labels = {
        "대": re.compile(r"생계.*대|대.*생계"),
        "중": re.compile(r"생계.*중|중.*생계"),
        "소": re.compile(r"생계.*소|소.*생계"),
    }

    broiler, live = {}, {}
    for df in dfs:
        blob = " ".join(map(str, df.columns)) + " " + " ".join(map(str, df.values.flatten()))
        if ("금일" in blob and "전일" in blob):
            broiler.update(pick_prices_from_df(df, broiler_labels))
            live.update(pick_prices_from_df(df, live_labels))

    needed_broiler = ["9~10호","11호","12호"]
    needed_live = ["대","중","소"]
    if not all(k in broiler for k in needed_broiler) or not all(k in live for k in needed_live):
        print("DEBUG broiler:", broiler, file=sys.stderr)
        print("DEBUG live:", live, file=sys.stderr)
        raise RuntimeError("필수 항목을 모두 찾지 못했습니다.")

    now = datetime.now(tz=KST)
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")

    rows = []
    for k in needed_broiler:
        rows.append({
            "date": date_str, "time_kst": time_str,
            "분류": "육계(공장도가)", "항목": k,
            "금일(원/kg)": broiler[k]["금일"],
            "전일(원/kg)": broiler[k]["전일"],
        })
    for k in needed_live:
        rows.append({
            "date": date_str, "time_kst": time_str,
            "분류": "생계", "항목": k,
            "금일(원/kg)": live[k]["금일"],
            "전일(원/kg)": live[k]["전일"],
        })

    os.makedirs("data", exist_ok=True)
    latest_path = "data/latest.csv"
    hist_path = "data/history.csv"

    df_out = pd.DataFrame(rows)
    df_out.to_csv(latest_path, index=False, encoding="utf-8-sig")

    if os.path.exists(hist_path):
        old = pd.read_csv(hist_path)
        new = pd.concat([old, df_out], ignore_index=True)
    else:
        new = df_out
    new.to_csv(hist_path, index=False, encoding="utf-8-sig")

if __name__ == "__main__":
    asyncio.run(fetch())
