import importlib.util
import io
import os
import re
import sqlite3
import tempfile
import time
import urllib.robotparser as robotparser
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urldefrag, urljoin, urlparse

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

if "sid" not in st.session_state:
    st.session_state.sid = uuid.uuid4().hex[:8]  # each visitor gets their own DB
DB = os.path.join(tempfile.gettempdir(), f"crawl_{st.session_state.sid}.db")
HAS_PW = importlib.util.find_spec("playwright") is not None
COLS = ["url", "status", "redirect_to", "content_type", "title", "title_len",
        "meta_description", "meta_desc_len", "h1", "h1_count", "canonical",
        "meta_robots", "indexable", "word_count", "internal_links",
        "response_ms", "error"]
SKIP_EXT = re.compile(r"\.(jpe?g|png|gif|svg|webp|pdf|zip|css|js|mp4|mp3|ico|woff2?)$", re.I)


def get_db():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute(f"CREATE TABLE IF NOT EXISTS pages ({', '.join(COLS)})")
    c.execute("CREATE TABLE IF NOT EXISTS links (source, target)")
    return c


def clean(href, base):
    u = urldefrag(urljoin(base, href))[0]
    if urlparse(u).scheme not in ("http", "https") or SKIP_EXT.search(urlparse(u).path):
        return None
    return u


def parse(url, status, ctype, html, ms, redirect="", error=""):
    row = dict.fromkeys(COLS, "")
    row.update(url=url, status=status, content_type=ctype, response_ms=ms,
               redirect_to=redirect, error=error)
    links = []
    if html and "html" in ctype and status == 200:
        soup = BeautifulSoup(html, "html.parser")
        t = soup.title.get_text(strip=True) if soup.title else ""
        md = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
        mr = soup.find("meta", attrs={"name": re.compile("^robots$", re.I)})
        cn = soup.find("link", rel="canonical")
        h1s = [h.get_text(strip=True) for h in soup.find_all("h1")]
        for a in soup.find_all("a", href=True):
            u = clean(a["href"], url)
            if u:
                links.append(u)
        for tag in soup(["script", "style"]):
            tag.decompose()
        row.update(
            title=t, title_len=len(t),
            meta_description=(md.get("content", "") if md else "").strip(),
            h1=h1s[0] if h1s else "", h1_count=len(h1s),
            canonical=cn.get("href", "") if cn else "",
            meta_robots=(mr.get("content", "") if mr else "").lower(),
            word_count=len(soup.get_text(" ").split()),
        )
        row["meta_desc_len"] = len(row["meta_description"])
    row["indexable"] = (status == 200 and "noindex" not in row["meta_robots"]
                        and row["canonical"] in ("", url))
    return row, links


def fetch(url, session, timeout, page=None):
    t0 = time.time()
    try:
        if page:  # JavaScript rendering via Playwright
            resp = page.goto(url, wait_until="networkidle", timeout=timeout * 1000)
            html = page.content()
            return parse(url, resp.status if resp else 0, "text/html", html,
                         int((time.time() - t0) * 1000))
        r = session.get(url, timeout=timeout, allow_redirects=False)
        ms = int((time.time() - t0) * 1000)
        if 300 <= r.status_code < 400:
            loc = urljoin(url, r.headers.get("Location", ""))
            row, _ = parse(url, r.status_code, "", "", ms, redirect=loc)
            return row, [loc]
        return parse(url, r.status_code, r.headers.get("content-type", ""), r.text, ms)
    except Exception as e:
        return parse(url, 0, "", "", int((time.time() - t0) * 1000), error=str(e)[:200])


def crawl(start, max_pages, delay, workers, obey_robots, ua, timeout, js, ui):
    conn = get_db()
    conn.execute("DELETE FROM pages")
    conn.execute("DELETE FROM links")
    host = urlparse(start).netloc
    rob = None
    if obey_robots:
        rob = robotparser.RobotFileParser(urljoin(start, "/robots.txt"))
        try:
            rob.read()
        except Exception:
            rob = None
    s = requests.Session()
    s.headers["User-Agent"] = ua
    pw = browser = page = None
    if js:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        browser = pw.chromium.launch()
        page = browser.new_page(user_agent=ua)
        workers = 1  # one shared browser page, so crawl sequentially
    q, seen, done = deque([start]), {start}, 0
    bar, status_box = ui
    try:
        with ThreadPoolExecutor(workers) as pool:
            while q and done < max_pages:
                n = min(len(q), workers, max_pages - done)
                batch = [q.popleft() for _ in range(n)]
                batch = [u for u in batch if not rob or rob.can_fetch(ua, u)]
                for row, links in pool.map(lambda u: fetch(u, s, timeout, page), batch):
                    conn.execute(f"INSERT INTO pages VALUES ({','.join('?' * len(COLS))})",
                                 [row[c] for c in COLS])
                    internal = [u for u in links if urlparse(u).netloc == host]
                    row_links = 0
                    for u in internal:
                        row_links += 1
                        conn.execute("INSERT INTO links VALUES (?,?)", (row["url"], u))
                        if u not in seen:
                            seen.add(u)
                            q.append(u)
                    conn.execute("UPDATE pages SET internal_links=? WHERE url=?",
                                 (row_links, row["url"]))
                done += len(batch)
                conn.commit()
                bar.progress(min(done / max_pages, 1.0))
                status_box.write(f"Crawled **{done:,}** | Queue **{len(q):,}** | "
                                 f"Last: {batch[-1] if batch else ''}")
                time.sleep(delay)
    finally:
        conn.commit()
        if browser:
            browser.close()
            pw.stop()


def to_excel(df):
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    return buf.getvalue()


st.set_page_config(page_title="SEO Crawler", layout="wide")
st.title("SEO Crawler")

try:
    _pw = st.secrets.get("APP_PASSWORD")
except Exception:
    _pw = None
if _pw and st.text_input("Password", type="password") != _pw:
    st.stop()

with st.sidebar:
    start = st.text_input("Start URL", "https://example.com")
    max_pages = st.number_input("Max pages", 1, 50_000, 500)
    workers = st.slider("Parallel requests", 1, 20, 5)
    delay = st.number_input("Delay between batches (sec)", 0.0, 10.0, 0.5, 0.1)
    timeout = st.number_input("Timeout (sec)", 5, 120, 20)
    obey = st.checkbox("Respect robots.txt", True)
    js = st.checkbox("JavaScript rendering (needs Playwright; not on free cloud)", False, disabled=not HAS_PW)
    ua = st.text_input("User agent", "MySEOCrawler/1.0")
    go = st.button("Start crawl", type="primary")

if go:
    if not start.startswith("http"):
        st.error("Enter a full URL starting with http:// or https://")
    else:
        bar, box = st.progress(0.0), st.empty()
        crawl(start, int(max_pages), delay, workers, obey, ua, int(timeout), js, (bar, box))
        st.success("Crawl finished")

try:
    df = pd.read_sql("SELECT * FROM pages", get_db())
except Exception:
    df = pd.DataFrame(columns=COLS)

if df.empty:
    st.info("Set a start URL in the sidebar and click Start crawl.")
else:
    for c in ["status", "title_len", "meta_desc_len", "h1_count", "word_count", "response_ms"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    ok = df[df.status == 200]
    issues = {
        "Missing title": ok[ok.title_len == 0],
        "Title too long (>60)": ok[ok.title_len > 60],
        "Missing meta description": ok[ok.meta_desc_len == 0],
        "Missing H1": ok[ok.h1_count == 0],
        "Multiple H1": ok[ok.h1_count > 1],
        "Non-indexable": ok[ok.indexable.astype(str).isin(["0", "False"])],
        "Client/server errors": df[df.status >= 400],
        "Redirects": df[(df.status >= 300) & (df.status < 400)],
        "Connection errors": df[df.status == 0],
    }
    dup_t = ok[(ok.title != "") & ok.duplicated("title", keep=False)]
    dup_m = ok[(ok.meta_description != "") & ok.duplicated("meta_description", keep=False)]
    issues["Duplicate titles"], issues["Duplicate meta descriptions"] = dup_t, dup_m

    c1, c2, c3 = st.columns(3)
    c1.metric("URLs crawled", f"{len(df):,}")
    c2.metric("Indexable (200 OK)", f"{len(ok):,}")
    c3.metric("Errors (4xx/5xx/failed)", f"{len(issues['Client/server errors']) + len(issues['Connection errors']):,}")

    t_all, t_issues, t_broken = st.tabs(["All pages", "Issues", "Broken links"])
    with t_all:
        st.dataframe(df.head(2000), use_container_width=True)
        st.caption("Showing first 2,000 rows. Downloads contain everything.")
    with t_issues:
        st.table(pd.DataFrame({"Issue": list(issues), "Count": [len(v) for v in issues.values()]}))
        pick = st.selectbox("View issue", list(issues))
        st.dataframe(issues[pick].head(2000), use_container_width=True)
    with t_broken:
        bad = df[(df.status >= 400) | (df.status == 0)][["url", "status"]]
        edges = pd.read_sql("SELECT source, target FROM links", get_db())
        br = edges.merge(bad, left_on="target", right_on="url")[["source", "target", "status"]]
        st.dataframe(br.head(5000), use_container_width=True)

    d1, d2 = st.columns(2)
    d1.download_button("Download CSV", df.to_csv(index=False), "crawl.csv", "text/csv")
    d2.download_button("Download Excel", to_excel(df), "crawl.xlsx")
