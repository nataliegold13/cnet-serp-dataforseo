import os, json, re, requests, pandas as pd, streamlit as st
from bs4 import BeautifulSoup
from datetime import timezone
from dateutil import parser as dateparser

# ---- Config ----
SERPAPI_API_KEY = st.secrets.get("SERPAPI_API_KEY", os.getenv("SERPAPI_API_KEY", "")).strip()
USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/119.0.0.0 Safari/537.36")

st.set_page_config(page_title="CNET SERP Recency (with SerpAPI)", layout="wide")
st.title("CNET SERP Recency Checker")
st.caption("Auto-pulls Google’s top 3 organic results per keyword (excluding CNET/Reddit) and compares freshness. Threshold: >7 days newer.")

# ---- Date helpers ----
def _safe_parse_date(v):
    try:
        if not v: return None
        dt = dateparser.parse(str(v), fuzzy=True)
        if not dt: return None
        if not dt.tzinfo: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def _last_modified_header(headers):
    v = headers.get("Last-Modified") or headers.get("last-modified")
    dt = _safe_parse_date(v)
    return ("header:last-modified", dt, 0.3) if dt else None

def _extract_jsonld(soup):
    out = []
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.get_text(strip=True))
        except Exception:
            continue

        def walk(o):
            if isinstance(o, dict):
                # Prefer dateModified > datePublished > dateCreated/uploadDate
                if "dateModified" in o:
                    dt = _safe_parse_date(o.get("dateModified"))
                    if dt: out.append(("jsonld:dateModified", dt, 0.95))
                if "datePublished" in o:
                    dt = _safe_parse_date(o.get("datePublished"))
                    if dt: out.append(("jsonld:datePublished", dt, 0.75))
                for k in ("dateCreated", "uploadDate"):
                    if k in o:
                        dt = _safe_parse_date(o.get(k))
                        if dt: out.append((f"jsonld:{k}", dt, 0.70))
                for k in ("@graph", "mainEntity", "itemListElement"):
                    if k in o:
                        walk(o[k])
            elif isinstance(o, list):
                for x in o:
                    walk(x)

        walk(data)
    return out
def _extract_meta(soup):
    out = []

    def add(sel, conf):
        for m in soup.select(sel):
            dt = _safe_parse_date(m.get("content"))
            if dt: out.append((f"meta:{sel}", dt, conf))

    # High trust for modified
    add("meta[property='article:modified_time']", 0.95)
    add("meta[itemprop='dateModified']", 0.95)

    # Still strong
    add("meta[property='og:updated_time']", 0.90)

    # Published (medium)
    add("meta[property='article:published_time']", 0.75)
    add("meta[itemprop='datePublished']", 0.75)
    add("meta[name='parsely-pub-date']", 0.75)

    # Generic date (lower)
    add("meta[name='date']", 0.60)
    return out

def _extract_time_tags(soup):
    out = []
    for t in soup.find_all("time"):
        cand = t.get("datetime") or t.get_text(strip=True)
        dt = _safe_parse_date(cand)
        if not dt:
            continue
        host = " ".join([
            t.get_text(" ", strip=True),
            (t.parent.get_text(" ", strip=True) if t.parent else "")
        ]).lower()
        conf = 0.90 if "updated" in host else 0.60
        out.append(("time", dt, conf))
    return out

def _extract_textish(soup):
    text = soup.get_text(" ", strip=True)
    out=[]
    for pat in [
        r"(Updated|Published|Last updated|Last modified)[:\s]+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
        r"(Updated|Published|Last updated|Last modified)[:\s]+(\d{4}-\d{1,2}-\d{1,2})",
    ]:
        for m in re.finditer(pat, text, flags=re.I):
            dt=_safe_parse_date(m.group(2))
            if dt: out.append(("text:"+m.group(1), dt, 0.4))
    return out

def _cnet_adapter(soup):
    """Prefer CNET's byline <time datetime> inside the 'Article updated' area."""
    out = []
    selectors = [
        ".c-globalUpdatedDate time[datetime]",
        ".BylineCard_date-updated time[datetime]",
        "[data-cy='globalUpdatedDate'] time[datetime]",
        "[data-testid='globalUpdatedDate'] time[datetime]",
        "time[datetime][itemprop='dateModified']",
    ]
    for sel in selectors:
        t = soup.select_one(sel)
        if t and t.get("datetime"):
            dt = _safe_parse_date(t.get("datetime"))
            if dt:
                host = " ".join([
                    t.get_text(" ", strip=True),
                    (t.parent.get_text(" ", strip=True) if t.parent else "")
                ]).lower()
                conf = 0.95 if "updated" in host else 0.90
                out.append((f"cnet:{sel}", dt, conf))
    return out

def best_date(url, timeout=15):
    from urllib.parse import urlparse

    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout, allow_redirects=True)
    except Exception:
        return None, 0.0

    soup = BeautifulSoup(r.text, "html.parser")

    # Domain
    try:
        domain = urlparse(url).netloc.lower()
        if domain.startswith("www."): domain = domain[4:]
    except Exception:
        domain = ""

    cands = []

    # 1) Site-specific: CNET byline <time>
    if domain.endswith("cnet.com"):
        try:
            cands += _cnet_adapter(soup)
        except Exception:
            pass

    # 2) Generic strong signals
    for fn in (_extract_jsonld, _extract_meta, _extract_time_tags, _extract_textish):
        try:
            for label, dt, conf in fn(soup):
                cands.append((label, dt, conf))
        except Exception:
            pass

    # 3) VERY weak fallback (HTTP Last-Modified) — only if nothing else found
    if not cands:
        lm = _last_modified_header(r.headers)
        if lm: cands.append(lm)

    if not cands:
        return None, 0.0

    # Pick by highest confidence, then most recent within that tier
    max_conf = max(c[2] for c in cands)
    top = [c for c in cands if c[2] == max_conf]
    top.sort(key=lambda x: x[1], reverse=True)
    label, dt, conf = top[0]

    # Small boost if multiple sources agree within 2 days
    agree = [c for c in cands if abs((c[1] - dt).days) <= 2 and c[2] >= 0.6]
    if len(agree) >= 2 and conf < 1.0:
        conf = min(1.0, conf + 0.1)

    return dt, conf

# ---- SerpAPI ----
def serp_top3(keyword, hl="en", gl="us", exclude=("cnet.com","reddit.com")):
    if not SERPAPI_API_KEY:
        raise RuntimeError("SERPAPI_API_KEY is not set. Add it in Streamlit → Settings → Secrets.")
    params={
        "engine":"google","q":keyword,"num":10,"hl":hl,"gl":gl,"api_key":SERPAPI_API_KEY
    }
    r=requests.get("https://serpapi.com/search.json", params=params, timeout=25)
    r.raise_for_status()
    organic = r.json().get("organic_results", [])
    out=[]
    for it in organic:
        url=it.get("link"); title=it.get("title")
        if not url: continue
        if any(dom in url for dom in exclude): continue
        out.append({"title": title, "url": url})
        if len(out)>=3: break
    return out

def process(df, days_threshold=7, hl="en", gl="us"):
    recs=[]
    for _,row in df.iterrows():
        kw=str(row["keyword"]).strip()
        cnet=str(row["cnet_url"]).strip()
        c_dt,c_conf = best_date(cnet) if cnet else (None,0.0)

        comps = serp_top3(kw, hl=hl, gl=gl)
        newest=None
        out={"keyword":kw,"cnet_url":cnet,
             "cnet_date": c_dt.isoformat() if c_dt else None,
             "cnet_date_confidence": c_conf}
        for i,comp in enumerate(comps, start=1):
            dt,conf = best_date(comp["url"])
            out[f"comp{i}_title"]=comp["title"]
            out[f"comp{i}_url"]=comp["url"]
            out[f"comp{i}_date"]=dt.isoformat() if dt else None
            out[f"comp{i}_date_confidence"]=conf
            if dt and (newest is None or dt>newest): newest=dt
        out["max_comp_date"]= newest.isoformat() if newest else None
        diff = (newest - c_dt).days if (newest and c_dt) else None
        out["date_diff_days"]=diff
        out["needs_update"]= bool(diff is not None and diff>7)
        recs.append(out)
    return pd.DataFrame(recs)

# ---- UI ----
st.subheader("1) Provide your data")
st.write("Upload or paste a CSV with **keyword,cnet_url** headers. (We’ll fetch the top 3 competitors automatically.)")

csv_text = st.text_area("Paste CSV", height=140, value="keyword,cnet_url\nBest cordless vacuum,https://www.cnet.com/home/kitchen-and-household/best-cordless-vacuum/")
uploaded = st.file_uploader("...or upload CSV", type=["csv"])
hl = st.text_input("hl (language)", value="en")
gl = st.text_input("gl (country)", value="us")
st.caption("Defaults: hl=en, gl=us. Excludes: cnet.com, reddit.com. Threshold: >7 days.")

if st.button("Run"):
    try:
        if uploaded is not None:
            df = pd.read_csv(uploaded)
        else:
            from io import StringIO
            df = pd.read_csv(StringIO(csv_text))
        df.columns=[c.strip().lower() for c in df.columns]
        if not {"keyword","cnet_url"}.issubset(df.columns):
            st.error("CSV must include: keyword,cnet_url")
            st.stop()
    except Exception as e:
        st.error(f"Could not read CSV: {e}")
        st.stop()

    if not SERPAPI_API_KEY:
        st.error("Missing SERPAPI_API_KEY. Add it in Streamlit → Settings → Secrets.")
        st.stop()

    with st.spinner("Checking..."):
        res = process(df, days_threshold=7, hl=hl, gl=gl)
    st.subheader("2) Results")
    st.dataframe(res, use_container_width=True)
    st.download_button("Download CSV", res.to_csv(index=False).encode("utf-8"),
                       file_name="recency_results.csv", mime="text/csv")
else:
    if SERPAPI_API_KEY:
        st.success("SERPAPI_API_KEY found ✓")
    else:
        st.warning("No SERPAPI_API_KEY yet. You can still paste CSV, but you must add the key before running.")
