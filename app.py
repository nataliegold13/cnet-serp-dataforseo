import os, json, re, requests, pandas as pd, streamlit as st
from bs4 import BeautifulSoup
from datetime import timezone, datetime
from dateutil import parser as dateparser
import base64

# ---- Config ----
# DataForSEO credentials
DATAFORSEO_LOGIN = st.secrets.get("DATAFORSEO_LOGIN", os.getenv("DATAFORSEO_LOGIN", "")).strip()
DATAFORSEO_PASSWORD = st.secrets.get("DATAFORSEO_PASSWORD", os.getenv("DATAFORSEO_PASSWORD", "")).strip()

USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/119.0.0.0 Safari/537.36")

st.set_page_config(page_title="CNET SERP Recency (with DataForSEO)", layout="wide")
st.title("CNET SERP Recency Checker - DataForSEO Version")
st.caption("Auto-pulls Google's top 3 organic results per keyword (excluding CNET/Reddit) and compares freshness. Threshold: >7 days newer.")

# ---- Date helpers ----
def _safe_parse_date(v):
    try:
        if not v: return None
        # Handle various date formats
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
        # Check surrounding text for "updated" keyword
        parent_text = ""
        if t.parent:
            parent_text = t.parent.get_text(" ", strip=True).lower()
        
        # Higher confidence if "updated" is near the time tag
        if "updated" in parent_text or "modified" in parent_text:
            conf = 0.90
        else:
            conf = 0.60
        out.append(("time", dt, conf))
    return out

def _extract_textish(soup):
    text = soup.get_text(" ", strip=True)
    out=[]
    for pat in [
        r"(Updated|Published|Last updated|Last modified)[:\s]+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
        r"(Updated|Published|Last updated|Last modified)[:\s]+(\d{4}-\d{1,2}-\d{1,2})",
        r"(Updated|Published|Last updated|Last modified)[:\s]+(\d{1,2}/\d{1,2}/\d{4})",
    ]:
        for m in re.finditer(pat, text, flags=re.I):
            dt=_safe_parse_date(m.group(2))
            if dt: 
                # Higher confidence for "updated" vs "published"
                confidence = 0.5 if "updated" in m.group(1).lower() or "modified" in m.group(1).lower() else 0.4
                out.append(("text:"+m.group(1), dt, confidence))
    return out

def _cnet_adapter(soup):
    """Enhanced CNET-specific date extraction."""
    out = []
    
    # CNET-specific selectors - prioritize these
    selectors = [
        # Most specific CNET selectors
        ".c-globalUpdatedDate time[datetime]",
        ".c-globalPublishedDate time[datetime]",
        ".BylineCard_date-updated time[datetime]",
        ".BylineCard_date-published time[datetime]",
        "[data-cy='globalUpdatedDate'] time[datetime]",
        "[data-cy='globalPublishedDate'] time[datetime]",
        "[data-testid='globalUpdatedDate'] time[datetime]",
        "[data-testid='globalPublishedDate'] time[datetime]",
        "time[datetime][itemprop='dateModified']",
        "time[datetime][itemprop='datePublished']",
        # Broader selectors
        ".c-articleMeta time[datetime]",
        ".byline time[datetime]",
        ".article-meta time[datetime]",
    ]
    
    for sel in selectors:
        elements = soup.select(sel)
        for t in elements:
            if t.get("datetime"):
                dt = _safe_parse_date(t.get("datetime"))
                if dt:
                    parent_text = ""
                    if t.parent:
                        parent_text = t.parent.get_text(" ", strip=True).lower()
                    
                    # Determine confidence based on selector and context
                    if "updated" in sel.lower() or "modified" in sel.lower() or "updated" in parent_text:
                        conf = 0.95
                    elif "published" in sel.lower() or "published" in parent_text:
                        conf = 0.85
                    else:
                        conf = 0.80
                    
                    out.append((f"cnet:{sel}", dt, conf))
    
    # Also check for CNET's specific date patterns in text
    date_divs = soup.find_all(['div', 'span', 'p'], class_=re.compile(r'(date|time|updated|published)', re.I))
    for div in date_divs:
        text = div.get_text(strip=True)
        if text:
            dt = _safe_parse_date(text)
            if dt:
                is_updated = "update" in div.get('class', []) or "update" in text.lower()
                conf = 0.85 if is_updated else 0.75
                out.append(("cnet:text", dt, conf))
    
    return out

def best_date(url, timeout=15):
    from urllib.parse import urlparse

    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
    except Exception as e:
        st.warning(f"Could not fetch {url}: {e}")
        return None, 0.0

    soup = BeautifulSoup(r.text, "html.parser")

    # Domain
    try:
        domain = urlparse(url).netloc.lower()
        if domain.startswith("www."): domain = domain[4:]
    except Exception:
        domain = ""

    cands = []

    # 1) Site-specific: CNET byline <time> - PRIORITIZE for CNET
    if domain.endswith("cnet.com"):
        try:
            cnet_dates = _cnet_adapter(soup)
            if cnet_dates:
                cands += cnet_dates
        except Exception as e:
            st.warning(f"CNET adapter error: {e}")

    # 2) Generic strong signals
    for fn in (_extract_jsonld, _extract_meta, _extract_time_tags, _extract_textish):
        try:
            for label, dt, conf in fn(soup):
                cands.append((label, dt, conf))
        except Exception:
            pass

    # 3) VERY weak fallback (HTTP Last-Modified) – only if nothing else found
    if not cands:
        lm = _last_modified_header(r.headers)
        if lm: cands.append(lm)

    if not cands:
        return None, 0.0

    # Pick by highest confidence, then most recent within that tier
    max_conf = max(c[2] for c in cands)
    top = [c for c in cands if c[2] >= max_conf - 0.05]  # Allow slight variance
    top.sort(key=lambda x: x[1], reverse=True)
    label, dt, conf = top[0]

    # Small boost if multiple sources agree within 2 days
    agree = [c for c in cands if abs((c[1] - dt).days) <= 2 and c[2] >= 0.6]
    if len(agree) >= 2 and conf < 1.0:
        conf = min(1.0, conf + 0.1)

    return dt, conf

# ---- DataForSEO API ----
def dataforseo_search(keyword, location_code=2840, language_code="en", exclude=("cnet.com", "reddit.com")):
    """
    Use DataForSEO to get top organic results
    location_code: 2840 is United States
    """
    if not DATAFORSEO_LOGIN or not DATAFORSEO_PASSWORD:
        raise RuntimeError("DataForSEO credentials not set. Add DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD in Streamlit → Settings → Secrets.")
    
    # Create base64 encoded credentials
    cred_string = f"{DATAFORSEO_LOGIN}:{DATAFORSEO_PASSWORD}"
    cred_bytes = cred_string.encode('utf-8')
    cred_b64 = base64.b64encode(cred_bytes).decode('utf-8')
    
    headers = {
        'Authorization': f'Basic {cred_b64}',
        'Content-Type': 'application/json'
    }
    
    # DataForSEO request body
    data = [{
        "keyword": keyword,
        "location_code": location_code,
        "language_code": language_code,
        "device": "desktop",
        "os": "windows",
        "depth": 10,
       "calculate_rectangles": False
    }]
    
    try:
        # Post to DataForSEO
        response = requests.post(
            'https://api.dataforseo.com/v3/serp/google/organic/live/advanced',
            headers=headers,
            json=data,
            timeout=30
        )
        response.raise_for_status()
        
        result = response.json()
        
        # Extract organic results
        out = []
        if result.get("tasks") and len(result["tasks"]) > 0:
            task = result["tasks"][0]
            if task.get("result") and len(task["result"]) > 0:
                items = task["result"][0].get("items", [])
                
                for item in items:
                    if item.get("type") == "organic":
                        url = item.get("url", "")
                        title = item.get("title", "")
                        
                        # Skip excluded domains
                        if any(dom in url.lower() for dom in exclude):
                            continue
                        
                        out.append({"title": title, "url": url})
                        
                        if len(out) >= 3:
                            break
        
        return out
        
    except Exception as e:
        st.error(f"DataForSEO API error: {e}")
        return []

def process(df, days_threshold=7, location_code=2840, language_code="en"):
    recs = []
    progress_bar = st.progress(0)
    total_rows = len(df)
    
    for idx, row in df.iterrows():
        progress_bar.progress((idx + 1) / total_rows)
        
        kw = str(row["keyword"]).strip()
        cnet = str(row["cnet_url"]).strip()
        
        st.write(f"Processing: {kw}")
        
        # Get CNET date
        c_dt, c_conf = best_date(cnet) if cnet else (None, 0.0)
        
        # Get competitor results from DataForSEO
        comps = dataforseo_search(kw, location_code=location_code, language_code=language_code)
        
        newest = None
        out = {
            "keyword": kw,
            "cnet_url": cnet,
            "cnet_date": c_dt.isoformat() if c_dt else None,
            "cnet_date_confidence": c_conf
        }
        
        for i, comp in enumerate(comps, start=1):
            dt, conf = best_date(comp["url"])
            out[f"comp{i}_title"] = comp["title"]
            out[f"comp{i}_url"] = comp["url"]
            out[f"comp{i}_date"] = dt.isoformat() if dt else None
            out[f"comp{i}_date_confidence"] = conf
            
            if dt and (newest is None or dt > newest):
                newest = dt
        
        out["max_comp_date"] = newest.isoformat() if newest else None
        diff = (newest - c_dt).days if (newest and c_dt) else None
        out["date_diff_days"] = diff
        out["needs_update"] = bool(diff is not None and diff > days_threshold)
        
        recs.append(out)
    
    progress_bar.empty()
    return pd.DataFrame(recs)

# ---- UI ----
st.subheader("1) Configure DataForSEO Credentials")
with st.expander("⚙️ DataForSEO Settings"):
    st.write("You need to add your DataForSEO login email and password.")
    st.write("**In Streamlit Cloud:**")
    st.write("1. Go to your app settings (three dots menu → Settings)")
    st.write("2. Navigate to Secrets section")
    st.write("3. Add these two lines:")
    st.code("""DATAFORSEO_LOGIN = "your-email@example.com"
DATAFORSEO_PASSWORD = "your-password" """)
    
    if DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD:
        st.success(f"✅ DataForSEO credentials found for: {DATAFORSEO_LOGIN}")
    else:
        st.error("❌ DataForSEO credentials not found. Please add them in Secrets.")

st.subheader("2) Provide your data")
st.write("Upload or paste a CSV with **keyword,cnet_url** headers. (We'll fetch the top 3 competitors automatically.)")

csv_text = st.text_area(
    "Paste CSV", 
    height=140, 
    value="keyword,cnet_url\nBest cordless vacuum,https://www.cnet.com/home/kitchen-and-household/best-cordless-vacuum/"
)
uploaded = st.file_uploader("...or upload CSV", type=["csv"])

# Location settings
col1, col2 = st.columns(2)
with col1:
    location = st.selectbox(
        "Location",
        options=[
            ("United States", 2840),
            ("United Kingdom", 2826),
            ("Canada", 2124),
            ("Australia", 2036),
            ("Germany", 2276),
            ("France", 2250),
        ],
        format_func=lambda x: x[0]
    )
    location_code = location[1]

with col2:
    language_code = st.selectbox(
        "Language",
        options=["en", "es", "fr", "de", "it", "pt"],
        index=0
    )

days_threshold = st.slider("Days threshold for update recommendation", 1, 30, 7)
st.caption(f"Pages will be marked as needing update if competitors are >{days_threshold} days newer.")

if st.button("🚀 Run Analysis", type="primary"):
    try:
        if uploaded is not None:
            df = pd.read_csv(uploaded)
        else:
            from io import StringIO
            df = pd.read_csv(StringIO(csv_text))
        
        df.columns = [c.strip().lower() for c in df.columns]
        if not {"keyword", "cnet_url"}.issubset(df.columns):
            st.error("CSV must include: keyword,cnet_url")
            st.stop()
    except Exception as e:
        st.error(f"Could not read CSV: {e}")
        st.stop()

    if not DATAFORSEO_LOGIN or not DATAFORSEO_PASSWORD:
        st.error("Missing DataForSEO credentials. Add them in Streamlit → Settings → Secrets.")
        st.stop()

    with st.spinner("🔍 Analyzing pages..."):
        res = process(df, days_threshold=days_threshold, location_code=location_code, language_code=language_code)
    
    st.subheader("3) Results")
    
    # Summary metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        total = len(res)
        st.metric("Total Pages Analyzed", total)
    with col2:
        needs_update = res["needs_update"].sum()
        st.metric("Pages Needing Update", needs_update)
    with col3:
        pct = (needs_update / total * 100) if total > 0 else 0
        st.metric("Update Rate", f"{pct:.1f}%")
    
    # Show results
    st.dataframe(res, use_container_width=True)
    
    # Download button
    csv_data = res.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download Results CSV",
        csv_data,
        file_name=f"recency_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )
    
    # Show pages needing updates
    if needs_update > 0:
        st.subheader("📝 Pages Requiring Updates")
        updates_needed = res[res["needs_update"] == True][["keyword", "cnet_url", "date_diff_days"]]
        st.dataframe(updates_needed, use_container_width=True)
else:
    if DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD:
        st.success("✅ DataForSEO credentials found. Ready to run!")
    else:
        st.warning("⚠️ No DataForSEO credentials yet. You can still paste CSV, but you must add the credentials before running.")
