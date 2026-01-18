import csv
import re
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup

BASE_URL = "https://www.cnbc.com"

MARKET_SYMBOL_RE = re.compile(r"(?i)symbol|ticker")
MARKET_STOCKPOS_RE = re.compile(r"(?i)stockposition|price|value")
MARKET_CHANGEPCT_RE = re.compile(r"(?i)changepct|change|percentage")

LATEST_TS_RE = re.compile(r"(?i)timestamp|time|date")

def _text(el) -> str:
    return el.get_text(" ", strip=True) if el else ""

def _find_card_root(symbol_el):
    cur = symbol_el
    max_depth = 20
    for _ in range(max_depth):
        if cur is None or getattr(cur, "name", None) is None:
            break
        if (cur.find(string=MARKET_STOCKPOS_RE) or cur.find(class_=MARKET_STOCKPOS_RE)) and \
           (cur.find(string=MARKET_CHANGEPCT_RE) or cur.find(class_=MARKET_CHANGEPCT_RE)):
            return cur
        cur = cur.parent
    return symbol_el.parent if symbol_el.parent else symbol_el

def extract_market_data(soup: BeautifulSoup):
    print("[INFO] Filtering fields: Market banner (symbol, stockPosition, changePct)")
   
    rows = []
    symbol_nodes = []
   
    symbol_nodes.extend(soup.find_all(class_=MARKET_SYMBOL_RE))
   
    potential_symbols = soup.find_all(string=re.compile(r'^[A-Z]+$'))
    for text_node in potential_symbols:
        if len(text_node) <= 5:
            symbol_nodes.append(text_node.parent)
   
    seen = set()
    for sym_el in symbol_nodes:
        card = _find_card_root(sym_el)
       
        symbol = _text(sym_el)
       
        stock_pos_el = (card.find(class_=MARKET_STOCKPOS_RE) or
                       card.find(string=re.compile(r'\$?\d+\.?\d*')))
        stock_pos = _text(stock_pos_el)
       
        change_pct_el = (card.find(class_=MARKET_CHANGEPCT_RE) or
                        card.find(string=re.compile(r'[+-]?\d+\.?\d*%')))
        change_pct = _text(change_pct_el)
       
        key = (symbol, stock_pos, change_pct)
        if not symbol or len(symbol) > 10 or key in seen:
            continue
        seen.add(key)
       
        rows.append(
            {
                "marketCard_symbol": symbol,
                "marketCard_stockPosition": stock_pos,
                "marketCard_changePct": change_pct,
            }
        )
   
    print(f"[OK] Market entries found: {len(rows)}")
    return rows

def extract_latest_news(soup: BeautifulSoup):
    print("[INFO] Filtering fields: Latest News (timestamp, title, link)")
   
    news_rows = []
    seen_links = set()
   
    latest_headers = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
    latest_section = None
   
    for header in latest_headers:
        header_text = header.get_text(strip=True).lower()
        if 'latest' in header_text and 'news' in header_text:
            parent = header.parent
            for _ in range(5):
                if parent and len(parent.find_all('a', href=True)) > 3:
                    latest_section = parent
                    break
                parent = parent.parent if parent else None
            break
   
    if not latest_section:
        news_containers = soup.find_all(['section', 'div', 'ul'],
                                        class_=re.compile(r'(?i)news|latest|article|feed'))
        for container in news_containers:
            if len(container.find_all('a', href=True)) > 2:
                latest_section = container
                break
   
    if not latest_section:
        latest_section = soup
   
    all_links = latest_section.find_all('a', href=True)
   
    for link in all_links:
        title = link.get_text(" ", strip=True)
        href = link.get('href', '')
       
        if not title or not href:
            continue
       
        # Skip navigation, social media, etc.
        if any(skip in href.lower() for skip in ['login', 'register', 'facebook', 'twitter', 'instagram', 'linkedin']):
            continue
       
        if len(title) < 10:
            continue
       
        full_link = urljoin(BASE_URL, href)
        if full_link in seen_links:
            continue
       
        timestamp = ""
        parent_elem = link.parent
        for _ in range(3):
            if parent_elem:
                time_el = parent_elem.find('time') or parent_elem.find(class_=LATEST_TS_RE)
                if time_el:
                    timestamp = _text(time_el)
                    break
               
                time_text = parent_elem.find(string=re.compile(r'\d+:\d+|hours?|minutes?|AM|PM'))
                if time_text:
                    timestamp = time_text.strip()
                    break
               
                parent_elem = parent_elem.parent
            else:
                break
       
        seen_links.add(full_link)
        news_rows.append({
            "LatestNews_timestamp": timestamp,
            "title": title,
            "link": full_link,
        })
       
        if len(news_rows) >= 20:
            break
   
    print(f"[OK] Latest News entries found: {len(news_rows)}")
    return news_rows

def write_csv(path: Path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def main() -> None:
    root = Path(__file__).resolve().parents[1]
   
    raw_html_path = root / "data" / "raw_data" / "web_data.html"
    processed_dir = root / "data" / "processed_data"
    market_csv = processed_dir / "market_data.csv"
    news_csv = processed_dir / "news_data.csv"
   
    print("[INFO] Reading raw HTML into a Python list:", raw_html_path)
   
    if not raw_html_path.exists():
        raise FileNotFoundError(f"Missing file: {raw_html_path}. Run scripts/web_scraper.py first.")
   
    lines = []
    try:
        with raw_html_path.open("r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        with raw_html_path.open("r", encoding="latin-1", errors="replace") as f:
            lines = f.readlines()
   
    html = "".join(lines)
   
    print(f"[INFO] Parsing HTML with BeautifulSoup... (Length: {len(html)} chars)")
    soup = BeautifulSoup(html, "html.parser")
   
    market_rows = extract_market_data(soup)
    print("[INFO] Storing Market data ->", market_csv)
    write_csv(
        market_csv,
        fieldnames=["marketCard_symbol", "marketCard_stockPosition", "marketCard_changePct"],
        rows=market_rows,
    )
    print("[OK] CSV created:", market_csv)
   
    news_rows = extract_latest_news(soup)
    print("[INFO] Storing Latest News data ->", news_csv)
    write_csv(
        news_csv,
        fieldnames=["LatestNews_timestamp", "title", "link"],
        rows=news_rows,
    )
    print("[OK] CSV created:", news_csv)

if __name__ == "__main__":
    main()
