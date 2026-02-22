import argparse
import csv
import json
import random
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.zkh.com/"
DEFAULT_CATEGORY_URL = "https://www.zkh.com/list/c-10287403.html"
CSV_OUTPUT = "parts.csv"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def create_session() -> requests.Session:
    """Create a requests session with browser-like headers and retry policy."""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
    )

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def rate_limit(min_delay: float = 1.0, max_delay: float = 2.0) -> None:
    """Sleep for a random delay between requests."""
    time.sleep(random.uniform(min_delay, max_delay))


def fetch_html(session: requests.Session, url: str, timeout: int = 20) -> Optional[str]:
    """Fetch raw html text with retry-enabled session."""
    rate_limit()
    print(f"[FETCH] {url}")
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response.text
    except requests.RequestException as exc:
        print(f"[ERROR] Failed to fetch {url}: {exc}")
        return None


def fetch_soup(session: requests.Session, url: str, timeout: int = 20) -> Optional[BeautifulSoup]:
    """Fetch a URL and parse HTML into BeautifulSoup."""
    html = fetch_html(session, url, timeout=timeout)
    if html is None:
        return None
    return BeautifulSoup(html, "html.parser")


def first_text(element: Tag, selectors: List[str]) -> str:
    for selector in selectors:
        node = element.select_one(selector)
        if node:
            text = " ".join(node.get_text(" ", strip=True).split())
            if text:
                return text
    return ""


def first_attr(element: Tag, selectors: List[str], attr: str) -> str:
    for selector in selectors:
        node = element.select_one(selector)
        if node and node.has_attr(attr):
            value = str(node.get(attr, "")).strip()
            if value:
                return value
    return ""


def find_product_cards(soup: BeautifulSoup) -> List[Tag]:
    candidate_selectors = [
        ".product-item",
        ".goods-item",
        ".sku-item",
        ".list-item",
        ".product-list li",
        ".goods-list li",
        "li[data-sku]",
        "div[data-sku]",
        "article",
    ]

    for selector in candidate_selectors:
        cards = [card for card in soup.select(selector) if isinstance(card, Tag)]
        if cards:
            return cards

    fallback_cards: List[Tag] = []
    for link in soup.select("a[href]"):
        href = link.get("href", "")
        if any(token in href.lower() for token in ["/product", "/goods", "/item", "/sku", "/detail"]):
            container = link.find_parent(["li", "div", "article"]) or link
            if isinstance(container, Tag):
                fallback_cards.append(container)

    deduped: List[Tag] = []
    seen = set()
    for item in fallback_cards:
        marker = str(item)[:220]
        if marker not in seen:
            seen.add(marker)
            deduped.append(item)
    return deduped


def parse_product_from_card(card: Tag, base_url: str) -> Dict[str, str]:
    product_name = first_text(
        card,
        [".product-name", ".goods-name", ".title", "h3", "h2", "a[title]", "a"],
    )

    sku = first_text(
        card,
        [".product-model", ".sku", ".model", ".item-code", ".code", "[data-sku]", "[class*='sku']"],
    ) or card.get("data-sku", "")

    part_description = first_text(card, [".description", ".desc", ".product-desc", ".sub-title", "p"])

    price = first_text(card, [".price", ".product-price", ".goods-price", "[class*='price']"])

    detail_href = first_attr(
        card,
        ["a.product-link", "a.goods-link", "a[href*='product']", "a[href*='item']", "a[href*='detail']", "a[href]"],
        "href",
    )

    return {
        "product_name": product_name,
        "product_model_or_SKU": sku,
        "part_description": part_description,
        "price": price,
        "detail_page_url": urljoin(base_url, detail_href),
    }


def traverse(obj: Any) -> Iterable[Any]:
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from traverse(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from traverse(item)


def pick_first(d: Dict[str, Any], keys: List[str]) -> str:
    for key in keys:
        value = d.get(key)
        if value is not None:
            text = str(value).strip()
            if text:
                return text
    return ""


def normalize_price(raw: Any) -> str:
    if raw is None:
        return ""
    text = str(raw).strip()
    if not text:
        return ""
    if re.search(r"\d", text):
        return text
    return ""


def extract_products_from_embedded_json(html: str, base_url: str) -> List[Dict[str, str]]:
    """Fallback for JS-rendered pages: parse embedded JSON blobs for product entries."""
    soup = BeautifulSoup(html, "html.parser")
    json_blocks: List[str] = []

    for script in soup.find_all("script"):
        script_text = script.string or script.get_text("", strip=False)
        if not script_text:
            continue

        script_type = (script.get("type") or "").lower()
        if "json" in script_type:
            json_blocks.append(script_text)

        for pattern in [
            r"__NEXT_DATA__\s*=\s*(\{.*?\})\s*;",
            r"__INITIAL_STATE__\s*=\s*(\{.*?\})\s*;",
            r"window\.__NUXT__\s*=\s*(\{.*?\})\s*;",
            r"window\.__data\s*=\s*(\{.*?\})\s*;",
        ]:
            match = re.search(pattern, script_text, flags=re.DOTALL)
            if match:
                json_blocks.append(match.group(1))

    candidates: List[Dict[str, str]] = []
    seen_urls: Set[str] = set()

    for block in json_blocks:
        try:
            payload = json.loads(block)
        except json.JSONDecodeError:
            continue

        for item in traverse(payload):
            if not isinstance(item, dict):
                continue

            name = pick_first(item, ["productName", "skuName", "name", "title", "goodsName", "spuName"])
            sku = pick_first(item, ["sku", "skuNo", "skuCode", "model", "itemCode", "materialCode", "productCode"])
            desc = pick_first(item, ["description", "desc", "subTitle", "brief", "sellingPoint"])
            price = normalize_price(
                item.get("price")
                or item.get("salePrice")
                or item.get("minPrice")
                or item.get("showPrice")
            )
            detail_href = pick_first(item, ["detailUrl", "detailPageUrl", "url", "href", "link"])

            # 通过 ID 字段构造详情地址的兜底方式
            if not detail_href:
                for k in ["skuId", "itemId", "productId", "id"]:
                    if k in item and str(item[k]).strip().isdigit():
                        detail_href = f"/product/{item[k]}.html"
                        break

            detail_url = urljoin(base_url, detail_href) if detail_href else ""

            if not (name or sku or detail_url):
                continue
            if detail_url and detail_url in seen_urls:
                continue

            row = {
                "product_name": name,
                "product_model_or_SKU": sku,
                "part_description": desc,
                "price": price,
                "detail_page_url": detail_url,
            }

            if any(row.values()):
                candidates.append(row)
                if detail_url:
                    seen_urls.add(detail_url)

    return candidates


def extract_detail_specs(soup: BeautifulSoup) -> Dict[str, str]:
    specs: Dict[str, str] = {}

    for row in soup.select("table tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) >= 2:
            key = cells[0].get_text(" ", strip=True)
            value = cells[1].get_text(" ", strip=True)
            if key and value:
                specs[key] = value

    for dl in soup.select("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = dt.get_text(" ", strip=True)
            value = dd.get_text(" ", strip=True)
            if key and value:
                specs[key] = value

    for li in soup.select(".spec li, .specs li, .product-spec li, .param li"):
        text = li.get_text(" ", strip=True)
        if ":" in text:
            key, value = text.split(":", 1)
            key = key.strip()
            value = value.strip()
            if key and value:
                specs[key] = value

    return specs


def scrape_category(session: requests.Session, category_url: str) -> List[Dict[str, str]]:
    html = fetch_html(session, category_url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    cards = find_product_cards(soup)
    rows: List[Dict[str, str]] = []

    if cards:
        print(f"[INFO] Found {len(cards)} product candidates via HTML selectors.")
        rows = [parse_product_from_card(card, BASE_URL) for card in cards]
    else:
        print("[INFO] No product cards found in static HTML, trying embedded JSON fallback...")
        rows = extract_products_from_embedded_json(html, BASE_URL)
        print(f"[INFO] Extracted {len(rows)} product candidates from embedded JSON.")

    results: List[Dict[str, str]] = []

    for idx, product in enumerate(rows, start=1):
        detail_url = product.get("detail_page_url", "")
        if not detail_url:
            print(f"[WARN] [{idx}/{len(rows)}] Missing detail URL; keeping row without detail specs.")
            product["detailed_specs"] = "{}"
            results.append(product)
            continue

        print(f"[PROGRESS] [{idx}/{len(rows)}] {product.get('product_name') or 'Unnamed product'}")
        detail_soup = fetch_soup(session, detail_url)
        specs = extract_detail_specs(detail_soup) if detail_soup else {}
        product["detailed_specs"] = json.dumps(specs, ensure_ascii=False)
        results.append(product)

    return results


def write_csv(rows: List[Dict[str, str]], output_file: str) -> None:
    headers = [
        "product_name",
        "product_model_or_SKU",
        "part_description",
        "price",
        "detail_page_url",
        "detailed_specs",
    ]

    with open(output_file, "w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[DONE] Wrote {len(rows)} rows to {output_file}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape parts listing from zkh.com")
    parser.add_argument(
        "--category-url",
        default=DEFAULT_CATEGORY_URL,
        help="Category/listing URL to scrape",
    )
    parser.add_argument(
        "--output",
        default=CSV_OUTPUT,
        help="Output CSV file path",
    )
    args = parser.parse_args()

    session = create_session()
    rows = scrape_category(session, args.category_url)
    write_csv(rows, args.output)


if __name__ == "__main__":
    main()
