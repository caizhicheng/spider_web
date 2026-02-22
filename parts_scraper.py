import argparse
import csv
import json
import random
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.zkh.com/"
DEFAULT_CATEGORY_URL = "https://www.zkh.com/"
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
            "Accept-Language": "en-US,en;q=0.9",
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
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)


def fetch_soup(session: requests.Session, url: str, timeout: int = 20) -> Optional[BeautifulSoup]:
    """Fetch a URL and parse HTML into BeautifulSoup."""
    rate_limit()
    print(f"[FETCH] {url}")
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"[ERROR] Failed to fetch {url}: {exc}")
        return None

    return BeautifulSoup(response.text, "html.parser")


def first_text(element: Tag, selectors: List[str]) -> str:
    """Find the first non-empty text among candidate CSS selectors."""
    for selector in selectors:
        node = element.select_one(selector)
        if node:
            text = " ".join(node.get_text(" ", strip=True).split())
            if text:
                return text
    return ""


def first_attr(element: Tag, selectors: List[str], attr: str) -> str:
    """Find the first non-empty attribute among candidate selectors."""
    for selector in selectors:
        node = element.select_one(selector)
        if node and node.has_attr(attr):
            value = str(node.get(attr, "")).strip()
            if value:
                return value
    return ""


def find_product_cards(soup: BeautifulSoup) -> List[Tag]:
    """Find product listing card elements from a category/list page."""
    candidate_selectors = [
        ".product-item",
        ".goods-item",
        ".sku-item",
        ".list-item",
        "li[data-sku]",
        "div[data-sku]",
        "article",
    ]

    for selector in candidate_selectors:
        cards = [card for card in soup.select(selector) if isinstance(card, Tag)]
        if cards:
            return cards

    fallback_cards = []
    for link in soup.select("a[href]"):
        href = link.get("href", "")
        if any(token in href.lower() for token in ["/product", "/goods", "/item", "/sku"]):
            container = link.find_parent(["li", "div", "article"]) or link
            if isinstance(container, Tag):
                fallback_cards.append(container)

    deduped: List[Tag] = []
    seen = set()
    for item in fallback_cards:
        marker = str(item)[:200]
        if marker not in seen:
            seen.add(marker)
            deduped.append(item)

    return deduped


def parse_product_from_card(card: Tag, base_url: str) -> Dict[str, str]:
    """Parse key product fields from a single listing card."""
    product_name = first_text(
        card,
        [
            ".product-name",
            ".goods-name",
            ".title",
            "h3",
            "h2",
            "a[title]",
            "a",
        ],
    )

    sku = first_text(
        card,
        [
            ".product-model",
            ".sku",
            ".model",
            ".item-code",
            ".code",
            "[data-sku]",
        ],
    ) or card.get("data-sku", "")

    part_description = first_text(
        card,
        [
            ".description",
            ".desc",
            ".product-desc",
            "p",
        ],
    )

    price = first_text(
        card,
        [
            ".price",
            ".product-price",
            ".goods-price",
            "[class*='price']",
        ],
    )

    detail_href = first_attr(
        card,
        [
            "a.product-link",
            "a.goods-link",
            "a[href*='product']",
            "a[href*='item']",
            "a[href]",
        ],
        "href",
    )
    detail_page_url = urljoin(base_url, detail_href)

    return {
        "product_name": product_name,
        "product_model_or_SKU": sku,
        "part_description": part_description,
        "price": price,
        "detail_page_url": detail_page_url,
    }


def extract_detail_specs(soup: BeautifulSoup) -> Dict[str, str]:
    """Extract detailed specifications from product detail page HTML."""
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

    for li in soup.select(".spec li, .specs li, .product-spec li"):
        text = li.get_text(" ", strip=True)
        if ":" in text:
            key, value = text.split(":", 1)
            key = key.strip()
            value = value.strip()
            if key and value:
                specs[key] = value

    return specs


def scrape_category(session: requests.Session, category_url: str) -> List[Dict[str, str]]:
    """Scrape category listing and then each product detail page."""
    soup = fetch_soup(session, category_url)
    if not soup:
        return []

    cards = find_product_cards(soup)
    print(f"[INFO] Found {len(cards)} product candidates on listing page.")

    results: List[Dict[str, str]] = []

    for idx, card in enumerate(cards, start=1):
        product = parse_product_from_card(card, BASE_URL)
        if not product["detail_page_url"]:
            print(f"[WARN] [{idx}/{len(cards)}] Missing detail URL; skipping.")
            continue

        print(
            f"[PROGRESS] [{idx}/{len(cards)}] "
            f"{product['product_name'] or 'Unnamed product'}"
        )

        detail_soup = fetch_soup(session, product["detail_page_url"])
        specs = extract_detail_specs(detail_soup) if detail_soup else {}
        product["detailed_specs"] = json.dumps(specs, ensure_ascii=False)

        results.append(product)

    return results


def write_csv(rows: List[Dict[str, str]], output_file: str) -> None:
    """Write scraped rows to CSV."""
    headers = [
        "product_name",
        "product_model_or_SKU",
        "part_description",
        "price",
        "detail_page_url",
        "detailed_specs",
    ]

    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
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
