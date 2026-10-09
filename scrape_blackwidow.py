#!/usr/bin/env python3
"""Scrape blackwidowexhausts.co.uk (EKM store) into a Shopify product-import CSV.

Pipeline per product:
    price on site (GBP, or any other currency) -> convert to INR -> +35% markup

Usage examples:
    python scrape_blackwidow.py --limit 25                 # quick test run
    python scrape_blackwidow.py                            # full crawl (~22k products)
    python scrape_blackwidow.py --markup 35                # price = site price (inc. VAT) -> INR -> +35%
    python scrape_blackwidow.py --rate 105.5               # override today's live GBP->INR rate

Scraped data is cached in a JSONL file, so an interrupted crawl resumes where it
stopped and the CSV can be rebuilt (e.g. with a different rate) without re-crawling.
"""
import argparse
import csv
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlsplit

import requests
from bs4 import BeautifulSoup

BASE = "https://www.blackwidowexhausts.co.uk"
SITEMAP = BASE + "/sitemap.xml"
UA = "Mozilla/5.0 (compatible; ProductCatalogueScraper/1.0)"
RATE_API = "https://open.er-api.com/v6/latest/{cur}"

SHOPIFY_COLUMNS = [
    "Handle", "Title", "Body (HTML)", "Vendor", "Product Category", "Type", "Tags",
    "Published", "Option1 Name", "Option1 Value", "Variant SKU", "Variant Grams",
    "Variant Inventory Tracker", "Variant Inventory Qty", "Variant Inventory Policy",
    "Variant Fulfillment Service", "Variant Price", "Variant Compare At Price",
    "Variant Requires Shipping", "Variant Taxable", "Variant Barcode", "Image Src",
    "Image Position", "Image Alt Text", "Gift Card", "SEO Title", "SEO Description",
    "Variant Image", "Variant Weight Unit", "Status",
]

_session = requests.Session()
_session.headers["User-Agent"] = UA
_print_lock = threading.Lock()


def log(msg):
    with _print_lock:
        print(msg, file=sys.stderr, flush=True)


def get(url, retries=4, timeout=40):
    for attempt in range(retries):
        try:
            r = _session.get(url, timeout=timeout)
            if r.status_code == 200:
                return r.text
            if r.status_code == 404:
                return None
        except requests.RequestException:
            pass
        time.sleep(2 ** attempt)
    return None


# --------------------------------------------------------------------------- scraping
def product_urls():
    xml = get(SITEMAP)
    if not xml:
        sys.exit("Could not download sitemap.xml")
    return re.findall(r"<loc>([^<]+-p\.asp)</loc>", xml)


def clean_description(html_fragment, fallback_text=""):
    """Strip EKM's inline-style / font / span noise, keep structural HTML."""
    if not html_fragment:
        return f"<p>{fallback_text}</p>" if fallback_text else ""
    soup = BeautifulSoup(html_fragment, "html.parser")
    for t in soup(["script", "style", "iframe", "form", "input"]):
        t.decompose()
    for t in soup.find_all(["span", "font", "div"]):
        t.unwrap()
    for t in soup.find_all(True):
        keep = {k: v for k, v in t.attrs.items()
                if (t.name == "a" and k == "href") or (t.name == "img" and k in ("src", "alt"))}
        t.attrs = keep
    for t in soup.find_all(["p", "li", "strong", "b", "em", "u"]):
        if not t.get_text(strip=True) and not t.find("img"):
            t.decompose()
    out = re.sub(r"(<br\s*/?>\s*){3,}", "<br/><br/>", str(soup))
    return re.sub(r"\s+", " ", out).strip()


def parse_product(url, html):
    soup = BeautifulSoup(html, "html.parser")
    ld = None
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            d = json.loads(s.string or "")
        except ValueError:
            continue
        if isinstance(d, dict) and d.get("@type") == "Product":
            ld = d
            break
    if not ld:
        return None

    offers = ld.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    spec = offers.get("priceSpecification") or {}
    price_inc = offers.get("price") or spec.get("price")
    currency = offers.get("priceCurrency") or spec.get("priceCurrency") or "GBP"
    if price_inc in (None, ""):
        return None

    # ex-VAT price is shown on the page
    ex_tag = soup.find(id="_EKM_PRODUCTPRICE_EX_VAT")
    price_ex = None
    if ex_tag:
        try:
            price_ex = float(ex_tag.get_text(strip=True).replace(",", ""))
        except ValueError:
            pass

    sku = ld.get("sku")
    if isinstance(sku, list):
        sku = str(sku[0]) if sku else ""
    sku = str(sku or "")
    product_id = re.search(r"-(\d+)-p\.asp$", url)
    product_id = product_id.group(1) if product_id else ""

    desc_tag = soup.select_one(".full-product-desc > span[itemprop=description]") \
        or soup.select_one(".full-product-desc")
    body = clean_description(desc_tag.decode_contents() if desc_tag else "", ld.get("description", "").strip())

    # gallery images, in order; fall back to the JSON-LD / og image
    images = []
    for a in soup.select("a[data-lightbox-order]"):
        href = a.get("href")
        if href:
            images.append((int(a.get("data-lightbox-order") or 0), href.split("?")[0]))
    images = [u for _, u in sorted(images)]
    if not images:
        main = ld.get("image")
        main = main[0] if isinstance(main, list) else main
        if main:
            images = [main.split("?")[0]]
    images = list(dict.fromkeys(images))

    # stock
    qty = None
    q = soup.find(id="_EKM_PRODUCTSTOCK_3")
    if q and q.get_text(strip=True).isdigit():
        qty = int(q.get_text(strip=True))
    in_stock = "InStock" in str(offers.get("availability", ""))
    if qty is None:
        qty = 0 if not in_stock else 10

    # categories -> tags (bike make / model / part type = the "compatibility" info)
    cats = []
    for m in re.finditer(r'"category":"([^"]+)"', html):
        cats += [c.strip() for c in m.group(1).split(",")]
    crumbs = [a.get_text(strip=True) for a in soup.select(".product-page-breadcrumbs .ekmps-location a")][1:]
    tags = []
    for entry in cats:
        tags += [p.strip() for p in entry.split(">")]
    tags += crumbs
    tags = list(dict.fromkeys(t for t in tags if t))
    ptype = tags[-1] if tags else "Exhaust"

    n_opts = soup.find("input", attrs={"name": "numberofoptions"})
    n_opts = int(n_opts["value"]) if n_opts and (n_opts.get("value") or "").isdigit() else 0

    return {
        "url": url,
        "product_id": product_id,
        "name": (ld.get("name") or "").strip(),
        "sku": sku or product_id,
        "description_html": body,
        "brand": (ld.get("brand") or {}).get("name", "Black Widow Exhausts"),
        "price_inc_vat": float(price_inc),
        "price_ex_vat": price_ex,
        "currency": currency,
        "images": images,
        "stock_qty": qty,
        "in_stock": in_stock,
        "tags": tags,
        "type": ptype,
        "has_options": n_opts > 0,
    }


def load_cache(path):
    done = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                rec = json.loads(line)
                done[rec["url"]] = rec
            except (ValueError, KeyError):
                pass
    return done


def crawl(urls, cache_path, workers, delay):
    done = load_cache(cache_path)
    todo = [u for u in urls if u not in done]
    log(f"{len(done)} cached, {len(todo)} to fetch")
    lock = threading.Lock()

    def work(u):
        time.sleep(delay)
        html = get(u)
        return u, (parse_product(u, html) if html else None)

    failed = []
    with cache_path.open("a", encoding="utf-8") as fh, ThreadPoolExecutor(workers) as ex:
        futs = [ex.submit(work, u) for u in todo]
        for i, f in enumerate(as_completed(futs), 1):
            u, rec = f.result()
            if rec:
                with lock:
                    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    fh.flush()
                done[u] = rec
            else:
                failed.append(u)
            if i % 100 == 0:
                log(f"  {i}/{len(todo)} fetched ({len(failed)} failed)")
    if failed:
        Path(cache_path.with_suffix(".failed.txt")).write_text("\n".join(failed))
        log(f"{len(failed)} pages failed/skipped -> {cache_path.with_suffix('.failed.txt')}")
    return [done[u] for u in urls if u in done]


# --------------------------------------------------------------------------- pricing
def fetch_rate_to_inr(currency):
    if currency == "INR":
        return 1.0
    r = requests.get(RATE_API.format(cur=currency), timeout=30).json()
    if r.get("result") != "success" or "INR" not in r.get("rates", {}):
        raise RuntimeError(f"Could not get {currency}->INR rate: {r}")
    return float(r["rates"]["INR"])


class Pricer:
    def __init__(self, markup_pct, fixed_gbp_rate=None):
        self.markup = 1 + markup_pct / 100.0
        self.fixed_gbp_rate = fixed_gbp_rate
        self.rates = {}

    def rate(self, currency):
        currency = currency.upper()
        if currency not in self.rates:
            if currency == "GBP" and self.fixed_gbp_rate:
                self.rates[currency] = self.fixed_gbp_rate
            else:
                self.rates[currency] = fetch_rate_to_inr(currency)
            log(f"Exchange rate ({time.strftime('%Y-%m-%d')}): 1 {currency} = {self.rates[currency]:.4f} INR")
        return self.rates[currency]

    def to_inr_with_markup(self, amount, currency):
        # already in rupees (INR) -> no conversion; any other currency -> convert to INR
        inr = amount * self.rate(currency)
        return round(inr * self.markup, 2)


# --------------------------------------------------------------------------- CSV
def handle_from_url(url):
    slug = urlsplit(url).path.rsplit("/", 1)[-1]
    return re.sub(r"-p\.asp$", "", slug)


def write_shopify_csv(products, out_path, pricer, published):
    seen_handles = set()
    rows = 0
    with open(out_path, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=SHOPIFY_COLUMNS)
        w.writeheader()
        for p in products:
            # price you would pay on the site (VAT-inclusive) -> INR -> +markup
            price = pricer.to_inr_with_markup(p["price_inc_vat"], p["currency"])
            handle = handle_from_url(p["url"])
            if handle in seen_handles:
                handle = f"{handle}-{p['product_id']}"
            seen_handles.add(handle)
            imgs = p["images"] or [""]
            first = {
                "Handle": handle,
                "Title": p["name"],
                "Body (HTML)": p["description_html"],
                "Vendor": p["brand"],
                "Product Category": "Vehicles & Parts > Vehicle Parts & Accessories > Motor Vehicle Parts > Motor Vehicle Exhaust",
                "Type": p["type"],
                "Tags": ", ".join(p["tags"]),
                "Published": "TRUE" if published else "FALSE",
                "Option1 Name": "Title",
                "Option1 Value": "Default Title",
                "Variant SKU": p["sku"],
                "Variant Grams": "",
                "Variant Inventory Tracker": "shopify",
                "Variant Inventory Qty": p["stock_qty"],
                "Variant Inventory Policy": "deny",
                "Variant Fulfillment Service": "manual",
                "Variant Price": f"{price:.2f}",
                "Variant Compare At Price": "",
                "Variant Requires Shipping": "TRUE",
                "Variant Taxable": "TRUE",
                "Variant Barcode": "",
                "Image Src": imgs[0],
                "Image Position": 1 if imgs[0] else "",
                "Image Alt Text": p["name"] if imgs[0] else "",
                "Gift Card": "FALSE",
                "SEO Title": p["name"][:70],
                "SEO Description": BeautifulSoup(p["description_html"], "html.parser").get_text(" ", strip=True)[:320],
                "Variant Image": "",
                "Variant Weight Unit": "kg",
                "Status": "active" if published else "draft",
            }
            w.writerow(first)
            rows += 1
            for pos, src in enumerate(imgs[1:], start=2):
                w.writerow({"Handle": handle, "Image Src": src, "Image Position": pos, "Image Alt Text": p["name"]})
                rows += 1
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="shopify_products.csv")
    ap.add_argument("--cache", default="scraped_products.jsonl")
    ap.add_argument("--limit", type=int, help="only scrape the first N products (testing)")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--delay", type=float, default=0.3, help="seconds each worker waits before a request")
    ap.add_argument("--markup", type=float, default=35.0, help="percent added after conversion to INR")
    ap.add_argument("--rate", type=float, help="fixed GBP->INR rate (default: today's live rate)")
    ap.add_argument("--draft", action="store_true", help="import as draft (unpublished) products")
    args = ap.parse_args()

    urls = product_urls()
    log(f"{len(urls)} product URLs in sitemap")
    if args.limit:
        urls = urls[: args.limit]

    products = crawl(urls, Path(args.cache), args.workers, args.delay)
    if not products:
        sys.exit("No products scraped")

    pricer = Pricer(args.markup, args.rate)
    rows = write_shopify_csv(products, args.out, pricer, published=not args.draft)
    with_opts = [p["url"] for p in products if p["has_options"]]
    if with_opts:
        Path("products_with_options.txt").write_text("\n".join(with_opts))
        log(f"{len(with_opts)} products have selectable options; exported at base price "
            f"(listed in products_with_options.txt for manual review)")
    log(f"Wrote {len(products)} products ({rows} CSV rows) to {args.out}")


if __name__ == "__main__":
    main()
