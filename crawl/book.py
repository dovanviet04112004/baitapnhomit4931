import requests
from bs4 import BeautifulSoup
import json
import os
from urllib.parse import urljoin

BASE_URL = "https://books.toscrape.com/catalogue/page-1.html"
MAX_PAGES = 50
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

CRAWL_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(CRAWL_DIR, "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "books_toscrape_raw.json")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

data = []

for page in range(1, MAX_PAGES + 1):
    url = f"https://books.toscrape.com/catalogue/page-{page}.html"
    print(f"[36m▶ Đang crawl trang {page}: {url}[0m")

    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
    except Exception as e:
        print(f"⚠ Lỗi khi tải trang {page}: {e}")
        break

    soup = BeautifulSoup(response.text, "html.parser")

    items = soup.select("article.product_pod")

    if not items:
        print(f"⛔ Trang {page} không có sách, dừng.")
        break

    for item in items:
        title_el = item.select_one("h3 a")
        price_el = item.select_one("p.price_color")
        availability_el = item.select_one("p.instock.availability")
        rating_el = item.select_one("p.star-rating")

        title = title_el["title"].strip() if title_el and title_el.has_attr("title") else (title_el.get_text(strip=True) if title_el else None)
        price = price_el.get_text(strip=True) if price_el else None
        availability = availability_el.get_text(strip=True) if availability_el else None
        in_stock = None
        if availability is not None:
            in_stock = "In stock" in availability
        rating = None
        if rating_el:
            for cls in rating_el.get("class", []):
                if cls != "star-rating":
                    rating = cls
                    break

        category = None
        book_url = None
        if title_el and title_el.has_attr("href"):
            book_url = urljoin(url, title_el["href"])

        if book_url:
            try:
                detail_resp = requests.get(book_url, headers=HEADERS, timeout=15)
                detail_resp.raise_for_status()
                detail_soup = BeautifulSoup(detail_resp.text, "html.parser")
                cat_el = detail_soup.select_one("ul.breadcrumb li:nth-of-type(3) a")
                if cat_el:
                    category = cat_el.get_text(strip=True)
            except Exception as e:
                print(f"⚠ Lỗi khi lấy category cho sách '{title}': {e}")

        data.append({
            "title": title,
            "price": price,
            "availability": availability,
            "rating": rating,
            "in_stock": in_stock,
            "category": category,
        })

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

if not data:
    print("⚠ Không tìm thấy sách nào, có thể cấu trúc trang đã đổi.")
else:
    print(f"✅ Crawl thành công {len(data)} sách")
    print(f"📁 File lưu tại: {os.path.abspath(OUTPUT_FILE)}")
