# Data Sources Documentation

## 1. Selected E-commerce Websites

### 1.1 Primary Source: Demo E-commerce Site
**Website:** https://webscraper.io/test-sites/e-commerce/allinone  
**Type:** Demo site designed for web scraping practice  
**Language:** English (for demonstration purposes)

**Advantages:**
- ✅ Scraping-friendly (designed for testing)
- ✅ No rate limiting
- ✅ Consistent HTML structure
- ✅ Multiple product categories
- ✅ Price information available
- ✅ Rating system present

**Limitations:**
- ❌ Static prices (no real-time changes)
- ❌ Not Vietnamese
- ❌ Limited product variety

### 1.2 Alternative Source: Books to Scrape (Enhanced)
**Website:** https://books.toscrape.com  
**Type:** Demo book catalog  

**Note:** We will enhance the existing crawler to simulate price changes over time for demonstration purposes.

### 1.3 Future Production Sources (Vietnamese E-commerce)
For production deployment, consider these Vietnamese platforms:

1. **Tiki.vn**
   - Category pages: `https://tiki.vn/dien-thoai-may-tinh-bang/c1789`
   - Product pages: `https://tiki.vn/[product-slug]/p[product-id].html`
   - ⚠️ Requires: User-Agent rotation, rate limiting, robots.txt compliance

2. **Shopee.vn**
   - Category pages: `https://shopee.vn/[category-name]-cat.[category-id]`
   - Product pages: `https://shopee.vn/[product-name]-i.[shop-id].[product-id]`
   - ⚠️ Requires: API reverse engineering, anti-bot measures

3. **Sendo.vn**
   - Category pages: `https://www.sendo.vn/[category-name]`
   - Product pages: `https://www.sendo.vn/[product-slug]`
   - ⚠️ Requires: Session management, CAPTCHA handling

---

## 2. Data Schema Design

### 2.1 Raw Product Schema (Kafka Topic: `raw_products`)

```json
{
  "crawl_time": "2025-12-30T17:05:00+07:00",
  "source": "webscraper.io",
  "product_id": "product-12345",
  "product_url": "https://webscraper.io/test-sites/e-commerce/allinone/product/123",
  "product_name": "Laptop Dell Inspiron 15",
  "category": "Computers",
  "subcategory": "Laptops",
  "price": 799.99,
  "currency": "USD",
  "discount_price": 699.99,
  "discount_percentage": 12.5,
  "availability": "In Stock",
  "in_stock": true,
  "stock_quantity": 15,
  "rating": 4.5,
  "num_reviews": 128,
  "location": "Hanoi, Vietnam",
  "seller_name": "Tech Store VN",
  "image_url": "https://example.com/images/laptop.jpg",
  "description": "High-performance laptop with Intel i7...",
  "raw_html_snapshot_path": "hdfs://localhost:9000/data/snapshots/2025-12-30/product-12345.html",
  "raw_json": "{...}",
  "metadata": {
    "crawler_version": "2.0",
    "crawl_duration_ms": 1250,
    "http_status": 200
  }
}
```

### 2.2 Clean Product Schema (Kafka Topic: `clean_products`)

After data cleaning and validation:

```json
{
  "crawl_time": "2025-12-30T17:05:00+07:00",
  "source": "webscraper.io",
  "product_id": "product-12345",
  "product_url": "https://webscraper.io/test-sites/e-commerce/allinone/product/123",
  "product_name": "Laptop Dell Inspiron 15",
  "category": "Computers",
  "subcategory": "Laptops",
  "price": 799.99,
  "currency": "USD",
  "discount_price": 699.99,
  "discount_percentage": 12.5,
  "final_price": 699.99,
  "availability": "in_stock",
  "stock_quantity": 15,
  "rating": 4.5,
  "num_reviews": 128,
  "location": "Hanoi",
  "seller_name": "Tech Store VN",
  "image_url": "https://example.com/images/laptop.jpg",
  "is_valid": true,
  "data_quality_score": 0.95
}
```

### 2.3 Alert Schema (Kafka Topic: `alerts`)

For price anomaly detection:

```json
{
  "alert_time": "2025-12-30T17:10:00+07:00",
  "alert_type": "price_spike",
  "severity": "high",
  "product_id": "product-12345",
  "product_name": "Laptop Dell Inspiron 15",
  "category": "Computers",
  "previous_price": 699.99,
  "current_price": 899.99,
  "price_change_percent": 28.57,
  "price_change_absolute": 200.00,
  "z_score": 3.5,
  "threshold": 2.0,
  "message": "Price increased by 28.57% (z-score: 3.5)",
  "source": "webscraper.io"
}
```

---

## 3. Data Challenges & Solutions

### 3.1 Missing Fields

**Challenge:**
- Not all products have ratings/reviews
- Discount prices may be absent
- Stock quantity often unavailable
- Location data inconsistent

**Solutions:**
- ✅ Use nullable fields in schema
- ✅ Set default values: `rating=null`, `num_reviews=0`
- ✅ Calculate `data_quality_score` based on field completeness
- ✅ Flag incomplete records for manual review

**Implementation:**
```python
def calculate_quality_score(record):
    required_fields = ['product_id', 'product_name', 'price']
    optional_fields = ['rating', 'num_reviews', 'discount_price', 'location']
    
    required_score = sum(1 for f in required_fields if record.get(f)) / len(required_fields)
    optional_score = sum(1 for f in optional_fields if record.get(f)) / len(optional_fields)
    
    return 0.7 * required_score + 0.3 * optional_score
```

---

### 3.2 Data Inconsistency Between Categories/Sources

**Challenge:**
- Different categories use different units (VND vs USD)
- Price formats vary: "799.99", "$799", "799,99 đ"
- Date formats inconsistent
- Category naming differs across sources

**Solutions:**
- ✅ Normalize currency to single standard (USD or VND)
- ✅ Use regex to extract numeric price values
- ✅ Standardize timestamps to ISO 8601 format
- ✅ Create category mapping table

**Implementation:**
```python
import re
from datetime import datetime

def normalize_price(price_str, currency_hint='USD'):
    # Extract numeric value
    price_numeric = re.sub(r'[^\d.]', '', price_str)
    price = float(price_numeric) if price_numeric else None
    
    # Currency conversion if needed
    if currency_hint == 'VND' and price:
        price = price / 23000  # Convert VND to USD
    
    return price

def normalize_timestamp(dt_str):
    # Parse various formats and convert to ISO 8601
    return datetime.fromisoformat(dt_str).isoformat()
```

---

### 3.3 Schema Drift (HTML Structure Changes)

**Challenge:**
- Websites update their HTML structure
- CSS selectors become invalid
- New fields added/removed

**Solutions:**
- ✅ Use multiple selector strategies (CSS, XPath, fallback)
- ✅ Log schema changes to monitoring system
- ✅ Version crawler code with schema version
- ✅ Implement graceful degradation

**Implementation:**
```python
def extract_with_fallback(soup, selectors):
    """Try multiple selectors until one works"""
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)
        except Exception:
            continue
    return None

# Usage
price = extract_with_fallback(soup, [
    'span.price-current',
    'div.product-price',
    'p[data-price]',
    'span[itemprop="price"]'
])
```

---

### 3.4 Duplicate Products

**Challenge:**
- Same product appears multiple times (different URLs)
- Product ID changes over time
- Duplicate entries from multiple crawls

**Solutions:**
- ✅ Generate stable product fingerprint (name + category + price range)
- ✅ Deduplication in Spark using `dropDuplicates()`
- ✅ Track product URL history
- ✅ Use fuzzy matching for product names

**Implementation:**
```python
import hashlib

def generate_product_fingerprint(product):
    """Create stable ID for deduplication"""
    key = f"{product['product_name']}|{product['category']}|{int(product['price'])}"
    return hashlib.md5(key.encode()).hexdigest()
```

---

### 3.5 Rate Limiting & Bot Detection

**Challenge:**
- Websites block aggressive crawling
- IP bans after too many requests
- CAPTCHA challenges
- Session timeouts

**Solutions:**
- ✅ Implement exponential backoff
- ✅ Rotate User-Agent headers
- ✅ Add random delays between requests (1-3 seconds)
- ✅ Use proxy rotation (for production)
- ✅ Respect robots.txt
- ✅ Limit concurrent requests

**Implementation:**
```python
import time
import random
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def create_session():
    session = requests.Session()
    
    # Retry strategy
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    # Rotate User-Agent
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
    ]
    session.headers['User-Agent'] = random.choice(user_agents)
    
    return session

def crawl_with_rate_limit(url):
    time.sleep(random.uniform(1, 3))  # Random delay
    response = session.get(url, timeout=15)
    return response
```

---

## 4. Data Volume Estimates

### Initial Crawl (Day 1)
- **Products per category:** ~50-100
- **Categories:** 5-10
- **Total products:** 500-1,000
- **Storage (raw JSON):** ~5-10 MB

### Daily Incremental Crawl
- **New products:** 10-50
- **Price updates:** 500-1,000 (all existing products)
- **Storage per day:** ~5 MB

### Monthly Data
- **Total records:** ~30,000 (1,000 products × 30 days)
- **Storage:** ~150 MB (compressed Parquet)

### Yearly Data
- **Total records:** ~365,000
- **Storage:** ~1.8 GB (compressed)

---

## 5. Compliance & Ethics

### Legal Considerations
- ✅ Use only publicly accessible data
- ✅ Respect robots.txt directives
- ✅ Do not overload servers (rate limiting)
- ✅ Attribute data sources properly
- ⚠️ Check Terms of Service before production deployment

### Ethical Guidelines
- ✅ Do not scrape personal user data
- ✅ Do not use data for competitive harm
- ✅ Implement data retention policies
- ✅ Anonymize sensitive information

---

## 6. Monitoring & Alerts

### Crawler Health Metrics
- Success rate (% of successful requests)
- Average response time
- Error rate by type (404, 500, timeout)
- Data quality score trends

### Data Quality Alerts
- Sudden drop in success rate (< 80%)
- High rate of missing fields (> 30%)
- Schema drift detected
- Duplicate rate spike (> 10%)

---

## Summary

This data sources documentation provides:
1. ✅ Selected websites with URLs
2. ✅ Comprehensive schema design (raw, clean, alerts)
3. ✅ Documented 5 major data challenges with solutions
4. ✅ Implementation examples for each challenge
5. ✅ Data volume estimates
6. ✅ Compliance guidelines

**Next Steps:**
- Implement enhanced crawler with this schema
- Set up Kafka topics with these schemas
- Implement data quality monitoring
