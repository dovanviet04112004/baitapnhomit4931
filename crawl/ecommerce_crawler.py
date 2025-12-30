"""
Enhanced E-commerce Crawler for Real-time Price Analytics
Supports time-series data collection with comprehensive schema
"""

import requests
from bs4 import BeautifulSoup
import json
import os
import time
import random
import hashlib
import re
from datetime import datetime, timezone
from urllib.parse import urljoin
from typing import Dict, List, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configuration
BASE_URL = "https://webscraper.io/test-sites/e-commerce/allinone"
CRAWL_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(CRAWL_DIR, "output")
RAW_OUTPUT_FILE = os.path.join(OUTPUT_DIR, "ecommerce_raw.json")
SNAPSHOTS_DIR = os.path.join(OUTPUT_DIR, "snapshots")

# Create directories
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(SNAPSHOTS_DIR, exist_ok=True)

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]


class EcommerceCrawler:
    """Enhanced crawler with time-series and quality tracking"""
    
    def __init__(self, source_name: str = "webscraper.io"):
        self.source = source_name
        self.session = self._create_session()
        self.crawl_time = datetime.now(timezone.utc).isoformat()
        self.products = []
        self.stats = {
            "total_attempts": 0,
            "successful": 0,
            "failed": 0,
            "missing_fields": 0
        }
    
    def _create_session(self) -> requests.Session:
        """Create session with retry strategy and random User-Agent"""
        session = requests.Session()
        
        # Retry strategy for resilience
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        # Random User-Agent
        session.headers['User-Agent'] = random.choice(USER_AGENTS)
        
        return session
    
    def _rate_limit(self):
        """Random delay to avoid rate limiting"""
        time.sleep(random.uniform(0.5, 2.0))
    
    def _normalize_price(self, price_str: str) -> Optional[float]:
        """Extract numeric price from string"""
        if not price_str:
            return None
        
        # Remove non-numeric characters except decimal point
        price_numeric = re.sub(r'[^\d.]', '', price_str)
        
        try:
            return float(price_numeric) if price_numeric else None
        except ValueError:
            return None
    
    def _extract_with_fallback(self, soup: BeautifulSoup, selectors: List[str]) -> Optional[str]:
        """Try multiple selectors until one works"""
        for selector in selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    return element.get_text(strip=True)
            except Exception:
                continue
        return None
    
    def _generate_product_id(self, product_name: str, category: str) -> str:
        """Generate stable product ID for deduplication"""
        key = f"{product_name}|{category}"
        return hashlib.md5(key.encode()).hexdigest()[:16]
    
    def _calculate_quality_score(self, product: Dict) -> float:
        """Calculate data quality score based on field completeness"""
        required_fields = ['product_id', 'product_name', 'price', 'category']
        optional_fields = ['rating', 'num_reviews', 'discount_price', 'description', 'image_url']
        
        required_score = sum(1 for f in required_fields if product.get(f)) / len(required_fields)
        optional_score = sum(1 for f in optional_fields if product.get(f)) / len(optional_fields)
        
        return round(0.7 * required_score + 0.3 * optional_score, 2)
    
    def _save_html_snapshot(self, product_id: str, html_content: str) -> str:
        """Save raw HTML snapshot for audit purposes"""
        timestamp = datetime.now().strftime("%Y%m%d")
        snapshot_dir = os.path.join(SNAPSHOTS_DIR, timestamp)
        os.makedirs(snapshot_dir, exist_ok=True)
        
        snapshot_path = os.path.join(snapshot_dir, f"{product_id}.html")
        
        with open(snapshot_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return snapshot_path
    
    def crawl_category_page(self, category_url: str, category_name: str) -> List[str]:
        """Crawl category page with pagination support and return all product URLs"""
        print(f"\n📂 Crawling category: {category_name}")
        print(f"   URL: {category_url}")
        
        all_product_links = []
        page_num = 1
        
        while True:
            # Construct page URL
            if page_num == 1:
                page_url = category_url
            else:
                page_url = f"{category_url}?page={page_num}"
            
            self._rate_limit()
            start_time = time.time()
            
            try:
                response = self.session.get(page_url, timeout=15)
                response.raise_for_status()
                
                duration_ms = int((time.time() - start_time) * 1000)
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract product links from current page
                page_product_links = []
                for link in soup.select('a.title'):
                    href = link.get('href')
                    if href:
                        full_url = urljoin(page_url, href)
                        if full_url not in all_product_links:  # Avoid duplicates
                            page_product_links.append(full_url)
                            all_product_links.append(full_url)
                
                if page_product_links:
                    print(f"   � Page {page_num}: Found {len(page_product_links)} products ({duration_ms}ms)")
                else:
                    # No products found, we've reached the end
                    break
                
                # Check if there's a next page
                next_button = soup.select_one('a[rel="next"]') or soup.select_one('li.page-item:not(.disabled) a[aria-label="Next"]')
                if not next_button:
                    break
                
                page_num += 1
                
            except Exception as e:
                print(f"   ⚠️  Error on page {page_num}: {e}")
                break
        
        print(f"   ✅ Total products found: {len(all_product_links)} across {page_num} page(s)")
        return all_product_links
    
    def discover_categories(self) -> List[tuple]:
        """Auto-discover all categories and subcategories from the website"""
        print(f"\n🔍 Discovering categories from {BASE_URL}...")
        
        try:
            response = self.session.get(BASE_URL, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            categories = []
            
            # Find all category links in navigation
            # Look for main categories and subcategories
            nav_links = soup.select('a.category-link, div.side-collapse a, nav a')
            
            seen_urls = set()
            for link in nav_links:
                href = link.get('href', '')
                category_name = link.get_text(strip=True)
                
                # Filter for valid category URLs
                if href and ('computers' in href.lower() or 'phones' in href.lower() or 
                            'tablets' in href.lower() or 'touch' in href.lower()):
                    full_url = urljoin(BASE_URL, href)
                    
                    # Avoid duplicates
                    if full_url not in seen_urls and category_name:
                        categories.append((category_name, full_url))
                        seen_urls.add(full_url)
            
            # If auto-discovery fails, fall back to known categories
            if not categories:
                print("   ⚠️  Auto-discovery failed, using default categories")
                categories = [
                    ("Computers", f"{BASE_URL}/computers"),
                    ("Phones", f"{BASE_URL}/phones"),
                    ("Laptops", f"{BASE_URL}/computers/laptops"),
                    ("Tablets", f"{BASE_URL}/computers/tablets"),
                    ("Touch", f"{BASE_URL}/phones/touch"),
                ]
            
            print(f"   ✅ Found {len(categories)} categories:")
            for name, url in categories:
                print(f"      • {name}")
            
            return categories
            
        except Exception as e:
            print(f"   ❌ Discovery error: {e}")
            # Return default categories as fallback
            return [
                ("Computers", f"{BASE_URL}/computers"),
                ("Phones", f"{BASE_URL}/phones"),
                ("Laptops", f"{BASE_URL}/computers/laptops"),
                ("Tablets", f"{BASE_URL}/computers/tablets"),
                ("Touch", f"{BASE_URL}/phones/touch"),
            ]
    
    def crawl_product_page(self, product_url: str, category: str) -> Optional[Dict]:
        """Crawl individual product page and extract data"""
        self.stats["total_attempts"] += 1
        self._rate_limit()
        start_time = time.time()
        
        try:
            response = self.session.get(product_url, timeout=15)
            response.raise_for_status()
            
            duration_ms = int((time.time() - start_time) * 1000)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract product data
            product_name = self._extract_with_fallback(soup, [
                'h4.card-title',
                'h1.product-title',
                'div.product-name'
            ])
            
            price_str = self._extract_with_fallback(soup, [
                'h4.price',
                'span.price',
                'div.product-price'
            ])
            price = self._normalize_price(price_str)
            
            # Rating
            rating_element = soup.select_one('p[data-rating]')
            rating = None
            if rating_element:
                rating_str = rating_element.get('data-rating')
                try:
                    rating = float(rating_str) if rating_str else None
                except ValueError:
                    rating = None
            
            # Reviews count
            reviews_element = soup.select_one('p.review-count')
            num_reviews = 0
            if reviews_element:
                reviews_text = reviews_element.get_text(strip=True)
                reviews_match = re.search(r'(\d+)', reviews_text)
                if reviews_match:
                    num_reviews = int(reviews_match.group(1))
            
            # Description
            description = self._extract_with_fallback(soup, [
                'div.description',
                'p.product-description',
                'div.product-details'
            ])
            
            # Image
            image_element = soup.select_one('img.img-fluid')
            image_url = None
            if image_element:
                image_url = urljoin(product_url, image_element.get('src', ''))
            
            # Generate product ID
            product_id = self._generate_product_id(product_name or "unknown", category)
            
            # Build product record
            product = {
                "crawl_time": self.crawl_time,
                "source": self.source,
                "product_id": product_id,
                "product_url": product_url,
                "product_name": product_name,
                "category": category,
                "price": price,
                "currency": "USD",
                "discount_price": None,  # Will be simulated later
                "discount_percentage": None,
                "availability": "In Stock",  # Default for demo site
                "in_stock": True,
                "stock_quantity": random.randint(5, 50),  # Simulated
                "rating": rating,
                "num_reviews": num_reviews,
                "location": None,  # Not available on demo site
                "seller_name": None,
                "image_url": image_url,
                "description": description,
                "raw_html_snapshot_path": None,
                "raw_json": None,
                "metadata": {
                    "crawler_version": "2.0",
                    "crawl_duration_ms": duration_ms,
                    "http_status": response.status_code
                }
            }
            
            # Simulate discount for some products (30% chance)
            if random.random() < 0.3 and price:
                discount_percent = random.uniform(5, 30)
                product["discount_price"] = round(price * (1 - discount_percent / 100), 2)
                product["discount_percentage"] = round(discount_percent, 1)
            
            # Save HTML snapshot
            snapshot_path = self._save_html_snapshot(product_id, response.text)
            product["raw_html_snapshot_path"] = snapshot_path
            
            # Calculate quality score
            quality_score = self._calculate_quality_score(product)
            product["data_quality_score"] = quality_score
            
            # Track missing fields
            if quality_score < 0.8:
                self.stats["missing_fields"] += 1
            
            self.stats["successful"] += 1
            return product
            
        except Exception as e:
            print(f"   ⚠️  Error crawling {product_url}: {e}")
            self.stats["failed"] += 1
            return None
    
    def crawl_all_categories(self):
        """Crawl all categories from the demo site with auto-discovery"""
        print(f"\n{'='*60}")
        print(f"🚀 Starting E-commerce Crawler v2.1 (Enhanced)")
        print(f"{'='*60}")
        print(f"⏰ Crawl Time: {self.crawl_time}")
        print(f"🌐 Source: {self.source}")
        print(f"📁 Output: {RAW_OUTPUT_FILE}")
        
        # Auto-discover categories
        categories = self.discover_categories()
        
        for category_name, category_url in categories:
            product_urls = self.crawl_category_page(category_url, category_name)
            
            for i, product_url in enumerate(product_urls, 1):
                print(f"   [{i}/{len(product_urls)}] Crawling product...", end='\r')
                product = self.crawl_product_page(product_url, category_name)
                
                if product:
                    self.products.append(product)
            
            print(f"   ✅ Completed {category_name}: {len(product_urls)} products")
        
        self._save_results()
        self._print_summary()
    
    def _save_results(self):
        """Save crawled data to JSON file"""
        with open(RAW_OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.products, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Saved {len(self.products)} products to {RAW_OUTPUT_FILE}")
    
    def _print_summary(self):
        """Print crawl statistics"""
        print(f"\n{'='*60}")
        print(f"📊 Crawl Summary")
        print(f"{'='*60}")
        print(f"Total Attempts:    {self.stats['total_attempts']}")
        print(f"✅ Successful:     {self.stats['successful']}")
        print(f"❌ Failed:         {self.stats['failed']}")
        print(f"⚠️  Missing Fields: {self.stats['missing_fields']}")
        
        if self.stats['total_attempts'] > 0:
            success_rate = (self.stats['successful'] / self.stats['total_attempts']) * 100
            print(f"📈 Success Rate:   {success_rate:.1f}%")
        
        if self.products:
            avg_quality = sum(p.get('data_quality_score', 0) for p in self.products) / len(self.products)
            print(f"⭐ Avg Quality:    {avg_quality:.2f}")
        
        print(f"{'='*60}\n")


def main():
    """Main entry point"""
    crawler = EcommerceCrawler(source_name="webscraper.io")
    crawler.crawl_all_categories()


if __name__ == "__main__":
    main()
