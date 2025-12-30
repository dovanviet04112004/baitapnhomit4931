"""
Enhanced E-commerce Crawler with Kafka Streaming
Continuously crawls products and streams to Kafka in real-time
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

# Import Kafka producer
try:
    from kafka_producer import EcommerceKafkaProducer
    KAFKA_ENABLED = True
except ImportError:
    print("⚠️  Kafka producer not available. Running in file-only mode.")
    KAFKA_ENABLED = False

# Configuration
BASE_URL = "https://webscraper.io/test-sites/e-commerce/allinone"
CRAWL_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(CRAWL_DIR, "output")
RAW_OUTPUT_FILE = os.path.join(OUTPUT_DIR, "ecommerce_raw.json")
SNAPSHOTS_DIR = os.path.join(OUTPUT_DIR, "snapshots")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:19092'
KAFKA_TOPIC_RAW = 'raw_products'

# Streaming configuration
CRAWL_INTERVAL_SECONDS = 300  # 5 minutes
CONTINUOUS_MODE = False  # Set to True for continuous crawling

# Create directories
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(SNAPSHOTS_DIR, exist_ok=True)

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]


class StreamingEcommerceCrawler:
    """Enhanced crawler with Kafka streaming support"""
    
    def __init__(self, source_name: str = "webscraper.io", enable_kafka: bool = True):
        self.source = source_name
        self.session = self._create_session()
        self.products = []
        self.stats = {
            "total_attempts": 0,
            "successful": 0,
            "failed": 0,
            "missing_fields": 0,
            "kafka_sent": 0,
            "kafka_failed": 0
        }
        
        # Initialize Kafka producer
        self.kafka_enabled = enable_kafka and KAFKA_ENABLED
        self.kafka_producer = None
        
        if self.kafka_enabled:
            try:
                self.kafka_producer = EcommerceKafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
                )
                print("✅ Kafka producer initialized")
            except Exception as e:
                print(f"⚠️  Failed to initialize Kafka producer: {e}")
                print("   Continuing in file-only mode...")
                self.kafka_enabled = False
    
    def _create_session(self) -> requests.Session:
        """Create session with retry strategy and random User-Agent"""
        session = requests.Session()
        
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        session.headers['User-Agent'] = random.choice(USER_AGENTS)
        
        return session
    
    def _rate_limit(self):
        """Random delay to avoid rate limiting"""
        time.sleep(random.uniform(0.5, 2.0))
    
    def _normalize_price(self, price_str: str) -> Optional[float]:
        """Extract numeric price from string"""
        if not price_str:
            return None
        
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
    
    def crawl_product_page(self, product_url: str, category: str, crawl_time: str) -> Optional[Dict]:
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
            
            # Add price variation to simulate real-time changes
            if price:
                price_variation = random.uniform(-0.05, 0.05)  # ±5% variation
                price = round(price * (1 + price_variation), 2)
            
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
                "crawl_time": crawl_time,
                "source": self.source,
                "product_id": product_id,
                "product_url": product_url,
                "product_name": product_name,
                "category": category,
                "price": price,
                "currency": "USD",
                "discount_price": None,
                "discount_percentage": None,
                "availability": "In Stock",
                "in_stock": True,
                "stock_quantity": random.randint(5, 50),
                "rating": rating,
                "num_reviews": num_reviews,
                "location": None,
                "seller_name": None,
                "image_url": image_url,
                "description": description,
                "raw_html_snapshot_path": None,
                "raw_json": None,
                "metadata": {
                    "crawler_version": "2.0-streaming",
                    "crawl_duration_ms": duration_ms,
                    "http_status": response.status_code
                }
            }
            
            # Simulate discount for some products
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
            
            if quality_score < 0.8:
                self.stats["missing_fields"] += 1
            
            # Send to Kafka
            if self.kafka_enabled and self.kafka_producer:
                if self.kafka_producer.send_product(product, topic=KAFKA_TOPIC_RAW):
                    self.stats["kafka_sent"] += 1
                else:
                    self.stats["kafka_failed"] += 1
            
            self.stats["successful"] += 1
            return product
            
        except Exception as e:
            print(f"   ⚠️  Error crawling {product_url}: {e}")
            self.stats["failed"] += 1
            return None
    
    def crawl_single_run(self):
        """Perform a single crawl run"""
        crawl_time = datetime.now(timezone.utc).isoformat()
        
        print(f"\n{'='*60}")
        print(f"🚀 Crawl Run Started")
        print(f"{'='*60}")
        print(f"⏰ Time: {crawl_time}")
        print(f"🌐 Source: {self.source}")
        print(f"📡 Kafka: {'Enabled' if self.kafka_enabled else 'Disabled'}")
        
        categories = [
            ("Computers", f"{BASE_URL}/computers"),
            ("Phones", f"{BASE_URL}/phones"),
        ]
        
        run_products = []
        
        for category_name, category_url in categories:
            print(f"\n📂 Category: {category_name}")
            
            # Get product URLs (simplified for demo)
            self._rate_limit()
            try:
                response = self.session.get(category_url, timeout=15)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                
                product_links = []
                for link in soup.select('a.title'):
                    href = link.get('href')
                    if href:
                        full_url = urljoin(category_url, href)
                        product_links.append(full_url)
                
                print(f"   Found {len(product_links)} products")
                
                # Crawl products
                for i, product_url in enumerate(product_links, 1):
                    print(f"   [{i}/{len(product_links)}] Crawling...", end='\r')
                    product = self.crawl_product_page(product_url, category_name, crawl_time)
                    
                    if product:
                        run_products.append(product)
                        self.products.append(product)
                
                print(f"   ✅ Completed: {len(product_links)} products")
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        # Save to file
        self._save_results()
        
        print(f"\n{'='*60}")
        print(f"📊 Run Summary")
        print(f"{'='*60}")
        print(f"Products crawled: {len(run_products)}")
        print(f"Kafka sent: {self.stats['kafka_sent']}")
        print(f"Total products: {len(self.products)}")
        print(f"{'='*60}\n")
        
        return len(run_products)
    
    def crawl_continuous(self):
        """Continuously crawl at intervals"""
        print(f"\n{'='*60}")
        print(f"🔄 Starting Continuous Crawling Mode")
        print(f"{'='*60}")
        print(f"Interval: {CRAWL_INTERVAL_SECONDS} seconds")
        print(f"Press Ctrl+C to stop")
        print(f"{'='*60}\n")
        
        run_count = 0
        
        try:
            while True:
                run_count += 1
                print(f"\n🔄 Run #{run_count}")
                
                self.crawl_single_run()
                
                if CONTINUOUS_MODE:
                    print(f"\n⏳ Waiting {CRAWL_INTERVAL_SECONDS} seconds until next run...")
                    time.sleep(CRAWL_INTERVAL_SECONDS)
                else:
                    break
                    
        except KeyboardInterrupt:
            print("\n\n⏹️  Stopping continuous crawl...")
        finally:
            self._cleanup()
    
    def _save_results(self):
        """Save crawled data to JSON file"""
        with open(RAW_OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.products, f, ensure_ascii=False, indent=2)
    
    def _cleanup(self):
        """Cleanup resources"""
        if self.kafka_producer:
            self.kafka_producer.close()
        
        self._print_final_stats()
    
    def _print_final_stats(self):
        """Print final statistics"""
        print(f"\n{'='*60}")
        print(f"📊 Final Statistics")
        print(f"{'='*60}")
        print(f"Total Attempts:    {self.stats['total_attempts']}")
        print(f"✅ Successful:     {self.stats['successful']}")
        print(f"❌ Failed:         {self.stats['failed']}")
        print(f"⚠️  Missing Fields: {self.stats['missing_fields']}")
        print(f"📡 Kafka Sent:     {self.stats['kafka_sent']}")
        print(f"📡 Kafka Failed:   {self.stats['kafka_failed']}")
        
        if self.stats['total_attempts'] > 0:
            success_rate = (self.stats['successful'] / self.stats['total_attempts']) * 100
            print(f"📈 Success Rate:   {success_rate:.1f}%")
        
        if self.products:
            avg_quality = sum(p.get('data_quality_score', 0) for p in self.products) / len(self.products)
            print(f"⭐ Avg Quality:    {avg_quality:.2f}")
        
        print(f"💾 Output File:    {RAW_OUTPUT_FILE}")
        print(f"{'='*60}\n")


def main():
    """Main entry point"""
    crawler = StreamingEcommerceCrawler(
        source_name="webscraper.io",
        enable_kafka=True
    )
    
    if CONTINUOUS_MODE:
        crawler.crawl_continuous()
    else:
        crawler.crawl_single_run()
        crawler._cleanup()


if __name__ == "__main__":
    main()
