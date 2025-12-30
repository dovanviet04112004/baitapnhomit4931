"""
Kafka Producer for E-commerce Crawler
Sends crawled product data to Kafka topics in real-time
"""

import json
import logging
from typing import Dict, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EcommerceKafkaProducer:
    """Kafka producer for streaming product data"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:19092'):
        """
        Initialize Kafka producer
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
        """
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.stats = {
            'sent': 0,
            'failed': 0,
            'retries': 0
        }
        
        self._connect()
    
    def _connect(self):
        """Establish connection to Kafka cluster"""
        try:
            logger.info(f"Connecting to Kafka brokers: {self.bootstrap_servers}")
            
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                max_in_flight_requests_per_connection=1,  # Ensure ordering
                compression_type='snappy',
                linger_ms=10,  # Batch messages for efficiency
                batch_size=16384,
                buffer_memory=33554432,
                request_timeout_ms=30000,
                api_version=(2, 8, 0)
            )
            
            logger.info("✅ Successfully connected to Kafka")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            raise
    
    def send_product(self, product: Dict, topic: str = 'raw_products') -> bool:
        """
        Send product data to Kafka topic
        
        Args:
            product: Product data dictionary
            topic: Target Kafka topic
            
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.producer:
            logger.error("Producer not initialized")
            return False
        
        try:
            # Use product_id as key for partitioning
            key = product.get('product_id', 'unknown')
            
            # Send to Kafka
            future = self.producer.send(
                topic=topic,
                key=key,
                value=product
            )
            
            # Wait for acknowledgment (with timeout)
            record_metadata = future.get(timeout=10)
            
            self.stats['sent'] += 1
            
            logger.debug(
                f"✅ Sent to {topic} | "
                f"Partition: {record_metadata.partition} | "
                f"Offset: {record_metadata.offset} | "
                f"Product: {product.get('product_name', 'N/A')}"
            )
            
            return True
            
        except KafkaError as e:
            self.stats['failed'] += 1
            logger.error(f"❌ Kafka error sending product: {e}")
            return False
            
        except Exception as e:
            self.stats['failed'] += 1
            logger.error(f"❌ Unexpected error sending product: {e}")
            return False
    
    def send_batch(self, products: list, topic: str = 'raw_products') -> Dict[str, int]:
        """
        Send batch of products to Kafka
        
        Args:
            products: List of product dictionaries
            topic: Target Kafka topic
            
        Returns:
            Dictionary with success/failure counts
        """
        results = {'success': 0, 'failed': 0}
        
        logger.info(f"Sending batch of {len(products)} products to {topic}")
        
        for product in products:
            if self.send_product(product, topic):
                results['success'] += 1
            else:
                results['failed'] += 1
        
        # Flush to ensure all messages are sent
        self.producer.flush()
        
        logger.info(
            f"Batch complete: {results['success']} sent, "
            f"{results['failed']} failed"
        )
        
        return results
    
    def send_alert(self, alert: Dict) -> bool:
        """
        Send price alert to alerts topic
        
        Args:
            alert: Alert data dictionary
            
        Returns:
            True if sent successfully
        """
        return self.send_product(alert, topic='alerts')
    
    def get_stats(self) -> Dict[str, int]:
        """Get producer statistics"""
        return self.stats.copy()
    
    def close(self):
        """Close producer connection"""
        if self.producer:
            logger.info("Closing Kafka producer...")
            self.producer.flush()
            self.producer.close()
            logger.info("✅ Kafka producer closed")
            
            # Print final stats
            logger.info(
                f"Final stats - Sent: {self.stats['sent']}, "
                f"Failed: {self.stats['failed']}"
            )


def test_producer():
    """Test function to verify Kafka producer"""
    print("\n" + "="*60)
    print("Testing Kafka Producer")
    print("="*60)
    
    # Create producer
    producer = EcommerceKafkaProducer(bootstrap_servers='localhost:19092')
    
    # Test product
    test_product = {
        "crawl_time": "2025-12-30T17:10:00+07:00",
        "source": "test",
        "product_id": "test-001",
        "product_name": "Test Product",
        "category": "Test Category",
        "price": 99.99,
        "currency": "USD",
        "in_stock": True
    }
    
    # Send test message
    print("\nSending test product...")
    success = producer.send_product(test_product, topic='raw_products')
    
    if success:
        print("✅ Test message sent successfully!")
    else:
        print("❌ Failed to send test message")
    
    # Test alert
    test_alert = {
        "alert_time": "2025-12-30T17:10:00+07:00",
        "alert_type": "price_spike",
        "severity": "high",
        "product_id": "test-001",
        "product_name": "Test Product",
        "price_change_percent": 25.0,
        "message": "Test alert"
    }
    
    print("\nSending test alert...")
    success = producer.send_alert(test_alert)
    
    if success:
        print("✅ Test alert sent successfully!")
    else:
        print("❌ Failed to send test alert")
    
    # Print stats
    stats = producer.get_stats()
    print(f"\nProducer Stats: {stats}")
    
    # Close
    producer.close()
    
    print("\n" + "="*60)
    print("Test Complete")
    print("="*60 + "\n")


if __name__ == "__main__":
    test_producer()
