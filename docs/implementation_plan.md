# Real-time E-commerce Price & Trend Analytics Pipeline - Implementation Plan

## Overview

The current project implements a **simple batch ETL pipeline** for crawling static book data from books.toscrape.com. However, the project requirements demand a **comprehensive real-time e-commerce price analytics system** with streaming ingestion, distributed processing, fault tolerance, and scalability testing on Kubernetes.

### Current State
- ✅ Basic crawler (Python) for static website
- ✅ HDFS storage
- ✅ Spark batch processing (data cleaning)
- ✅ Elasticsearch indexing
- ✅ Kibana visualization (basic)

### Missing Critical Components
- ❌ **Streaming layer** (Kafka for real-time ingestion)
- ❌ **Real-time processing** (Spark Structured Streaming)
- ❌ **Time-series data** (no crawl_time tracking, no historical price tracking)
- ❌ **Anomaly detection** (price spike/drop alerts)
- ❌ **Distributed deployment** (Kubernetes with multi-node setup)
- ❌ **Scalability tests** (data volume & compute scaling)
- ❌ **Fault tolerance tests** (service failure recovery)
- ❌ **E-commerce data source** (currently using books, need real product prices)
- ❌ **Lambda architecture** (batch + streaming combined)
- ❌ **Comprehensive dashboard** (only 6+ charts required)

---

## Gap Analysis

### 1. Data Sources (Current: ❌ NOT COMPLIANT)
**Required:**
- Vietnamese e-commerce websites with price data
- Schema: crawl_time, product_id, price, discount_price, availability, rating, etc.
- Document challenges: missing fields, schema drift, duplicates, rate limits

**Current:**
- Static book catalog (no price changes over time)
- Missing: crawl_time, product_id, discount_price, location, raw_html_snapshot

**Gap:** Need to identify real e-commerce sources and redesign schema

---

### 2. Architecture Requirements

#### 2.1 Data Collection Layer (Current: ❌ NOT COMPLIANT)
**Required:**
- Near real-time crawling (1-5 min intervals)
- Kafka streaming with topics: `raw_products`, `clean_products`, `alerts`

**Current:**
- One-time batch crawl
- No streaming infrastructure

**Gap:** Need Kafka setup + streaming crawler

---

#### 2.2 Storage Layer (Current: ⚠️ PARTIAL)
**Required:**
- HDFS/S3 with partitioning: `dt=YYYY-MM-DD/hr=HH`
- Parquet/JSONL format

**Current:**
- ✅ HDFS configured
- ❌ No time-based partitioning
- ⚠️ Mixed JSON/Parquet usage

**Gap:** Implement proper partitioning strategy

---

#### 2.3 Processing Layer (Current: ❌ NOT COMPLIANT)
**Required:**
- **Batch:** Daily aggregations (avg price, top volatile products, distributions)
- **Real-time:** Spark Structured Streaming for latest prices + anomaly detection

**Current:**
- ✅ Batch cleaning only
- ❌ No real-time stream processing
- ❌ No aggregations or analytics

**Gap:** Add Spark Structured Streaming + batch analytics jobs

---

#### 2.4 Serving Layer (Current: ⚠️ PARTIAL)
**Required:**
- Elasticsearch indices: `products_latest`, `products_history`, `alerts`
- Full-text search, filters, aggregations

**Current:**
- ✅ Single `books` index
- ❌ No separation of latest vs history
- ❌ No alerts index

**Gap:** Redesign ES index strategy

---

#### 2.5 Query Layer (Current: ❌ NOT COMPLIANT)
**Required:**
- Statistical queries (trend, avg, percent change)
- Search queries (full-text + filters)

**Current:**
- Basic Kibana queries only

**Gap:** Document and implement query examples

---

#### 2.6 Visualization Layer (Current: ❌ NOT COMPLIANT)
**Required:** 6+ charts:
1. Total products by category
2. Price trend by day (line chart)
3. Top 10 price changes in 24h
4. Price distribution heatmap
5. Alert count by day/hour
6. Product table with search/filter

**Current:**
- Unspecified basic visualizations

**Gap:** Create comprehensive dashboard

---

### 3. Environment Constraints (Current: ❌ NOT COMPLIANT)
**Required:**
- Kubernetes (multi-node) or Cloud (EKS/GKE/AKS)
- Minimum 2 worker nodes
- Distributed services: Kafka, Spark, HDFS, Elasticsearch, Kibana

**Current:**
- Likely single-machine Docker or local setup

**Gap:** Deploy on Kubernetes with documented topology

---

### 4. Scalability Testing (Current: ❌ NOT COMPLIANT)
**Required:**
- Test A: Scale data 10k → 100k, measure throughput/latency
- Test B: Scale workers 1 → 2 → 3, compare performance
- Document with tables and charts

**Current:**
- No testing infrastructure

**Gap:** Implement test scenarios and measurement

---

### 5. Fault Tolerance Testing (Current: ❌ NOT COMPLIANT)
**Required:**
- FT1: Kill Spark worker, verify recovery
- FT2: Restart Elasticsearch/Kafka, verify pipeline recovery

**Current:**
- No fault tolerance mechanisms

**Gap:** Implement and test failure scenarios

---

## Proposed Changes

This is a **major architectural overhaul**. The implementation will be organized into phases:

---

### Phase 1: Foundation & Data Sources

#### [NEW] [data_sources.md](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/docs/data_sources.md)
Document Vietnamese e-commerce websites, URLs, schema design, and data challenges.

#### [MODIFY] [crawl/ecommerce_crawler.py](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/crawl/ecommerce_crawler.py)
New crawler with:
- Time-series support (crawl_time timestamp)
- Product ID tracking
- Price, discount_price, availability
- Rating, reviews, location
- Raw HTML/JSON snapshot storage
- Error handling for missing fields

---

### Phase 2: Streaming Infrastructure

#### [NEW] [kafka/docker-compose.yml](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/kafka/docker-compose.yml)
Kafka cluster setup (3 brokers for distributed testing).

#### [NEW] [kafka/create_topics.sh](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/kafka/create_topics.sh)
Script to create topics: `raw_products`, `clean_products`, `alerts`.

#### [NEW] [crawl/kafka_producer.py](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/crawl/kafka_producer.py)
Kafka producer integrated with crawler for real-time data ingestion.

---

### Phase 3: Real-time Processing

#### [NEW] [spark/streaming_processor.py](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/spark/streaming_processor.py)
Spark Structured Streaming consumer:
- Read from Kafka `raw_products`
- Clean and transform data
- Detect price anomalies (z-score/percent change)
- Write to `clean_products` and `alerts` topics
- Write to HDFS with partitioning: `dt=YYYY-MM-DD/hr=HH`

---

### Phase 4: Batch Analytics

#### [NEW] [spark/batch_aggregations.py](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/spark/batch_aggregations.py)
Daily batch jobs:
- Average price by day/category
- Top volatile products (price change %)
- Price distribution histograms
- Write aggregated results to HDFS and Elasticsearch

---

### Phase 5: Elasticsearch Indexing

#### [MODIFY] [spark/spark_to_es.py](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/spark/spark_to_es.py)
Update to create multiple indices:
- `products_latest`: Current product state (upsert by product_id)
- `products_history`: All historical price records
- `alerts`: Anomaly alerts with timestamps

#### [NEW] [elasticsearch/index_mappings.json](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/elasticsearch/index_mappings.json)
Define mappings for full-text search and aggregations.

---

### Phase 6: Kubernetes Deployment

#### [NEW] [k8s/namespace.yaml](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/k8s/namespace.yaml)
Create dedicated namespace for the project.

#### [NEW] [k8s/kafka/](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/k8s/kafka/)
Kafka StatefulSet with 3 replicas (distributed brokers).

#### [NEW] [k8s/spark/](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/k8s/spark/)
Spark master + 2+ worker deployments.

#### [NEW] [k8s/elasticsearch/](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/k8s/elasticsearch/)
Elasticsearch StatefulSet with 2+ nodes.

#### [NEW] [k8s/hdfs/](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/k8s/hdfs/)
HDFS NameNode + DataNode deployments.

#### [NEW] [k8s/crawler/](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/k8s/crawler/)
CronJob for periodic crawling.

#### [NEW] [k8s/kibana/](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/k8s/kibana/)
Kibana deployment.

#### [NEW] [docs/topology.md](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/docs/topology.md)
Document which services run on which nodes.

---

### Phase 7: Dashboard & Queries

#### [NEW] [kibana/dashboards/](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/kibana/dashboards/)
Export JSON for 6+ required visualizations:
1. Products by category (bar/pie)
2. Price trend line chart
3. Top 10 price changes (bar)
4. Price distribution heatmap
5. Alert timeline
6. Product search table

#### [NEW] [docs/query_examples.md](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/docs/query_examples.md)
Document statistical and search queries with examples.

---

### Phase 8: Testing Infrastructure

#### [NEW] [tests/scalability/](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/tests/scalability/)
Scripts to:
- Generate test data (10k, 100k records)
- Measure batch job execution time
- Measure stream throughput and latency
- Scale workers and compare performance

#### [NEW] [tests/fault_tolerance/](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/tests/fault_tolerance/)
Scripts to:
- Kill Spark worker pods
- Restart Kafka/Elasticsearch
- Verify recovery and data integrity
- Capture logs and screenshots

---

### Phase 9: Documentation

#### [MODIFY] [README.md](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/README.md)
Update with complete architecture, deployment guide, and usage instructions.

#### [NEW] [docs/architecture.md](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/docs/architecture.md)
End-to-end pipeline diagram with component descriptions.

#### [NEW] [docs/report.md](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/docs/report.md)
Comprehensive project report covering:
- Data sources and challenges
- Schema design
- Pipeline flow (batch vs real-time)
- Query examples
- Scalability test results (tables + charts)
- Fault tolerance test results (logs + screenshots)
- Insights and conclusions

---

## User Review Required

> [!WARNING]
> **This is a complete architectural redesign, not an incremental update.**

The current project is a simple batch ETL demo. The requirements specify a **production-grade distributed system** with:
- Real-time streaming (Kafka + Spark Structured Streaming)
- Kubernetes deployment (multi-node)
- Comprehensive testing (scalability + fault tolerance)

**Key Decisions Needed:**

1. **Data Sources:** Which Vietnamese e-commerce websites should we target? (Need public, crawl-friendly sites)
   - Suggestions: Tiki.vn, Shopee.vn (if allowed), or demo e-commerce sites
   
2. **Kubernetes Environment:** 
   - Use local Minikube/Kind with multi-node setup?
   - Or deploy to cloud (GKE/EKS/AKS)?
   
3. **Scope Prioritization:** This is a large project. Should we implement in phases and deliver incrementally?
   - Phase 1: Streaming + Real-time processing
   - Phase 2: K8s deployment
   - Phase 3: Testing infrastructure

4. **Timeline:** What is the deadline for this project?

---

## Verification Plan

### Automated Tests

#### 1. Unit Tests for Data Processing
```bash
# Test data cleaning logic
pytest tests/unit/test_data_cleaning.py

# Test anomaly detection algorithms
pytest tests/unit/test_anomaly_detection.py
```

#### 2. Integration Tests for Streaming Pipeline
```bash
# Test Kafka producer/consumer
python tests/integration/test_kafka_pipeline.py

# Test Spark Structured Streaming
spark-submit tests/integration/test_streaming_processor.py
```

#### 3. Scalability Tests
```bash
# Test A: Data volume scaling
cd tests/scalability
python test_data_volume_scaling.py --records 10000,100000

# Test B: Compute scaling
python test_compute_scaling.py --workers 1,2,3
```

#### 4. Fault Tolerance Tests
```bash
# FT1: Spark worker failure
cd tests/fault_tolerance
./test_spark_worker_failure.sh

# FT2: Service restart
./test_service_restart.sh
```

---

### Manual Verification

#### 1. Kubernetes Deployment
- [ ] Verify all pods are running: `kubectl get pods -n bigdata`
- [ ] Check service topology matches documentation
- [ ] Verify persistent volumes are mounted

#### 2. Dashboard Verification
- [ ] Access Kibana at `http://<kibana-service>:5601`
- [ ] Verify all 6+ charts are present and displaying data
- [ ] Test search and filter functionality
- [ ] Verify real-time updates (data appears within 1-5 min)

#### 3. End-to-End Pipeline Test
- [ ] Trigger crawler manually
- [ ] Verify data appears in Kafka topics: `kafka-console-consumer --topic raw_products`
- [ ] Check HDFS for partitioned data: `hdfs dfs -ls /data/raw/dt=*/hr=*`
- [ ] Verify Elasticsearch indices have new documents
- [ ] Confirm dashboard updates with new data

#### 4. Query Examples
- [ ] Run statistical query: "Average price trend for category X over last 7 days"
- [ ] Run search query: "Find products with name containing 'laptop' priced between 10M-20M VND"
- [ ] Verify aggregation performance (< 1 second for typical queries)

---

### Performance Benchmarks

Document the following metrics in the final report:

| Metric | Target | Actual |
|--------|--------|--------|
| Stream throughput | > 1000 records/sec | TBD |
| End-to-end latency | < 5 minutes | TBD |
| Batch job time (100k records) | < 10 minutes | TBD |
| Query response time | < 1 second | TBD |
| Worker scaling efficiency | Linear improvement | TBD |

---

## Implementation Timeline (Suggested)

| Phase | Duration | Deliverables |
|-------|----------|--------------|
| 1. Data Sources & Schema | 2 days | data_sources.md, new crawler |
| 2. Streaming Infrastructure | 3 days | Kafka setup, producer integration |
| 3. Real-time Processing | 3 days | Spark Structured Streaming |
| 4. Batch Analytics | 2 days | Aggregation jobs |
| 5. Elasticsearch Indexing | 2 days | Multi-index setup |
| 6. Kubernetes Deployment | 4 days | K8s manifests, deployment |
| 7. Dashboard & Queries | 2 days | Kibana dashboards, query docs |
| 8. Testing Infrastructure | 3 days | Scalability + fault tolerance tests |
| 9. Documentation | 2 days | Report, architecture diagram |
| **Total** | **23 days** | Complete system |

---

## Next Steps

1. **User Review:** Please review this plan and provide feedback on:
   - Data source selection
   - Kubernetes vs Cloud deployment preference
   - Timeline and scope prioritization

2. **Approval:** Once approved, we will proceed to EXECUTION mode, starting with Phase 1 (Data Sources & Schema Design).

3. **Incremental Delivery:** We can deliver and test each phase incrementally to ensure quality and allow for adjustments.
