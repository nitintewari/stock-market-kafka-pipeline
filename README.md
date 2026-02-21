# ⚡ Real-Time Stock Market Data Engineering Pipeline

> End-to-end real-time data engineering pipeline streaming live stock market data using Apache Kafka on AWS — enhanced with anomaly detection for intelligent price spike alerting.

---

## 🏗️ Architecture

```
Stock Market CSV / Live Feed
         │
         ▼
┌─────────────────────┐
│   producer.py       │  Kafka Producer — streams 10K+ events/hour
│   (AWS EC2)         │
└────────┬────────────┘
         │ Kafka Topic: stock-market-data
         ▼
┌─────────────────────┐
│   consumer.py       │  Kafka Consumer + Anomaly Detection
│   (AWS EC2)         │
└────────┬────────────┘
         │
    ┌────┴──────────────────┐
    ▼                       ▼
┌──────────┐      ┌──────────────────────┐
│  AWS S3  │      │  anomaly_detector.py │  
│  Storage │      │  Z-Score + ROC       │
└────┬─────┘      │  Real-time alerting  │
     │            └──────────────────────┘
     ▼
┌──────────────┐
│  AWS Glue    │  Schema discovery & Data Catalog
│  Crawler     │
└──────┬───────┘
       ▼
┌──────────────┐
│  AWS Athena  │  SQL queries on streaming data
└──────────────┘
```

---

## ✨ Features

### Base 
- Real-time data streaming with Apache Kafka
- AWS S3 storage for raw stock data
- AWS Glue Crawler for schema discovery
- AWS Athena for SQL querying

### 🆕 
| Feature | Description |
|---|---|
| | Z-Score + Rate-of-Change detection on live price stream |
| | Flags sudden price spikes/crashes with severity scoring |
| | Converted notebooks → modular, CLI-ready `.py` files |
| | Maintains live mean/std of recent prices for smarter detection |
| Each stored record includes `anomaly_detected`, `z_score`, `reason` |

---

## 🚀 Setup & Usage

### 1. Clone the repo
```bash
git clone https://github.com/nitintewari/stock-market-kafka-pipeline.git
cd stock-market-kafka-pipeline
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Start Kafka on AWS EC2
```bash
# SSH into EC2 instance
ssh -i "your-key.pem" ec2-user@your-ec2-public-ip

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka broker
bin/kafka-server-start.sh config/server.properties

# Create topic
bin/kafka-topics.sh --create --topic stock-market-data \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 4. Run the Producer
```bash
python producer.py --broker localhost:9092 --topic stock-market-data --csv indexProcessed.csv --delay 0.5
```

### 5. Run the Consumer
```bash
python consumer.py --broker localhost:9092 --topic stock-market-data --bucket your-s3-bucket
```

### 6. Test Anomaly Detection standalone
```bash
python anomaly_detector.py
```

---

## 🔍 Anomaly Detection — How It Works

Each price tick is evaluated against two signals:

**Signal 1 — Z-Score**
```
Z = |price - rolling_mean| / rolling_std
```
If Z > 2.5 → price is statistically unusual relative to recent history

**Signal 2 — Rate of Change (ROC)**
```
ROC = |(current_price - last_price) / last_price| × 100
```
If ROC > 5% → sudden jump between consecutive ticks

**Sample output:**
```
Tick 01 | Price: $151.23 | Z: 0.21 | ✅ normal
Tick 02 | Price: $149.87 | Z: 0.18 | ✅ normal
...
Tick 16 | Price: $185.00 | Z: 3.84 | 🚨 ANOMALY | Z-score 3.84 exceeds threshold AND price jumped 22.3%
Tick 21 | Price: $110.00 | Z: 4.12 | 🚨 ANOMALY | Sudden price crash of 40.5% from last tick
```

**Enriched S3 record example:**
```json
{
  "Index": "SPY",
  "Close": 185.00,
  "timestamp": "2025-05-14T10:23:41.123456",
  "anomaly_detected": true,
  "z_score": 3.84,
  "anomaly_reason": "Z-score 3.84 exceeds threshold 2.5 AND price jumped 22.3%"
}
```

---

## 📁 File Structure

```
stock-market-kafka-pipeline/
├── producer.py            # Kafka Producer — streams CSV data to topic
├── consumer.py            # Kafka Consumer — reads topic, stores to S3
├── anomaly_detector.py    # 🆕 Real-time anomaly detection module
├── requirements.txt       # Python dependencies
├── indexProcessed.csv     # Sample stock market dataset
└── README.md
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Streaming | Apache Kafka |
| Compute | AWS EC2 |
| Storage | AWS S3 |
| Schema | AWS Glue Crawler + Data Catalog |
| Query | AWS Athena (SQL) |
| Anomaly Detection | Python (Z-Score + ROC) |
| Language | Python 3.10+ |

---

## 📊 Dataset

Stock market index data (`indexProcessed.csv`) containing OHLCV fields.


---

## 👤 Author

**Nitin Tewari**
MS Data Science, Indiana University Bloomington
[LinkedIn](https://linkedin.com/in/nitintewari39) · [GitHub](https://github.com/nitintewari)

---
