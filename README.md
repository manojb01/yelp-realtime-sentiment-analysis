# Real-Time Streaming Analytics with AI Sentiment Analysis

A production-ready real-time data streaming pipeline that processes Yelp business reviews with AI-powered sentiment analysis. The system ingests streaming data via TCP sockets, processes it with Apache Spark, enriches reviews with sentiment classification using local LLMs, streams to Kafka, and indexes in Elasticsearch for full-text search and analytics.

## Overview

This project demonstrates an end-to-end real-time data engineering pipeline using:
- **TCP/IP Socket** for streaming data ingestion
- **Apache Spark Structured Streaming** for real-time processing
- **Ollama + Llama 3.2** for AI-powered sentiment analysis
- **Confluent Kafka Cloud** for message streaming
- **Elasticsearch** for indexing and full-text search

## Architecture

```
Yelp Dataset (JSON - 7M+ records)
    ↓
TCP Socket Stream (Port 9999)
    ↓
Apache Spark Structured Streaming
    ↓ (sentiment analysis via Ollama/Llama 3.2)
    ↓
Confluent Kafka Cloud
├── Environment: ReviewsEnvironment
├── Cluster: review_cluster (AWS us-east-1)
├── Topic: customers_review (6 partitions)
└── Schema Registry: Avro schema (data contract)
    ↓
Elasticsearch Sink Connector (auto-sync)
    ↓
Elastic Cloud (v8.x, AWS us-east-1)
└── Index: customers_review (auto-created with dynamic mappings)
```

## Technology Stack

- **Stream Processing**: Apache Spark 4.0.1 (Structured Streaming)
- **Message Broker**: Confluent Kafka Cloud (SASL_SSL)
- **Search Engine**: Elasticsearch 8.x (Elastic Cloud)
- **AI/ML**: Ollama + Llama 3.2 (local LLM for sentiment analysis)
- **Data Format**: JSON, Avro schemas
- **Containerization**: Docker, Docker Compose
- **Language**: Python 3.x

## Key Features

1. **Real-Time Processing**: Event-driven architecture with no batch windows
2. **AI-Powered Enrichment**: Sentiment classification (POSITIVE/NEGATIVE/NEUTRAL) using Llama 3.2 via Ollama
3. **Cloud-Native Infrastructure**: Confluent Kafka Cloud + Elastic Cloud deployment
4. **Schema Registry Integration**: Avro schemas for message validation and data contracts
5. **Auto-Sync Pipeline**: Elasticsearch Sink Connector automatically syncs Kafka → Elasticsearch
6. **Fault Tolerance**: Spark checkpointing, Kafka persistence, socket reconnection with resume capability
7. **Full-Text Search**: Elasticsearch indexing enables complex queries and aggregations
8. **Containerized Development**: Reproducible Spark environment via Docker Compose

## Project Structure

```
.
├── src/
│   ├── config/
│   │   ├── config.py              # Kafka/Elasticsearch credentials (gitignored)
│   │   └── config.example.py      # Configuration template
│   ├── jobs/
│   │   ├── spark-streaming.py     # Main Spark streaming job
│   │   └── streaming-socket.py    # Socket server for data ingestion
│   ├── schemas/
│   │   └── reviews.schema.avsc    # Avro schema definition
│   ├── datasets/
│   │   └── yelp_academic_dataset_review.json  # Source data
│   ├── docker-compose.yml         # Spark cluster orchestration
│   ├── Dockerfile.spark           # Custom Spark image
│   └── requirements.txt           # Python dependencies
├── tests/
│   └── test_llm.py                # LLM testing script
├── .gitignore
└── README.md
```

## Prerequisites

- Docker and Docker Compose installed
- Ollama installed (with Llama 3.2 model)
- Confluent Cloud account with Kafka cluster
- Elastic Cloud account with Elasticsearch 8.x cluster
- Yelp Open Dataset (~5GB, instructions below)

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/realtime-sentiment-streaming.git
cd realtime-sentiment-streaming
```

### 2. Download Yelp Dataset

Download the Yelp Open Dataset (reviews JSON file, ~5GB):

1. Visit: https://business.yelp.com/data/resources/open-dataset/
2. Download the dataset (requires agreeing to terms)
3. Extract `yelp_academic_dataset_review.json`
4. Place it in `src/datasets/yelp_academic_dataset_review.json`

**Note:** The dataset is gitignored due to its large size (~5GB).

### 3. Configure Credentials

Copy the example configuration and add your credentials:

```bash
cp src/config/config.example.py src/config/config.py
```

Edit `src/config/config.py` with your:
- Confluent Kafka Cloud credentials
- Schema Registry credentials
- Elasticsearch endpoint and credentials

### 4. Set Up Ollama

Install and start Ollama with Llama 3.2:

```bash
# Install Ollama (if not already installed)
# Visit: https://ollama.ai

# Pull Llama 3.2 model
ollama pull llama3.2

# Verify Ollama is running
curl http://localhost:11434/v1/models
```

### 5. Start Spark Cluster

```bash
cd src
docker-compose up -d
```

Verify the cluster is running:
- **Spark Master UI**: http://localhost:9090
- Check containers: `docker ps`

### 6. Submit Spark Streaming Job

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  jobs/spark-streaming.py
```

### 7. Start Socket Stream (in separate terminal)

```bash
docker exec -it spark-master python3 jobs/streaming-socket.py
```

### 8. Verify Data Flow

- **Kafka**: Check Confluent Cloud console → `customers_review` topic
- **Elasticsearch**: Query the `customers_review` index in Elastic Cloud
- **Connector**: Verify Elasticsearch Sink Connector shows "Running" status

## Data Flow

1. **Ingestion**: Socket server reads Yelp JSON file line-by-line
2. **Streaming**: Records sent over TCP socket (port 9999) with newline delimiters
3. **Reception**: Spark receives raw strings from socket
4. **Parsing**: `from_json()` converts strings to structured DataFrames
5. **Enrichment**: UDF calls Ollama API for sentiment analysis on review text
6. **Transformation**: Adds `feedback` column with sentiment label (POSITIVE/NEGATIVE/NEUTRAL)
7. **Serialization**: Converts DataFrame to JSON format
8. **Publishing**: Writes to Confluent Kafka Cloud topic with review_id as key
9. **Schema Validation**: Avro schema in Schema Registry validates message structure
10. **Auto-Sync**: Elasticsearch Sink Connector pulls from Kafka and indexes to Elastic Cloud
11. **Searchable**: Full-text queries and aggregations available in Elasticsearch

## Schema Evolution

| Stage | Fields |
|-------|--------|
| **Source** | review_id, user_id, business_id, stars, date, text |
| **Spark Output** | review_id, user_id, business_id, stars, date, text, **feedback** |
| **Elasticsearch** | review_id, user_id, business_id, stars, date, text, feedback |

## Components

### Socket Streaming Server (`streaming-socket.py`)
- Listens on `0.0.0.0:9999`
- Streams JSON records in configurable chunks (default: 2 records)
- 5-second delay between records for realistic simulation
- Tracks `last_sent_index` for resumption on reconnection
- Auto-reconnection with 10-second retry delays

### Spark Streaming Consumer (`spark-streaming.py`)
- Connects to socket stream on `localhost:9999`
- Parses JSON using predefined schema
- Applies sentiment analysis via UDF calling Ollama API
- Outputs enriched records with new `feedback` field
- Writes to Kafka with SASL_SSL authentication
- Checkpointing at `/tmp/checkpoint` for fault tolerance

### Sentiment Analysis Engine
- **Model**: Llama 3.2 via Ollama
- **Endpoint**: `http://host.docker.internal:11434/v1` (from containers)
- **Classification**: POSITIVE | NEGATIVE | NEUTRAL
- **Integration**: Spark User Defined Function (UDF)
- **Latency**: Real-time per-record inference

### Kafka Infrastructure
- **Platform**: Confluent Cloud
- **Environment**: ReviewsEnvironment
- **Cluster**: review_cluster (AWS us-east-1)
- **Topic**: `customers_review` (6 partitions)
- **Authentication**: SASL/PLAIN with API Key
- **Protocol**: SASL_SSL
- **Schema Registry**: Avro schema registered as data contract

### Elasticsearch Sink
- **Type**: Confluent managed Elasticsearch Sink Connector
- **Version**: Elasticsearch 8.x (CRITICAL: Must use 8.x, not 9.x)
- **Status**: Running (auto-sync)
- **Source Topic**: `customers_review`
- **Destination**: Elastic Cloud (AWS us-east-1)
- **Index**: `customers_review` (auto-created with dynamic mappings)
- **Input Format**: JSON

## Elasticsearch Queries

### Match All Documents
```json
GET customers_review/_search
{
  "query": {
    "match_all": {}
  }
}
```

### Full-Text Search
```json
GET customers_review/_search
{
  "query": {
    "match_phrase": {
      "text": "amazing"
    }
  }
}
```

### Sentiment Aggregation
```json
GET customers_review/_search
{
  "size": 0,
  "aggs": {
    "group_by_feedback": {
      "terms": {
        "field": "feedback.keyword"
      }
    }
  }
}
```

## Monitoring & Observability

- **Spark Master UI**: http://localhost:9090 (job monitoring, DAG visualization)
- **Confluent Cloud**: Kafka topic metrics, throughput, lag monitoring
- **Elastic Cloud**: Index stats, query performance, document counts
- **Container Logs**: `docker logs spark-master` for debugging

## Troubleshooting

### Socket Connection Refused
- Ensure socket server is running: `docker exec -it spark-master python3 jobs/streaming-socket.py`
- Check port 9999 is not blocked by firewall

### Kafka Authentication Failed
- Verify credentials in `src/config/config.py`
- Check Confluent Cloud API key has topic write permissions

### Ollama Connection Error
- Confirm Ollama is running on host: `ollama list`
- Verify `host.docker.internal` resolves from container
- Check firewall allows container access to host port 11434

### Elasticsearch Connector Fails
- **CRITICAL**: Ensure Elasticsearch version is 8.x (not 9.x)
- Confluent Elasticsearch Sink Connector does NOT support Elasticsearch 9.x
- Verify connector credentials and endpoint
- Check Elasticsearch cluster is running and accessible

## Performance Characteristics

- **Throughput**: Configurable via chunk size and delay settings
- **Latency**: Near real-time (seconds from ingestion to searchable)
- **Scalability**: Horizontal scaling via additional Spark workers
- **Resource Usage**: Default 2 workers × (2 cores, 1GB RAM each)

## Security

- **Kafka**: SASL/PLAIN authentication + SSL encryption
- **Elasticsearch**: Cloud-managed access control
- **Credentials**: Externalized in config.py (gitignored)
- **Network**: Docker internal networking, exposed ports only for UI

## Limitations

- **Elasticsearch Version**: Must use 8.x (9.x not supported by Confluent connector)
- **Ollama Access**: Requires host machine to run Ollama service
- **Single Source**: Currently designed for Yelp dataset structure
- **Local Development**: Not production-hardened for enterprise scale

## Future Enhancements

- Add more NLP features (entity extraction, topic modeling)
- Implement data quality monitoring and alerting
- Scale to handle higher throughput volumes
- Add real-time dashboard with Kibana visualization
- Implement CI/CD pipeline for automated deployment
- Add unit and integration tests
- Integrate with data catalog for metadata management
- Implement dead letter queues for failed records

## License

This project is licensed under the MIT License.

## Acknowledgments

Built with Apache Spark, Confluent Kafka, Elasticsearch, and Ollama.
