# Monitoring Setup Documentation

This document explains how to set up and use the monitoring dashboard for the Telecom Data Pipeline.

## Prerequisites

- Docker and Docker Compose installed
- Kafka running on localhost:9092
- The telecom data pipeline running

## Quick Start

1. **Navigate to monitoring directory:**

   ```bash
   cd Monitoring
   ```

2. **Start the monitoring stack:**

   ```bash
   docker-compose up -d
   ```

3. **Access Kafdrop Web Interface:**
   - URL: http://localhost:9000
   - No authentication required

## What You Can Monitor

### Kafka Topics

- **telecom_usage_records**: Raw incoming data from producers
- **normalized_records**: Validated records after mediation
- **rated_records**: Priced records ready for billing

### Key Metrics

- **Message Count**: Total messages in each topic
- **Partition Distribution**: How messages are spread across partitions
- **Consumer Lag**: How far behind consumers are
- **Message Content**: Actual message payloads and headers

## Screenshots

The main monitoring interface shows:

![Kafdrop Monitoring Interface](../Imgs/monitoring.png)

> **Note**: Make sure to add your actual monitoring screenshot to the `Imgs/monitoring.png` path

## Stopping Monitoring

```bash
cd Monitoring
docker-compose down
```

## Troubleshooting

### Common Issues

1. **Can't connect to Kafka:**

   - Verify Kafka is running: `ps aux | grep kafka`
   - Check Kafka port: `netstat -ln | grep 9092`

2. **No topics visible:**

   - Ensure your producers are running and sending data
   - Check topic creation: `kafka-topics.sh --list --bootstrap-server localhost:9092`

3. **Container won't start:**
   - Check Docker daemon: `sudo systemctl status docker`
   - Check port conflicts: `netstat -ln | grep 9000`

## Advanced Configuration

To customize the monitoring setup, edit the `docker-compose.yml` file in the Monitoring directory.

### Port Changes

```yaml
ports:
  - '9001:9000' # Change external port to 9001
```

### Resource Limits

```yaml
deploy:
  resources:
    limits:
      memory: 512M
    reservations:
      memory: 256M
```
