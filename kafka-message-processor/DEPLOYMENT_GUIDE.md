# Deployment Guide

## OCP Deployment Steps

### 1. Build Application
```bash
./gradlew clean build
```

### 2. Create Docker Image
```bash
docker build -t kafka-message-processor:1.0.0 .
```

### 3. Tag and Push to Registry
```bash
docker tag kafka-message-processor:1.0.0 registry.ocp.example.com/messaging/kafka-processor:1.0.0
docker push registry.ocp.example.com/messaging/kafka-processor:1.0.0
```

### 4. Deploy via Harness
- Login to Harness
- Select EPLX pipeline
- Configure environment variables
- Trigger deployment

### 5. Verify Deployment
```bash
curl http://kafka-processor-svc:8080/actuator/health
```

## Configuration Checklist

- [ ] Database connection strings
- [ ] Kafka bootstrap servers
- [ ] Redis endpoint
- [ ] Sequence cache configuration
- [ ] Thread pool sizes
- [ ] Log levels
- [ ] Splunk configuration
