# Kafka Message Processor - Enterprise Messaging Application

## Overview

A high-performance, scalable enterprise messaging application built with Java 21 and Spring Boot 3.2, designed to process **10,000+ messages per second** from Kafka topics with zero message loss and guaranteed delivery.

### Key Features

✅ **High Throughput Processing**: Handles 10,000+ messages/second  
✅ **Zero Message Loss**: Guaranteed delivery with ACK mechanisms  
✅ **Duplicate Detection**: Hash-based duplicate detection with configurable time windows  
✅ **Automatic Retry**: Configurable retry mechanism with exponential backoff  
✅ **Auto-Recovery**: Autoloader for processing in-progress messages on restart  
✅ **Priority Processing**: Source and event-type based message prioritization  
✅ **Comprehensive Logging**: Splunk-compatible structured logging with MDC correlation  
✅ **Database Optimization**: HikariCP connection pooling with sequence caching  
✅ **Redis Caching**: High-speed caching for enrichment and duplicate detection  
✅ **Batch Processing**: Optimized batch processing with 200ms threshold  
✅ **RESTful Reprocessing**: API endpoints for failed message reprocessing  
✅ **Production Ready**: Complete with monitoring, metrics, and health checks

## Architecture

### System Architecture
```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   Kafka     │─────▶│   Consumer   │─────▶│  Database   │
│   Topic     │      │   (Batch)    │      │  (Oracle)   │
└─────────────┘      └──────────────┘      └─────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │  Validator   │
                     └──────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │  Performer   │──▶ Redis Cache
                     └──────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │  Processor   │
                     └──────────────┘
                            │
                            ▼
                     ┌──────────────┐      ┌─────────────┐
                     │   Worker     │─────▶│   Kafka     │
                     │  (Publisher) │      │  Outgoing   │
                     └──────────────┘      └─────────────┘
```

### Processing Pipeline

1. **Kafka Consumer** - Receives messages in batches
2. **Persistence Layer** - Stores in `INCOMING_MESSAGE_TABLE` with status "IN_PROGRESS"
3. **ACK to Kafka** - Immediate acknowledgment to prevent rebalancing
4. **Validator** - Schema validation, duplicate detection via Redis
5. **Performer** - Customer subscription checks, data enrichment
6. **Processor** - Route determination, message transformation
7. **Worker** - Publishes to downstream channels, updates status to "PUBLISHED"
8. **ACK Publisher** - Sends final status to source-specific ACK topics

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Language | Java 21 |
| Framework | Spring Boot 3.2.2 |
| Messaging | Apache Kafka 3.6.1 |
| Database | Oracle Database 12c+ |
| Cache | Redis 6.x with Lettuce |
| Connection Pool | HikariCP 5.1.0 |
| Build Tool | Gradle 8.5 |
| Testing | JUnit 5, Mockito, Testcontainers |
| Deployment | OCP (OpenShift), Harness, EPLX |

## Project Structure

```
kafka-message-processor/
├── src/main/java/com/enterprise/messaging/
│   ├── MessageProcessorApplication.java   # Main application entry point
│   ├── config/                            # Configuration classes
│   │   ├── DatabaseConfig.java            # HikariCP configuration
│   │   ├── KafkaConfig.java               # Kafka consumer/producer setup
│   │   ├── RedisConfig.java               # Redis cache configuration
│   │   └── AsyncConfig.java               # Thread pool configuration
│   ├── model/                             # Entity classes
│   │   ├── IncomingMessage.java           # Incoming message table entity
│   │   ├── OutgoingMessage.java           # Outgoing message table entity
│   │   ├── DuplicateMessage.java          # Duplicate tracking entity
│   │   ├── ProcessLock.java               # Distributed lock entity
│   │   └── IncomingTask.java              # Processing pipeline DTO
│   ├── repository/                        # JPA repositories
│   ├── service/                           # Business logic
│   │   ├── MessagePersistenceService.java # Database operations
│   │   ├── DuplicateDetectionService.java # Duplicate detection
│   │   ├── MessageProcessingOrchestrator.java # Pipeline orchestration
│   │   ├── AutoloaderService.java         # Startup recovery
│   │   └── RetryService.java              # Retry logic
│   ├── consumer/                          # Kafka consumers
│   │   └── KafkaMessageConsumer.java      # Main message consumer
│   ├── publisher/                         # Kafka producers
│   │   └── MessagePublisher.java          # Message publisher
│   ├── validator/                         # Validation logic
│   │   └── MessageValidator.java          # Schema & duplicate validation
│   ├── performer/                         # Enrichment logic
│   │   └── MessagePerformer.java          # Data enrichment
│   ├── processor/                         # Transformation logic
│   │   └── MessageProcessor.java          # Message routing
│   ├── worker/                            # Publishing logic
│   │   └── MessageWorker.java             # Final message publishing
│   ├── cache/                             # Caching services
│   │   └── SequenceCacheService.java      # Sequence caching
│   ├── controller/                        # REST endpoints
│   │   └── ReprocessController.java       # Reprocessing API
│   ├── scheduler/                         # Scheduled jobs
│   │   └── RetryScheduler.java            # Hourly retry job
│   ├── util/                              # Utility classes
│   │   ├── MDCUtil.java                   # Logging correlation
│   │   ├── HashCodeGenerator.java         # Hash generation
│   │   └── PriorityCalculator.java        # Priority calculation
│   └── exception/                         # Custom exceptions
├── src/main/resources/
│   ├── application.yml                    # Main configuration
│   ├── application.properties             # Simple properties
│   └── sql/                               # Database scripts
│       ├── 01_create_sequences.sql
│       ├── 02_create_tables.sql
│       ├── 03_create_indexes.sql
│       ├── 04_create_history_tables.sql
│       └── 05_archiving_procedure.sql
├── src/test/java/                         # Test cases
└── build.gradle                           # Build configuration
```

## Database Schema

### Tables

#### INCOMING_MESSAGE_TABLE
Primary table for tracking incoming messages from Kafka.

| Column | Type | Description |
|--------|------|-------------|
| MSG_ID | NUMBER(16,0) | Auto-increment primary key |
| SOURCE | VARCHAR2(100) | Message source system |
| X_CORRELATION_ID | VARCHAR2(200) | Unique correlation identifier |
| INTERNAL_SOURCE_ID | NUMBER(16,0) | Internal sequence ID |
| FINAL_STATUS | VARCHAR2(50) | Final processing status |
| INTERNAL_STATUS | VARCHAR2(200) | Current processing class.method |
| PHASE | VARCHAR2(50) | Processing phase |
| EVENT_TYPE | VARCHAR2(100) | Type of event |
| CYCLE_DATE | DATE | Processing date (partition key) |
| ATTRIBUTE_KEY4 | VARCHAR2(500) | Message hashcode for duplicate detection |
| ORIGINAL_MSG | CLOB | Original message content |
| TRANSFORMED_MSG | CLOB | Transformed message |
| PROCESS_LOG | CLOB | Processing logs (max 2000 chars) |

**Indexes:** MSG_ID, FINAL_STATUS, CYCLE_DATE, ATTRIBUTE_KEY4 (hashcode)

#### OUTGOING_MESSAGE_TABLE
Tracks messages published to downstream systems.

| Column | Type | Description |
|--------|------|-------------|
| MSG_ID | NUMBER(16,0) | Auto-increment primary key |
| INTERNAL_SOURCE_ID | NUMBER(16,0) | Links to incoming message |
| OUTGOING_MSG_ID | VARCHAR2(200) | Format: {INTERNAL_SOURCE_ID}-{SEQ} |
| PHASE | VARCHAR2(50) | CREATED, PUBLISHED, ACKNOWLEDGED |
| ... | ... | Similar to INCOMING_MESSAGE_TABLE |

**Partitioning:** Both tables partitioned by `CYCLE_DATE` (daily intervals)

## Configuration

### Application Properties

#### application.yml
```yaml
application:
  name: messaging_app
  db-name: MESSAGING_APP
  
  processing:
    batch:
      size: 500              # Messages per batch
      timeout-ms: 200        # Batch timeout
    thread-pool:
      core-size: 20          # Core thread pool size
      max-size: 50           # Maximum threads
    retry:
      max-attempts: 3        # Maximum retry attempts
      initial-delay-ms: 1000 # Initial retry delay
      multiplier: 2.0        # Backoff multiplier
      non-retriable-errors:  # Errors that won't retry
        - VALIDATION_ERROR
        - SCHEMA_INVALID
  
  sequence:
    names: INTERNAL_SOURCE_SEQUENCE,MSG_ID_SEQUENCE
    max-size: 10000          # Sequences cached
    threshold: 2000          # Replenish threshold
    refresh-date-based-cache: true  # Refresh on date change
  
  autoloader:
    enabled: true            # Enable autoloader
    process-on-startup: true # Process in-progress on startup
    lock-timeout-minutes: 15 # Lock timeout
  
  duplicate:
    detection:
      enabled: true
      check-window-hours: 24 # Duplicate check window
  
  priority:
    default: 5
    mappings:                # Format: source|event_type|priority
      - "CRITICAL_SOURCE|*|1"
      - "*|EMERGENCY|1"
      - "*|NORMAL|5"
```

### Environment Variables

```bash
# Database
DB_URL=jdbc:oracle:thin:@hostname:1521:ORCL
DB_USERNAME=messaging_user
DB_PASSWORD=<password>

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka-broker:9092
KAFKA_CONSUMER_GROUP=message-processor-group

# Redis
REDIS_HOST=redis-server
REDIS_PORT=6379

# Logging
LOG_LEVEL=INFO
SPLUNK_ENABLED=true
PROJECT_CODE=MSG_PROC
BUSINESS_UNIT=ENTERPRISE
```

## Running the Application

### Prerequisites
- Java 21+
- Gradle 8.5+
- Oracle Database 12c+
- Apache Kafka 3.6+
- Redis 6.x

### Build
```bash
./gradlew clean build
```

### Run with Dev Profile
```bash
./gradlew bootRunDev
```

### Run with Local Profile (ActiveMQ for testing)
```bash
./gradlew bootRunLocal
```

### Run Tests
```bash
./gradlew test jacocoTestReport
```

### Build for Deployment
```bash
./gradlew buildForDeployment
```

## Database Setup

Execute SQL scripts in order:

```bash
sqlplus messaging_user/<password>@ORCL

@src/main/resources/sql/01_create_sequences.sql
@src/main/resources/sql/02_create_tables.sql
@src/main/resources/sql/03_create_indexes.sql
@src/main/resources/sql/04_create_history_tables.sql
@src/main/resources/sql/05_archiving_procedure.sql
```

### Weekly Archiving
Schedule this procedure to run weekly:
```sql
BEGIN
    ARCHIVE_MESSAGES_WEEKLY;
END;
/
```

## API Endpoints

### Reprocess Incoming Message
```http
POST /api/reprocess/incoming/{msgId}
```

### Reprocess Outgoing Message
```http
POST /api/reprocess/outgoing/{msgId}
```

### Health Check
```http
GET /actuator/health
```

### Metrics (Prometheus)
```http
GET /actuator/metrics
GET /actuator/prometheus
```

## Monitoring & Logging

### MDC Logging
All logs include correlation context:
```json
{
  "correlationId": "CORR-123",
  "msgId": "1001",
  "source": "SYSTEM_A",
  "eventType": "NOTIFICATION",
  "timestamp": "2024-01-15T10:30:00Z",
  "message": "Message processing completed"
}
```

### Splunk Configuration
Configure these environment variables:
- `SPLUNK_ENABLED=true`
- `PROJECT_CODE=MSG_PROC`
- `BUSINESS_UNIT=ENTERPRISE`
- `ENVIRONMENT=PROD`

### Metrics
Available via Prometheus endpoint:
- Message processing rate
- Error rates by type
- Database connection pool stats
- Kafka consumer lag
- Redis cache hit ratio
- Sequence cache levels

## High Availability

### Kafka Consumer Groups
Multiple instances automatically distribute partitions via CooperativeStickyAssignor.

### Autoloader Lock Mechanism
Only one instance processes in-progress messages on startup using distributed database locks.

### Circuit Breakers
Resilience4j circuit breakers protect:
- Database connections
- Redis cache operations
- External API calls

## Performance Optimizations

1. **Batch Processing**: 500 messages per batch, 200ms timeout
2. **Sequence Caching**: 10,000 sequences cached in memory
3. **HikariCP**: 50 max connections, statement caching enabled
4. **Kafka Tuning**: Cooperative rebalancing, compression enabled
5. **Thread Pools**: Dedicated pools for processing (50 threads)
6. **Database Partitioning**: Daily partitions on CYCLE_DATE
7. **Indexing**: Optimized indexes on frequently queried columns

## Error Handling

### Error Codes
Errors follow format: `{COMPONENT}_{TYPE}_{DETAIL}`

Examples:
- `VALIDATION_ERROR` - Non-retriable
- `SCHEMA_INVALID` - Non-retriable
- `TIMEOUT` - Retriable
- `CONNECTION_ERROR` - Retriable

### Retry Strategy
- **Incoming Messages**: Retry via orchestrator with exponential backoff
- **Outgoing Messages**: Hourly scheduler retries failed publications
- **Non-retriable**: Marked as FAILED immediately

## Testing

### Unit Tests
```bash
./gradlew test
```

### Integration Tests
```bash
./gradlew integrationTest
```

### Coverage Report
```bash
./gradlew jacocoTestReport
open build/reports/jacoco/test/html/index.html
```

Target: **95% code coverage**

## Deployment

### OCP (OpenShift) Deployment
```bash
# Build Docker image
./gradlew build
docker build -t kafka-message-processor:latest .

# Push to registry
docker tag kafka-message-processor:latest registry.example.com/kafka-message-processor:1.0.0
docker push registry.example.com/kafka-message-processor:1.0.0

# Deploy via Harness
# Configuration managed in Harness pipeline
```

### Environment-Specific Configs
- **dev**: Development environment with debug logging
- **qa**: Quality assurance with reduced thread pools
- **prod**: Production with full capacity, INFO logging

## Troubleshooting

### High Memory Usage
- Check sequence cache sizes
- Review thread pool configurations
- Analyze heap dump

### Kafka Consumer Lag
- Increase `concurrency` in application.yml
- Review batch size and timeout
- Check database performance

### Database Deadlocks
- Review transaction isolation levels
- Check indexing on frequently updated tables
- Analyze slow query logs

### Duplicate Detection Issues
- Verify Redis connectivity
- Check hash generation consistency
- Review time window configuration

## Contributing

1. Create feature branch
2. Implement changes with tests
3. Ensure 95% test coverage
4. Submit pull request

## License

Copyright © 2024 Enterprise Messaging Team

## Support

For issues and questions:
- Email: messaging-support@enterprise.com
- Slack: #kafka-message-processor
- JIRA: MSG-PROC project
