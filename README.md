# Transactional Outbox Pattern - Play Framework Implementation

A demo implementation of the **Transactional Outbox Pattern** using Scala 3, Play Framework 3.0, and PostgreSQL. This pattern ensures reliable event delivery to external systems by saving events transactionally with business data, then processing them asynchronously.

## Why the Outbox Pattern?

When building distributed systems, you often need to:

1. Save data to your database
2. Notify external systems about the change

**The Problem**: If you call external APIs directly in your transaction, you risk:

- Inconsistent state if the API call fails after the database commit
- Slow transactions blocking your connection pool
- No retry mechanism for failed deliveries

**The Solution**: The Outbox Pattern

1. Save your business data AND an event record in the **same database transaction**
2. A background processor polls the outbox table and publishes events
3. External API calls happen outside the transaction with automatic retries
4. Failed events move to a Dead Letter Queue (DLQ) after max retries

## Features

### Core Pattern
- **Transactional Consistency**: Events and business data saved atomically
- **At-Least-Once Delivery**: Automatic retries with configurable limits
- **Dead Letter Queue**: Failed events captured with full error context
- **Idempotency Support**: Event deduplication using unique keys
- **Stale Event Recovery**: Automatically resets stuck PROCESSING events back to PENDING

### Performance & Scalability
- **PostgreSQL LISTEN/NOTIFY**: Near-instant event processing (optional, configurable)
- **Concurrent Processing**: Optional parallel workers using `FOR UPDATE SKIP LOCKED`
- **Efficient Polling**: Optimized indexes for fast event queries
- **Connection Pooling**: Proper database connection management

### Developer Experience
- **Demo UI**: Interactive web interface to test the pattern
- **Webhook Simulator**: Built-in HTTP server to simulate success/failure scenarios
- **Comprehensive Logging**: Play Framework logger for debugging
- **Graceful Shutdown**: Clean actor termination on application stop

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Application Layer                          │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐       │
│  │ OrderService │──┬──▶│ OrderRepo    │      │ OutboxHelper │       │
│  └──────────────┘  │   └──────────────┘      └──────────────┘       │
│                    │                                                │
│                    │   ┌────────────────────────────────────┐       │
│                    └──▶│  PostgreSQL (Single Transaction)   │       │
│                        │  - orders table                    │       │
│                        │  - outbox_events table             │       │
│                        └──────────┬─────────────────────────┘       │
└───────────────────────────────────┼─────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
                    ▼               ▼               ▼
         ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
         │ Poll-based   │  │ LISTEN/NOTIFY│  │ Stale Cleanup│
         │ Processing   │  │ (Optional)   │  │ (Background) │
         └──────────────┘  └──────────────┘  └──────────────┘
                    │               │               │
                    └───────────────┼───────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Background Processing                          │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │  OutboxProcessor (Pekko Actor)                           │       │
│  │  - Configurable polling interval                         │       │
│  │  - SELECT ... FOR UPDATE SKIP LOCKED                     │       │
│  │  - Publishes to external systems                         │       │
│  └──────────────────────────────────────────────────────────┘       │
│                                    │                                │
│                    ┌───────────────┼───────────────┐                │
│                    ▼               ▼               ▼                │
│           ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│           │  Success    │  │  Retryable  │  │  Max Retries│         │
│           │  (PROCESSED)│  │  (retry++)  │  │  (→ DLQ)    │         │
│           └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Java 11+
- PostgreSQL 12+
- sbt 1.9+

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/play-outbox-pattern.git
cd play-outbox-pattern

# Start PostgreSQL
docker-compose up -d postgres

# Create database and schema
psql -U postgres -h localhost -c "CREATE DATABASE outbox_demo;"
psql -U postgres -h localhost -d outbox_demo -f sql/schema.sql

# Run the application
sbt run

# Application starts on http://localhost:9000
```

Open your browser to `http://localhost:9000` to see the interactive demo.

### Load Testing

Generate 1000 random orders to test throughput:

```bash
./create-random-orders.sh
```

## Key Components

### 1. OutboxHelper - Transactional Event Creation

Mix this trait into your repositories to save events atomically:

```scala
class OrderRepository extends OutboxHelper {
  def createWithEvent(order: Order): DBIO[Long] =
    withEventFactory((orders returning orders.map(_.id)) += order) { orderId =>
      OrderCreatedEvent(
        orderId     = orderId,
        customerId  = order.customerId,
        totalAmount = order.totalAmount
      )
    }

  def updateStatusWithEvent(orderId: Long, newStatus: String): DBIO[Int] =
    for {
      order <- findById(orderId)
      updated <- withEvent(
        OrderStatusUpdatedEvent(
          orderId   = orderId,
          oldStatus = order.orderStatus,
          newStatus = newStatus
        )
      ) {
        orders.filter(_.id === orderId)
          .map(o => (o.orderStatus, o.updatedAt))
          .update((newStatus, Instant.now()))
      }
    } yield updated
}
```

### 2. OutboxProcessor - Background Publishing

Pekko Typed Actor that:

- Polls `outbox_events` table periodically (when LISTEN/NOTIFY is disabled)
- OR reacts instantly to PostgreSQL NOTIFY events (when enabled)
- Publishes to external systems via `EventPublisher`
- Handles retries and moves failed events to DLQ
- Cleans up stale PROCESSING events (prevents stuck events)

Configuration in `application.conf`:

```hocon
outbox {
  pollInterval = 5 seconds          # How often to poll (when LISTEN/NOTIFY disabled)
  batchSize = 100                   # Max events per batch
  maxRetries = 2                    # Max retry attempts before DLQ
  useListenNotify = true            # Enable PostgreSQL LISTEN/NOTIFY
  staleCleanupEnabled = true        # Enable stale event recovery
  staleTimeoutMinutes = 5           # Mark events as stale after N minutes
  cleanupInterval = 1 minute        # How often to run stale cleanup
}
```

### 3. EventPublisher - External Delivery

Implement this trait for your specific destination:

```scala
trait EventPublisher {
  def publish(event: OutboxEvent): Future[PublishResult]
}

sealed trait PublishResult
object PublishResult {
  case object Success extends PublishResult
  case class Retryable(error: String) extends PublishResult    // Will retry
  case class NonRetryable(error: String) extends PublishResult // Move to DLQ
}
```

Included implementation: `HttpEventPublisher` for webhook delivery.

### 4. Dead Letter Queue (DLQ)

Failed events move to `dead_letter_events` table after:

- **Configurable retries** with retryable errors (5xx, timeouts)
- **Immediate failure** with non-retryable errors (4xx)

View DLQ statistics in the UI or query directly:

```sql
SELECT *
FROM dead_letter_events
ORDER BY failed_at DESC;
```

To reprocess a DLQ event:

```sql
-- Re-insert the event as new with retry count reset
INSERT INTO outbox_events (aggregate_id, event_type, payload, created_at, retry_count)
SELECT aggregate_id, event_type, payload, NOW(), 0
FROM dead_letter_events
WHERE id = <dlq_event_id>;
```

## PostgreSQL LISTEN/NOTIFY Integration

Enable near-instant event processing (microsecond latency) instead of polling:

### How It Works

1. PostgreSQL trigger fires on INSERT to `outbox_events`
2. `pg_notify()` sends notification to listening connections
3. `OutboxProcessor` receives notification and processes immediately
4. Falls back to restart logic on connection failures

### Configuration

```hocon
outbox {
  useListenNotify = true  # Enable LISTEN/NOTIFY mode
  pollInterval = 5.seconds # Only used as fallback when LISTEN/NOTIFY disabled
}
```

### Benefits

- **Sub-second latency**: Events processed within milliseconds
- **Reduced database load**: No constant polling queries
- **Automatic failover**: Falls back to polling if connection drops

### Trade-offs

- Requires dedicated database connection per listener
- Connection must be kept alive (handled automatically)
- May not scale to hundreds of workers (polling is better for massive parallelism)

## Stale Event Recovery

Prevents events from getting stuck in PROCESSING state due to crashes or network issues.

### How It Works

1. Background cleanup task runs every `cleanupInterval`
2. Finds events in PROCESSING state older than `staleTimeoutMinutes`
3. Resets them to PENDING for reprocessing
4. Logs count of recovered events

### Configuration

```hocon
outbox {
  staleCleanupEnabled = true      # Enable/disable stale cleanup
  staleTimeoutMinutes = 5         # Consider events stale after N minutes
  cleanupInterval = 1 minute      # How often to run cleanup
}
```

## Demo Features

### Interactive UI (http://localhost:9000)

- **Create Orders**: Submit orders with customer ID and amount
- **View Statistics**: Real-time metrics for pending/processed/failed events
- **Order Management**: Ship, cancel, or remove orders
- **Auto-Refresh**: HTMX polls for updates every 2-3 seconds

### Webhook Simulator

Built-in endpoints to test different failure scenarios:

```bash
# Random failures (50% by default)
POST http://localhost:9000/simulator/webhook/default

# Always succeeds
POST http://localhost:9000/simulator/always-succeed

# Always fails with 500 (retryable)
POST http://localhost:9000/simulator/always-fail

# Always fails with 400 (non-retryable, immediate DLQ)
POST http://localhost:9000/simulator/always-400
```

Configure failure rates in `application.conf`:

```hocon
simulator {
  failureRate = 0.5           # 50% of requests fail
  slowResponseRate = 0.3      # 30% have delays
  slowResponseDelayMs = 1000  # 1 second delay
}
```

## Configuration

### Event Routing

Route different event types to different endpoints:

```hocon
outbox.http {
  baseUrl = "http://localhost:9000/simulator"
  defaultUrl = "http://localhost:9000/simulator/webhook/default"
  defaultMethod = "POST"

  routes {
    OrderCreated {
      url = "http://localhost:9000/simulator/always-succeed"
      method = "POST"
      timeout = 5 seconds
    }

    OrderCancelled {
      url = "http://localhost:9000/simulator/always-400"
      method = "POST"
      timeout = 10 seconds
    }
  }
}
```

### Database Schema

Key tables in the schema:

```sql
-- Business data
CREATE TABLE orders (
    id           BIGSERIAL PRIMARY KEY,
    customer_id  VARCHAR(255) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    order_status VARCHAR(50) NOT NULL DEFAULT 'PENDING',  -- Order fulfillment state
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted      BOOLEAN NOT NULL DEFAULT FALSE
);

-- Outbox pattern table
CREATE TABLE outbox_events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_id    VARCHAR(255) NOT NULL,
    event_type      VARCHAR(255) NOT NULL,
    payload         TEXT NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',  -- Event processing state
    processed_at    TIMESTAMP,
    retry_count     INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    moved_to_dlq    BOOLEAN NOT NULL DEFAULT FALSE,
    idempotency_key VARCHAR(512) NOT NULL
);

-- Dead letter queue
CREATE TABLE dead_letter_events (
    id                BIGSERIAL PRIMARY KEY,
    original_event_id BIGINT NOT NULL,
    aggregate_id      VARCHAR(255) NOT NULL,
    event_type        VARCHAR(255) NOT NULL,
    payload           TEXT NOT NULL,
    created_at        TIMESTAMP NOT NULL,
    failed_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    retry_count       INT NOT NULL,
    last_error        TEXT NOT NULL,
    reason            VARCHAR(1024) NOT NULL
);
```

## Production Considerations

### Connection Pooling

Size your pool appropriately for your workload:

```hocon
slick.dbs.default.db {
  numThreads = 20
  maximumPoolSize = 20
  minimumIdle = 5
  connectionTimeout = 5000
  maxLifetime = 1800000  # 30 minutes
}

# Dedicated dispatcher for blocking JDBC operations
blocking-jdbc {
  type = Dispatcher
  executor = "thread-pool-executor"
  thread-pool-executor {
    fixed-pool-size = 10
  }
  throughput = 1
}
```

### LISTEN/NOTIFY vs Polling

**Use LISTEN/NOTIFY when:**
- You need low latency (< 1 second)
- You have moderate throughput (< 1000 events/sec)
- You can afford dedicated connections

**Use Polling when:**
- You need massive parallelism (100+ workers)
- Your database doesn't support LISTEN/NOTIFY
- Connection overhead is a concern

### Monitoring

Key metrics to monitor:

- **Pending events count**: Should remain low under normal operation
- **DLQ growth rate**: High rate indicates integration issues
- **Processing latency**: Time from event creation to publish
- **Retry count distribution**: High retries indicate flaky endpoints

Query for monitoring:

```sql
-- Current outbox health
SELECT
  status,
  COUNT(*) as count,
  AVG(retry_count) as avg_retries,
  MAX(created_at) as latest_event
FROM outbox_events
WHERE status != 'PROCESSED'
GROUP BY status;

-- DLQ trends
SELECT
  DATE(failed_at) as date,
  reason,
  COUNT(*) as count
FROM dead_letter_events
GROUP BY DATE(failed_at), reason
ORDER BY date DESC;
```

## License

MIT License - see LICENSE file for details

## Contributing

Contributions welcome! Please open an issue or submit a pull request.

## Resources

- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [PostgreSQL LISTEN/NOTIFY Documentation](https://www.postgresql.org/docs/current/sql-notify.html)
- [Play Framework Documentation](https://www.playframework.com/documentation/3.0.x/Home)
- [Pekko Actors](https://pekko.apache.org/docs/pekko/current/typed/index.html)