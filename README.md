# Transactional Outbox Pattern - Play Framework Implementation

A demo implementation of the **Transactional Outbox Pattern** using Scala 3, Play Framework 3.0, and PostgreSQL. This
pattern ensures reliable event delivery to external systems by saving events transactionally with business data, then
processing them asynchronously.

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

- **Transactional Consistency**: Events and business data saved atomically
- **At-Least-Once Delivery**: Automatic retries with exponential backoff
- **Dead Letter Queue**: Failed events captured with full error context
- **Concurrent Processing**: Optional parallel workers using `FOR UPDATE SKIP LOCKED`
- **HTMX Demo UI**: Interactive web interface to test the pattern
- **Webhook Simulator**: Built-in HTTP server to simulate success/failure scenarios
- **Idempotency Support**: Event deduplication using aggregate IDs
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
│                        └────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Background Processing                          │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │  OutboxProcessor (Pekko Actor)                           │       │
│  │  - Polls every N seconds                                 │       │
│  │  - SELECT ... FOR UPDATE SKIP LOCKED                     │       │
│  │  - Publishes to external systems                         │       │
│  └──────────────────────────────────────────────────────────┘       │
│                                    │                                │
│                    ┌───────────────┼───────────────┐                │
│                    ▼               ▼               ▼                │
│           ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│           │  Success    │  │  Retryable  │  │  Max Retries│         │
│           │  (marked)   │  │  (retry++)  │  │  (→ DLQ)    │         │
│           └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────────────┘
```

### Run the Application

```bash
# Start postgres
docker-compose up -d postgres

# Compile and run
sbt run

# Application starts on http://localhost:9000
```

Open your browser to `http://localhost:9000` to see the interactive demo.

## Key Components

### 1. OutboxHelper - Transactional Event Creation

Mix this trait into your repositories to save events atomically:

```scala
class OrderRepository extends OutboxHelper {
  def createOrder(order: Order): DBIO[Long] =
    withEvent(OrderCreatedEvent(order.id,.
  ..

  ) ) (
    // Both saved in same transaction
    (orders returning orders.map(_.id)) += order
    )
}
```

### 2. OutboxProcessor - Background Publishing

Pekko Typed Actor that:

- Polls `outbox_events` table periodically
- Publishes to external systems via `EventPublisher`
- Handles retries and moves failed events to DLQ

Configuration in `application.conf`:

```hocon
outbox {
  pollInterval = 2 seconds  # How often to check for new events
  batchSize = 10            # Max events per batch
  poolSize = 1              # Number of parallel workers (1 = single processor)
}
```

### 3. EventPublisher - External Delivery

Implement this trait for your specific destination:

```scala
trait EventPublisher {
  def publish(event: OutboxEvent): Future[PublishResult]
}

sealed trait PublishResult

case object Success extends PublishResult

case class Retryable(error: String) extends PublishResult // Will retry

case class NonRetryable(error: String) extends PublishResult // Move to DLQ
```

Included implementation: `HttpEventPublisher` for webhook delivery.

### 4. Dead Letter Queue (DLQ)

Failed events move to `dead_letter_events` table after:

- **3 retries** with retryable errors (5xx, timeouts)
- **Immediate failure** with non-retryable errors (4xx)

View DLQ statistics in the UI or query directly:

```sql
SELECT *
FROM dead_letter_events
ORDER BY failed_at DESC;
```

To reprocess a DLQ event:

```sql
INSERT INTO outbox_events (aggregate_id, event_type, payload, created_at, retry_count)
SELECT aggregate_id, event_type, payload, NOW(), 0
FROM dead_letter_events
WHERE id = < dlq_event_id >;
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

# Always fails with 500
POST http://localhost:9000/simulator/always-fail

# Always fails with 400 (non-retryable)
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
  baseUrl = "https://api.example.com"
  defaultUrl = "https://api.example.com/events"
  defaultMethod = "POST"

  routes {
    OrderCreated {
      url = "https://api.example.com/orders/created"
      method = "POST"
      timeout = 5 seconds
    }

    OrderCancelled {
      url = "https://api.example.com/orders/cancelled"
      method = "POST"
      timeout = 10 seconds
    }
  }
}
```

### Parallel Processing

Enable multiple workers for higher throughput:

```hocon
outbox {
  poolSize = 4  # 4 parallel workers
}
```

Workers use `FOR UPDATE SKIP LOCKED` to avoid processing the same events.

**Important**: Parallel processing means **events may be delivered out of order**. Ensure your consumers handle this.

### Database Connection Pool

Size your pool appropriately:

```hocon
slick.dbs.default.db {
  numThreads = 8
  maximumPoolSize = 8  # Must be ≥ (poolSize * 2) + other app needs
  minimumIdle = 0
}
```