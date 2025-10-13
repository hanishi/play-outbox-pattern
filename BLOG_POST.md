# Never Call APIs Inside Database Transactions

> **TL;DR:** Never call external APIs inside a database transaction. If the external call succeeds but the DB commit fails, you have an inconsistent state you cannot roll back. Use **Transactional Outbox + Result Table + Saga Compensation ** to ensure distributed consistency without 3AM cleanup scripts.


*Or: Why putting HTTP calls inside a database transaction will eventually wake you up at 3 AM*

---

## The 3 AM Wake‑Up Call

It's 3 AM. Your phone buzzes. PagerDuty.

**"CRITICAL: Order reconciliation job failed - 47 orders missing from database"**

You open your laptop. The logs show orders from business hours - normal traffic, nothing unusual. Payment API responses? All 200 OK. Shipping confirmations? All sent.

But the database?

**47 orders are missing.**

Customers have been charged. Shipments have been created. Confirmation emails sent. But your `orders` table has no record of these transactions.

You dig deeper. The pattern becomes clear:
```
14:23:15 INFO  Payment API → 200 OK (order-8473)
14:23:15 INFO  Shipping API → 200 OK (order-8473)
14:23:16 ERROR Database transaction → DEADLOCK
14:23:16 INFO  Retrying transaction... (attempt 2/3)
14:23:17 ERROR Database transaction → ROLLBACK
```

This happens because someone wrote code like this:
```scala
// Looks transactional... but isn't.
db.run {
  (for {
    orderId      <- orders += order                            // DB write
    inventoryRes <- DBIO.from(inventoryApi.reserve(orderId))   // API call ✅
    shippingRes  <- DBIO.from(shippingApi.schedule(orderId))   // API call ✅
    billingRes   <- DBIO.from(billingApi.charge(orderId))      // API call ✅
    _            <- shipments += Shipment(orderId, shippingRes.id)  // DB write - DEADLOCK!
  } yield orderId).transactionally
}
```
What actually happened: 
1. ✅ Insert order → succeeds 
2. ✅ Call inventory API → 200 OK (inventory reserved)
3. ✅ Call shipping API → 200 OK (shipment scheduled)
4. ✅ Call billing API → 200 OK (customer charged)
5. ❌ Insert shipment record →  **Fails** (network error, deadlock - doesn't matter)
6. 🔄 Transaction rolls back

Result: Money charged, shipment scheduled, inventory reserved... but no order exists in your database.
The external APIs don't know your transaction failed. They already did their work.
---

## Understanding the Patterns

Before diving into the fix, let's understand **why** we need two patterns working together.

### Problem 1: The Naive Approach (Why it breaks)

Here's what most developers try first:

```scala
// Step 1: Save to database
val orderId = db.run { orders += order }.transactionally

// Step 2: Call external services
inventoryApi.reserve(orderId)
shippingApi.schedule(orderId)
billingApi.charge(orderId)
```

**Seems reasonable, right? Wrong. Here's what breaks:**

#### Failure Mode 1: App crashes between Step 1 and Step 2

```scala
val orderId = db.run { orders += order }.transactionally  // ✅ Succeeds
// 💥 App crashes here (out of memory, deployment, server restart)
inventoryApi.reserve(orderId)  // ← Never executed
```

**Result:** Order saved in the table, but no inventory reserved, no shipping scheduled. 
The order is stuck forever. 
No retry will happen because the app lost the `orderId` from memory.

#### Failure Mode 2: First API succeeds, second API fails

```scala
val orderId = db.run { orders += order }.transactionally  // ✅ Succeeds
inventoryApi.reserve(orderId)  // ✅ Succeeds
shippingApi.schedule(orderId)  // ❌ Network timeout after 30 seconds
```

**Result:** Order saved, inventory reserved, but no shipping. 
The warehouse has reserved items for an order that will never ship. 
And you don't know if you should retry shipping (what if it partially succeeded?).

#### Failure Mode 3: Kafka publish fails

```scala
val orderId = db.run { orders += order }.transactionally  // ✅ Succeeds
kafka.publish("order-created", orderId)  // ❌ Kafka broker down
```

**Result:** Order saved, but the event is lost forever. Downstream services never receive the notification. **No way to recover** - you don't even know the event failed to publish.

**The fundamental problem:** You can't make external systems (HTTP APIs, Kafka, message queues) participate in your database transaction. 
They're separate systems with their own failure modes.

---

### The Transactional Outbox Pattern (Making it reliable)

**The insight:** Instead of calling external systems directly, write a **durable to-do list** in the same transaction.

Think of it like leaving a sticky note on your desk before going to bed:
- ❌ **Naive approach:** "I'll remember to call the API tomorrow" (you won't)
- ✅ **Outbox approach:** "I'll write it down on paper tonight" (it survives even if you forget)

```scala
// Write BOTH the order AND the to-do list atomically
db.run {
  (for {
    orderId <- orders += order
    _       <- outboxEvents += OutboxEvent(
      aggregateId = orderId.toString,
      eventType = "OrderCreated",
      payloads = Map(
        "inventory" -> ...,
        "shipping" -> ...,
        "billing" -> ...
      )
    )
  } yield orderId).transactionally
}

// Later: A background processes the to-do list
```

**What this solves:**

| Failure Scenario | Without Outbox | With Outbox |
|------------------|----------------|-------------|
| App crashes after DB commit | ❌ Event lost forever (in memory) | ✅ Event persisted, processed on restart |
| Kafka broker down | ❌ Event lost, no retry | ✅ Event stays in outbox, retried automatically |
| Network timeout | ❌ Don't know if request succeeded | ✅ Tracked in result table |
| API succeeds, DB rolls back | ❌ **Can happen with DBIO.from()** | ✅ **Impossible** - APIs only called after commit |

**External APIs are only called AFTER the database transaction commits**. 
This makes the "API succeeds but DB rolls back" scenario physically impossible.

The outbox table is your **durable to-do list** that survives:
- App crashes
- Deployments
- Server restarts
- Network failures
- Kafka outages

---

### But wait - the Outbox doesn't solve partial failures!

---

### Problem 2: Partial Failures

Now imagine your event fans out to 3 services:

```
1. Reserve inventory  ✅ succeeds → reservationId = "RES-456"
2. Schedule shipping  ✅ succeeds → shippingId = "SHIP-789"
3. Charge payment     ❌ fails after 3 retries
```

You can't just leave it like this. You have:
- Inventory reserved (blocking stock for other customers)
- Shipment scheduled (warehouse will ship nothing)
- No payment (customer wasn't charged)

**You need to undo steps 1 and 2.**

### The Saga Pattern

A **Saga** is a sequence of transactions where:
- Each transaction is local to one service
- If any transaction fails, **compensating transactions** undo previous steps
- Compensation happens in **reverse order** (LIFO - Last In, First Out)

Think of it as a distributed "undo" button.

**Two approaches:**
1. **Choreography**: Each service publishes events, others react (decentralized)
2. **Orchestration**: A central coordinator controls the flow (centralized)

We use **orchestration** because:
- ✅ Easier to debug (all logic in one place)
- ✅ You know exactly what succeeded (for compensation)
- ✅ Conditional routing is straightforward

---

### How They Work Together

| Pattern | Solves | How |
|---------|--------|-----|
| **Transactional Outbox** | Dual-write problem | Write order + event to DB atomically |
| **Saga (Orchestration)** | Partial failures | Track what succeeded, compensate in LIFO order |

**Together:**
1. **Outbox** ensures the event is never lost (even if app crashes)
2. **Saga** ensures partial failures are compensated (automatically undo)

**How do we know what to compensate?** 

→ **The Result Table** tracks every API call:
- Which calls succeeded
- What IDs they returned (needed for revert)
- Complete audit trail

---

### The Overview of the Solution 

```
┌─────────────────────────────────────────────────────────────────┐
│ Step 1: Atomic Write (Transactional Outbox)                     │
└─────────────────────────────────────────────────────────────────┘

POST /api/orders
    │
    ▼
┌─────────────────────────────────┐
│ db.run { ... }.transactionally  │
│                                 │
│  1. INSERT INTO orders          │  ← Both succeed or both fail
│  2. INSERT INTO outbox_events   │    (atomic)
└──────────────┬──────────────────┘
               │
               ▼ pg_notify('outbox_events_channel')

┌─────────────────────────────────────────────────────────────────┐
│ Step 2: Fan-Out Publishing (Saga Orchestration)                 │
└─────────────────────────────────────────────────────────────────┘

OutboxProcessor (Pekko Actor)
    │
    ├─→ POST /api/inventory    ✅ 200 OK  → UPSERT aggregate_results
    │                                         (aggregate_id, "inventory", "OrderCreated")
    │
    ├─→ POST /api/shipping     ✅ 200 OK  → UPSERT aggregate_results
    │                                         (aggregate_id, "shipping", "OrderCreated")
    │
    └─→ POST /api/billing      ❌ 500 ERR → UPSERT aggregate_results
                                             (aggregate_id, "billing", "OrderCreated")

        After 3 retries:
          ↓
        INSERT INTO dead_letter_events
          ↓
        pg_notify('dlq_events_channel')

┌─────────────────────────────────────────────────────────────────┐
│ Step 3: Compensation (Saga Rollback - LIFO)                     │
└─────────────────────────────────────────────────────────────────┘

DLQProcessor (Pekko Actor)
    │
    ├─ SELECT * FROM aggregate_results
    │  WHERE aggregate_id = 'ORDER-123'
    │    AND success = true
    │    AND event_type = 'OrderCreated'  -- Not ':REVERT'
    │
    │  Returns: [inventory ✅, shipping ✅]
    │
    ├─ Reverse order: [shipping, inventory]  ← LIFO!
    │
    ├─→ POST /api/shipping/{id}/cancel ✅ → UPSERT aggregate_results
    │                                         (aggregate_id, "shipping", "OrderCreated:REVERT")
    │
    └─→ DELETE /api/inventory/{id}     ✅ → UPSERT aggregate_results
                                             (aggregate_id, "inventory", "OrderCreated:REVERT")
```

**Key points:**
1. **Transactional Outbox** guarantees the event exists in the database (even if app crashes)
2. **Result Table** tracks every HTTP call's outcome (success/failure + response data)
3. **Saga Compensation** queries the result table and undoes successful steps in LIFO order
4. **The `:REVERT` suffix** ensures compensation results don't overwrite forward results

---

## The Fix: Outbox + Result Table + LIFO Compensation

Now the pieces fit together:

If the third call fails, we can't "roll back" the first two — those systems **do not support rollback**. So we design for **compensation**, not atomicity.

This means we intentionally track:

| What we track | Stored in | Why |
|---------------|-----------|-----|
| Which API calls succeeded | **Result Table** | So we know what needs to be undone |
| What each call returned | **Result Table** | So we know *which IDs* to undo with |
| When retries are exhausted | **Dead Letter Queue (DLQ)** | So we know when to start compensation |

### The Recovery Sequence

When something fails:

1. Move the event to the **DLQ**
2. Look up all **successful** calls from the **Result Table**
3. **Undo them in reverse order** (LIFO), because dependencies matter
4. Mark the DLQ event **processed** once compensation succeeds

Example:

```
Forward direction:
1. reserveInventory → reservationId = "RES-456"
2. scheduleShipping  → shippingId    = "SHIP-789"
3. chargePayment     → ❌ fails

Compensating direction:
1. cancelShipping("SHIP-789")
2. releaseInventory("RES-456")
```

This guarantees:
- No stuck shipments
- No dangling inventory holds
- No orphan state that needs a human to fix it later

---

### Implementation Sketch (Simplified from actual code)

```scala
// 1) Inside the DB transaction: persist order + outbox event atomically
// Using the OutboxHelper.withEventFactory pattern
class OrderRepository extends OutboxHelper {

  def createWithEvent(order: Order): DBIO[Long] =
    withEventFactory((orders returning orders.map(_.id)) += order) { orderId =>
      OrderCreatedEvent(
        orderId      = orderId,
        customerId   = order.customerId,
        totalAmount  = order.totalAmount,
        shippingType = order.shippingType
      )
    }
}

// The domain event expands to multiple destination payloads:
case class OrderCreatedEvent(...) extends DomainEvent {
  def toOutboxEvent: OutboxEvent = OutboxEvent(
    aggregateId = orderId.toString,
    eventType   = "OrderCreated",
    payloads    = Map(
      "inventory" -> DestinationConfig(
        payload = Some(Json.obj("orderId" -> orderId, "items" -> ...))
      ),
      "shipping" -> DestinationConfig(...),
      "billing" -> DestinationConfig(...)
    )
  )
}

// 2) Outside the transaction: Pekko OutboxProcessor + EventPublisher
// Process each destination, record results in aggregate_results table
for {
  inv  <- publishTo("inventory", invPayload).recordResult
  ship <- publishTo("shipping", shipPayload).recordResult
  bill <- publishTo("billing", billPayload).recordResult
} yield {
  if (bill.success == false) {
    // ❌ Billing failed → move to DLQ for compensation
    dlqRepo.moveFromOutbox(event)
  } else {
    outboxRepo.markAsProcessed(event.id)
  }
}

// 3) DLQProcessor queries aggregate_results for successful calls
val successfulResults = resultRepo.findByAggregateId(
  aggregateId = orderId.toString,
  filter = Result.Success,
  includeReverts = false  // Only forward flow
)

// 4) Compensate in LIFO order with ":REVERT" suffix
successfulResults.reverse.foreach { result =>
  val revertEndpoint = buildRevertEndpoint(result)
  publishTo(s"${result.destination}:REVERT", revertEndpoint)
}
```

**Why this works:**
- **Outbox**: never blocks on HTTP while the transaction is open
- **Result Table**: you know *exactly* which calls succeeded and the IDs needed to undo
- **Saga (LIFO)**: undo in reverse so dependencies are safe (cancel shipment → release inventory)

---

## Configuration-Driven Compensation 

### The Problem: How do you cancel a shipment?

When billing fails, you need to call:
```
POST /api/shipping/{shippingId}/cancel
```

But where does `{shippingId}` come from? **The response from the original shipping API call!**

```json
// Original request:
POST /api/shipping
{"orderId": 123, "address": "..."}

// Response stored in aggregate_results.response_payload:
{
  "shippingId": "SHIP-789",
  "trackingNumber": "1Z999AA1234567890",
  "estimatedDelivery": "2025-01-10"
}
```

### Configuration-Driven Extraction

In `application.conf`, you define **where to extract values** and **how to use them**:

```hocon
shipping {
  method = "POST"
  url = "http://localhost:9000/api/shipping"

  revert {
    url = "http://localhost:9000/api/shipping/{id}/cancel"
    method = "POST"

    # Extract variables from request/response
    extract {
      id = "response:$.shippingId"           # From API response
      customerId = "request:$.customerId"     # From original request
      totalAmount = "request:$.totalAmount"   # From original request
    }

    # Template with placeholders
    payload = """
      {
        "reason": "payment_failed",
        "shippingId": "{id}",
        "customerId": "{customerId}",
        "refundAmount": "{totalAmount}"
      }
    """
  }
}
```

### The Full Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ Step 1: Forward Flow - Call Shipping API                        │
└─────────────────────────────────────────────────────────────────┘

OutboxProcessor sends:
  POST /api/shipping
  Body: {"orderId": 123, "customerId": "CUST-456", "totalAmount": 99.99}

Shipping API responds:
  200 OK
  Body: {"shippingId": "SHIP-789", "trackingNumber": "..."}

Stored in aggregate_results:
  aggregate_id: "123"
  destination: "shipping"
  event_type: "OrderCreated"
  request_payload: {"orderId": 123, "customerId": "CUST-456", "totalAmount": 99.99}
  response_payload: {"shippingId": "SHIP-789", ...}
  success: true

┌─────────────────────────────────────────────────────────────────┐
│ Step 2: Compensation - Build Revert Endpoint                    │
└─────────────────────────────────────────────────────────────────┘

DLQProcessor queries aggregate_results for successful calls:
  SELECT * FROM aggregate_results
  WHERE aggregate_id = '123'
    AND success = true
    AND event_type = 'OrderCreated'

For each result, extract values using JSONPath:
  id = "response:$.shippingId"           → "SHIP-789"
  customerId = "request:$.customerId"     → "CUST-456"
  totalAmount = "request:$.totalAmount"   → 99.99

Replace placeholders in URL template:
  "/api/shipping/{id}/cancel" → "/api/shipping/SHIP-789/cancel"

Replace placeholders in payload template:
  {
    "reason": "payment_failed",
    "shippingId": "{id}",           → "SHIP-789"
    "customerId": "{customerId}",   → "CUST-456"
    "refundAmount": "{totalAmount}" → 99.99
  }

Send revert request:
  POST /api/shipping/SHIP-789/cancel
  Body: {"reason": "payment_failed", "shippingId": "SHIP-789", ...}

Store compensation result:
  event_type: "OrderCreated:REVERT"  ← Note the :REVERT suffix
  success: true
```

### Path Parameters in Action

The framework automatically replaces `{placeholders}` in URLs:

```hocon
inventory {
  url = "http://localhost:9000/api/inventory"

  revert {
    # Multiple path parameters
    url = "http://localhost:9000/api/warehouses/{warehouseId}/inventory/{reservationId}/release"
    method = "DELETE"

    extract {
      warehouseId = "response:$.warehouseId"      # From API response
      reservationId = "response:$.reservationId"   # From API response
    }
  }
}
```

**What happens:**

1. **Forward call response:**
   ```json
   {"reservationId": "RES-456", "warehouseId": "WH-NY-01", "items": [...]}
   ```

2. **DLQProcessor extracts:**
   - `warehouseId` → `"WH-NY-01"`
   - `reservationId` → `"RES-456"`

3. **URL template becomes:**
   ```
   http://localhost:9000/api/warehouses/WH-NY-01/inventory/RES-456/release
   ```

4. **Compensation call:**
   ```
   DELETE /api/warehouses/WH-NY-01/inventory/RES-456/release
   ```

### Conditional Routing: Dynamic Destination Selection
**Routes can be selected dynamically** based on request data or previous API responses.

**Example 1:** Shipping based on original request (no `previousDestination`):

```hocon
shipping {
  method = "POST"
  routes = [
    {
      url = "http://localhost:9000/simulator/webhook/domestic-shipping"
      condition {
        jsonPath = "$.shippingType"
        operator = "eq"
        value = "domestic"
      }
    },
    {
      url = "http://localhost:9000/simulator/webhook/international-shipping"
      condition {
        jsonPath = "$.shippingType"
        operator = "eq"
        value = "international"
      }
    }
  ]
}
```

**Example 2:** Billing based on fraud check (using `previousDestination`):

```hocon
# Sequence: inventory → fraudCheck → shipping → billing
fanout {
  OrderCreated = ["inventory", "fraudCheck", "shipping", "billing"]
}

# Fraud check returns risk score
fraudCheck {
  url = "http://localhost:9000/simulator/fraud/check"
  method = "POST"
  # Response: {"riskScore": 75, "riskLevel": "high", ...}
}

# Billing routes based on fraudCheck response
billing {
  method = "POST"
  routes = [
    {
      # Low-risk: standard billing
      url = "http://localhost:9000/simulator/webhook/billing"
      condition {
        jsonPath = "$.riskScore"
        operator = "lt"
        value = "50"
        previousDestination = "fraudCheck"  # ← Check fraudCheck API response!
      }
      revert {
        url = "http://localhost:9000/simulator/webhook/billing/{transactionId}/refund"
        method = "POST"
        extract {
          transactionId = "response:$.transaction.id"
          amount = "request:$.amount"
        }
        payload = """{"transactionId": "{transactionId}", "amount": "{amount}", "reason": "order_failed"}"""
      }
    },
    {
      # High-risk: high-value processor with additional verification
      url = "http://localhost:9000/simulator/webhook/high-value-processing"
      condition {
        jsonPath = "$.riskScore"
        operator = "gte"
        value = "50"
        previousDestination = "fraudCheck"  # ← Check fraudCheck API response!
      }
      revert {
        url = "http://localhost:9000/simulator/webhook/high-value-processing/{processingId}/cancel"
        method = "POST"
        extract {
          processingId = "response:$.processingId"
        }
      }
    }
  ]
}
```

**What happens:**

1. **Fraud check API responds:**
   ```json
   {
     "riskScore": 75,
     "riskLevel": "high",
     "checks": {
       "cardVerification": "passed",
       ... 
     }
   }
   ```

2. **Billing evaluates conditions:**
   - **Without `previousDestination`**: Checks `$.riskScore` in **original request** → Not found!
   - **With `previousDestination="fraudCheck"`**: Checks `$.riskScore` in **fraudCheck response** → 75
   - Evaluates: `75 >= 50` → TRUE
   - Selects: `high-value-processing` route

3. **Records which route was taken:**
   ```
   aggregate_results.endpoint_url = "http://localhost:9000/simulator/webhook/high-value-processing"
   ```

4. **During compensation:**
   - Queries `aggregate_results` for the exact URL used
   - Calls high-value processor's cancel endpoint (not standard billing refund)

**Key insight:** `previousDestination` enables **decision chaining** - one API's response influences the next API's route. Essential for risk-based routing, feature flags, and context-dependent decisions.

---

### Respecting Rate Limits: Retry-After Header Support

Real-world APIs impose rate limits and experience temporary outages. The system automatically respects server-specified retry delays using the `Retry-After` header.

**The Problem:**

```
Your system: Makes 100 requests/second to billing API
Billing API: Rate limit is 10 requests/second
Result: 90% of your requests get 429 Too Many Requests
```

**Without Retry-After handling:** You keep retrying immediately, making the problem worse (retry storm).

**With Retry-After handling:** You wait exactly as long as the server tells you to.

#### Example 1: Fraud Check API with Auto-Detection

Real-world fraud check APIs (Stripe Radar, Sift, etc.) often impose rate limits:

```hocon
fraudCheck {
  url = "http://localhost:9000/simulator/fraud/check"
  method = "POST"

  retryAfter {
    headerName = "Retry-After"  # Standard HTTP header
    format = "auto"              # Auto-detect format (seconds, unix, ISO 8601, etc.)
    fallbackSeconds = 30         # If header missing, wait 30 seconds
  }
}
```

**What happens:**

```
Request → POST /simulator/fraud/check
Response → 429 Too Many Requests
           Retry-After: 15

System schedules retry for 15 seconds from now (not immediately!)
```

#### Example 2: Custom Header with Unix Timestamp (GitHub-style)

GitHub uses `X-RateLimit-Reset` with Unix epoch timestamp:

```hocon
github {
  url = "https://api.github.com/repos/user/repo"
  method = "GET"

  retryAfter {
    headerName = "X-RateLimit-Reset"  # Custom header name
    format = "unix"                    # Unix epoch seconds
    fallbackSeconds = 3600             # Wait 1 hour if header missing
  }
}
```

**Response headers:**
```
X-RateLimit-Limit: 60
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1704585600  # Unix timestamp
```

The system parses the timestamp and schedules retry at that exact time.

#### Example 3: Retry Info in JSON Body

Some APIs return retry timing in the response body:

```hocon
customApi {
  url = "https://api.example.com/process"
  method = "POST"

  retryAfter {
    fromBody = true                        # Look in response body, not headers
    jsonPath = "$.rateLimit.retryAfter"   # JSONPath to retry delay
    format = "seconds"
    fallbackSeconds = 30
  }
}
```

**Response:**
```json
{
  "status": "unavailable",
  "message": "Service temporarily unavailable",
  "rateLimit": {
    "retryAfter": 20,
    "reason": "maintenance_window"
  }
}
```

System extracts `20` from `$.rateLimit.retryAfter` and waits 20 seconds.

#### Supported Formats

The system automatically detects and parses multiple formats:

| Format | Example | Description |
|--------|---------|-------------|
| `auto` | Tries all formats below | Default - smartly detects format |
| `seconds` | `15` | Delay in seconds from now |
| `milliseconds` | `15000` | Delay in milliseconds from now |
| `unix` | `1704585600` | Absolute Unix epoch seconds |
| `httpDate` | `Wed, 21 Oct 2025 07:28:00 GMT` | RFC 1123 HTTP date format |
| `iso8601` | `2025-10-21T07:28:00Z` | ISO 8601 timestamp |

#### What Happens When Rate Limited

```
1. OutboxProcessor tries to publish event
   ↓
2. API responds: 429 Too Many Requests
   Retry-After: 30
   ↓
3. System parses header: "Wait 30 seconds"
   ↓
4. UPDATE outbox_events SET
   status = 'PENDING',
   next_retry_at = NOW() + INTERVAL '30 seconds',
   retry_count = retry_count + 1
   ↓
5. PostgreSQL trigger fires when next_retry_at <= NOW()
   ↓
6. OutboxProcessor wakes up at exact time and retries
```
---

### What's Config-Driven vs What's Not

Let's be clear about what still requires code changes:

#### ❌ NOT Config-Driven: Forward Flow

**You still need Scala code** to add a new destination to the forward flow:

```scala
// Must update domain event to include new destination
case class OrderCreatedEvent(...) extends DomainEvent {
  def toOutboxEvent: OutboxEvent = OutboxEvent(
    aggregateId = orderId.toString,
    eventType   = "OrderCreated",
    payloads    = Map(
      "inventory" -> DestinationConfig(payload = ...),
      "shipping"  -> DestinationConfig(payload = ...),
      "billing"   -> DestinationConfig(payload = ...),
      "email"     -> DestinationConfig(payload = ...)  // ← New: requires code change
    )
  )
}
```

#### ✅ Config-Driven: Compensation + Routing

**No code changes needed** for:
- Revert endpoint URLs and placeholders
- JSONPath extraction from request/response
- Conditional routing
- Payload templates for compensation

```hocon
# Just add this to application.conf - no Scala code needed
inventory {
  method = "POST"
  url = "http://localhost:9000/api/inventory/reserve"

  revert {
    url = "http://localhost:9000/api/inventory/{reservationId}/release"
    method = "DELETE"
    extract {
      reservationId = "response:$.reservationId"
    }
  }
}
```

## The Three Tables (Why Each Exists)

### 1. `outbox_events` — The Work Queue

```sql
CREATE TABLE outbox_events (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id VARCHAR(255) NOT NULL,  -- "ORDER-123"
    event_type VARCHAR(255) NOT NULL,     -- "OrderCreated"
    payloads JSONB NOT NULL,              -- {"inventory": {...}, "shipping": {...}}
    status VARCHAR(20) DEFAULT 'PENDING', -- PENDING → PROCESSING → PROCESSED
    retry_count INT DEFAULT 0,
    next_retry_at TIMESTAMP,              -- NULL = ready now
    idempotency_key VARCHAR(512) NOT NULL -- Prevent duplicates
);

-- Unique constraint: prevent duplicate events
CREATE UNIQUE INDEX idx_outbox_idempotency ON outbox_events (idempotency_key)
  WHERE status != 'PROCESSED';
```

**Purpose**: One row = one business event that fans out to N destinations.

### 2. `aggregate_results` — The Audit Log (Enables Compensation)

```sql
CREATE TABLE aggregate_results (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id VARCHAR(255) NOT NULL,   -- "ORDER-123"
    event_type VARCHAR(255) NOT NULL,      -- "OrderCreated" or "OrderCreated:REVERT"
    destination VARCHAR(255) NOT NULL,     -- "inventory", "shipping", etc.
    endpoint_url VARCHAR(1024) NOT NULL,   -- Actual URL called
    request_payload JSONB,                 -- What we sent
    response_payload JSONB,                -- What they returned (contains IDs!)
    success BOOLEAN NOT NULL,              -- Did it work?
    published_at TIMESTAMP DEFAULT NOW()
);

-- Unique constraint: one result per aggregate+destination+event_type
CREATE UNIQUE INDEX idx_aggregate_results_unique
  ON aggregate_results (aggregate_id, destination, event_type);
```

**Purpose**: One row = one HTTP call to one destination. Tracks:
- Which calls succeeded (for compensation)
- Response IDs needed for revert (e.g., `shippingId` to cancel)
- Complete audit trail

**The `:REVERT` suffix** ensures forward and revert results coexist:
- `event_type = "OrderCreated"` → forward flow
- `event_type = "OrderCreated:REVERT"` → compensation flow

### 3. `dead_letter_events` — The Compensation Queue

```sql
CREATE TABLE dead_letter_events (
    id BIGSERIAL PRIMARY KEY,
    original_event_id BIGINT NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payloads JSONB NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING',  -- Compensation status
    revert_retry_count INT DEFAULT 0,      -- Retry compensation
    failed_at TIMESTAMP DEFAULT NOW(),
    reason VARCHAR(1024) NOT NULL
);
```

**Purpose**: Events that exhausted retries. Triggers compensation.

---

## PostgreSQL LISTEN/NOTIFY: Instant Event Processing

Instead of polling every N seconds, we use database triggers:

```sql
CREATE OR REPLACE FUNCTION notify_new_outbox_event()
RETURNS trigger AS $$
BEGIN
    IF NEW.status = 'PENDING' AND (
        TG_OP = 'INSERT' OR
        (OLD.status IS DISTINCT FROM 'PENDING' AND
         (NEW.next_retry_at IS NULL OR NEW.next_retry_at <= NOW()))
    ) THEN
        PERFORM pg_notify('outbox_events_channel', NEW.id::text);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_event_inserted
  AFTER INSERT OR UPDATE OF status ON outbox_events
  FOR EACH ROW EXECUTE FUNCTION notify_new_outbox_event();
```

The Pekko actor subscribes to `outbox_events_channel` and wakes up **instantly** when:
- New events are inserted
- Events become ready for retry (`next_retry_at <= NOW()`)

---

## The Pekko Actors Architecture

```
┌─────────────────┐
│ OrderService    │ ← Writes order + outbox event transactionally
└────────┬────────┘
         │
         ▼ pg_notify('outbox_events_channel')
┌──────────────────────────────────────────────┐
│ OutboxProcessor (Pekko Actor)                │
│ - Listens to NOTIFY                          │
│ - Claims events (UPDATE status='PROCESSING') │
│ - Publishes to each destination              │
│ - Records results in aggregate_results       │
└────────┬─────────────────────────────────────┘
         │
         ├─ Success → markAsProcessed()
         └─ Failure (max retries) → moveFromOutbox()
                                       │
                                       ▼
                      ┌────────────────────────────┐
                      │ dead_letter_events         │
                      └────────┬───────────────────┘
                               │ pg_notify('dlq_events_channel')
                               ▼
           ┌────────────────────────────────────────┐
           │ DLQProcessor (Pekko Actor)             │
           │ 1. Query aggregate_results for SUCCESS │
           │ 2. Reverse order (LIFO)                │
           │ 3. Build revert endpoints              │
           │ 4. Publish with ":REVERT" suffix       │
           └────────────────────────────────────────┘
```

---

## Running the Example

```bash
# 1. Start PostgreSQL
docker-compose up -d postgres

# 3. Run the app
sbt run

# 4. Create an order from UI
http://localhost:9000/

# 5. Watch the compensation flow
# When billing fails after retries, you'll see:
# - DLQ event created
# - Successful destinations identified (inventory, shipping)
# - Revert calls in LIFO order
# - Clean rollback with no orphaned state
```

---

## Let me be honest with you.

### This Isn't a Silver Bullet

The patterns in this blog post don't make distributed systems "easy." 
They make them **survivable**.

- You'll still get paged at 3 AM
- You'll still have to write runbooks for edge cases
- You'll still need monitoring, alerting, and operational discipline
- You'll still encounter failure modes you never imagined

### But Here's What You Gain

Instead of debugging:
- "Why was the customer charged, but there's no order in the database?"
- "Why is inventory reserved for orders that don't exist?"
- "How do I find all the orphaned shipments from last week's outage?"

You'll be debugging:
- "Why is this DLQ retry taking longer than expected?"
- "Should we increase the retry backoff for this destination?"
- "Can we optimize this compensation query?"

**The failure modes become manageable.** 
The 3 AM pages become fixable. The data inconsistencies become **impossible by design**.

### If You Remember Nothing Else

**Never call external APIs inside a database transaction.**

That one rule - enforced by these patterns - prevents an entire class of distributed systems bugs that have haunted production systems since the dawn of microservices.

Distributed systems are hard. But they don't have to be **that** hard.

---



