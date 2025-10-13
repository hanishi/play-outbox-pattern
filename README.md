# Pekko Actors for Background Processing in Play Framework

> Demonstrating Pekko Actors as background processors in Play Framework, 
> using Transactional Outbox + Orchestration-based Saga Pattern as a practical example

**Built with**: Scala 3 · Play Framework 3.0 · PostgreSQL · **Pekko Actors**

## What This Demonstrates

This project shows **one way to implement background processing using Pekko Actors** in Play Framework, 
through a real-world use case: reliable event delivery with automatic compensation.

### The Use Case: Outbox + Saga Pattern

When building microservices, you need to:
1. Save data to your database
2. Notify other services about the change

**The catch**: What if the notification fails? What if some services succeed and others fail?

### Why Pekko Actors for This?

- 🎭 **Message-Driven**: React to database LISTEN/NOTIFY events instantly (not just polling)
- 🎭 **Clean Lifecycle**: Actors start/stop cleanly with Play's `ApplicationLifecycle`
- 🎭 **State Management**: Actors naturally hold state (timers, retry schedules)
- 🎭 **Thread-Safe**: Actor message processing avoids concurrency bugs
- 🎭 **Natural Fit**: Works well with Play's async, non-blocking model

## Quick Start

```bash
# 1. Start PostgreSQL
docker-compose up -d postgres

# 2. Run the app
sbt run

# 3. Open http://localhost:9000
```
Create an order in the UI and watch the automatic rollback when payment fails!

## Key Features

- **Reliable Delivery**: Events saved transactionally, delivered with retries respecting Retry-After headers
- **Automatic Saga Compensation**: Partial failures trigger automatic rollback (LIFO order)
- **Conditional Routing**: Route events to different endpoints based on payload conditions
- **Fan-out**: Single event → multiple http endpoints
- **Dead Letter Queue**: Failed events moved to DLQ with automatic revert operations
- **PostgreSQL LISTEN/NOTIFY**: Near-instant processing
- **Crash-Safe**: Events are durable, processing resumes on restart

## Example Scenario

```
Order Created → Warehouse ✅ → Shipping ✅ → Payment ❌

Automatic Rollback:
  Payment failed → Cancel Shipping ✅ → Release Inventory ✅
```

## Architecture

### Orchestration-based Saga
This project uses **orchestration** (centralized coordinator) rather than **choreography** (event-driven):

```
Orchestrator (OutboxProcessor + DLQProcessor)
  ↓
  ├→ Inventory Service (reserve) ✅
  ├→ Shipping Service (schedule) ✅
  ├→ Payment Service (charge) ❌ FAILS
  ↓
  DLQ triggers compensation (reverse order):
  ├→ Shipping Service (cancel) ✅
  └→ Inventory Service (release) ✅
```

## License

MIT License - see LICENSE file for details
