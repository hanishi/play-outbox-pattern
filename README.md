# Transactional Outbox + Saga Compensation Pattern

Reference implementation solving the dual-write problem with automatic compensation.

[![Scala](https://img.shields.io/badge/Scala-3.3.6-red.svg)](https://www.scala-lang.org/)
[![Apache Pekko](https://img.shields.io/badge/Apache%20Pekko-1.0.3-blue.svg)](https://pekko.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16.10-blue.svg)](https://www.postgresql.org/)

## Overview

Accompanies blog post: *
*[Never Call APIs Inside Database Transactions](https://rockthejvm.com/articles/never-call-apis-inside-database-transactions)
**

**Problem:** External API succeeds, DB transaction fails → irrecoverable inconsistency

**Solution:**

- **Transactional Outbox** - Write events atomically with business data
- **Result Table** - Track every API call for compensation
- **Saga** - Automatically undo partial failures

## Quick Start

```bash
# Start PostgreSQL
docker-compose up -d postgres

# Run the app
sbt run
# Open http://localhost:9000
```

### Trigger Compensation

Edit `conf/application.conf`:

```hocon
service.failure.rates.billing.charge = 1.0  # Force billing failures
```

Create an order - watch automatic compensation in logs:

```
[info] ✓ inventory → 200 OK
[info] ✓ shipping → 200 OK
[warn] ❌ billing → 503 (attempt 3/3)
[info] DLQProcessor - Compensation: [shipping, inventory]
[info] ✓ shipping.revert → 200 OK (cancelled)
[info] ✓ inventory.revert → 200 OK (released)
```

## Key Features

- **PostgreSQL LISTEN/NOTIFY** for instant event processing
- **Configuration-driven compensation** - no code changes for new endpoints
- **Conditional routing** based on previous API responses
- **Rate limit handling** with Retry-After headers
- **Automatic + manual compensation** flows
- **Complete audit trail** in `aggregate_results` table

## Configuration Example

```hocon
inventory {
  url = "http://localhost:9000/api/inventory/reserve"
  revert {
    url = "http://localhost:9000/api/inventory/{reservationId}/release"
    method = "DELETE"
    extract.reservationId = "response:$.reservationId"
  }
}

# Conditional routing based on fraud check
billing.routes = [{
  url = "http://localhost:9000/api/billing"
  condition {
    jsonPath = "$.riskScore"
    operator = "lt"
    value = "50"
    previousDestination = "fraudCheck"
  }
}]
```

## Learn More

- 📖 [Blog Post](https://rockthejvm.com/articles/never-call-apis-inside-database-transactions)

## License

This work is licensed under
a [Creative Commons Attribution 4.0 International License](https://creativecommons.org/licenses/by/4.0/).

[![CC BY 4.0](https://licensebuttons.net/l/by/4.0/88x31.png)](https://creativecommons.org/licenses/by/4.0/)

**You are free to:**

- **Share** — copy and redistribute the material
- **Adapt** — remix, transform, and build upon the material for any purpose