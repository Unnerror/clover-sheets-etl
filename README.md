# Clover POS → Google Sheets ETL Pipeline

An automated data pipeline that extracts transaction data from the **Clover POS REST API** and loads it into a structured **Google Sheets** reporting system — built with Google Apps Script.

Developed as a freelance project for a Montreal-based small business (café). Running in production since 2025.

---

## What it does

The business had months of transaction history locked inside Clover with no reliable way to query or analyze it. Clover's built-in reporting was limited and the data model was messy (payments ≠ orders, modifiers nested inside line items, offline payments mixed with online, etc.).

This pipeline pulls raw data nightly, normalizes it, resolves relationships between payments and orders, and populates a multi-sheet reporting dashboard — automatically, every night.

---

## Architecture

```
Clover REST API
      │
      ▼
┌─────────────────────┐
│  1. pullRawPayments │  ← paginated fetch with cursor, deduplication
│     ForDate()       │
└────────┬────────────┘
         │ RawPayments sheet (raw JSON blobs)
         ▼
┌─────────────────────┐
│  2. processRaw      │  ← parse JSON, normalize fields, map employee IDs
│     Payments()      │
└────────┬────────────┘
         │ FormattedPayments sheet
         ▼
┌──────────────────────────┐
│  3. linkPaymentIds       │  ← join payments → orders by orderId
│     ToFormattedOrders()  │
└────────┬─────────────────┘
         │
         ▼
┌──────────────────────────┐
│  4. markPrimarySuccess   │  ← identify canonical payment per order
│     InFormattedPayments()│     (handles split payments, refunds, offline)
└────────┬─────────────────┘
         │
         ▼
┌──────────────────────────┐
│  5. updateRawOrders      │  ← fetch full order JSON with lineItems
│     FromFormatted()      │     + modifications (batched, rate-limited)
└────────┬─────────────────┘
         │ RawSuccessedOrders sheet
         ▼
┌──────────────────────────┐
│  6. buildFormatted        │  ← flatten lineItems + modifiers,
│     OrderItems()          │     apply item/modifier/employee maps
└────────┬──────────────────┘
         │ FormattedOrderItemsModifiers sheet
         ▼
┌──────────────────────────┐
│  7. populateMargin()     │  ← calculate margin = price - cost
└──────────────────────────┘
         │
         ▼
   Google Sheets Dashboard
   ├── OrdersView       (payments with employee, discount, payment type)
   ├── ItemsView        (line items with category, margin)
   ├── Stats by Range   (dynamic date-range summary)
   ├── Top 20 Products  (by revenue and by quantity)
   └── Avg Price/Month  (per-product price trends with dropdown)
```

---

## Key Engineering Decisions

### Incremental processing
Each pipeline stage uses Script Properties to track the last processed row. Re-running the pipeline never re-processes already-handled data — safe to run multiple times.

### Idempotency & deduplication
Every write stage checks for existing records before inserting. PaymentIds, OrderIds, and row markers (`USED`) prevent duplicate rows even if the pipeline is interrupted mid-run.

### Cursor-based pagination
Clover uses cursor pagination for large result sets. The payment fetch loop follows `json.cursor` until exhausted, handling arbitrarily large date ranges correctly.

### Primary payment resolution
One order can have multiple payment attempts (split payments, retries, offline sync). `markPrimarySuccessInFormattedPayments()` selects the canonical payment per order using a priority system: exact amount match > largest amount > most recent `createdTime`.

### Race condition prevention
`formattingItemsAndModifiers()` uses a dual lock system — `LockService.getScriptLock()` (prevents concurrent executions) + a Script Properties flag (prevents re-entry within the same execution context). The `finally` block ensures the flag is only cleared by the execution that set it.

### In-memory caching
`RAW_ORDERS_CACHE` and `FMT_ORDER_IDS_CACHE` are populated once per execution and reused across all orders in a batch — avoiding repeated `getValues()` calls to Sheets for each order (which would hit Apps Script quotas quickly with 100+ orders).

### Credential management
All sensitive values (`CLOVER_MERCHANT_ID`, `CLOVER_ACCESS_TOKEN`) are stored in **Script Properties**, never in code.

---

## File Structure

```
src/
├── Main.gs        — pipeline stages 1–7, all core ETL logic (~1,000 lines)
├── Mappings.gs    — mapping loaders + Clover sync for items/modifiers/employees
└── Pipeline.gs    — master orchestrator functions + nightly cron trigger
```

**Mapping sheets** (maintained in Google Sheets, synced from Clover):
- `MappingItems` — item ID → name, price, cost, category
- `MappingModifiers` — modifier ID → group, name, price, type (MODIFIER / TAG)
- `MappingEmployees` — employee ID → name

This design keeps the mapping data editable by the business owner without touching code.

---

## Dashboard Sheets

| Sheet | Description |
|---|---|
| `OrdersView` | One row per payment. Date, time, employee, subtotal, tax, tip, payment type, discount info, week number |
| `ItemsView` | One row per line item / modifier. Price, cost, margin, category, base item |
| `Stats by DateRange` | Dynamic summary: total orders, revenue, tax, tips, avg order value for any date range |
| `Top 20 Products` | All-time top 20 by total CAD revenue and by units sold |
| `Avg Prices by Month` | Per-product monthly avg price, units sold, total sales — with dropdown filter |

---

## Running the Pipeline

**Nightly (automated):**
`nightlyCloverFetchingPipeline()` is triggered via Apps Script time-based trigger (runs at ~2 AM). Processes the previous day's data end-to-end.

**Manual backfill (single date):**
Call individual functions with a date string:
```javascript
pullRawPaymentsForDate('2025-09-12');
processRawPayments();
linkPaymentIdsToFormattedOrders();
markPrimarySuccessInFormattedPayments();
updateRawOrdersFromFormattedForDateFull('2025-09-12');
formattingItemsAndModifiers();
```

**Re-sync mappings only:**
```javascript
syncAllMappings(); // refreshes items, modifiers, employees from Clover
```

---

## Tech Stack

- **Google Apps Script** (JavaScript runtime)
- **Clover REST API v3** — payments, orders, line items, modifiers, employees
- **Google Sheets API** (via SpreadsheetApp)
- **LockService** — concurrency control
- **PropertiesService** — incremental state, credential storage

---

*Freelance project — Montreal, 2025. Client anonymized.*
