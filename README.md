# pgch

This project compares PostgreSQL and ClickHouse performance for storing and querying historical stock data under different scenarios.

## dataset

- **50+ million records** of historical stock data
- **10 years of trading data** (2014-2023, weekdays only)
- **Realistic stock data** with proper price relationships and volume correlation

## setup

1. Start Databases

```bash
docker compose up -d
```
This starts:
- PostgreSQL on port 5432
- ClickHouse on ports 8123 (HTTP) and 9000 (native)

2. dependencies

```bash
bun install
```

3. database schemas

```bash
bun run setup
```

Creates tables with proper indexing:
- PostgreSQL: Primary key, secondary indexes on date fields
- ClickHouse: MergeTree engine with optimized column ordering

4. insertion of 50M records

```bash
bun run src/stream-insert-50m.ts
```

Streams 50+ million records directly to both databases:
- Generates realistic stock price data with proper relationships
- Volume correlation with price movements  
- Parallel insertion to both PostgreSQL and ClickHouse
- Real-time performance metrics

### performance

```bash
bun run benchmark
```

| Query Type | PostgreSQL (ms) | ClickHouse (ms) | Speedup |
|------------|-----------------|-----------------|---------|
| Simple SELECT 5yr | 2 | 14 | 0.14x |
| Simple SELECT 10yr | 3 | 19 | 0.16x |
| Complex analytics multi-year | 5,447 | 1,883 | 2.89x |
| Cross-year volatility | 4,971 | 1,446 | 3.44x |
| Simple SELECT 1yr | 81 | 22 | 3.68x |
| Range aggregation 1yr | 6,258 | 1,015 | 6.17x |
| Range aggregation 5yr | 5,201 | 792 | 6.57x |
| Large timespan aggregation | 7,983 | 962 | 8.30x |
| Multi-year price avg | 4,267 | 385 | 11.08x |
| Yearly volume analysis | 5,100 | 402 | 12.69x |
| Decade high volume scan | 4,613 | 13 | 354.85x |

## storage efficiency

| Database | Storage Size | Compression Ratio |
|----------|--------------|-------------------|
| PostgreSQL | 3,657 MB | 1.0x (baseline) |
| ClickHouse | 1,311 MB | 2.79x smaller |

ClickHouse's columnar storage achieves nearly 3x better compression than PostgreSQL's row-based storage on the same 50M+ record dataset.

## usage notes

- Ensure Docker is running before starting
- The data directory is persistent across container restarts  
- 50M record insertion takes approximately 15 minutes
- Generated data is deterministic for reproducible benchmarks
- All scripts include error handling and progress reporting
- Results include detailed timing and throughput metrics

## license

[MIT](./LICENSE)
