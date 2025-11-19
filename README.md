# PostgreSQL vs ClickHouse Stock Data Benchmark

This project compares PostgreSQL and ClickHouse performance for storing and querying historical stock data at enterprise scale.

## Dataset

- **50+ million records** of historical stock data
- **10 years of trading data** (2014-2023, weekdays only)
- **Realistic stock data** with proper price relationships and volume correlation

## Setup

### 1. Start Databases

```bash
docker compose up -d
```

This starts:
- PostgreSQL on port 5432
- ClickHouse on ports 8123 (HTTP) and 9000 (native)

### 2. Install Dependencies

```bash
bun install
```

### 3. Set Up Database Schemas

```bash
bun run setup
```

Creates tables with proper indexing:
- PostgreSQL: Primary key, secondary indexes on date fields
- ClickHouse: MergeTree engine with optimized column ordering

### 4. Generate and Insert 50M Records

```bash
bun run src/stream-insert-50m.ts
```

Streams 50+ million records directly to both databases:
- Generates realistic stock price data with proper relationships
- Volume correlation with price movements  
- Parallel insertion to both PostgreSQL and ClickHouse
- Real-time performance metrics

## Benchmark Tests

### Query Performance Benchmarks

```bash
bun run benchmark
```

Tests various SELECT query patterns on 50M records:
- **Time range queries**: 1 day, 5 days, 1 month, 1 year, 5 years
- **Aggregation queries**: Daily volume sums, average prices, filtered scans

### Parallel Query Benchmarks

```bash
bun run benchmark-parallel
```

Tests concurrent query performance:
- 1, 2, 4, and 6 concurrent connections
- Multiple queries per connection
- Measures throughput (QPS) and latency

## Database Configuration

### PostgreSQL
- Table: `stock_data` with DECIMAL precision
- B-tree indexes on primary and date fields
- Row-oriented storage optimized for OLTP workloads

### ClickHouse  
- Table: `stock_data` with MergeTree engine
- Columnar storage with optimal ordering
- Decimal64 types for precise financial data
- Secondary indexes for enhanced query performance

## Results

All benchmark results are saved in the `./data/` directory:
- `stream-insert-50m-results.json`: 50M record insertion performance
- `benchmark-results.json`: SELECT query performance on 50M records
- `parallel-benchmark-results.json`: Concurrent query performance results

## Performance Results (50M Records)

| Query Type | Query Pattern | PostgreSQL | ClickHouse | Speedup |
|------------|---------------|------------|------------|---------|
| Simple SELECT 5yr | Simple point query | 3ms | 9ms | 0.33x |
| Parallel simple queries (2 connections) | Concurrent point queries | 533 QPS | 129 QPS | 0.24x |
| Parallel simple queries (4 connections) | Concurrent point queries | 457 QPS | 372 QPS | 0.81x |
| Parallel simple queries (6 connections) | Concurrent point queries | 462 QPS | 453 QPS | 0.98x |
| Parallel simple queries (1 connection) | Concurrent point queries | 55 QPS | 71 QPS | 1.30x |
| Complex analytics multi-year | Complex aggregation with sorting | 3,274ms | 1,534ms | 2.13x |
| Cross-year volatility | Advanced analytical query | 1,764ms | 800ms | 2.21x |
| Range aggregation 5yr | Multi-year GROUP BY | 2,831ms | 705ms | 4.02x |
| Simple SELECT 1yr | Simple point query | 102ms | 24ms | 4.25x |
| Range aggregation 1yr | Single-year GROUP BY | 3,594ms | 743ms | 4.84x |
| Large timespan aggregation | Cross-decade aggregation | 1,738ms | 336ms | 5.17x |
| Multi-year price avg | Multi-year analytical | 1,996ms | 364ms | 5.48x |
| Yearly volume analysis | Annual volume processing | 2,593ms | 255ms | 10.17x |
| Decade high volume scan | Large-scale data scanning | 1,375ms | 8ms | 171.88x |

### Key Findings
- **PostgreSQL excels** at simple point queries and high-concurrency OLTP workloads
- **ClickHouse becomes superior** as queries grow in analytical complexity
- **ClickHouse delivers extraordinary performance** with up to 171x speedup on complex analytical scans
- **Query complexity correlation**: Simple queries favor PostgreSQL, while aggregations favor ClickHouse
- At enterprise scale (50M+ records), database choice depends on query patterns: PostgreSQL for OLTP, ClickHouse for OLAP

## Project Structure

```
├── docker-compose.yml          # Database services
├── package.json               # Dependencies and scripts
├── src/
│   ├── setup.ts                    # Database schema creation
│   ├── stream-insert-50m.ts        # 50M record streaming insertion
│   ├── benchmark.ts                # SELECT query benchmarks
│   └── benchmark-parallel.ts       # Concurrent query benchmarks
└── data/                           # Benchmark results
```

## Usage Notes

- Ensure Docker is running before starting
- The data directory is persistent across container restarts  
- 50M record insertion takes approximately 15 minutes
- Generated data is deterministic for reproducible benchmarks
- All scripts include error handling and progress reporting
- Results include detailed timing and throughput metrics

## Web Interface

ClickHouse provides a web interface at http://localhost:8123 for manual query testing and exploration.