# Database Performance Benchmark: PostgreSQL vs ClickHouse vs TimescaleDB

A comprehensive performance comparison of PostgreSQL, ClickHouse, and TimescaleDB for analytical workloads using 25 million stock market records.

## Dataset

- **25 million records** of synthetic stock market data
- **10,000 different instruments** with realistic price movements
- **7+ years of trading data** (2014-2021) with proper date progression
- **Realistic OHLCV data** with volume correlation and price relationships

## Quick Start

1. **Start databases**
```bash
docker compose up -d
```

2. **Install dependencies**
```bash
bun install
```

3. **Generate 25M records**
```bash
bun run src/seed_data.ts
```

4. **Run performance report**
```bash
bun run src/report.ts
```

## Performance Results

### Database Scale
- **PostgreSQL**: 25,000,000 rows
- **ClickHouse**: 25,000,000 rows  
- **TimescaleDB**: 25,000,000 rows

### Storage Efficiency

| Database | Storage Size | Compression vs PostgreSQL |
|----------|--------------|---------------------------|
| PostgreSQL | 2.3 GB | 1.0x (baseline) |
| ClickHouse | 1.78 GB (~1,825 MB) | 1.29x better |
| TimescaleDB | 2.7 GB | 0.86x (16% larger) |

### Query Performance Comparison

| Query Type | PostgreSQL (ms) | ClickHouse (ms) | TimescaleDB (ms) | Best Performer |
|------------|------------------|------------------|-------------------|----------------|
| Simple SELECT 1yr | 49 | 87 | **23** | TimescaleDB |
| Simple SELECT 5yr | **2** | 18 | **2** | PostgreSQL/TimescaleDB |
| Simple SELECT 10yr | **1** | 20 | **2** | PostgreSQL |
| Range aggregation 1yr | 2,689 | **32** | 347 | **ClickHouse (84x faster)** |
| Range aggregation 5yr | 2,352 | **53** | 3,621 | **ClickHouse (44x faster)** |
| Multi-year price avg | 2,347 | **28** | 3,720 | **ClickHouse (84x faster)** |
| Yearly volume analysis | 2,309 | **28** | 3,373 | **ClickHouse (82x faster)** |
| Large timespan aggregation | 2,502 | **48** | 3,244 | **ClickHouse (52x faster)** |
| Complex analytics multi-year | 2,270 | **58** | 3,138 | **ClickHouse (39x faster)** |
| Cross-year volatility | 2,101 | **28** | 2,875 | **ClickHouse (75x faster)** |
| Decade high volume scan | 2,242 | 174 | 3,418 | **ClickHouse (13x faster)** |

## Key Insights

### 🏆 Winners by Category

- **Point queries (SELECT with LIMIT)**: PostgreSQL/TimescaleDB excel
- **Analytical aggregations**: ClickHouse dominates with 10x-80x speedups
- **Storage efficiency**: ClickHouse provides best compression
- **Complex analytics**: ClickHouse consistently outperforms

### 📊 Performance Patterns

1. **PostgreSQL**: Best for simple point queries and small result sets
2. **ClickHouse**: Exceptional for aggregations, analytics, and large scans
3. **TimescaleDB**: Good for time-series point queries, struggles with complex aggregations

### 💾 Storage Analysis

- ClickHouse achieves moderate compression gains (~29% better than PostgreSQL)
- TimescaleDB uses more storage due to hypertable overhead
- All databases handle 25M records efficiently

## Architecture

### Database Configurations
- **PostgreSQL**: Standard B-tree indexes on (instrument_id, date)
- **ClickHouse**: MergeTree engine optimized for analytical queries  
- **TimescaleDB**: Hypertables with time-based partitioning

### Data Model
```sql
CREATE TABLE stock_data (
    instrument_id INTEGER,
    date DATE,
    open DECIMAL(10,2),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    close DECIMAL(10,2),
    volume BIGINT,
    PRIMARY KEY (instrument_id, date)
);
```

## Conclusion

**ClickHouse emerges as the clear winner for analytical workloads**, delivering:
- 10x-80x performance improvements on aggregation queries
- Consistent sub-100ms response times for complex analytics
- Better storage efficiency through columnar compression

**PostgreSQL and TimescaleDB** remain excellent choices for:
- Simple point queries and small result sets
- Applications requiring full SQL compatibility
- Mixed OLTP/OLAP workloads

## License

[MIT](./LICENSE)