import { DatabaseClients } from "./client";

async function getRowCounts() {
  const db = new DatabaseClients();
  await db.connect();

  try {
    // PostgreSQL count
    const pgResult = await db.pgClient.query('SELECT COUNT(*) FROM stock_data');
    const pgCount = parseInt(pgResult.rows[0].count);

    // ClickHouse count
    const chResult = await db.chClient.query({ query: 'SELECT COUNT(*) as count FROM stock_data' });
    const chData = await chResult.json();
    const chCount = parseInt(chData.data[0].count);

    // TimescaleDB count
    const tsResult = await db.timescaleClient.query('SELECT COUNT(*) FROM stock_data');
    const tsCount = parseInt(tsResult.rows[0].count);

    return { postgresql: pgCount, clickhouse: chCount, timescaledb: tsCount };
  } finally {
    await db.disconnect();
  }
}

async function getStorageSizes() {
  const db = new DatabaseClients();
  await db.connect();

  try {
    // PostgreSQL storage size
    const pgSizeResult = await db.pgClient.query(`
      SELECT pg_size_pretty(pg_total_relation_size('stock_data')) as size
    `);
    const pgSize = pgSizeResult.rows[0].size;

    // ClickHouse storage size
    const chSizeResult = await db.chClient.query({
      query: `SELECT formatReadableSize(sum(bytes_on_disk)) as size FROM system.parts WHERE table = 'stock_data'`
    });
    const chSizeData = await chSizeResult.json();
    const chSize = chSizeData.data[0]?.size || 'N/A';

    // TimescaleDB storage size  
    const tsSizeResult = await db.timescaleClient.query(`
      SELECT pg_size_pretty(pg_total_relation_size('stock_data')) as size
    `);
    const tsSize = tsSizeResult.rows[0].size;

    return { postgresql: pgSize, clickhouse: chSize, timescaledb: tsSize };
  } finally {
    await db.disconnect();
  }
}

async function runPerformanceTests() {
  const db = new DatabaseClients();
  await db.connect();

  const queries = [
    {
      name: "Simple SELECT 1yr",
      pg: "SELECT * FROM stock_data WHERE date >= '2014-01-01' AND date < '2015-01-01' LIMIT 1000",
      ch: "SELECT * FROM stock_data WHERE date >= '2014-01-01' AND date < '2015-01-01' LIMIT 1000",
      ts: "SELECT * FROM stock_data WHERE date >= '2014-01-01' AND date < '2015-01-01' LIMIT 1000"
    },
    {
      name: "Simple SELECT 5yr",
      pg: "SELECT * FROM stock_data WHERE date >= '2014-01-01' AND date < '2019-01-01' LIMIT 1000",
      ch: "SELECT * FROM stock_data WHERE date >= '2014-01-01' AND date < '2019-01-01' LIMIT 1000",
      ts: "SELECT * FROM stock_data WHERE date >= '2014-01-01' AND date < '2019-01-01' LIMIT 1000"
    },
    {
      name: "Simple SELECT 10yr",
      pg: "SELECT * FROM stock_data WHERE date >= '2014-01-01' AND date < '2024-01-01' LIMIT 1000",
      ch: "SELECT * FROM stock_data WHERE date >= '2014-01-01' AND date < '2024-01-01' LIMIT 1000",
      ts: "SELECT * FROM stock_data WHERE date >= '2014-01-01' AND date < '2024-01-01' LIMIT 1000"
    },
    {
      name: "Range aggregation 1yr",
      pg: "SELECT instrument_id, AVG(close), MIN(low), MAX(high), SUM(volume) FROM stock_data WHERE date >= '2014-01-01' AND date < '2015-01-01' GROUP BY instrument_id",
      ch: "SELECT instrument_id, AVG(close), MIN(low), MAX(high), SUM(volume) FROM stock_data WHERE date >= '2014-01-01' AND date < '2015-01-01' GROUP BY instrument_id",
      ts: "SELECT instrument_id, AVG(close), MIN(low), MAX(high), SUM(volume) FROM stock_data WHERE date >= '2014-01-01' AND date < '2015-01-01' GROUP BY instrument_id"
    },
    {
      name: "Range aggregation 5yr",
      pg: "SELECT instrument_id, AVG(close), MIN(low), MAX(high), SUM(volume) FROM stock_data WHERE date >= '2014-01-01' AND date < '2019-01-01' GROUP BY instrument_id",
      ch: "SELECT instrument_id, AVG(close), MIN(low), MAX(high), SUM(volume) FROM stock_data WHERE date >= '2014-01-01' AND date < '2019-01-01' GROUP BY instrument_id",
      ts: "SELECT instrument_id, AVG(close), MIN(low), MAX(high), SUM(volume) FROM stock_data WHERE date >= '2014-01-01' AND date < '2019-01-01' GROUP BY instrument_id"
    },
    {
      name: "Multi-year price avg",
      pg: "SELECT EXTRACT(YEAR FROM date) as year, AVG(close) as avg_price FROM stock_data WHERE date >= '2014-01-01' AND date < '2020-01-01' GROUP BY EXTRACT(YEAR FROM date) ORDER BY year",
      ch: "SELECT toYear(date) as year, AVG(close) as avg_price FROM stock_data WHERE date >= '2014-01-01' AND date < '2020-01-01' GROUP BY toYear(date) ORDER BY year",
      ts: "SELECT EXTRACT(YEAR FROM date) as year, AVG(close) as avg_price FROM stock_data WHERE date >= '2014-01-01' AND date < '2020-01-01' GROUP BY EXTRACT(YEAR FROM date) ORDER BY year"
    },
    {
      name: "Yearly volume analysis",
      pg: "SELECT EXTRACT(YEAR FROM date) as year, instrument_id, SUM(volume) as total_volume FROM stock_data WHERE date >= '2014-01-01' AND date < '2020-01-01' GROUP BY EXTRACT(YEAR FROM date), instrument_id ORDER BY total_volume DESC LIMIT 100",
      ch: "SELECT toYear(date) as year, instrument_id, SUM(volume) as total_volume FROM stock_data WHERE date >= '2014-01-01' AND date < '2020-01-01' GROUP BY toYear(date), instrument_id ORDER BY total_volume DESC LIMIT 100",
      ts: "SELECT EXTRACT(YEAR FROM date) as year, instrument_id, SUM(volume) as total_volume FROM stock_data WHERE date >= '2014-01-01' AND date < '2020-01-01' GROUP BY EXTRACT(YEAR FROM date), instrument_id ORDER BY total_volume DESC LIMIT 100"
    },
    {
      name: "Large timespan aggregation",
      pg: "SELECT date_trunc('month', date) as month, COUNT(*), AVG(close), SUM(volume) FROM stock_data WHERE date >= '2014-01-01' AND date < '2021-01-01' GROUP BY date_trunc('month', date) ORDER BY month",
      ch: "SELECT toStartOfMonth(date) as month, COUNT(*), AVG(close), SUM(volume) FROM stock_data WHERE date >= '2014-01-01' AND date < '2021-01-01' GROUP BY toStartOfMonth(date) ORDER BY month",
      ts: "SELECT date_trunc('month', date) as month, COUNT(*), AVG(close), SUM(volume) FROM stock_data WHERE date >= '2014-01-01' AND date < '2021-01-01' GROUP BY date_trunc('month', date) ORDER BY month"
    },
    {
      name: "Complex analytics multi-year",
      pg: "SELECT instrument_id, EXTRACT(YEAR FROM date) as year, AVG(close) as avg_close, STDDEV(close) as volatility, MIN(low) as year_low, MAX(high) as year_high FROM stock_data WHERE date >= '2014-01-01' AND date < '2018-01-01' GROUP BY instrument_id, EXTRACT(YEAR FROM date) HAVING COUNT(*) > 100 ORDER BY volatility DESC LIMIT 1000",
      ch: "SELECT instrument_id, toYear(date) as year, AVG(close) as avg_close, stddevPop(close) as volatility, MIN(low) as year_low, MAX(high) as year_high FROM stock_data WHERE date >= '2014-01-01' AND date < '2018-01-01' GROUP BY instrument_id, toYear(date) HAVING COUNT(*) > 100 ORDER BY volatility DESC LIMIT 1000",
      ts: "SELECT instrument_id, EXTRACT(YEAR FROM date) as year, AVG(close) as avg_close, STDDEV(close) as volatility, MIN(low) as year_low, MAX(high) as year_high FROM stock_data WHERE date >= '2014-01-01' AND date < '2018-01-01' GROUP BY instrument_id, EXTRACT(YEAR FROM date) HAVING COUNT(*) > 100 ORDER BY volatility DESC LIMIT 1000"
    },
    {
      name: "Cross-year volatility",
      pg: "SELECT instrument_id, STDDEV(close) as price_volatility, AVG(volume) as avg_volume FROM stock_data WHERE date >= '2014-01-01' AND date < '2019-01-01' GROUP BY instrument_id HAVING STDDEV(close) > 5 ORDER BY price_volatility DESC LIMIT 500",
      ch: "SELECT instrument_id, stddevPop(close) as price_volatility, AVG(volume) as avg_volume FROM stock_data WHERE date >= '2014-01-01' AND date < '2019-01-01' GROUP BY instrument_id HAVING stddevPop(close) > 5 ORDER BY price_volatility DESC LIMIT 500",
      ts: "SELECT instrument_id, STDDEV(close) as price_volatility, AVG(volume) as avg_volume FROM stock_data WHERE date >= '2014-01-01' AND date < '2019-01-01' GROUP BY instrument_id HAVING STDDEV(close) > 5 ORDER BY price_volatility DESC LIMIT 500"
    },
    {
      name: "Decade high volume scan",
      pg: "SELECT * FROM stock_data WHERE volume > 1000000 AND date >= '2014-01-01' AND date < '2024-01-01' ORDER BY volume DESC LIMIT 10000",
      ch: "SELECT * FROM stock_data WHERE volume > 1000000 AND date >= '2014-01-01' AND date < '2024-01-01' ORDER BY volume DESC LIMIT 10000",
      ts: "SELECT * FROM stock_data WHERE volume > 1000000 AND date >= '2014-01-01' AND date < '2024-01-01' ORDER BY volume DESC LIMIT 10000"
    }
  ];

  const results = [];

  for (const query of queries) {
    console.log(`Running ${query.name}...`);
    
    // PostgreSQL
    const pgStart = Date.now();
    try {
      await db.pgClient.query(query.pg);
    } catch (e) {
      console.log(`PostgreSQL error for ${query.name}:`, e);
    }
    const pgTime = Date.now() - pgStart;

    // ClickHouse
    const chStart = Date.now();
    try {
      await db.chClient.query({ query: query.ch });
    } catch (e) {
      console.log(`ClickHouse error for ${query.name}:`, e);
    }
    const chTime = Date.now() - chStart;

    // TimescaleDB
    const tsStart = Date.now();
    try {
      await db.timescaleClient.query(query.ts);
    } catch (e) {
      console.log(`TimescaleDB error for ${query.name}:`, e);
    }
    const tsTime = Date.now() - tsStart;

    const speedupPgVsCh = pgTime / chTime;
    const speedupTsVsCh = tsTime / chTime;

    results.push({
      name: query.name,
      postgresql: pgTime,
      clickhouse: chTime,
      timescaledb: tsTime,
      speedupPgVsCh,
      speedupTsVsCh
    });
  }

  await db.disconnect();
  return results;
}

async function generateReport() {
  console.log('🔍 Generating Database Report...\n');

  // 1. Row counts
  console.log('📊 Row Counts:');
  const rowCounts = await getRowCounts();
  console.log(`PostgreSQL: ${rowCounts.postgresql.toLocaleString()}`);
  console.log(`ClickHouse: ${rowCounts.clickhouse.toLocaleString()}`);
  console.log(`TimescaleDB: ${rowCounts.timescaledb.toLocaleString()}\n`);

  // 2. Storage sizes
  console.log('💾 Storage Sizes:');
  const storageSizes = await getStorageSizes();
  console.log(`PostgreSQL: ${storageSizes.postgresql}`);
  console.log(`ClickHouse: ${storageSizes.clickhouse}`);
  console.log(`TimescaleDB: ${storageSizes.timescaledb}\n`);

  // 3. Performance comparison
  console.log('⚡ Performance Comparison:');
  const perfResults = await runPerformanceTests();
  
  console.log('| Query Type | PostgreSQL (ms) | ClickHouse (ms) | TimescaleDB (ms) | PG vs CH Speedup | TS vs CH Speedup |');
  console.log('|------------|------------------|------------------|-------------------|-------------------|-------------------|');
  
  for (const result of perfResults) {
    console.log(`| ${result.name} | ${result.postgresql} | ${result.clickhouse} | ${result.timescaledb} | ${result.speedupPgVsCh.toFixed(2)}x | ${result.speedupTsVsCh.toFixed(2)}x |`);
  }

  console.log('\n✅ Report generation complete!');
}

await generateReport();