import { Client } from 'pg';
import { createClient } from '@clickhouse/client';

const POSTGRESQL_URL = process.env.POSTGRESQL_URL || 'postgresql://postgres:postgres@localhost:5432/stockdata';
const CLICKHOUSE_CONFIG = {
  host: 'localhost',
  port: 9000,
  username: 'default',
  password: ''
};

interface BenchmarkResult {
  database: string;
  query_type: string;
  duration_ms: number;
  rows_returned: number;
  rows_per_second: number;
}

class Benchmark {
  private pgClient: Client;
  private chClient: any;
  
  constructor() {
    this.pgClient = new Client({ connectionString: POSTGRESQL_URL });
    this.chClient = createClient(CLICKHOUSE_CONFIG);
  }
  
  async connect() {
    await this.pgClient.connect();
    await this.chClient.ping();
    console.log('✓ Connected to both databases');
  }
  
  async disconnect() {
    await this.pgClient.end();
    await this.chClient.close();
  }
  
  private getDateRange(days: number): { startDate: string, endDate: string } {
    const endDate = new Date('2023-12-29'); // Last trading day of 2023
    const startDate = new Date(endDate);
    startDate.setDate(startDate.getDate() - days);
    
    return {
      startDate: startDate.toISOString().split('T')[0],
      endDate: endDate.toISOString().split('T')[0]
    };
  }
  
  private async benchmarkPostgreSQL(queryName: string, query: string): Promise<BenchmarkResult> {
    const startTime = Date.now();
    const result = await this.pgClient.query(query);
    const endTime = Date.now();
    
    const duration = endTime - startTime;
    const rowCount = result.rows.length;
    
    return {
      database: 'PostgreSQL',
      query_type: queryName,
      duration_ms: duration,
      rows_returned: rowCount,
      rows_per_second: rowCount / (duration / 1000)
    };
  }
  
  private async benchmarkClickHouse(queryName: string, query: string): Promise<BenchmarkResult> {
    const startTime = Date.now();
    const result = await this.chClient.query({ query });
    const rows = await result.json();
    const endTime = Date.now();
    
    const duration = endTime - startTime;
    const rowCount = rows.data.length;
    
    return {
      database: 'ClickHouse',
      query_type: queryName,
      duration_ms: duration,
      rows_returned: rowCount,
      rows_per_second: rowCount / (duration / 1000)
    };
  }
  
  async runTimeRangeBenchmarks(): Promise<BenchmarkResult[]> {
    const results: BenchmarkResult[] = [];
    
    // Test different query complexities
    const testInstrumentId = 1001;
    const testQueries = [
      {
        name: 'simple_select_1yr',
        pg: `SELECT date, close FROM stock_data WHERE instrument_id = ${testInstrumentId} AND date >= '2022-12-29' AND date <= '2023-12-29' ORDER BY date`,
        ch: `SELECT date, close FROM stock_data WHERE instrument_id = ${testInstrumentId} AND date >= '2022-12-29' AND date <= '2023-12-29' ORDER BY date`
      },
      {
        name: 'simple_select_5yr',
        pg: `SELECT date, close FROM stock_data WHERE instrument_id = ${testInstrumentId} AND date >= '2018-12-30' AND date <= '2023-12-29' ORDER BY date`,
        ch: `SELECT date, close FROM stock_data WHERE instrument_id = ${testInstrumentId} AND date >= '2018-12-30' AND date <= '2023-12-29' ORDER BY date`
      },
      {
        name: 'range_aggregation_1yr',
        pg: `SELECT date, SUM(volume) as total_volume, AVG(close) as avg_price, COUNT(*) as record_count FROM stock_data WHERE date >= '2022-12-29' AND date <= '2023-12-29' GROUP BY date ORDER BY date`,
        ch: `SELECT date, SUM(volume) as total_volume, AVG(close) as avg_price, COUNT(*) as record_count FROM stock_data WHERE date >= '2022-12-29' AND date <= '2023-12-29' GROUP BY date ORDER BY date`
      },
      {
        name: 'range_aggregation_5yr',
        pg: `SELECT date, SUM(volume) as total_volume, AVG(close) as avg_price, COUNT(*) as record_count FROM stock_data WHERE date >= '2018-12-30' AND date <= '2023-12-29' GROUP BY date ORDER BY date`,
        ch: `SELECT date, SUM(volume) as total_volume, AVG(close) as avg_price, COUNT(*) as record_count FROM stock_data WHERE date >= '2018-12-30' AND date <= '2023-12-29' GROUP BY date ORDER BY date`
      },
      {
        name: 'complex_analytics_multi_year',
        pg: `SELECT date, SUM(volume) as total_volume, AVG(close) as avg_price, MAX(high) - MIN(low) as price_range FROM stock_data WHERE date >= '2018-01-01' AND date <= '2023-12-31' GROUP BY date ORDER BY total_volume DESC LIMIT 100`,
        ch: `SELECT date, SUM(volume) as total_volume, AVG(close) as avg_price, MAX(high) - MIN(low) as price_range FROM stock_data WHERE date >= '2018-01-01' AND date <= '2023-12-31' GROUP BY date ORDER BY total_volume DESC LIMIT 100`
      }
    ];
    
    for (const query of testQueries) {
      console.log(`\n📊 Benchmarking ${query.name}...`);
      
      try {
        const pgResult = await this.benchmarkPostgreSQL(query.name, query.pg);
        console.log(`  PostgreSQL: ${pgResult.duration_ms}ms, ${pgResult.rows_returned} rows`);
        results.push(pgResult);
        
        const chResult = await this.benchmarkClickHouse(query.name, query.ch);
        console.log(`  ClickHouse: ${chResult.duration_ms}ms, ${chResult.rows_returned} rows`);
        results.push(chResult);
        
        const speedup = pgResult.duration_ms / chResult.duration_ms;
        console.log(`  Speedup: ${speedup.toFixed(2)}x`);
        
      } catch (error) {
        console.error(`Error in ${query.name} benchmark:`, error);
      }
    }
    
    return results;
  }
  
  async runAggregationBenchmarks(): Promise<BenchmarkResult[]> {
    const results: BenchmarkResult[] = [];
    
    console.log('\n📊 Running range-based analytical benchmarks...');
    
    const aggregationQueries = [
      {
        name: 'yearly_volume_analysis',
        pg: `SELECT date, SUM(volume) as total_volume FROM stock_data WHERE date >= '2020-01-01' AND date <= '2023-12-31' GROUP BY date ORDER BY date`,
        ch: `SELECT date, SUM(volume) as total_volume FROM stock_data WHERE date >= '2020-01-01' AND date <= '2023-12-31' GROUP BY date ORDER BY date`
      },
      {
        name: 'multi_year_price_avg',
        pg: `SELECT date, AVG(close) as avg_close FROM stock_data WHERE date >= '2018-01-01' AND date <= '2023-12-31' GROUP BY date ORDER BY date`,
        ch: `SELECT date, AVG(close) as avg_close FROM stock_data WHERE date >= '2018-01-01' AND date <= '2023-12-31' GROUP BY date ORDER BY date`
      },
      {
        name: 'decade_high_volume_scan',
        pg: `SELECT * FROM stock_data WHERE date >= '2014-01-01' AND date <= '2023-12-31' AND volume > 500000 ORDER BY volume DESC LIMIT 1000`,
        ch: `SELECT * FROM stock_data WHERE date >= '2014-01-01' AND date <= '2023-12-31' AND volume > 500000 ORDER BY volume DESC LIMIT 1000`
      },
      {
        name: 'cross_year_volatility',
        pg: `SELECT date, MAX(high) - MIN(low) as daily_range FROM stock_data WHERE date >= '2019-01-01' AND date <= '2023-12-31' GROUP BY date ORDER BY daily_range DESC LIMIT 500`,
        ch: `SELECT date, MAX(high) - MIN(low) as daily_range FROM stock_data WHERE date >= '2019-01-01' AND date <= '2023-12-31' GROUP BY date ORDER BY daily_range DESC LIMIT 500`
      },
      {
        name: 'large_timespan_aggregation',
        pg: `SELECT COUNT(*) as record_count, SUM(volume) as total_volume, AVG(close) as avg_price FROM stock_data WHERE date >= '2015-01-01' AND date <= '2022-12-31'`,
        ch: `SELECT COUNT(*) as record_count, SUM(volume) as total_volume, AVG(close) as avg_price FROM stock_data WHERE date >= '2015-01-01' AND date <= '2022-12-31'`
      }
    ];
    
    for (const query of aggregationQueries) {
      console.log(`\n  Testing ${query.name}...`);
      
      try {
        const pgResult = await this.benchmarkPostgreSQL(query.name, query.pg);
        console.log(`    PostgreSQL: ${pgResult.duration_ms}ms, ${pgResult.rows_returned} rows`);
        results.push(pgResult);
        
        const chResult = await this.benchmarkClickHouse(query.name, query.ch);
        console.log(`    ClickHouse: ${chResult.duration_ms}ms, ${chResult.rows_returned} rows`);
        results.push(chResult);
        
        const speedup = pgResult.duration_ms / chResult.duration_ms;
        console.log(`    Speedup: ${speedup.toFixed(2)}x`);
        
      } catch (error) {
        console.error(`Error in ${query.name} benchmark:`, error);
      }
    }
    
    return results;
  }
}

function analyzeResults(results: BenchmarkResult[]) {
  console.log('\n--- Benchmark Analysis ---\n');
  
  const pgResults = results.filter(r => r.database === 'PostgreSQL');
  const chResults = results.filter(r => r.database === 'ClickHouse');
  
  console.log('Query Performance Comparison:');
  console.log('Query Type'.padEnd(20) + 'PostgreSQL (ms)'.padEnd(18) + 'ClickHouse (ms)'.padEnd(18) + 'Speedup');
  console.log('-'.repeat(70));
  
  const queryTypes = [...new Set(results.map(r => r.query_type))];
  
  for (const queryType of queryTypes) {
    const pgResult = pgResults.find(r => r.query_type === queryType);
    const chResult = chResults.find(r => r.query_type === queryType);
    
    if (pgResult && chResult) {
      const speedup = pgResult.duration_ms / chResult.duration_ms;
      console.log(
        queryType.padEnd(20) + 
        pgResult.duration_ms.toString().padEnd(18) + 
        chResult.duration_ms.toString().padEnd(18) + 
        speedup.toFixed(2) + 'x'
      );
    }
  }
  
  // Overall statistics
  const avgPgDuration = pgResults.reduce((sum, r) => sum + r.duration_ms, 0) / pgResults.length;
  const avgChDuration = chResults.reduce((sum, r) => sum + r.duration_ms, 0) / chResults.length;
  const overallSpeedup = avgPgDuration / avgChDuration;
  
  console.log('\nOverall Performance:');
  console.log(`PostgreSQL average: ${avgPgDuration.toFixed(2)}ms`);
  console.log(`ClickHouse average: ${avgChDuration.toFixed(2)}ms`);
  console.log(`Overall speedup: ${overallSpeedup.toFixed(2)}x`);
}

async function main() {
  const benchmark = new Benchmark();
  
  try {
    await benchmark.connect();
    
    console.log('🚀 Starting benchmark tests...\n');
    
    const timeRangeResults = await benchmark.runTimeRangeBenchmarks();
    const aggregationResults = await benchmark.runAggregationBenchmarks();
    
    const allResults = [...timeRangeResults, ...aggregationResults];
    
    analyzeResults(allResults);
    
    // Save results
    const benchmarkData = {
      timestamp: new Date().toISOString(),
      results: allResults,
      summary: {
        total_queries: allResults.length,
        postgresql_avg_duration: allResults.filter(r => r.database === 'PostgreSQL').reduce((sum, r) => sum + r.duration_ms, 0) / allResults.filter(r => r.database === 'PostgreSQL').length,
        clickhouse_avg_duration: allResults.filter(r => r.database === 'ClickHouse').reduce((sum, r) => sum + r.duration_ms, 0) / allResults.filter(r => r.database === 'ClickHouse').length,
      }
    };
    
    require('fs').writeFileSync('./data/benchmark-results.json', JSON.stringify(benchmarkData, null, 2));
    console.log('\n✅ Results saved to ./data/benchmark-results.json');
    
  } catch (error) {
    console.error('Benchmark failed:', error);
    process.exit(1);
  } finally {
    await benchmark.disconnect();
  }
}

if (import.meta.main) {
  main();
}