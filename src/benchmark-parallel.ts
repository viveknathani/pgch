import { Client } from 'pg';
import { createClient } from '@clickhouse/client';

const POSTGRESQL_URL = process.env.POSTGRESQL_URL || 'postgresql://postgres:postgres@localhost:5432/stockdata';
const CLICKHOUSE_CONFIG = {
  host: 'localhost',
  port: 9000,
  username: 'default',
  password: ''
};

interface ParallelBenchmarkResult {
  database: string;
  concurrent_queries: number;
  total_duration_ms: number;
  queries_completed: number;
  average_query_time_ms: number;
  queries_per_second: number;
  individual_timings: number[];
}

class ParallelBenchmark {
  
  async createPGConnection(): Promise<Client> {
    const client = new Client({ connectionString: POSTGRESQL_URL });
    await client.connect();
    return client;
  }
  
  async createCHConnection() {
    const client = createClient(CLICKHOUSE_CONFIG);
    await client.ping();
    return client;
  }
  
  private getRandomInstrumentId(): number {
    return Math.floor(Math.random() * 5000) + 1;
  }
  
  private getRandomDateRange(): { startDate: string, endDate: string } {
    const endYear = 2023;
    const startYear = 2014;
    const year = startYear + Math.floor(Math.random() * (endYear - startYear + 1));
    
    const startMonth = Math.floor(Math.random() * 12) + 1;
    const endMonth = Math.min(startMonth + Math.floor(Math.random() * 6) + 1, 12);
    
    const startDate = `${year}-${startMonth.toString().padStart(2, '0')}-01`;
    const endDate = `${year}-${endMonth.toString().padStart(2, '0')}-28`;
    
    return { startDate, endDate };
  }
  
  private generateQueries(count: number): Array<{ instrumentId: number, startDate: string, endDate: string }> {
    return Array.from({ length: count }, () => {
      const instrumentId = this.getRandomInstrumentId();
      const { startDate, endDate } = this.getRandomDateRange();
      return { instrumentId, startDate, endDate };
    });
  }
  
  async executePostgreSQLQuery(client: Client, queryParams: { instrumentId: number, startDate: string, endDate: string }): Promise<number> {
    const startTime = Date.now();
    
    const query = `
      SELECT date, close, volume 
      FROM stock_data 
      WHERE instrument_id = $1 
      AND date >= $2 
      AND date <= $3
      ORDER BY date
    `;
    
    await client.query(query, [queryParams.instrumentId, queryParams.startDate, queryParams.endDate]);
    
    return Date.now() - startTime;
  }
  
  async executeClickHouseQuery(client: any, queryParams: { instrumentId: number, startDate: string, endDate: string }): Promise<number> {
    const startTime = Date.now();
    
    const query = `
      SELECT date, close, volume 
      FROM stock_data 
      WHERE instrument_id = ${queryParams.instrumentId}
      AND date >= '${queryParams.startDate}' 
      AND date <= '${queryParams.endDate}'
      ORDER BY date
    `;
    
    const result = await client.query({ query });
    await result.json(); // Consume the result
    
    return Date.now() - startTime;
  }
  
  async benchmarkPostgreSQLParallel(concurrency: number): Promise<ParallelBenchmarkResult> {
    const queries = this.generateQueries(concurrency * 4); // Each connection will run multiple queries
    const connections = await Promise.all(
      Array.from({ length: concurrency }, () => this.createPGConnection())
    );
    
    console.log(`  Starting ${concurrency} concurrent PostgreSQL connections...`);
    
    const startTime = Date.now();
    const results: number[][] = [];
    
    try {
      // Distribute queries among connections
      const queryPromises = connections.map(async (client, index) => {
        const clientQueries = queries.filter((_, i) => i % concurrency === index);
        const timings: number[] = [];
        
        for (const queryParams of clientQueries) {
          const duration = await this.executePostgreSQLQuery(client, queryParams);
          timings.push(duration);
        }
        
        return timings;
      });
      
      const allResults = await Promise.all(queryPromises);
      const totalDuration = Date.now() - startTime;
      const allTimings = allResults.flat();
      const totalQueries = allTimings.length;
      
      return {
        database: 'PostgreSQL',
        concurrent_queries: concurrency,
        total_duration_ms: totalDuration,
        queries_completed: totalQueries,
        average_query_time_ms: allTimings.reduce((sum, t) => sum + t, 0) / totalQueries,
        queries_per_second: totalQueries / (totalDuration / 1000),
        individual_timings: allTimings
      };
      
    } finally {
      await Promise.all(connections.map(client => client.end()));
    }
  }
  
  async benchmarkClickHouseParallel(concurrency: number): Promise<ParallelBenchmarkResult> {
    const queries = this.generateQueries(concurrency * 4);
    const connections = await Promise.all(
      Array.from({ length: concurrency }, () => this.createCHConnection())
    );
    
    console.log(`  Starting ${concurrency} concurrent ClickHouse connections...`);
    
    const startTime = Date.now();
    
    try {
      const queryPromises = connections.map(async (client, index) => {
        const clientQueries = queries.filter((_, i) => i % concurrency === index);
        const timings: number[] = [];
        
        for (const queryParams of clientQueries) {
          const duration = await this.executeClickHouseQuery(client, queryParams);
          timings.push(duration);
        }
        
        return timings;
      });
      
      const allResults = await Promise.all(queryPromises);
      const totalDuration = Date.now() - startTime;
      const allTimings = allResults.flat();
      const totalQueries = allTimings.length;
      
      return {
        database: 'ClickHouse',
        concurrent_queries: concurrency,
        total_duration_ms: totalDuration,
        queries_completed: totalQueries,
        average_query_time_ms: allTimings.reduce((sum, t) => sum + t, 0) / totalQueries,
        queries_per_second: totalQueries / (totalDuration / 1000),
        individual_timings: allTimings
      };
      
    } finally {
      await Promise.all(connections.map(client => client.close()));
    }
  }
  
  async runParallelBenchmarks(): Promise<ParallelBenchmarkResult[]> {
    const results: ParallelBenchmarkResult[] = [];
    const concurrencyLevels = [1, 2, 4, 6];
    
    for (const concurrency of concurrencyLevels) {
      console.log(`\n📊 Testing ${concurrency} concurrent connections...\n`);
      
      try {
        const pgResult = await this.benchmarkPostgreSQLParallel(concurrency);
        console.log(`  PostgreSQL: ${pgResult.queries_completed} queries in ${pgResult.total_duration_ms}ms`);
        console.log(`    Avg query time: ${pgResult.average_query_time_ms.toFixed(2)}ms`);
        console.log(`    QPS: ${pgResult.queries_per_second.toFixed(2)}`);
        results.push(pgResult);
        
        await new Promise(resolve => setTimeout(resolve, 1000)); // Brief pause
        
        const chResult = await this.benchmarkClickHouseParallel(concurrency);
        console.log(`  ClickHouse: ${chResult.queries_completed} queries in ${chResult.total_duration_ms}ms`);
        console.log(`    Avg query time: ${chResult.average_query_time_ms.toFixed(2)}ms`);
        console.log(`    QPS: ${chResult.queries_per_second.toFixed(2)}`);
        results.push(chResult);
        
        const qpsSpeedup = chResult.queries_per_second / pgResult.queries_per_second;
        const avgTimeSpeedup = pgResult.average_query_time_ms / chResult.average_query_time_ms;
        console.log(`    QPS Speedup: ${qpsSpeedup.toFixed(2)}x`);
        console.log(`    Avg Time Speedup: ${avgTimeSpeedup.toFixed(2)}x`);
        
      } catch (error) {
        console.error(`Error with concurrency level ${concurrency}:`, error);
      }
    }
    
    return results;
  }
}

function analyzeParallelResults(results: ParallelBenchmarkResult[]) {
  console.log('\n--- Parallel Query Analysis ---\n');
  
  console.log('Concurrency Performance:');
  console.log('Concurrency'.padEnd(12) + 'Database'.padEnd(12) + 'QPS'.padEnd(10) + 'Avg Time (ms)'.padEnd(15) + 'Total Queries');
  console.log('-'.repeat(65));
  
  const concurrencyLevels = [...new Set(results.map(r => r.concurrent_queries))].sort();
  
  for (const concurrency of concurrencyLevels) {
    const pgResult = results.find(r => r.database === 'PostgreSQL' && r.concurrent_queries === concurrency);
    const chResult = results.find(r => r.database === 'ClickHouse' && r.concurrent_queries === concurrency);
    
    if (pgResult) {
      console.log(
        concurrency.toString().padEnd(12) +
        'PostgreSQL'.padEnd(12) +
        pgResult.queries_per_second.toFixed(1).padEnd(10) +
        pgResult.average_query_time_ms.toFixed(1).padEnd(15) +
        pgResult.queries_completed.toString()
      );
    }
    
    if (chResult) {
      console.log(
        concurrency.toString().padEnd(12) +
        'ClickHouse'.padEnd(12) +
        chResult.queries_per_second.toFixed(1).padEnd(10) +
        chResult.average_query_time_ms.toFixed(1).padEnd(15) +
        chResult.queries_completed.toString()
      );
    }
    
    if (pgResult && chResult) {
      const speedup = chResult.queries_per_second / pgResult.queries_per_second;
      console.log(`  → Speedup: ${speedup.toFixed(2)}x`);
    }
    console.log();
  }
  
  // Scalability analysis
  console.log('\nScalability Analysis:');
  const pgResults = results.filter(r => r.database === 'PostgreSQL').sort((a, b) => a.concurrent_queries - b.concurrent_queries);
  const chResults = results.filter(r => r.database === 'ClickHouse').sort((a, b) => a.concurrent_queries - b.concurrent_queries);
  
  if (pgResults.length > 1) {
    const pgScaling = pgResults[pgResults.length - 1].queries_per_second / pgResults[0].queries_per_second;
    console.log(`PostgreSQL QPS scaling (1x to ${pgResults[pgResults.length - 1].concurrent_queries}x): ${pgScaling.toFixed(2)}x`);
  }
  
  if (chResults.length > 1) {
    const chScaling = chResults[chResults.length - 1].queries_per_second / chResults[0].queries_per_second;
    console.log(`ClickHouse QPS scaling (1x to ${chResults[chResults.length - 1].concurrent_queries}x): ${chScaling.toFixed(2)}x`);
  }
}

async function main() {
  console.log('🚀 Starting parallel query benchmark...\n');
  
  const benchmark = new ParallelBenchmark();
  
  try {
    const results = await benchmark.runParallelBenchmarks();
    
    analyzeParallelResults(results);
    
    // Save detailed results
    const parallelBenchmarkData = {
      timestamp: new Date().toISOString(),
      test_description: 'Parallel query performance test with multiple concurrent connections per database',
      results: results,
      summary: {
        max_concurrency_tested: Math.max(...results.map(r => r.concurrent_queries)),
        postgresql_max_qps: Math.max(...results.filter(r => r.database === 'PostgreSQL').map(r => r.queries_per_second)),
        clickhouse_max_qps: Math.max(...results.filter(r => r.database === 'ClickHouse').map(r => r.queries_per_second)),
      }
    };
    
    require('fs').writeFileSync('./data/parallel-benchmark-results.json', JSON.stringify(parallelBenchmarkData, null, 2));
    console.log('\n✅ Parallel benchmark results saved to ./data/parallel-benchmark-results.json');
    
  } catch (error) {
    console.error('Parallel benchmark failed:', error);
    process.exit(1);
  }
}

if (import.meta.main) {
  main();
}