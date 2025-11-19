import { Client } from 'pg';
import { createClient } from '@clickhouse/client';
import { readFileSync } from 'fs';

const POSTGRESQL_URL = process.env.POSTGRESQL_URL || 'postgresql://postgres:postgres@localhost:5432/stockdata';
const CLICKHOUSE_CONFIG = {
  host: 'localhost',
  port: 9000,
  username: 'default',
  password: ''
};
const BATCH_SIZE = 10000;

interface StockRecord {
  instrument_id: number;
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

async function insertToPostgreSQL(data: StockRecord[]) {
  const client = new Client({ connectionString: POSTGRESQL_URL });
  
  try {
    await client.connect();
    console.log('✓ Connected to PostgreSQL');
    
    const startTime = Date.now();
    let inserted = 0;
    
    // Process in batches
    for (let i = 0; i < data.length; i += BATCH_SIZE) {
      const batch = data.slice(i, i + BATCH_SIZE);
      
      const values = batch.map(record => 
        `(${record.instrument_id}, '${record.date}', ${record.open}, ${record.high}, ${record.low}, ${record.close}, ${record.volume})`
      ).join(', ');
      
      const query = `
        INSERT INTO stock_data (instrument_id, date, open, high, low, close, volume) 
        VALUES ${values}
        ON CONFLICT (instrument_id, date) DO NOTHING
      `;
      
      await client.query(query);
      inserted += batch.length;
      
      if (i % (BATCH_SIZE * 10) === 0) {
        console.log(`PostgreSQL: Inserted ${inserted}/${data.length} records (${Math.round(inserted/data.length*100)}%)`);
      }
    }
    
    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000;
    
    console.log(`✅ PostgreSQL insertion completed in ${duration.toFixed(2)}s`);
    console.log(`   Records inserted: ${inserted}`);
    console.log(`   Rate: ${Math.round(inserted / duration)} records/sec`);
    
    return { duration, inserted, rate: Math.round(inserted / duration) };
  } catch (error) {
    console.error('PostgreSQL insertion error:', error);
    throw error;
  } finally {
    await client.end();
  }
}

async function insertToClickHouse(data: StockRecord[]) {
  const client = createClient({ url: CLICKHOUSE_URL });
  
  try {
    await client.ping();
    console.log('✓ Connected to ClickHouse');
    
    const startTime = Date.now();
    let inserted = 0;
    
    // Process in batches
    for (let i = 0; i < data.length; i += BATCH_SIZE) {
      const batch = data.slice(i, i + BATCH_SIZE);
      
      await client.insert({
        table: 'stock_data',
        values: batch,
        format: 'JSONEachRow',
      });
      
      inserted += batch.length;
      
      if (i % (BATCH_SIZE * 10) === 0) {
        console.log(`ClickHouse: Inserted ${inserted}/${data.length} records (${Math.round(inserted/data.length*100)}%)`);
      }
    }
    
    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000;
    
    console.log(`✅ ClickHouse insertion completed in ${duration.toFixed(2)}s`);
    console.log(`   Records inserted: ${inserted}`);
    console.log(`   Rate: ${Math.round(inserted / duration)} records/sec`);
    
    return { duration, inserted, rate: Math.round(inserted / duration) };
  } catch (error) {
    console.error('ClickHouse insertion error:', error);
    throw error;
  } finally {
    await client.close();
  }
}

async function getDataSize() {
  try {
    const stats = require('fs').statSync('./data/stock_data.jsonl');
    return (stats.size / 1024 / 1024).toFixed(2);
  } catch {
    return 'unknown';
  }
}

async function main() {
  console.log('Loading stock data...');
  
  let data: StockRecord[];
  try {
    const rawData = readFileSync('./data/stock_data.jsonl', 'utf-8');
    data = rawData.trim().split('\n').map(line => JSON.parse(line));
    console.log(`✓ Loaded ${data.length} records (${await getDataSize()} MB)`);
  } catch (error) {
    console.error('Failed to load data file. Run "bun run generate-data" first.');
    process.exit(1);
  }
  
  const results = {
    postgresql: null as any,
    clickhouse: null as any
  };
  
  console.log('\n--- Starting Data Insertion ---\n');
  
  try {
    console.log('🔄 Inserting data to PostgreSQL...');
    results.postgresql = await insertToPostgreSQL(data);
    
    console.log('\n🔄 Inserting data to ClickHouse...');
    results.clickhouse = await insertToClickHouse(data);
    
    console.log('\n--- Insertion Results ---');
    console.log('\nPostgreSQL:');
    console.log(`  Duration: ${results.postgresql.duration.toFixed(2)}s`);
    console.log(`  Records: ${results.postgresql.inserted.toLocaleString()}`);
    console.log(`  Rate: ${results.postgresql.rate.toLocaleString()} records/sec`);
    
    console.log('\nClickHouse:');
    console.log(`  Duration: ${results.clickhouse.duration.toFixed(2)}s`);
    console.log(`  Records: ${results.clickhouse.inserted.toLocaleString()}`);
    console.log(`  Rate: ${results.clickhouse.rate.toLocaleString()} records/sec`);
    
    const speedup = results.postgresql.duration / results.clickhouse.duration;
    console.log(`\n📊 ClickHouse is ${speedup.toFixed(2)}x faster for insertions`);
    
    // Save results
    const insertResults = {
      timestamp: new Date().toISOString(),
      data_size_mb: await getDataSize(),
      total_records: data.length,
      postgresql: results.postgresql,
      clickhouse: results.clickhouse,
      speedup_factor: speedup
    };
    
    require('fs').writeFileSync('./data/insert-results.json', JSON.stringify(insertResults, null, 2));
    console.log('\n✅ Results saved to ./data/insert-results.json');
    
  } catch (error) {
    console.error('\n❌ Data insertion failed:', error);
    process.exit(1);
  }
}

if (import.meta.main) {
  main();
}