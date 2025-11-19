import { Client } from 'pg';
import { createClient } from '@clickhouse/client';

const POSTGRESQL_URL = process.env.POSTGRESQL_URL || 'postgresql://postgres:postgres@localhost:5432/stockdata';
const CLICKHOUSE_CONFIG = {
  host: 'localhost',
  port: 9000,
  username: 'default',
  password: ''
};

async function setupPostgreSQL() {
  const client = new Client({ connectionString: POSTGRESQL_URL });
  
  try {
    await client.connect();
    console.log('✓ Connected to PostgreSQL');
    
    // Drop table if exists
    await client.query('DROP TABLE IF EXISTS stock_data');
    
    // Create table with proper indexing
    await client.query(`
      CREATE TABLE stock_data (
        instrument_id INTEGER NOT NULL,
        date DATE NOT NULL,
        open DECIMAL(10,4) NOT NULL,
        high DECIMAL(10,4) NOT NULL,
        low DECIMAL(10,4) NOT NULL,
        close DECIMAL(10,4) NOT NULL,
        volume DECIMAL(15,2) NOT NULL,
        PRIMARY KEY (instrument_id, date)
      )
    `);
    
    // Create indexes
    await client.query('CREATE INDEX idx_stock_data_date ON stock_data (date)');
    await client.query('CREATE INDEX idx_stock_data_instrument_id ON stock_data (instrument_id)');
    
    console.log('✓ PostgreSQL table and indexes created');
  } catch (error) {
    console.error('PostgreSQL setup error:', error);
    throw error;
  } finally {
    await client.end();
  }
}

async function setupClickHouse() {
  const client = createClient(CLICKHOUSE_CONFIG);
  
  try {
    // Test connection
    await client.ping();
    console.log('✓ Connected to ClickHouse');
    
    // Drop table if exists
    await client.command({ query: 'DROP TABLE IF EXISTS stock_data' });
    
    // Create table with MergeTree engine and proper ordering
    await client.command({
      query: `
        CREATE TABLE stock_data (
          instrument_id UInt32,
          date Date,
          open Decimal64(4),
          high Decimal64(4),
          low Decimal64(4),
          close Decimal64(4),
          volume Decimal64(2)
        ) ENGINE = MergeTree()
        ORDER BY (instrument_id, date)
        SETTINGS index_granularity = 8192
      `
    });
    
    // Create secondary indexes for better query performance
    await client.command({
      query: 'ALTER TABLE stock_data ADD INDEX idx_date date TYPE minmax GRANULARITY 1'
    });
    
    await client.command({
      query: 'ALTER TABLE stock_data ADD INDEX idx_instrument instrument_id TYPE set(1000) GRANULARITY 1'
    });
    
    console.log('✓ ClickHouse table created');
  } catch (error) {
    console.error('ClickHouse setup error:', error);
    throw error;
  } finally {
    await client.close();
  }
}

async function main() {
  console.log('Setting up databases...\n');
  
  try {
    await setupPostgreSQL();
    await setupClickHouse();
    console.log('\n✅ Database setup completed successfully!');
  } catch (error) {
    console.error('\n❌ Database setup failed:', error);
    process.exit(1);
  }
}

if (import.meta.main) {
  main();
}