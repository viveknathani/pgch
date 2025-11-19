import { Client } from 'pg';
import { createClient } from '@clickhouse/client';

const POSTGRESQL_URL = process.env.POSTGRESQL_URL || 'postgresql://postgres:postgres@localhost:5432/stockdata';
const CLICKHOUSE_CONFIG = {
  host: 'localhost',
  port: 9000,
  username: 'default',
  password: ''
};
const BATCH_SIZE = 10000;
const TOTAL_INSTRUMENTS = 19200; // For ~50M rows
const INSTRUMENTS_PER_CHUNK = 200; // Process 200 instruments at a time

interface StockRecord {
  instrument_id: number;
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

class StreamingDataGenerator {
  private tradingDays: string[] = [];
  
  constructor() {
    this.generateTradingDays();
  }
  
  private generateTradingDays() {
    const startDate = new Date('2014-01-01');
    const endDate = new Date('2023-12-31');
    
    for (let d = new Date(startDate); d <= endDate; d.setDate(d.getDate() + 1)) {
      const day = d.getDay();
      if (day !== 0 && day !== 6) {
        this.tradingDays.push(d.toISOString().split('T')[0]);
      }
    }
    
    console.log(`Generated ${this.tradingDays.length} trading days`);
  }
  
  private generateStockPrice(instrumentId: number, date: string, previousClose?: number): Omit<StockRecord, 'instrument_id' | 'date'> {
    const seed = this.hashCode(instrumentId.toString() + date);
    const rng = this.seededRandom(seed);
    
    const basePrice = 50 + (instrumentId % 1000) * 0.1;
    const startPrice = previousClose || basePrice;
    
    const volatility = 0.005 + rng() * 0.045;
    const openChange = (rng() - 0.5) * volatility * 2;
    const open = Math.max(0.01, startPrice * (1 + openChange));
    
    const intradayVolatility = volatility * 0.5;
    const highChange = rng() * intradayVolatility;
    const lowChange = -rng() * intradayVolatility;
    
    const high = open * (1 + Math.abs(highChange));
    const low = open * (1 + lowChange);
    
    const closeRatio = rng();
    const close = low + (high - low) * closeRatio;
    
    const priceMovement = Math.abs((close - open) / open);
    const baseVolume = 100000 + (instrumentId % 10000) * 10;
    const volumeMultiplier = 1 + priceMovement * 5;
    const volume = Math.round(baseVolume * volumeMultiplier * (0.5 + rng()));
    
    return {
      open: Math.round(open * 10000) / 10000,
      high: Math.round(high * 10000) / 10000,
      low: Math.round(low * 10000) / 10000,
      close: Math.round(close * 10000) / 10000,
      volume: Math.round(volume * 100) / 100
    };
  }
  
  private hashCode(str: string): number {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash;
    }
    return Math.abs(hash);
  }
  
  private seededRandom(seed: number): () => number {
    let x = seed;
    return () => {
      x = (x * 9301 + 49297) % 233280;
      return x / 233280;
    };
  }
  
  // Generate data for a specific range of instruments
  *generateInstrumentChunk(startInstrumentId: number, endInstrumentId: number): Generator<StockRecord[]> {
    for (let instrumentId = startInstrumentId; instrumentId <= endInstrumentId; instrumentId++) {
      let previousClose: number | undefined;
      let batch: StockRecord[] = [];
      
      for (const date of this.tradingDays) {
        const priceData = this.generateStockPrice(instrumentId, date, previousClose);
        
        batch.push({
          instrument_id: instrumentId,
          date,
          ...priceData
        });
        
        previousClose = priceData.close;
        
        // Yield batch when it reaches BATCH_SIZE
        if (batch.length >= BATCH_SIZE) {
          yield [...batch];
          batch = [];
        }
      }
      
      // Yield remaining data for this instrument
      if (batch.length > 0) {
        yield [...batch];
      }
    }
  }
}

class StreamingInserter {
  private pgClient: Client;
  private chClient: any;
  private pgInsertCount = 0;
  private chInsertCount = 0;
  private startTime = Date.now();
  
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
  
  async insertBatchPostgreSQL(batch: StockRecord[]) {
    const values = batch.map(record => 
      `(${record.instrument_id}, '${record.date}', ${record.open}, ${record.high}, ${record.low}, ${record.close}, ${record.volume})`
    ).join(', ');
    
    const query = `
      INSERT INTO stock_data (instrument_id, date, open, high, low, close, volume) 
      VALUES ${values}
      ON CONFLICT (instrument_id, date) DO NOTHING
    `;
    
    await this.pgClient.query(query);
    this.pgInsertCount += batch.length;
    
    if (this.pgInsertCount % 100000 === 0) {
      const elapsed = (Date.now() - this.startTime) / 1000;
      const rate = Math.round(this.pgInsertCount / elapsed);
      console.log(`PostgreSQL: ${this.pgInsertCount.toLocaleString()} records in ${elapsed.toFixed(1)}s (${rate.toLocaleString()}/sec)`);
    }
  }
  
  async insertBatchClickHouse(batch: StockRecord[]) {
    await this.chClient.insert({
      table: 'stock_data',
      values: batch,
      format: 'JSONEachRow',
    });
    
    this.chInsertCount += batch.length;
    
    if (this.chInsertCount % 100000 === 0) {
      const elapsed = (Date.now() - this.startTime) / 1000;
      const rate = Math.round(this.chInsertCount / elapsed);
      console.log(`ClickHouse: ${this.chInsertCount.toLocaleString()} records in ${elapsed.toFixed(1)}s (${rate.toLocaleString()}/sec)`);
    }
  }
  
  getResults() {
    const elapsed = (Date.now() - this.startTime) / 1000;
    return {
      postgresql: {
        duration: elapsed,
        inserted: this.pgInsertCount,
        rate: Math.round(this.pgInsertCount / elapsed)
      },
      clickhouse: {
        duration: elapsed,
        inserted: this.chInsertCount,
        rate: Math.round(this.chInsertCount / elapsed)
      }
    };
  }
}

async function main() {
  const generator = new StreamingDataGenerator();
  const inserter = new StreamingInserter();
  
  await inserter.connect();
  
  console.log(`🚀 Starting streaming insertion of ~${(TOTAL_INSTRUMENTS * 2608).toLocaleString()} records...`);
  console.log(`Processing ${INSTRUMENTS_PER_CHUNK} instruments at a time, ${BATCH_SIZE} records per batch\\n`);
  
  try {
    let processedInstruments = 0;
    
    // Process instruments in chunks to avoid memory issues
    for (let startId = 1; startId <= TOTAL_INSTRUMENTS; startId += INSTRUMENTS_PER_CHUNK) {
      const endId = Math.min(startId + INSTRUMENTS_PER_CHUNK - 1, TOTAL_INSTRUMENTS);
      
      console.log(`Processing instruments ${startId}-${endId}...`);
      
      // Generate and insert data for this chunk
      for (const batch of generator.generateInstrumentChunk(startId, endId)) {
        // Insert to both databases in parallel
        await Promise.all([
          inserter.insertBatchPostgreSQL(batch),
          inserter.insertBatchClickHouse(batch)
        ]);
      }
      
      processedInstruments = endId;
      const progress = (processedInstruments / TOTAL_INSTRUMENTS * 100).toFixed(1);
      console.log(`Completed ${processedInstruments}/${TOTAL_INSTRUMENTS} instruments (${progress}%)\\n`);
    }
    
    const results = inserter.getResults();
    
    console.log('\\n--- Final Results ---');
    console.log('PostgreSQL:');
    console.log(`  Duration: ${results.postgresql.duration.toFixed(2)}s`);
    console.log(`  Records: ${results.postgresql.inserted.toLocaleString()}`);
    console.log(`  Rate: ${results.postgresql.rate.toLocaleString()} records/sec`);
    
    console.log('\\nClickHouse:');
    console.log(`  Duration: ${results.clickhouse.duration.toFixed(2)}s`);
    console.log(`  Records: ${results.clickhouse.inserted.toLocaleString()}`);
    console.log(`  Rate: ${results.clickhouse.rate.toLocaleString()} records/sec`);
    
    if (results.postgresql.duration > 0 && results.clickhouse.duration > 0) {
      const speedup = results.clickhouse.rate / results.postgresql.rate;
      console.log(`\\n📊 ClickHouse is ${speedup.toFixed(2)}x faster for insertions`);
    }
    
    // Save results
    const streamResults = {
      timestamp: new Date().toISOString(),
      total_records: results.postgresql.inserted,
      approach: 'streaming_insertion',
      postgresql: results.postgresql,
      clickhouse: results.clickhouse,
      speedup_factor: results.clickhouse.rate / results.postgresql.rate
    };
    
    require('fs').writeFileSync('./data/stream-insert-50m-results.json', JSON.stringify(streamResults, null, 2));
    console.log('\\n✅ Results saved to ./data/stream-insert-50m-results.json');
    
  } catch (error) {
    console.error('\\n❌ Streaming insertion failed:', error);
    process.exit(1);
  } finally {
    await inserter.disconnect();
  }
}

if (import.meta.main) {
  main();
}