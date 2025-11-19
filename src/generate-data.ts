interface StockRecord {
  instrument_id: number;
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

class StockDataGenerator {
  private tradingDays: string[] = [];
  
  constructor() {
    this.generateTradingDays();
  }
  
  private generateTradingDays() {
    const startDate = new Date('2014-01-01');
    const endDate = new Date('2023-12-31');
    
    for (let d = new Date(startDate); d <= endDate; d.setDate(d.getDate() + 1)) {
      const day = d.getDay();
      // Skip weekends (Saturday = 6, Sunday = 0)
      if (day !== 0 && day !== 6) {
        this.tradingDays.push(d.toISOString().split('T')[0]);
      }
    }
    
    console.log(`Generated ${this.tradingDays.length} trading days`);
  }
  
  private generateStockPrice(instrumentId: number, date: string, previousClose?: number): Omit<StockRecord, 'instrument_id' | 'date'> {
    // Use instrument_id and date as seeds for reproducible data
    const seed = this.hashCode(instrumentId.toString() + date);
    const rng = this.seededRandom(seed);
    
    // Base price influenced by instrument_id for variety
    const basePrice = 50 + (instrumentId % 1000) * 0.1;
    const startPrice = previousClose || basePrice;
    
    // Daily volatility between 0.5% and 5%
    const volatility = 0.005 + rng() * 0.045;
    
    // Random walk for opening price
    const openChange = (rng() - 0.5) * volatility * 2;
    const open = Math.max(0.01, startPrice * (1 + openChange));
    
    // Intraday movement
    const intradayVolatility = volatility * 0.5;
    const highChange = rng() * intradayVolatility;
    const lowChange = -rng() * intradayVolatility;
    
    const high = open * (1 + Math.abs(highChange));
    const low = open * (1 + lowChange);
    
    // Close price within the day's range
    const closeRatio = rng();
    const close = low + (high - low) * closeRatio;
    
    // Volume correlated with price movement (higher volume on bigger moves)
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
      hash = hash & hash; // Convert to 32-bit integer
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
  
  generateData(): StockRecord[] {
    const records: StockRecord[] = [];
    const instrumentIds = Array.from({ length: 5000 }, (_, i) => i + 1);
    
    console.log('Generating stock data...');
    let progress = 0;
    const total = instrumentIds.length;
    
    for (const instrumentId of instrumentIds) {
      let previousClose: number | undefined;
      
      for (const date of this.tradingDays) {
        const priceData = this.generateStockPrice(instrumentId, date, previousClose);
        
        records.push({
          instrument_id: instrumentId,
          date,
          ...priceData
        });
        
        previousClose = priceData.close;
      }
      
      progress++;
      if (progress % 500 === 0) {
        console.log(`Generated data for ${progress}/${total} instruments (${Math.round(progress/total*100)}%)`);
      }
    }
    
    console.log(`Generated ${records.length} total records`);
    return records;
  }
}

async function main() {
  console.log('Starting data generation...');
  const generator = new StockDataGenerator();
  
  // Save data to JSONL file for insertion scripts (more memory efficient)
  const fs = require('fs');
  const outputPath = './data/stock_data.jsonl';
  
  // Ensure data directory exists
  if (!fs.existsSync('./data')) {
    fs.mkdirSync('./data', { recursive: true });
  }
  
  console.log('Saving data to file...');
  const writeStream = fs.createWriteStream(outputPath);
  
  const instrumentIds = Array.from({ length: 19200 }, (_, i) => i + 1);
  let totalRecords = 0;
  let progress = 0;
  
  for (const instrumentId of instrumentIds) {
    let previousClose: number | undefined;
    
    for (const date of generator['tradingDays']) {
      const priceData = generator['generateStockPrice'](instrumentId, date, previousClose);
      
      const record = {
        instrument_id: instrumentId,
        date,
        ...priceData
      };
      
      writeStream.write(JSON.stringify(record) + '\n');
      totalRecords++;
      previousClose = priceData.close;
    }
    
    progress++;
    if (progress % 1000 === 0) {
      console.log(`Generated data for ${progress}/19200 instruments (${Math.round(progress/19200*100)}%)`);
    }
  }
  
  writeStream.end();
  
  await new Promise((resolve) => {
    writeStream.on('finish', resolve);
  });
  
  console.log(`✅ Data generation completed! Saved ${totalRecords} records to ${outputPath}`);
  console.log(`Data size: ${(fs.statSync(outputPath).size / 1024 / 1024).toFixed(2)} MB`);
}

if (import.meta.main) {
  main().catch(console.error);
}