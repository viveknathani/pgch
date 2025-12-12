import { DatabaseClients } from "./client";
import { StockRecord } from "./types";

async function seedData(totalRows: number): Promise<void> {
  console.log(
    `📊 Generating and inserting ${totalRows.toLocaleString()} stock records...`
  );

  const db = new DatabaseClients();
  await db.connect();

  const batchSize = 1000; // Insert in batches
  const instruments = 10000; // 10000 different instruments
  const recordsPerInstrument = Math.floor(totalRows / instruments);
  const startDate = new Date("2014-01-01");

  let totalInserted = 0;
  let batch: StockRecord[] = [];

  for (let instrument = 1; instrument <= instruments; instrument++) {
    const basePrice = 50 + instrument * 0.5; // Different base price per instrument

    for (let record = 0; record < recordsPerInstrument; record++) {
      // Create reasonable date progression (about 10 years of data)
      const date = new Date(startDate);
      const dayOffset = record + (instrument - 1) * 3; // Each instrument offset by 3 days, records progress daily
      date.setDate(date.getDate() + dayOffset);
      const dateStr = date.toISOString().split("T")[0];

      // Generate realistic OHLCV data
      const dayVariation = Math.sin(record * 0.01) * 5; // Smooth price movement
      const volatility = 1 + Math.random() * 0.1; // Small random volatility

      const open =
        Math.round((basePrice + dayVariation) * volatility * 100) / 100;
      const intraDay = Math.random() * 3; // Intraday movement up to $3
      const high = Math.round((open + intraDay) * 100) / 100;
      const low = Math.round((open - intraDay * 0.7) * 100) / 100;
      const close =
        Math.round((low + Math.random() * (high - low)) * 100) / 100;

      // Volume correlation with price movement
      const priceMovement = Math.abs(close - open);
      const baseVolume = 100000;
      const volumeMultiplier = 1 + priceMovement * 10; // Higher volume on bigger moves
      const volume = Math.floor(
        baseVolume * volumeMultiplier * (0.5 + Math.random())
      );

      batch.push({
        instrument_id: instrument,
        date: dateStr,
        open,
        high,
        low,
        close,
        volume,
      });

      // Insert batch when it reaches batchSize
      if (batch.length >= batchSize) {
        try {
          await db.insertBatch(batch);
          totalInserted += batch.length;
          batch = [];

          if (totalInserted % 10000 === 0) {
            console.log(
              `   Inserted ${totalInserted.toLocaleString()} records`
            );
          }
        } catch (e: any) {
          console.log(`⚠️  Batch insert failed:`, e.message.substring(0, 100));
          batch = []; // Reset batch on failure
        }
      }
    }

    if (instrument % 100 === 0) {
      console.log(
        `   Processed instrument ${instrument}/${instruments} (${totalInserted.toLocaleString()} records)`
      );
    }
  }

  // Insert any remaining records
  if (batch.length > 0) {
    try {
      await db.insertBatch(batch);
      totalInserted += batch.length;
    } catch (e: any) {
      console.log(
        `⚠️  Final batch insert failed:`,
        e.message.substring(0, 100)
      );
    }
  }

  await db.disconnect();
  console.log(
    `✅ Inserted ${totalInserted.toLocaleString()} total records to all databases`
  );
}

const TOTAL_ROWS = 25_000_000;

await seedData(TOTAL_ROWS);
