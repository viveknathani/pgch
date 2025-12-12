import { Client } from "pg";
import { createClient } from "@clickhouse/client";
import { StockRecord } from "./types";

// Database connection configs
const POSTGRESQL_URL =
  "postgresql://postgres:postgres@localhost:5432/stockdata";
const CLICKHOUSE_HOST = "localhost:8123";
const TIMESCALEDB_URL =
  "postgresql://postgres:postgres@localhost:5433/stockdata";

class DatabaseClients {
  public pgClient: Client;
  public chClient: any;
  public timescaleClient: Client;

  constructor() {
    this.pgClient = new Client({ connectionString: POSTGRESQL_URL });
    this.chClient = createClient({ host: `http://${CLICKHOUSE_HOST}` });
    this.timescaleClient = new Client({ connectionString: TIMESCALEDB_URL });
  }

  async connect() {
    await this.pgClient.connect();
    await this.timescaleClient.connect();
    console.log("✅ Connected to all databases");
  }

  async disconnect() {
    await this.pgClient.end();
    await this.chClient.close();
    await this.timescaleClient.end();
  }

  async insertBatch(records: StockRecord[]) {
    const pgValues = records
      .map(
        (r) =>
          `(${r.instrument_id}, '${r.date}', ${r.open}, ${r.high}, ${r.low}, ${r.close}, ${r.volume})`
      )
      .join(",");

    const tsValues = records
      .map(
        (r) =>
          `(${r.instrument_id}, '${r.date}', ${r.open}, ${r.high}, ${r.low}, ${r.close}, ${r.volume}, ${r.close}, NOW())`
      )
      .join(",");

    // Insert to PostgreSQL
    await this.pgClient.query(`
      INSERT INTO stock_data (instrument_id, date, open, high, low, close, volume) 
      VALUES ${pgValues}
    `);

    // Insert to ClickHouse using proper syntax
    try {
      const insertData = records.map((r) => [
        r.instrument_id,
        r.date,
        r.open,
        r.high,
        r.low,
        r.close,
        r.volume,
      ]);

      await this.chClient.insert({
        table: "stock_data",
        values: insertData,
      });
    } catch (e) {
      console.log(`⚠️  ClickHouse batch insert failed`, e);
    }

    // Insert to TimescaleDB
    await this.timescaleClient.query(`
      INSERT INTO stock_data (instrument_id, date, open, high, low, close, volume, adjusted_close, created_at) 
      VALUES ${tsValues}
    `);

  }
}

export { DatabaseClients };
