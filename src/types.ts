interface StockRecord {
  instrument_id: number;
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export { StockRecord };
