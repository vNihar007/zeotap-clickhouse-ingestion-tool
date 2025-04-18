// Moudules required 
import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
import split2 from 'split2';
import through2 from 'through2';  
import { Transform } from 'stream'; 
import { Readable, PassThrough } from 'stream'; 
import readline from 'readline';  
import { fileURLToPath } from 'url';
import {createClient}  from '@clickhouse/client';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);
// Load .env sitting next to this file
dotenv.config({ path: path.resolve(__dirname, './.env') });


// Initalize the clickhouse client
const clickhouse = createClient({
  url: `http://${process.env.CLICKHOUSE_URL}:${process.env.CLICKHOUSE_PORT}`,
  username: process.env.CLICKHOUSE_USER,
  password: process.env.CLICKHOUSE_TOKEN,
  database: process.env.CLICKHOUSE_DB,
});
/**
 * Ingests a CSV file into the specified ClickHouse table.
 *
 * @param {string} filePath  – path to the .csv file
 * @param {string} tableName – target table in ClickHouse
 */


/**
+ * Safely parse a single CSV line into fields, honoring quotes.
+ */

function parseCsvLine(line) {
  const fields = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"' ) {
      if (inQuotes && line[i+1] === '"') {
        cur += '"'; // double‐quote escape
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      fields.push(cur);
      cur = '';
    } else {
      cur += ch;
    }
  }
  fields.push(cur);
  return fields.map(f => f.trim());
}
// Read the header (first line) of a CSV to get column names.
async function readCsvHeader(filePath) {
    return new Promise((resolve, reject) => {
      const rs = fs.createReadStream(filePath);
      let buf = '';
      rs.on('data', chunk => {
        buf += chunk.toString();
        const idx = buf.indexOf('\n');
        if (idx !== -1) {
          rs.destroy();
          const header = buf.slice(0, idx).replace(/\r$/, '');
          resolve(parseCsvLine(header));
        }
      });
      rs.on('error', reject);
      rs.on('end', () => {
        // file too short
        resolve(buf ? parseCsvLine(buf) : []);
      });
    });
   }
  
// Create a table whose schema matches the CSV header (all String columns).
async function createTableIfNotExists(tableName, columns) {
    const colsDef = columns
      .map(name => `\`${name}\` String`)
      .map(name => {
      // escape any backticks in the name
      const safe = name.replace(/`/g, '``');
      return `\`${safe}\` String`;
    })
      .join(', ');
    const createQuery = `
      CREATE TABLE IF NOT EXISTS \`${tableName}\` (
        ${colsDef}
      ) ENGINE = MergeTree()
      ORDER BY tuple()
    `;
    console.log('▶️  Running DDL:\n', createQuery);
    await clickhouse.query({ query: createQuery });
    console.log('✅ Table ready or already existed.');
}

// ingesting csv 
/**
 * Ingests a slice of CSV into ClickHouse.
 * @returns number of rows written
 */
async function ingestCsv(
  filePath,
  tableName,
  startRow = 1,      // 1-based data-row index
  rowLimit = null,   // how many rows to ingest
  colLimit = null    // how many columns to ingest
) {
  // 1) figure out our CSV header line
  const allCols = await readCsvHeader(filePath);
  const selCols = colLimit ? allCols.slice(0, colLimit) : allCols;
  const headerLine = selCols.map(col => `\`${col}\``).map(c => `\`${c.replace(/`/g,'``')}\``).join(',') + '\n';

  let lineCount = 0;
  const dataStream = fs.createReadStream(filePath)
    .pipe(split2())
    .pipe(through2(function(chunk, _, cb) {
      const line = chunk.toString();
      lineCount++;

      // skip original header
      if (lineCount === 1) return cb();

      // dataRowIndex = lineCount-1
      const dataIdx = lineCount - 1;
      // drop rows before startRow
      if (dataIdx < startRow) return cb();
      // stop after startRow + rowLimit - 1
      if (rowLimit && dataIdx >= startRow + rowLimit) return cb();

      // slice columns
      const parts = parseCsvLine(line);
      const sel   = colLimit ? parts.slice(0, colLimit) : parts;
      cb(null, sel.join(',') + '\n');
    }));

  // 2) combine our header + data rows
  const pass = new PassThrough();
  pass.write(headerLine);
  dataStream.pipe(pass);

  // 3) send to ClickHouse
  await clickhouse.insert({
    table:  tableName,
    format: 'CSVWithNames',
    values: pass
  });

  // how many actual rows written?
  const totalDataRows = lineCount - 1;
  const written = rowLimit != null
    ? Math.max(0, Math.min(rowLimit, totalDataRows - (startRow - 1)))
    : Math.max(0, totalDataRows - (startRow - 1));

  return written;
}


// List all tables in the current database
async function listTables() {
  const result = await clickhouse.query({
    query: 'SHOW TABLES',
    format: 'JSON'
  });
  const { data } = await result.json();
  // data: [{ name: 'table1' }, …]
  return data.map(row => row.name);
}

// Return the schema (column names) of a single table
async function getTableColumns(tableName) {
  const result = await clickhouse.query({
    query: `DESCRIBE TABLE \`${tableName}\``,
    format: 'JSON'
  });
  const { data } = await result.json();
  // data: [{ name: 'col1', type: 'String', … }, …]
  return data.map(row => row.name);
}

// Preview N rows and up to M columns from a table
async function previewTable(tableName, limit = 10, colCount = null) {
  // 1. fetch all column names
  const allCols = await getTableColumns(tableName);
  // 2. pick first colCount or all
  const selected = colCount ? allCols.slice(0, colCount) : allCols;
  const colsSql = selected.map(c => `\`${c}\``).join(', ');
  // 3. run SELECT
  const query = `SELECT ${colsSql} FROM \`${tableName}\` LIMIT ${limit}`;
  const result = await clickhouse.query({ query, format: 'JSON' });
  const { data } = await result.json();
  return { columns: selected, rows: data };
};

async function exportCsv(tableName, rowLimit = null, colLimit = null) {
  // 1. get schema
  const allCols = await getTableColumns(tableName);
  const selCols = colLimit ? allCols.slice(0, colLimit) : allCols;
  const colsSql = selCols
    .map(c => `\`${c.replace(/`/g,'``')}\``)
    .join(', ');
  // 2. build SQL
  const limitSql = rowLimit ? `LIMIT ${rowLimit}` : '';
  const sql = `SELECT ${colsSql} FROM \`${tableName}\` ${limitSql} FORMAT CSVWithNames`;
  // 3. execute & return text
  const result = await clickhouse.query({ query: sql, format: 'CSVWithNames' });
  const csv    = await result.text();
  return { csv, filename: `${tableName}.csv` };
}

export {
  readCsvHeader,
  createTableIfNotExists,
  ingestCsv,
  listTables,
  getTableColumns,
  previewTable,
  exportCsv
};
