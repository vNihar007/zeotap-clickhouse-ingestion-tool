import express from 'express';
import fs from 'fs';
const router = express.Router();
import { upload } from '../Middlewear/required.js';
import { createClient } from '@clickhouse/client';
import {
  readCsvHeader,
  createTableIfNotExists,
  ingestCsv,
  listTables,
  getTableColumns,
  previewTable,
  exportCsv
} from '../Services/ingest-csv.js';

/*
  POST /api/source-csv/ingest
  - file + tableName ⇒ ingest, return { success, table, columns }
*/
router.post('/ingest', upload.single('file'), async (req, res) => {
  try {
    const { tableName, startRow, rows, cols } = req.body;
    if (!req.file || !tableName) {
      return res.status(400).json({ error: 'file and tableName are required' });
    }

    // parse slice parameters
    const sr = startRow ? parseInt(startRow, 10) : 1;
    const rl = rows     ? parseInt(rows, 10)     : null;
    const cl = cols     ? parseInt(cols, 10)     : null;

    const csvPath = req.file.path;

    // 1️⃣ read full header to know which columns to create
    const fullHeader = await readCsvHeader(csvPath);
    const schemaCols = cl ? fullHeader.slice(0, cl) : fullHeader;

    // 2️⃣ ensure the table exists with exactly those columns
    await createTableIfNotExists(tableName, schemaCols);

    // 3️⃣ ingest only the slice
    const ingestedRows = await ingestCsv(csvPath, tableName, sr, rl, cl);

    // 4️⃣ cleanup
    fs.unlinkSync(csvPath);

    return res.json({
      success: true,
      table: tableName,
      ingestedRows,
      ingestedCols: schemaCols.length
    });
  } catch (err) {
    console.error('Ingestion error:', err);
    return res.status(500).json({ error: err.message });
  }
});


/*
  GET /api/source-csv/tables
  ⇒ [ 'table1', 'table2', ... ]
*/

// helper to build a ClickHouse client from query params
function makeClient({ host, port, database, user, token }) {
  return createClient({
    url:      `http://${host}:${port}`,
    username: user,
    password: token,
    database
  });
}

router.get('/tables', async (req, res) => {
  try {
    const { host, port, database, user, token } = req.query;
    if (!host || !port || !database || !user || !token) {
      return res.status(400).json({ 
        error: 'host, port, database, user, token are required to list tables' 
      });
    }

    const ch = makeClient({ host, port, database, user, token });
    const result = await ch.query({ query: 'SHOW TABLES', format: 'JSON' });
    const { data } = await result.json();            // data: [{ name: '...' }, …]
    const tables = data.map(r => r.name);
    res.json({ tables });

  } catch (err) {
    console.error('List tables error:', err);
    res.status(500).json({ error: err.message });
  }
});

/*
  GET /api/source-csv/tables/:table/columns
  ⇒ [ 'col1', 'col2', ... ]
*/
router.get('/tables/:table/columns', async (req, res) => {
  try {
    const columns = await getTableColumns(req.params.table);
    res.json({ columns });
  } catch (err) {
    console.error('Get columns error:', err);
    res.status(500).json({ error: err.message });
  }
});

/*
  GET /api/source-csv/tables/:table/preview?rows=10&cols=5
  ⇒ { columns: [...], rows: [ {col1: val,...}, ... ] }
*/
router.get('/tables/:table/preview', async (req, res) => {
  try {
    const rows = parseInt(req.query.rows) || 10;
    const cols = parseInt(req.query.cols) || null;
    const data = await previewTable(req.params.table, rows, cols);
    res.json(data);
  } catch (err) {
    console.error('Preview error:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
