import express from 'express';
import { createClient } from '@clickhouse/client';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import { getTableColumns } from '../Services/ingest-csv.js';

dotenv.config({
  path: path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    '../Services/.env'
  )
});

const router = express.Router();

/**
 * Build a ClickHouse client, using:
 *  - the token from req.query if provided,
 *  - otherwise the CLICKHOUSE_TOKEN from .env
 */
function makeClient({ host, port, database, user, token }) {
  const pwd = token != null && token.trim() !== ''
    ? token.trim()
    : (process.env.CLICKHOUSE_TOKEN || '').trim();

  return createClient({
    url:      `http://${host.trim()}:${port.trim()}`,
    username: user.trim(),
    password: pwd,
    database: database.trim()
  });
}

// ─── List Tables ───────────────────────────────────────────────────────────────
router.get('/tables', async (req, res) => {
  const { host, port, database, user, token } = req.query;
  if (![host, port, database, user].every(x => x && x.trim())) {
    return res.status(400).json({
      error: 'host, port, database & user are required to list tables'
    });
  }

  const ch = makeClient({ host, port, database, user, token });
  try {
    const result = await ch.query({ query: 'SHOW TABLES', format: 'JSON' });
    const { data } = await result.json();
    res.json({ tables: data.map(r => r.name) });
  } catch (err) {
    console.error('List tables error:', err);
    const msg = err.type === 'REQUIRED_PASSWORD'
      ? 'Authentication failed: password required or incorrect.'
      : err.message;
    res.status(err.type === 'REQUIRED_PASSWORD' ? 401 : 500).json({ error: msg });
  }
});

// ─── Table Info ────────────────────────────────────────────────────────────────
// GET /api/source-csv/info/:table?host=…&port=…&database=…&user=…&token=…

router.get('/info/:table', async (req, res) => {
  const { table } = req.params;
  const { host, port, database, user, token } = req.query;
  if (![host, port, database, user].every(x => x && x.trim())) {
    return res.status(400).json({
      error: 'host, port, database & user are required to fetch table info'
    });
  }
  const ch = makeClient({ host, port, database, user, token });
  try {
    const cols = await getTableColumns.call({ clickhouse: ch }, table);
    const colCount = cols.length;
    const countRes = await ch.query({ query: `SELECT count() AS cnt FROM \`${table}\``, format: 'JSON' });
    const { data } = await countRes.json();
    const rowCount = data[0].cnt;
    res.json({ colCount, rowCount });
  } catch (err) {
    console.error('Table info error:', err);
    res.status(500).json({ error: err.message });
  }
});


// ─── Preview CSV Export ────────────────────────────────────────────────────────
router.get('/export/:table/preview', async (req, res) => {
  const { host, port, database, user, token, rows, cols } = req.query;
  const { table } = req.params;
  if (![host, port, database, user].every(x => x && x.trim())) {
    return res.status(400).json({
      error: 'host, port, database & user are required to preview table'
    });
  }

  const ch = makeClient({ host, port, database, user, token });
  try {
    const allCols = await getTableColumns.call({ clickhouse: ch }, table);
    const selCols = cols ? allCols.slice(0, +cols) : allCols;
    const limit   = rows ? +rows : 10;

    const q = `
      SELECT ${selCols.map(c => `\`${c}\``).join(',')}
      FROM \`${table}\`
      LIMIT ${limit}
    `;
    const result = await ch.query({ query: q, format: 'JSON' });
    const { data } = await result.json();
    res.json({ columns: selCols, rows: data });
  } catch (err) {
    console.error('Export preview error:', err);
    const msg = err.type === 'REQUIRED_PASSWORD'
      ? 'Authentication failed: password required or incorrect.'
      : err.message;
    res.status(err.type === 'REQUIRED_PASSWORD' ? 401 : 500).json({ error: msg });
  }
});

// ─── Download CSV ───────────────────────────────────────────────────────────────
router.get('/export/:table', async (req, res) => {
  const { table } = req.params;
  const { host, port, database, user, token, rows, cols } = req.query;

  // Validate required connection params (token may be empty)
  if (![host, port, database, user].every(x => typeof x === 'string' && x.trim())) {
    return res.status(400).json({
      error: 'host, port, database & user are required to export CSV'
    });
  }

  // Build ClickHouse client (falls back to .env CLICKHOUSE_TOKEN if token blank)
  const ch = makeClient({ host, port, database, user, token });

  try {
    // Determine which columns to select
    const allCols = await getTableColumns.call({ clickhouse: ch }, table);
    const selCols = cols ? allCols.slice(0, +cols) : allCols;
    const colSql  = selCols.map(c => `\`${c.replace(/`/g, '``')}\``).join(',');
    const limitSql= rows ? ` LIMIT ${+rows}` : '';

    // Build SQL without a FORMAT clause
    const sql = `SELECT ${colSql} FROM \`${table}\`${limitSql}`;

    // Execute, asking the client to format as CSVWithNames
    const result = await ch.query({ query: sql, format: 'CSVWithNames' });
    const csv    = await result.text();

    // Stream back as a CSV attachment
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="${table}.csv"`
    );
    res.send(csv);

  } catch (err) {
    console.error('CSV Export Error:', err);

    // Handle ClickHouse REQUIRED_PASSWORD specially
    if (err.type === 'REQUIRED_PASSWORD') {
      return res.status(401).json({
        error: 'Authentication failed: password required or incorrect.'
      });
    }

    // Fallback for other errors
    res.status(500).json({ error: err.message });
  }
});

export default router;
