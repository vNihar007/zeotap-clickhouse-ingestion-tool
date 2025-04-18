// backend/app.js

import dotenv from 'dotenv';
dotenv.config();
import express from 'express';
import cors from 'cors';

// Router Imports
// import infoRouter from './Routes/clickhouseInfoRoute.js';
import ingestRouter from './Routes/ingest-csv-route.js';
import exportRouter from './Routes/clickhouseExportRoute.js';

// Server Port
const PORT = process.env.PORT || 3000;
const app  = express();

// Middleware
app.use(cors({
  origin: 'http://127.0.0.1:5500',  // or 'http://localhost:5500'
  credentials: true
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Default route
app.get('/', (req, res) => {
  res.send('Bi‑Directional ClickHouse ⇆ Flatfile');
});

// Mount routers
// app.use('/api/info',infoRouter);
app.use('/api/source-csv', ingestRouter);
app.use('/api/source-csv', exportRouter);


// Start server
console.log('Starting server…');
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

// Export for testing or further composition
export default app;
