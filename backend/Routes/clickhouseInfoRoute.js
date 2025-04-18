import express from 'express';
import dotenv from 'dotenv';
import { createClient } from '@clickhouse/client';
import path from 'path';
import {fileURLToPath} from 'url';

// for the connection parameters
dotenv.config({
    path:path.resolve(
        path.dirname(fileURLToPath(import.meta.url)),
        '../Services/.env'
    )
});

const clickhouse = createClient({
    url:  `http://${process.env.CLICKHOUSE_URL}:${process.env.CLICKHOUSE_PORT}`,
    username: process.env.CLICKHOUSE_USER,
    password: process.env.CLICKHOUSE_TOKEN,
    database: process.env.CLICKHOUSE_DB,
  });
const router = express.Router();

//  GET THE TABLES NAMES ;
router.get('/tables',async(req,res)=>{
    try{
        const result = await clickhouse
        .query({query:'SHOW TABLES'})
        .then(r => r.json());
        res.json(result);
    }catch(err){
        res.status(500).json({error:err.message});
    }
})

//GET TABLE'S COL AND 10 ROWS OF DATA 
router.get('/tables/:table',async(req,res)=>{
    const {table} = req.params;
    const db = process.env.CLICKHOUSE_DB;
    try{
        // GET COL METADATA
        const colQuery = `
             SELECT name, type
             FROM system.columns
             WHERE database='${db}'
             AND table='${table}'
             ORDER BY position`;
        const columns = await clickhouse
        .query({query:colQuery})
        .then(r=>r.json());

        // GET FIRST 10 ROWS OF DATA 
        const dataQuery = `
            SELECT *
            FROM \`${db}\`.\`${table}\`
             LIMIT 10`;
        const sample = await clickhouse
        .query({ query: dataQuery })
        .then(r => r.json());

        // SEND THE DATA
        res.json({ columns, sample });
    }catch(err){
        console.error('Error fetching table info:', err);
        res.status(500).json({ error: err.message });
    }
});

export default router;
