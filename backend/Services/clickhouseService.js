const {ClickHouse} = require('clickhouse');

const clickhouse = new ClickHouse({
    url: process.env.CLICKHOUSE_URL,
    port: process.env.CLICKHOUSE_PORT,
    debug: false,
    basicAuth: {
      username: process.env.CLICKHOUSE_USER,
    //   password: process.env.CLICKHOUSE_TOKEN, // JWT token //  letsadd this at last
    },
    isUseGzip: false,
    format: "csv",
    config: {
      database: process.env.CLICKHOUSE_DB,
    },
  });
  
  module.exports = clickhouse;