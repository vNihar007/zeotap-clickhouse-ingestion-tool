require('dotenv').config();
const clickhouse = require('../Services/clickhouseService')

// testing the connection : 
async function testClickhouse(){
    try{
        // Running an example query
        const result = await clickhouse
        .query('SELECT currentUser(),currentDatabase()').toPromise();
        console.log("clickHouse connection is ok ✅");
        console.table(result)
    }catch(error){
        console.log('clickHouse Connection Failed ❌');
        console.log(error);
    }
}

testClickhouse();