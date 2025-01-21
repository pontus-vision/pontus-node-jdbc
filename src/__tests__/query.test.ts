import { after, beforeEach, describe, it,  } from "node:test";
import assert from 'node:assert/strict';

export const classPath = process.env['CLASSPATH']?.split(',');
import Pool, { IConnection } from '../pool'
import JDBC from '../jdbc'; // Use default import
import Jinst from '../jinst'; // Use default import
import ResultSet from "../resultset";
import { exit, exitCode } from "node:process";


describe('testing queries', ()=>{
    if (!Jinst.getInstance().isJvmCreated()) {
            Jinst.getInstance().addOption('-Xrs');
            Jinst.getInstance().setupClasspath(classPath || []); // Path to your JDBC driver JAR file
    }

    const config= {
        url: process.env['P_DELTA_TABLE_HIVE_SERVER'] || 'jdbc:hive2://localhost:10000', // Update the connection URL according to your setup
        drivername: 'org.apache.hive.jdbc.HiveDriver', // Driver class name
        properties: {
            user: 'NBuser',
            password: '',
        },
    };
    const jdbc = new JDBC(config);
    const createConnection = async():Promise<IConnection> => {
        const reservedConn = await jdbc.reserve()
        return reservedConn.conn
    };

    async function runQuery(query:string) {
        console.log(query)
        try {
            const connection = await createConnection()

        
            const preparedStatement = await connection.prepareStatement(query); // Replace `your_table` with your actual table name

            const resultSet = await preparedStatement.executeQuery();
            const results = await resultSet.toObjArray(); // Assuming you have a method to convert ResultSet to an array
            
            // Remember to release the connection after you are done
            // await pool.release(connection);
            await connection.close()
        
            return results
        } catch (error) {
            console.error('Error executing query:', error);
        }
    }
    const pool = new Pool({
        url: 'jdbc:hive2://node-pontus-jdbc-db-setup:10000',   // Replace with your JDBC URL
        properties: {
        user: 'admin',           // Database username
        password: 'user'        // Database password
        },
        drivername: 'org.apache.hive.jdbc.HiveDriver', // Driver class name
        minpoolsize: 2,
        maxpoolsize: 10,
        keepalive: {
        interval: 60000,
        query: 'SELECT 1',
        enabled: true
        },
        logging: {
        level: 'info'
        }
    });
    // Initialize pool
    async function initializePool() {
        try {
        await pool.initialize();
        console.log('Pool initialized successfully.');
        } catch (error) {
        console.error('Error initializing the pool:', error);
        }
    }

    beforeEach(async()=>{
    //    await initializePool()

       const createTable = await runQuery(`DELETE FROM delta.\`/data/pv/foobar\` `)
    })

    after(async()=>{
        exit()
    })
    it('should create table, insert and select',async()=>{
       const createTable = await runQuery(`CREATE TABLE IF NOT EXISTS foobar (id STRING, name STRING  ) USING DELTA LOCATION '/data/pv/foobar';`)
       const insertTable = await runQuery(`INSERT INTO delta.\`/data/pv/foobar\` (id , name) VALUES (1, 'foo')`)
       const selectTable = await runQuery('SELECT * FROM delta.`/data/pv/foobar`')

       console.log({selectTable: selectTable.length})

       assert.equal(selectTable.length, 1)
    })
})