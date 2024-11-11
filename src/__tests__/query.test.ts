import { beforeEach, describe, it } from "node:test";

export const classPath = process.env['CLASSPATH']?.split(',');
import Pool, { IConnection } from '../pool.js'
import JDBC from '../jdbc.js'; // Use default import
import Jinst from '../jinst.js'; // Use default import


describe('testing queries', ()=>{
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
        try {

            const connection = await createConnection()

        
            const preparedStatement = await connection.prepareStatement(query); // Replace `your_table` with your actual table name

            const resultSet = await preparedStatement.executeQuery();
            console.log({resultSet})
            const results = await resultSet.toObjArray(); // Assuming you have a method to convert ResultSet to an array

            
            
            // Remember to release the connection after you are done
            await pool.release(connection);
            return results
        } catch (error) {
            console.error('Error executing query:', error);
        }
    }
    const pool = new Pool({
        url: 'jdbc:hive2://delta-db:10000',   // Replace with your JDBC URL
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
        if (!Jinst.getInstance().isJvmCreated()) {
            Jinst.getInstance().addOption('-Xrs');
                Jinst.getInstance().setupClasspath(classPath || []); // Path to your JDBC driver JAR file
        }

        await initializePool()
    })
    it('should create table, insert and select',async()=>{
       const createTable = await runQuery(`CREATE TABLE IF NOT EXISTS foo (id STRING, name STRING  ) USING DELTA LOCATION '/data/pv/foo';`)
       const insertTable = await runQuery(`INSERT INTO foo (id , name) VALUES (1, 'foo') `)
       const selectTalbe = await runQuery('SELECT * FROM delta.`/data/pv/foo`')

       console.log({selectTalbe})
    })
})