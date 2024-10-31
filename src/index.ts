// export { default as Connection } from './connection';
// export { default as DriverManager } from './drivermanager';
// export { default as Jinst } from './jinst';
// export { default as Pool } from './pool';
// export { default as PreparedStatement } from './preparedstatement';
// export { default as ResultSet } from './resultset';
// export { default as ResultSetMetaData } from './resultsetmetadata';
// export { default as SQLWarning } from './sqlwarning';
// export { default as Statement } from './statement';

export const classPath = process.env['CLASSPATH']?.split(',');
import Connection from './connection.js';
import JDBC from './jdbc.js'; // Use default import
import Jinst from './jinst.js'; // Use default import

if (!Jinst.isJvmCreated()) {
	  Jinst.addOption('-Xrs');
	    Jinst.setupClasspath(classPath || []); // Path to your JDBC driver JAR file
}

import Pool from './pool.js'

export const config= {
  url: process.env['P_DELTA_TABLE_HIVE_SERVER'] || 'jdbc:hive2://localhost:10000', // Update the connection URL according to your setup
  drivername: 'org.apache.hive.jdbc.HiveDriver', // Driver class name
  properties: {
    user: 'NBuser',
    password: '',
  },
};
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
  const jdbc = new JDBC(config);
  // Initialize pool
  async function initializePool() {
    try {
      await pool.initialize();
      console.log('Pool initialized successfully.');
    } catch (error) {
      console.error('Error initializing the pool:', error);
    }
  }

  export const createConnection = async():Promise<Connection> => {
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

        console.log('Query Results:', results);
        
        // Remember to release the connection after you are done
        await pool.release(connection);
    } catch (error) {
        console.error('Error executing query:', error);
    }
}

  (async()=>{
    await initializePool()
    await runQuery(`CREATE TABLE IF NOT EXISTS foo (id STRING, name STRING  ) USING DELTA LOCATION '/data/pv/foo';`)
    await runQuery(`INSERT INTO foo (id , name) VALUES (1, 'foo') `)
    await runQuery('SELECT * FROM delta.`/data/pv/foo`')
  })()
