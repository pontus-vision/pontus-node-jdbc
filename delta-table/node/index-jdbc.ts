// Import necessary modules
import JDBC from 'jdbc'; // Use default import
import Jinst from 'jdbc/lib/jinst'; // Use default import

interface JDBCConfig {
  url: string;
  drivername: string;
  properties: {
    user: string;
    password: string;
  };
}

export interface Connection {
  createStatement(
    callback: (err: Error | null, statement: Statement) => void,
  ): void;
  close(callback: (err: Error | null) => void): void;
}

export interface Statement {
  executeQuery(
    sql: string,
    callback: (err: Error | null, resultSet: ResultSet) => void,
  ): void;
}

export interface ResultSet {
  toObjArray(callback: (err: Error | null, results: object[]) => void): void;
  close(callback: (err: Error | null) => void): void;
}

export interface ConnectionObject {
  conn: Connection;
}

export const classPath = process.env['CLASSPATH']?.split(',');

if (!Jinst.isJvmCreated()) {
  Jinst.addOption('-Xrs');
  Jinst.setupClasspath(classPath || []); // Path to your JDBC driver JAR file
}

export const config: JDBCConfig = {
  url: process.env['P_DELTA_TABLE_HIVE_SERVER'] || 'jdbc:hive2://localhost:10000', // Update the connection URL according to your setup
  drivername: 'org.apache.hive.jdbc.HiveDriver', // Driver class name
  properties: {
    user: 'NBuser',
    password: '',
  },
};

// Initialize the JDBC connection
const jdbc = new JDBC(config);
jdbc.initialize((err: Error | null) => {
  if (err) {
    console.log('Error initializing JDBC', err);
  } else {
    console.log('JDBC initialized');
  }
});

export const createConnection = (): Connection => {
  return jdbc.reserve((err: Error | null, connObj: ConnectionObject) => {
    if (connObj) {
      return connObj;
    }
    if (err) {
      throw err;
    }
  })?.conn;
};

// Function to execute a query
export function executeQuery(
  query: string,
  conn: Connection,
): Promise<object[]> {
  return new Promise((resolve, reject) => {
    console.log('IN EXECUTE QUERY - 1');
    conn.createStatement((stmtErr: Error | null, statement: Statement) => {
      console.log('IN EXECUTE QUERY -  2');

      if (stmtErr) {
        console.log('Error creating statement', stmtErr);
        return reject(`Error creating statement ${stmtErr}`);
      }
      console.log('IN EXECUTE QUERY -3  ');
      statement.executeQuery(
        query,
        (queryErr: Error | null, resultSet: ResultSet) => {
          if (queryErr) {
            console.log('Error executing query', queryErr);
            return reject(`Error executing query ${queryErr}`);
          }
          console.log('IN EXECUTE QUERY -4  ');
          resultSet.toObjArray((resultErr: Error | null, results: object[]) => {
            if (resultErr) {
              console.log('Error converting result set', resultErr);
              resultSet.close((e) => console.log(`${e}`));
              return reject(`Error converting result set ${resultErr}`);
            }
            console.log('IN EXECUTE QUERY -5  ');
            console.table(results);
            resultSet.close((closeErr) => {
              if (closeErr) {
                console.log('Error closing result set', closeErr);
                return reject(`Error closing result set ${closeErr}`);
              }
              console.log({ results });
              return resolve(results);
            });
          });
        },
      );
    });
  });
}

// (async function () {
//   console.log('CONNECTING TO DELTA');
//   const conn = createConnection();
//   console.log({ conn });
//   const res = await executeQuery('SELECT 1', conn);
//   console.log({ res });
//   console.log('END CONNECTION TO DELTA');
// })();

export default jdbc;
