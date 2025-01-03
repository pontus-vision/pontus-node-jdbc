"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
exports.executeQuery = exports.createConnection = exports.config = exports.classPath = void 0;
// Import necessary modules
const jdbc_1 = __importDefault(require("jdbc")); // Use default import
const jinst_1 = __importDefault(require("jdbc/lib/jinst")); // Use default import
exports.classPath = (_a = process.env['CLASSPATH']) === null || _a === void 0 ? void 0 : _a.split(',');
if (!jinst_1.default.isJvmCreated()) {
    jinst_1.default.addOption('-Xrs');
    jinst_1.default.setupClasspath(exports.classPath || []); // Path to your JDBC driver JAR file
}
exports.config = {
    url: process.env['P_DELTA_TABLE_HIVE_SERVER'] || 'jdbc:hive2://localhost:10000', // Update the connection URL according to your setup
    drivername: 'org.apache.hive.jdbc.HiveDriver', // Driver class name
    properties: {
        user: 'NBuser',
        password: '',
    },
};
// Initialize the JDBC connection
const jdbc = new jdbc_1.default(exports.config);
jdbc.initialize((err) => {
    if (err) {
        console.log('Error initializing JDBC', err);
    }
    else {
        console.log('JDBC initialized');
    }
});
const createConnection = () => {
    var _a;
    return (_a = jdbc.reserve((err, connObj) => {
        if (connObj) {
            return connObj;
        }
        if (err) {
            throw err;
        }
    })) === null || _a === void 0 ? void 0 : _a.conn;
};
exports.createConnection = createConnection;
// Function to execute a query
function executeQuery(query, conn) {
    return new Promise((resolve, reject) => {
        console.log('IN EXECUTE QUERY - 1');
        conn.createStatement((stmtErr, statement) => {
            console.log('IN EXECUTE QUERY -  2');
            if (stmtErr) {
                console.log('Error creating statement', stmtErr);
                return reject(`Error creating statement ${stmtErr}`);
            }
            console.log('IN EXECUTE QUERY -3  ');
            statement.executeQuery(query, (queryErr, resultSet) => {
                if (queryErr) {
                    console.log('Error executing query', queryErr);
                    return reject(`Error executing query ${queryErr}`);
                }
                console.log('IN EXECUTE QUERY -4  ');
                resultSet.toObjArray((resultErr, results) => {
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
            });
        });
    });
}
exports.executeQuery = executeQuery;
// (async function () {
//   console.log('CONNECTING TO DELTA');
//   const conn = createConnection();
//   console.log({ conn });
//   const res = await executeQuery('SELECT 1', conn);
//   console.log({ res });
//   console.log('END CONNECTION TO DELTA');
// })();
exports.default = jdbc;
