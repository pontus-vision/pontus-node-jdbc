# Pontus Node JDBC

A TypeScript wrapper for JDBC connections to work seamlessly with TypeScript applications.

## Installation

You can install the library via npm:

```bash
npm install pontus-node-jdbc
```
## Usage
Initialize a Connection Pool
First, create a connection pool to manage your database connections:

```typescript

import { Pool } from 'pontus-node-jdbc';

const pool = new Pool({
  url: 'jdbc:your_database_url',   // Replace with your JDBC URL
  properties: {
    user: 'your_username',           // Database username
    password: 'your_password'        // Database password
  },
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
```
Reserve a Connection and Run a Query
After initializing the pool, you can reserve a connection and run queries as follows:

```typescript

async function reserveConnection() {
  try {
    const connection = await pool.reserve();
    console.log('Connection reserved:', connection);

    // Example: running a query
    const preparedStatement = await connection.prepareStatement('SELECT * FROM your_table'); // Replace `your_table` with your actual table name

    const resultSet = await preparedStatement.executeQuery();
    const results = await resultSet.toArray(); // Assuming you have a method to convert ResultSet to an array

    console.log('Query Results:', results);
    
    // Release the connection back to the pool
    await pool.release(connection);
  } catch (error) {
    console.error('Error reserving connection:', error);
  }
}

// Example execution
initializePool();
reserveConnection();
```
Running a Query
The following example demonstrates how to run a query and process the results:

```typescript
async function runQuery() {
    try {
        const connection = await pool.reserve();
        const preparedStatement = await connection.prepareStatement('SELECT * FROM your_table'); // Replace `your_table` with your actual table name

        const resultSet = await preparedStatement.executeQuery();
        const results = await resultSet.toArray(); // Assuming you have a method to convert ResultSet to an array

        console.log('Query Results:', results);
        
        // Remember to release the connection after you are done
        await pool.release(connection);
    } catch (error) {
        console.error('Error executing query:', error);
    }
}

// Call the function to execute the query
runQuery();
```
## API Reference

*Pool*: Main class to manage the connection pool.

*PreparedStatement*: For preparing and executing SQL statements.

*ResultSet*: Represents the result set of a query.

*SQLWarning*: Represents SQLWarnings associated with a Statement or ResultSet.

*Statement*: General statement functionality for executing SQL queries.

## Contributing
Please read CONTRIBUTING.md for details on our code of conduct and the process for submitting pull requests to us.

## License
This project is licensed under the MIT License - see the LICENSE file for details.
