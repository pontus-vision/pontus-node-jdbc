/* jshint node: true */
"use strict";

import ResultSet from "./resultset.js";
import jinst from "./jinst.js";
const java = jinst.getInstance();

class Statement {
  private _s: any;

  public static CLOSE_CURRENT_RESULT: any;
  public static KEEP_CURRENT_RESULT: any;
  public static CLOSE_ALL_RESULTS: any;
  public static SUCCESS_NO_INFO: any;
  public static EXECUTE_FAILED: any;
  public static RETURN_GENERATED_KEYS: any;
  public static NO_GENERATED_KEYS: any;

  constructor(s: any) {
    this._s = s;
  }

  async addBatch(sql: string): Promise<void> {
    throw new Error("NOT IMPLEMENTED");
  }

  async cancel(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._s.cancelSync())
      } catch (error) {
        reject(error)
      };
    });
  }

  async clearBatch(): Promise<void> {
    return new Promise((resolve, reject) => {
        try {
         resolve(this._s.clearBatchSync()) 
        } catch (error) {
          reject(error)
        };
    });
  }

  async close(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._s.closeSync());
      } catch (error) {
        reject(error)
      }
    });
  }

  async executeUpdate(sql: string, arg1?: any): Promise<number> {
    const args = Array.from(arguments);

    if (!(typeof args[0] === "string" && args[1] === undefined)) {
      throw new Error("INVALID ARGUMENTS");
    }

    return new Promise((resolve, reject) => {
      try {
        resolve(this._s.executeUpdateSync(sql))
      } catch (error) {
        reject(error) 
      }
    });
  }

  async executeQuery(sql: string): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      try {
       resolve( new ResultSet(this._s.executeQuerySync(sql)));
      } catch (error) {
        reject(error)
      }
    });
  }

  async execute(sql: string): Promise<any> {
    if (typeof sql !== "string") {
      throw new Error("INVALID ARGUMENTS");
    }
  
    const s = this._s;
  
    // Execute the SQL command and determine if it returns a result set
    const isResultSet = await new Promise<boolean>((resolve, reject) => {
      try {
        resolve(s.executeSync(sql))
      } catch (error) {
        reject(error)
      }
    });
  
    if (isResultSet) {
      // If the result is a result set, retrieve it
      const resultset = await new Promise<any>((resolve, reject) => {
        try {
          resolve(s.getResultSetSync())
        } catch (error) {
          reject(error)
        };
      });
      return new ResultSet(resultset);
    } else {
      // Otherwise, get the update count
      const count = await new Promise<number>((resolve, reject) => {
        try {
          resolve(s.getUpdateCountSync())
        } catch (error) {
          reject(error)
        }
      });
      return count;
    }
  }
  

  async getFetchSize(): Promise<number> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._s.getFetchSizeSync())
      } catch (error) {
        reject(error)
      }
    });
  }

  async setFetchSize(rows: number): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._s.setFetchSizeSync(rows))
      } catch (error) {
        reject(error)
      };
    });
  }

  async getMaxRows(): Promise<number> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._s.getMaxRowsSync())
      } catch (error) {
        reject(error)
      };
    });
  }

  async setMaxRows(max: number): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._s.setMaxRowsSync(max))
      } catch (error) {
        reject(error)
      };
    });
  }

  async getQueryTimeout(): Promise<number> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._s.getQueryTimeoutSync())
      } catch (error) {
        reject(error)
      };
    });
  }

  async setQueryTimeout(seconds: number): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._s.setQueryTimeoutSync(seconds))
      } catch (error) {
        reject(error)
      };
    });
  }

  async getGeneratedKeys(): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._s.getGeneratedKeysSync())
      } catch (error) {
        reject(error)
      };
    });
  }
}

// Initialize constants like in the original code
jinst.events.once("initialized", function onInitialized() {
  Statement.CLOSE_CURRENT_RESULT = java.getStaticFieldValue(
    "java.sql.Statement",
    "CLOSE_CURRENT_RESULT"
  );
  Statement.KEEP_CURRENT_RESULT = java.getStaticFieldValue(
    "java.sql.Statement",
    "KEEP_CURRENT_RESULT"
  );
  Statement.CLOSE_ALL_RESULTS = java.getStaticFieldValue(
    "java.sql.Statement",
    "CLOSE_ALL_RESULTS"
  );
  Statement.SUCCESS_NO_INFO = java.getStaticFieldValue(
    "java.sql.Statement",
    "SUCCESS_NO_INFO"
  );
  Statement.EXECUTE_FAILED = java.getStaticFieldValue(
    "java.sql.Statement",
    "EXECUTE_FAILED"
  );
  Statement.RETURN_GENERATED_KEYS = java.getStaticFieldValue(
    "java.sql.Statement",
    "RETURN_GENERATED_KEYS"
  );
  Statement.NO_GENERATED_KEYS = java.getStaticFieldValue(
    "java.sql.Statement",
    "NO_GENERATED_KEYS"
  );
});

export default Statement;
