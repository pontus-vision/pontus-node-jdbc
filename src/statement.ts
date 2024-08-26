/* jshint node: true */
"use strict";

import _ from 'lodash';
import ResultSet from './resultset';
import jinst from './jinst';
const java = jinst.getInstance();

class Statement {
  private _s: any;

  constructor(s: any) {
    this._s = s;
  }

  async addBatch(sql: string): Promise<void> {
    throw new Error("NOT IMPLEMENTED");
  }

  async cancel(): Promise<void> {
    return new Promise((resolve, reject) => {
      this._s.cancel((err: any) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  async clearBatch(): Promise<void> {
    return new Promise((resolve, reject) => {
      this._s.clearBatch((err: any) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  async close(): Promise<void> {
    return new Promise((resolve, reject) => {
      this._s.close((err: any) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  async executeUpdate(sql: string, arg1?: any): Promise<number> {
    const args = Array.from(arguments);

    if (!(_.isString(args[0]) && _.isUndefined(args[1]))) {
      throw new Error('INVALID ARGUMENTS');
    }

    return new Promise((resolve, reject) => {
      args.push((err: any, count: number) => {
        if (err) {
          reject(err);
        } else {
          resolve(count);
        }
      });

      this._s.executeUpdate.apply(this._s, args);
    });
  }

  async executeQuery(sql: string): Promise<ResultSet> {
    if (typeof sql !== 'string') {
      throw new Error('INVALID ARGUMENTS');
    }

    return new Promise((resolve, reject) => {
      this._s.executeQuery(sql, (err: any, resultset: any) => {
        if (err) {
          reject(err);
        } else {
          resolve(new ResultSet(resultset));
        }
      });
    });
  }

  async execute(sql: string): Promise<any> {
    if (typeof sql !== 'string') {
      throw new Error('INVALID ARGUMENTS');
    }

    return new Promise((resolve, reject) => {
      const s = this._s;
      s.execute(sql, (err: any, isResultSet: boolean) => {
        if (err) {
          reject(err);
        } else if (isResultSet) {
          s.getResultSet((err: any, resultset: any) => {
            if (err) {
              reject(err);
            } else {
              resolve(new ResultSet(resultset));
            }
          });
        } else {
          s.getUpdateCount((err: any, count: number) => {
            if (err) {
              reject(err);
            } else {
              resolve(count);
            }
          });
        }
      });
    });
  }

  async getFetchSize(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._s.getFetchSize((err: any, fetchSize: number) => {
        if (err) {
          reject(err);
        } else {
          resolve(fetchSize);
        }
      });
    });
  }

  async setFetchSize(rows: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this._s.setFetchSize(rows, (err: any) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  async getMaxRows(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._s.getMaxRows((err: any, max: number) => {
        if (err) {
          reject(err);
        } else {
          resolve(max);
        }
      });
    });
  }

  async setMaxRows(max: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this._s.setMaxRows(max, (err: any) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  async getQueryTimeout(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._s.getQueryTimeout((err: any, queryTimeout: number) => {
        if (err) {
          reject(err);
        } else {
          resolve(queryTimeout);
        }
      });
    });
  }

  async setQueryTimeout(seconds: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this._s.setQueryTimeout(seconds, (err: any) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  async getGeneratedKeys(): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      this._s.getGeneratedKeys((err: any, resultset: any) => {
        if (err) {
          reject(err);
        } else {
          resolve(new ResultSet(resultset));
        }
      });
    });
  }
}

// Initialize constants like in the original code
jinst.events.once('initialized', function onInitialized() {
  Statement.CLOSE_CURRENT_RESULT = java.getStaticFieldValue('java.sql.Statement', 'CLOSE_CURRENT_RESULT');
  Statement.KEEP_CURRENT_RESULT = java.getStaticFieldValue('java.sql.Statement', 'KEEP_CURRENT_RESULT');
  Statement.CLOSE_ALL_RESULTS = java.getStaticFieldValue('java.sql.Statement', 'CLOSE_ALL_RESULTS');
  Statement.SUCCESS_NO_INFO = java.getStaticFieldValue('java.sql.Statement', 'SUCCESS_NO_INFO');
  Statement.EXECUTE_FAILED = java.getStaticFieldValue('java.sql.Statement', 'EXECUTE_FAILED');
  Statement.RETURN_GENERATED_KEYS = java.getStaticFieldValue('java.sql.Statement', 'RETURN_GENERATED_KEYS');
  Statement.NO_GENERATED_KEYS = java.getStaticFieldValue('java.sql.Statement', 'NO_GENERATED_KEYS');
});

export default Statement;
