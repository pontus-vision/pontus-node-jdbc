/* jshint node: true */
"use strict";

import ResultSet from './resultset';
import ResultSetMetaData from './resultsetmetadata';
import Statement from './statement';
import winston from 'winston';

class PreparedStatement extends Statement {
  private _ps: any;

  constructor(ps: any) {
    super(ps);
    this._ps = ps;
  }

  async addBatch(): Promise<void> {
    return new Promise((resolve, reject) => {
      this._ps.addBatch((err: any) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

  async clearParameters(): Promise<void> {
    return new Promise((resolve, reject) => {
      this._ps.clearParameters((err: any) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

  async execute(): Promise<any> {
    return new Promise((resolve, reject) => {
      this._ps.execute((err: any, result: any) => {
        if (err) {
          winston.error(err);
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  async executeBatch(): Promise<any> {
    return new Promise((resolve, reject) => {
      this._ps.executeBatch((err: any, result: any) => {
        if (err) {
          winston.error(err);
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  async executeQuery(): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      this._ps.executeQuery((err: any, resultset: any) => {
        if (err) {
          winston.error(err);
          return reject(err);
        }
        resolve(new ResultSet(resultset));
      });
    });
  }

  async executeUpdate(): Promise<any> {
    return new Promise((resolve, reject) => {
      this._ps.executeUpdate((err: any, result: any) => {
        if (err) {
          winston.error(err);
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  async getMetaData(): Promise<ResultSetMetaData> {
    return new Promise((resolve, reject) => {
      this._ps.getMetaData((err: any, result: any) => {
        if (err) return reject(err);
        resolve(new ResultSetMetaData(result));
      });
    });
  }

  async getParameterMetaData(): Promise<void> {
    throw new Error("NOT IMPLEMENTED");
  }

  async setArray(index: number, val: any): Promise<void> {
    throw new Error("NOT IMPLEMENTED");
  }

  async setAsciiStream(index: number, val: any, length?: number): Promise<void> {
    throw new Error("NOT IMPLEMENTED");
  }

  async setBigDecimal(index: number, val: any): Promise<void> {
    return new Promise((resolve, reject) => {
      this._ps.setBigDecimal(index, val, (err: any) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

  async setBinaryStream(index: number, val: any, length?: number): Promise<void> {
    throw new Error("NOT IMPLEMENTED");
  }

  async setBlob(index: number, val: any, length?: number): Promise<void> {
    throw new Error("NOT IMPLEMENTED");
  }

  async setBoolean(index: number, val: boolean): Promise<void> {
    return new Promise((resolve, reject) => {
      this._ps.setBoolean(index, val, (err: any) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

  async setByte(index: number, val: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this._ps.setByte(index, val, (err: any) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

  async setBytes(index: number, val: Buffer): Promise<void> {
    return new Promise((resolve, reject) => {
      this._ps.setBytes(index, val, (err: any) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

  async setCharacterStream(index: number, val: any, length?: number): Promise<void> {
    throw new Error("NOT IMPLEMENTED");
  }

  async setClob(index: number, val: any, length?: number): Promise<void> {
    throw new Error("NOT IMPLEMENTED");
  }

  async setDate(index: number, val: any, calendar?: any): Promise<void> {
    return new Promise((resolve, reject) => {
      if (calendar === null) {
        this._ps.setDate(index, val, (err: any) => {
          if (err) return reject(err);
          resolve();
        });
      } else {
        this._ps.setDate(index, val, calendar, (err: any) => {
          if (err) return reject(err);
          resolve();
        });
      }
    });
  }

  async setDouble(index: number, val: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this._ps.setDouble(index, val, (err: any) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

  async setFloat(index: number, val: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this._ps.setFloat(index, val, (err: any) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

  async setInt(index: number, val: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this._ps.setInt(index, val, (err: any) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

  async setLong(index: number, val: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this._ps.setLong(index, val, (err: any) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

  async setString(index: number, val: string): Promise<void> {
    return new Promise((resolve, reject) => {
      this._ps.setString(index, val, (err: any) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

  async setTime(index: number, val: any, calendar?: any): Promise<void> {
    return new Promise((resolve, reject) => {
      if (calendar === null) {
        this._ps.setTime(index, val, (err: any) => {
          if (err) return reject(err);
          resolve();
        });
      } else {
        this._ps.setTime(index, val, calendar, (err: any) => {
          if (err) return reject(err);
          resolve();
        });
      }
    });
  }

  async setTimestamp(index: number, val: any, calendar?: any): Promise<void> {
    return new Promise((resolve, reject) => {
      if (calendar === null) {
        this._ps.setTimestamp(index, val, (err: any) => {
          if (err) return reject(err);
          resolve();
        });
      } else {
        this._ps.setTimestamp(index, val, calendar, (err: any) => {
          if (err) return reject(err);
          resolve();
        });
      }
    });
  }
}

export default PreparedStatement;
