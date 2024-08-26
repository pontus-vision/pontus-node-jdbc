import { ResultSet } from './resultset';
import { ResultSetMetaData } from './resultsetmetadata';
import { Statement } from './statement';
import winston from 'winston';

export class PreparedStatement extends Statement {
  protected _ps: any;

  constructor(ps: any) {
    super(ps);
    this._ps = ps;
  }

  async addBatch(): Promise<void> {
    return new Promise((resolve, reject) => {
      this._ps.addBatch((err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  async clearParameters(): Promise<void> {
    return new Promise((resolve, reject) => {
      this._ps.clearParameters((err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  async execute(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._ps.execute((err: Error | null, result: boolean) => {
        if (err) {
          winston.error(err);
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  async executeBatch(): Promise<number[]> {
    return new Promise((resolve, reject) => {
      this._ps.executeBatch((err: Error | null, result: number[]) => {
        if (err) {
          winston.error(err);
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  async executeQuery(): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      this._ps.executeQuery((err: Error | null, resultset: any) => {
        if (err) {
          winston.error(err);
          reject(err);
        } else {
          resolve(new ResultSet(resultset));
        }
      });
    });
  }

  async executeUpdate(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._ps.executeUpdate((err: Error | null, result: number) => {
        if (err) {
          winston.error(err);
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  async getMetaData(): Promise<ResultSetMetaData> {
    return new Promise((resolve, reject) => {
      this._ps.getMetaData((err: Error | null, result: any) => {
        if (err) reject(err);
        else resolve(new ResultSetMetaData(result));
      });
    });
  }

  async getParameterMetaData(): Promise<any> {
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
      this._ps.setBigDecimal(index, val, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
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
      this._ps.setBoolean(index, val, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    
