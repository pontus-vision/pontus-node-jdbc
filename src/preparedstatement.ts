/* jshint node: true */
"use strict";

import ResultSet from './resultset';
import ResultSetMetaData from './resultsetmetadata';
import Statement from './statement';

class PreparedStatement extends Statement {
  private _ps: any;

  constructor(ps: any) {
    super(ps);
    this._ps = ps;
  }

  async addBatch(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._ps.addBatchSync());
      } catch (error) {
        reject(error)
      }
    });
  }

  async clearParameters(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._ps.clearParametersSync())
      } catch (error) {
        reject(error)
      }
    });
  }

  async execute(): Promise<any> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._ps.executeSync())
      } catch (error) {
        reject(error)
      }
    });
  }

  async executeBatch(): Promise<any> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._ps.executeBatchSync())
      } catch (error) {
        reject(error)
      }
    });
  }

  async executeQuery(): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      try {
        resolve( new ResultSet(this._ps.executeQuerySync()));
      } catch (error) {
        reject(error)
      }
    });
  }

  async executeUpdate(): Promise<any> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._ps.executeUpdateSync())
      } catch (error) {
        reject(error)
      }
    });
  }

  async getMetaData(): Promise<ResultSetMetaData> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._ps.getMetaDataSync());
      } catch (error) {
        reject(error)
      }
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
      try {
        resolve(this._ps.setBigDecimalSync(index, val))
      } catch (error) {
        reject(error)
      };
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
      try {
        resolve(this._ps.setBooleanSync(index, val));
      } catch (error) {
        reject(error) 
      }
    });
  }

  async setByte(index: number, val: number): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._ps.setByteSync(index, val))
      } catch (error) {
        reject(error)
      };
    });
  }

  async setBytes(index: number, val: Buffer): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._ps.setBytesSync(index, val))
      } catch (error) {
        reject(error)
      };
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
        try {
          resolve(this._ps.setDateSync(index, val));
        } catch (error) {
          reject(error)
        }
      } else {
        try {
          resolve(this._ps.setDateSync(index, val, calendar))
        } catch (error) {
          reject(error)
        }
      }
    });
  }

  async setDouble(index: number, val: number): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._ps.setDoubleSync(index, val))
      } catch (error) {
        reject(error)
      }
    });
  }

  async setFloat(index: number, val: number): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._ps.setFloatSync(index, val))
      } catch (error) {
        reject(error)
      };
    });
  }

  async setInt(index: number, val: number): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._ps.setIntSync(index, val))
      } catch (error) {
        reject(error)
      };
    });
  }

  async setLong(index: number, val: number): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._ps.setLongSync(index, val))
      } catch (error) {
        reject(error)
      };
    });
  }

  async setString(index: number, val: string): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._ps.setStringSync(index, val))
      } catch (error) {
        reject(error)
      };
    });
  }

  async setTime(index: number, val: any, calendar?: any): Promise<void> {
    return new Promise((resolve, reject) => {
      if (calendar === null) {
        try {
          resolve(this._ps.setTimeSync(index, val))
        } catch (error) {
          reject(error)
        }
      } else {
        try {
          resolve(this._ps.setTimeSync(index, val, calendar))
        } catch (error) {
          reject(error)
        };
      }
    });
  }

  async setTimestamp(index: number, val: any, calendar?: any): Promise<void> {
    return new Promise((resolve, reject) => {
      if (calendar === null) {
        try {
          resolve(this._ps.setTimestampSync(index, val))
        } catch (error) {
          reject(error)
        };
      } else {
        try {
          resolve(this._ps.setTimestampSync(index, val, calendar))
        } catch (error) {
          reject(error)
        };
      }
    });
  }
}

export default PreparedStatement;
