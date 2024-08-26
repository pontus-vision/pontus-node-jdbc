/* jshint node: true */
"use strict";

class ResultSetMetaData {
  private _rsmd: any;

  constructor(rsmd: any) {
    this._rsmd = rsmd;
  }

  async getColumnCount(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._rsmd.getColumnCount((err: any, count: number) => {
        if (err) {
          return reject(err);
        }
        resolve(count);
      });
    });
  }

  async getColumnName(column: number): Promise<string> {
    return new Promise((resolve, reject) => {
      this._rsmd.getColumnName(column, (err: any, name: string) => {
        if (err) {
          return reject(err);
        }
        resolve(name);
      });
    });
  }
}

export default ResultSetMetaData;
