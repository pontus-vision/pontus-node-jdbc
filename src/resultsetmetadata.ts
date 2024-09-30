class ResultSetMetaData {
  public _rsmd: any;

  constructor(rsmd: any) {
    this._rsmd = rsmd;
  }

  async getColumnCount(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._rsmd.getColumnCount((err: Error | null, count: number) => {
        if (err) {
          reject(err);
        } else {
          resolve(count);
        }
      });
    });
  }

  async getColumnName(column: number): Promise<string> {
    return new Promise((resolve, reject) => {
      this._rsmd.getColumnName(column, (err: Error | null, name: string) => {
        if (err) {
          reject(err);
        } else {
          resolve(name);
        }
      });
    });
  }
}

export default ResultSetMetaData;
