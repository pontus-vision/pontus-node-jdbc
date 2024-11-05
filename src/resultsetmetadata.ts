class ResultSetMetaData {
  public _rsmd: any;

  constructor(rsmd: any) {
    this._rsmd = rsmd;
  }

  async getColumnCount(): Promise<number> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._rsmd.getColumnCountSync())
      } catch (error) {
        reject(error)
      };
    });
  }

  async getColumnName(column: number): Promise<string> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._rsmd.getColumnNameSync(column));
      } catch (error) {
        reject(error)
      }
    });
  }
}

export default ResultSetMetaData;
