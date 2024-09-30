import _ from "lodash";
import jinst from "./jinst";
import { CallableStatement } from "./callablestatement";
import PreparedStatement from "./preparedstatement";
import DatabaseMetaData from "./databasemetadata";
import Statement from "./statement";
import SQLWarning from "./sqlwarning";

const java = jinst.getInstance();

if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
}

type ConnectionType = {
  clearWarnings: (callback: (err: Error | null) => void) => void;
  close: (callback: (err: Error | null) => void) => void;
  commit: (callback: (err: Error | null) => void) => void;
  createStatement: (
    arg1?: number,
    arg2?: number,
    arg3?: number,
    callback?: (err: Error | null, statement: any) => void
  ) => void;
  getAutoCommit: (
    callback: (err: Error | null, result: boolean) => void
  ) => void;
  getCatalog: (callback: (err: Error | null, catalog: string) => void) => void;
  getClientInfo: (
    name: string,
    callback: (err: Error | null, result: string) => void
  ) => void;
  getHoldability: (
    callback: (err: Error | null, holdability: number) => void
  ) => void;
  getMetaData: (callback: (err: Error | null, dbm: any) => void) => void;
  getNetworkTimeout: (
    callback: (err: Error | null, ms: number) => void
  ) => void;
  getSchema: (callback: (err: Error | null, schema: string) => void) => void;
  getTransactionIsolation: (
    callback: (err: Error | null, txniso: number) => void
  ) => void;
  getTypeMap: (callback: (err: Error | null, map: any) => void) => void;
  getWarnings: (callback: (err: Error | null, sqlwarning: any) => void) => void;
  isClosed: (callback: (err: Error | null, closed: boolean) => void) => void;
  isReadOnly: (
    callback: (err: Error | null, readonly: boolean) => void
  ) => void;
  isValid: (
    timeout: number,
    callback: (err: Error | null, valid: boolean) => void
  ) => void;
  prepareCall: (
    sql: string,
    rstype: number,
    rsconcurrency: number,
    rsholdability: number,
    callback: (err: Error | null, callablestatement: any) => void
  ) => void;
  prepareStatement: (
    sql: string,
    arg1?: number | number[] | string[],
    arg2?: number,
    arg3?: number,
    callback?: (err: Error | null, prepStmt: any) => void
  ) => void;
  releaseSavepoint: (
    savepoint: any,
    callback: (err: Error | null) => void
  ) => void;
  rollback: (savepoint: any, callback: (err: Error | null) => void) => void;
  setAutoCommit: (
    autocommit: boolean,
    callback: (err: Error | null) => void
  ) => void;
  setCatalog: (catalog: string, callback: (err: Error | null) => void) => void;
  setClientInfo: (
    props: any,
    name?: string,
    value?: string,
    callback?: (err: Error | null) => void
  ) => void;
  setHoldability: (
    holdability: number,
    callback: (err: Error | null) => void
  ) => void;
  setReadOnly: (
    readonly: boolean,
    callback: (err: Error | null) => void
  ) => void;
  setSavepoint: (callback: (err: Error | null) => void, name?: string) => void;
  setSchema: (schema: string, callback: (err: Error | null) => void) => void;
  setTransactionIsolation: (
    txniso: number,
    callback: (err: Error | null) => void
  ) => void;
  setTypeMap: (map: any, callback: (err: Error | null) => void) => void;
};

class Connection {
  private _conn: ConnectionType | null;
  private _txniso: { [key: number]: string };

  constructor(conn: ConnectionType) {
    this._conn = conn;
    this._txniso = this.initializeTxnIso();
  }

  private initializeTxnIso(): { [key: number]: string } {
    const txniso: { [key: number]: string } = {};
    txniso[
      java.getStaticFieldValue("java.sql.Connection", "TRANSACTION_NONE")
    ] = "TRANSACTION_NONE";
    txniso[
      java.getStaticFieldValue(
        "java.sql.Connection",
        "TRANSACTION_READ_COMMITTED"
      )
    ] = "TRANSACTION_READ_COMMITTED";
    txniso[
      java.getStaticFieldValue(
        "java.sql.Connection",
        "TRANSACTION_READ_UNCOMMITTED"
      )
    ] = "TRANSACTION_READ_UNCOMMITTED";
    txniso[
      java.getStaticFieldValue(
        "java.sql.Connection",
        "TRANSACTION_REPEATABLE_READ"
      )
    ] = "TRANSACTION_REPEATABLE_READ";
    txniso[
      java.getStaticFieldValue(
        "java.sql.Connection",
        "TRANSACTION_SERIALIZABLE"
      )
    ] = "TRANSACTION_SERIALIZABLE";
    return txniso;
  }

  abort(executor: any): Promise<void> {
    return Promise.reject(new Error("NOT IMPLEMENTED"));
  }

  clearWarnings(): Promise<void> {
    return new Promise((resolve, reject) => {
      this._conn?.clearWarnings((err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  close(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this._conn === null) {
        resolve();
        return;
      }

      this._conn.close((err: Error | null) => {
        if (err) reject(err);
        else {
          this._conn = null;
          resolve();
        }
      });
    });
  }

  commit(): Promise<void> {
    return new Promise((resolve, reject) => {
      this._conn?.commit((err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  createArrayOf(typename: string, objarr: any[]): Promise<void> {
    return Promise.reject(new Error("NOT IMPLEMENTED"));
  }

  createBlob(): Promise<void> {
    return Promise.reject(new Error("NOT IMPLEMENTED"));
  }

  createClob(): Promise<void> {
    return Promise.reject(new Error("NOT IMPLEMENTED"));
  }

  createNClob(): Promise<void> {
    return Promise.reject(new Error("NOT IMPLEMENTED"));
  }

  createSQLXML(): Promise<void> {
    return Promise.reject(new Error("NOT IMPLEMENTED"));
  }

  createStatement(arg1?: number, arg2?: number, arg3?: number): Promise<any> {
    return new Promise((resolve, reject) => {
      try {
        const args = [arg1, arg2, arg3].filter((arg) => arg !== undefined); // Filter out undefined arguments
        const statement = this._conn?.createStatement(...args); // Invoke without callback
        resolve(new Statement(statement)); // Assume createStatement returns a statement
      } catch (err) {
        reject(err); // Handle any errors that occur during the call
      }
    });
  }

  createStruct(typename: string, attrarr: any[]): Promise<void> {
    return Promise.reject(new Error("NOT IMPLEMENTED"));
  }

  getAutoCommit(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._conn?.getAutoCommit((err: Error | null, result: boolean) => {
        if (err) reject(err);
        else resolve(result);
      });
    });
  }

  getCatalog(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._conn?.getCatalog((err: Error | null, catalog: string) => {
        if (err) reject(err);
        else resolve(catalog);
      });
    });
  }

  getClientInfo(name: string): Promise<string> {
    return new Promise((resolve, reject) => {
      this._conn?.getClientInfo(name, (err: Error | null, result: string) => {
        if (err) reject(err);
        else resolve(result);
      });
    });
  }

  getHoldability(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._conn?.getHoldability((err: Error | null, holdability: number) => {
        if (err) reject(err);
        else resolve(holdability);
      });
    });
  }

  getMetaData(): Promise<any> {
    return new Promise((resolve, reject) => {
      this._conn?.getMetaData((err: Error | null, dbm: any) => {
        if (err) reject(err);
        else resolve(new DatabaseMetaData(dbm));
      });
    });
  }

  getNetworkTimeout(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._conn?.getNetworkTimeout((err: Error | null, ms: number) => {
        if (err) reject(err);
        else resolve(ms);
      });
    });
  }

  getSchema(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._conn?.getSchema((err: Error | null, schema: string) => {
        if (err) reject(err);
        else resolve(schema);
      });
    });
  }

  getTransactionIsolation(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._conn?.getTransactionIsolation(
        (err: Error | null, txniso: number) => {
          if (err) reject(err);
          else resolve(this._txniso[txniso]);
        }
      );
    });
  }

  getTypeMap(): Promise<any> {
    return new Promise((resolve, reject) => {
      this._conn?.getTypeMap((err: Error | null, map: any) => {
        if (err) reject(err);
        else resolve(map);
      });
    });
  }

  getWarnings(): Promise<any> {
    return new Promise((resolve, reject) => {
      this._conn?.getWarnings((err: Error | null, sqlwarning: any) => {
        if (err) reject(err);
        else resolve(sqlwarning);
      });
    });
  }

  isClosed(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._conn?.isClosed((err: Error | null, closed: boolean) => {
        if (err) reject(err);
        else resolve(closed);
      });
    });
  }

  isReadOnly(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._conn?.isReadOnly((err: Error | null, readonly: boolean) => {
        if (err) reject(err);
        else resolve(readonly);
      });
    });
  }

  isValid(timeout: number): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._conn?.isValid(timeout, (err: Error | null, valid: boolean) => {
        if (err) reject(err);
        else resolve(valid);
      });
    });
  }

  prepareCall(
    sql: string,
    rstype: number,
    rsconcurrency: number,
    rsholdability: number
  ): Promise<any> {
    return new Promise((resolve, reject) => {
      this._conn?.prepareCall(
        sql,
        rstype,
        rsconcurrency,
        rsholdability,
        (err: Error | null, callablestatement: any) => {
          if (err) reject(err);
          else resolve(new CallableStatement(callablestatement));
        }
      );
    });
  }

  prepareStatement(
    sql: string,
    arg1?: number | number[] | string[],
    arg2?: number,
    arg3?: number
  ): Promise<any> {
    return new Promise((resolve, reject) => {
      let args: [string, ...(number | number[] | string[])[]]; // Define args as a tuple type

      // Always start with `sql` as the first argument
      args = [sql];

      // Conditionally add `arg1`, `arg2`, and `arg3` to the `args` array
      if (arg1 !== undefined) args.push(arg1);
      if (arg2 !== undefined) args.push(arg2);
      if (arg3 !== undefined) args.push(arg3);

      // Spread `args` into the `prepareStatement` call
      this._conn?.prepareStatement(...args);
    });
  }

  releaseSavepoint(savepoint: any): Promise<void> {
    return new Promise((resolve, reject) => {
      this._conn?.releaseSavepoint(savepoint, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  rollback(savepoint?: any): Promise<void> {
    return new Promise((resolve, reject) => {
      if (savepoint) {
        this._conn?.rollback(savepoint, (err: Error | null) => {
          if (err) reject(err);
          else resolve();
        });
      } else {
        this._conn?.rollback(savepoint, (err: Error | null) => {
          if (err) reject(err);
          else resolve();
        });
      }
    });
  }

  setAutoCommit(autocommit: boolean): Promise<void> {
    return new Promise((resolve, reject) => {
      this._conn?.setAutoCommit(autocommit, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  setCatalog(catalog: string): Promise<void> {
    return new Promise((resolve, reject) => {
      this._conn?.setCatalog(catalog, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  setClientInfo(props: any, name?: string, value?: string): Promise<void> {
    return new Promise((resolve, reject) => {
      this._conn?.setClientInfo(props, name, value, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  setHoldability(holdability: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this._conn?.setHoldability(holdability, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  setReadOnly(readonly: boolean): Promise<void> {
    return new Promise((resolve, reject) => {
      this._conn?.setReadOnly(readonly, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  setSavepoint(name?: string): Promise<void> {
    return new Promise((resolve, reject) => {
      this._conn?.setSavepoint((err: Error | null) => {
        if (err) reject(err);
        else resolve();
      }, name);
    });
  }
  
  setSchema(schema: string): Promise<void> {
    return new Promise((resolve, reject) => {
      this._conn?.setSchema(schema, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  setTransactionIsolation(txniso: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this._conn?.setTransactionIsolation(txniso, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  setTypeMap(map: any): Promise<void> {
    return new Promise((resolve, reject) => {
      this._conn?.setTypeMap(map, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }
}

export default Connection;
