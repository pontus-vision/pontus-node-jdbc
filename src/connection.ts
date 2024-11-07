import Jinst from "./jinst.js";
import { CallableStatement } from "./callablestatement.js";
import PreparedStatement from "./preparedstatement.js";
import DatabaseMetaData from "./databasemetadata.js";
import Statement from "./statement.js";
import SQLWarning from "./sqlwarning.js";

const java = Jinst.getInstance();

if (!java.isJvmCreated()) {
  java.addOption("-Xrs");
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
    arg1?: Record<string, any|any[]>
  ) => void;
  prepareStatementSync: (
    sql: string,
  ) => PreparedStatement;
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
  getAutoCommitSync:() => boolean
  getCatalogSync:() => Promise<string>
  clearWarningsSync:() => Promise<void>
  closeSync:() => Promise<void>
  getClientInfoSync:(name: string) => string 
  getHoldabilitySync(): number;
  getMetaDataSync(): any;
  getNetworkTimeoutSync(): number;
  getSchemaSync(): string;
  getTransactionIsolationSync(): string
  getHoldabilitySync(): number;
  getMetaDataSync(): any;
  getNetworkTimeoutSync(): number;
  getSchemaSync(): string;
  getTransactionIsolationSync(): string;
  getTypeMapSync(): any;
  getWarningsSync(): any;
  isClosedSync(): boolean;
  isReadOnlySync(): boolean;
  isValidSync(timeout: number): boolean;
  prepareCallSync(
    sql: string,
    rstype: number,
    rsconcurrency: number,
    rsholdability: number
  ): any;
  releaseSavepointSync(savepoint: any): void;
  rollbackSync(savepoint?: any): void;
  setAutoCommitSync(autocommit: boolean): void;
  setCatalogSync(catalog: string): void;
  setClientInfoSync(props: any, name?: string, value?: string): void;
  setHoldabilitySync(holdability: number): void;
  setReadOnlySync(readonly: boolean): void;
  setSavepointSync(name?: string): void;
  setSchemaSync(schema: string): void;
  setTransactionIsolationSync(txniso: number): void;
  setTypeMapSync(map: any): void;

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

  isClosedSync(): boolean{
    return this.isClosedSync()
  }

  isReadOnlySync():boolean {
    return this.isReadOnlySync()
  }

  isValidSync(val: number):boolean {
    return this.isValidSync(val)
  }
  abort(executor: any): Promise<void> {
    return Promise.reject(new Error("NOT IMPLEMENTED"));
  }

  clearWarnings(): Promise<void> {
    return new Promise((resolve, reject) => {
      resolve(this._conn?.clearWarningsSync())
    });
  }

  close(): Promise<void> {
    return new Promise((resolve, reject) => {
      resolve(this._conn?.closeSync())
    });
  }

  commit(): Promise<void> {
    return new Promise((resolve, reject) => {
      resolve(this._conn?.commitSync());
    });
  }

  commitSync(): Promise<void> {
    return new Promise((resolve, reject) => {
      resolve(this._conn?.commitSync());
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

  getAutoCommit(): Promise<boolean | undefined> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.getAutoCommitSync());
      } catch (error) {
        reject(error)
      }
    });
  }

  getCatalog(): Promise<string | undefined> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.getCatalogSync())
      } catch (error) {
        reject(error)
      }
    });
  }

  getClientInfo(name: string): Promise<string | undefined> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.getClientInfoSync(name));
      } catch (error) {
        reject(error)
      }
    });
  }

  getHoldability(): Promise<number | undefined> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.getHoldabilitySync());
      } catch (error) {
        reject(error) 
      }
    });
  }

  getMetaData(): Promise<any> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.getMetaDataSync())
      } catch (error) {
        reject(error)
      }
    });
  }

  getNetworkTimeout(): Promise<number | undefined> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.getNetworkTimeoutSync())
      } catch (error) {
        reject(error)
      }
    });
  }

  getSchema(): Promise<string | undefined> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.getSchemaSync())
      } catch (error) {
        reject(error)
      }
    });
  }

  getTransactionIsolation(): Promise<string | undefined> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.getTransactionIsolationSync())
      } catch (error) {
        reject(error)
      }
    });
  }

  getTypeMap(): Promise<any> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.getTypeMapSync())
      } catch (error) {
        reject(error)
      }
    });
  }

  getWarnings(): Promise<any> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.getWarningsSync())
      } catch (error) {
        reject(error)
      }
    });
  }

  isClosed(): Promise<boolean | undefined> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.isClosedSync())
      } catch (error) {
        reject(error)
      }
    });
  }
  isReadOnly(): Promise<boolean | undefined > {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.isReadOnlySync())
      } catch (error) {
        reject(error)
      }
    });
  }

  isValid(timeout: number): Promise<boolean | undefined> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.isValidSync(timeout))
      } catch (error) {
        reject(error)
      }
    });
  }

  prepareCall(
    sql: string,
    rstype: number,
    rsconcurrency: number,
    rsholdability: number
  ): Promise<any> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.prepareCallSync(sql, rstype, rsconcurrency, rsholdability))
      } catch (error) {
        reject(error) 
      }
    });
  }



  // prepareStatement (
  //   sql: string,
  //   arg1?: number | number[] | string[],
  //   arg2?: number,
  //   arg3?: number,
  // ): Promise<any> {
  //   return new Promise((resolve, reject) => {
  //     // Check that `sql` is provided
  //     if (!sql) {
  //       return reject(new Error('INVALID ARGUMENTS'));
  //     }
  
  //     // Validate additional arguments (arg1, arg2, arg3)
  //     const validateArgs = (args: any[]): boolean => {
  //       return args.every((arg, idx) => {
  //         if (idx === 0 && Array.isArray(arg) && args.length === 1) {
  //           return arg.every(item => typeof item === 'string' || typeof item === 'number');
  //         }
  //         return typeof arg === 'number';
  //       });
  //     };
  
  //     // Collect only defined arguments
  //     const args = [sql, arg1, arg2, arg3].filter(arg => arg !== undefined) as [
  //       string,
  //       number | number[] | string[],
  //       number?,
  //       number?
  //     ];
  
  //     if (!validateArgs(args.slice(1))) {
  //       return reject(new Error('INVALID ARGUMENTS'));
  //     }
  
  //     // Call `prepareStatement` with explicit arguments and a callback for handling the result
  //     this._conn?.prepareStatement(
  //       args[0],
  //       args[1],
  //       args[2],
  //       args[3],
  //       (err: Error | null, ps: any) => {
  //         if (err) {
  //           reject(err);
  //         } else {
  //           resolve(new PreparedStatement(ps));
  //         }
  //       }
  //     );
  //   });
  // };

  prepareStatement = (
    sql: string,
    arg1?: Record<string, any | any[]>  

  ):Promise<PreparedStatement>=> {

    return new Promise((resolve, reject)=>{
 
    // Check arg1, arg2, and arg3 for validity.  These arguments must
    // be numbers if given, except for the special case when the first
    // of these arguments is an array and no other arguments are given.
    // In this special case, the array must be a string or number array.
    //
    // NOTE: _.tail returns all but the first argument, so we are only
    // processing arg1, arg2, and arg3; and not sql (or callback, which
    // was already removed from the args array).
  
    // if (invalidArgs) {
    //   return reject(new Error(errMsg));
    // }
  
    // Push a callback handler onto the arguments
    // args.push(function (err, ps) {
    //   if (err) {
    //     return callback(err);
    //   } else {
    //     return callback(null, new PreparedStatement(ps));
    //   }
    // });
  
    // Forward modified arguments to _conn.prepareStatement
    try {
      resolve(new PreparedStatement(this._conn?.prepareStatementSync(sql))); 
    } catch (error) {
      reject(error) 
    }
    })
  };

  releaseSavepoint(savepoint: any): Promise<void | undefined> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.releaseSavepointSync(savepoint));
      } catch (error) {
        reject(error)
      }
    });
  }

  rollback(savepoint?: any): Promise<void> {
    return new Promise((resolve, reject) => {
        try {
          resolve(this._conn?.rollbackSync(savepoint));
        } catch (error) {
          reject(error)
        }
      
    });
  }

  setAutoCommit(autocommit: boolean): Promise<void | undefined> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.setAutoCommitSync(autocommit));
      } catch (error) {
        reject(error)
      }
    });
  }

  setCatalog(catalog: string): Promise<void | undefined> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.setCatalogSync(catalog));
      } catch (error) {
        reject(error)
      }
    });
  }

  setClientInfo(props: any, name?: string, value?: string): Promise<void | undefined> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.setClientInfoSync(props, name, value));
      } catch (error) {
        reject(error)
      }
    });
  }

  setHoldability(holdability: number): Promise<void | undefined> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.setHoldabilitySync(holdability));
      } catch (error) {
        reject(error)
      }
    });
  }

  setReadOnly(readonly: boolean): Promise<void | undefined> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.setReadOnlySync(readonly));
      } catch (error) {
        reject(error)
      }
    });
  }

  setSavepoint(name?: string): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.setSavepointSync());
      } catch (error) {
        reject(error)
      }
    });
  }
  
  setSchema(schema: string): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.setSchemaSync(schema));
        
      } catch (error) {
        reject(error) 
      }
    });
  }

  setTransactionIsolation(txniso: number): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.setTransactionIsolationSync(txniso));
        
      } catch (error) {
        reject(error) 
      }
    });
  }

  setTypeMap(map: any): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._conn?.setTypeMapSync(map));
      } catch (error) {
        reject(error) 
      }
    });
  }
}

export default Connection;