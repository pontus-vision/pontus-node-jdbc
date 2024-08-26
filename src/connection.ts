import _ from 'lodash';
import { jinst } from './jinst';
import { CallableStatement } from './callablestatement';
import { PreparedStatement } from './preparedstatement';
import { DatabaseMetaData } from './databasemetadata';
import { Statement } from './statement';
import { SQLWarning } from './sqlwarning';

const java = jinst.getInstance();

if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
}

export class Connection {
  private _conn: any;
  private _txniso: string[];

  constructor(conn: any) {
    this._conn = conn;
    this._txniso = (() => {
      const txniso: string[] = [];

      txniso[java.getStaticFieldValue("java.sql.Connection", "TRANSACTION_NONE")] = "TRANSACTION_NONE";
      txniso[java.getStaticFieldValue("java.sql.Connection", "TRANSACTION_READ_COMMITTED")] = "TRANSACTION_READ_COMMITTED";
      txniso[java.getStaticFieldValue("java.sql.Connection", "TRANSACTION_READ_UNCOMMITTED")] = "TRANSACTION_READ_UNCOMMITTED";
      txniso[java.getStaticFieldValue("java.sql.Connection", "TRANSACTION_REPEATABLE_READ")] = "TRANSACTION_REPEATABLE_READ";
      txniso[java.getStaticFieldValue("java.sql.Connection", "TRANSACTION_SERIALIZABLE")] = "TRANSACTION_SERIALIZABLE";

      return txniso;
    })();
  }

  async abort(executor: any): Promise<void> {
    throw new Error("NOT IMPLEMENTED");
  }

  async clearWarnings(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this._conn.clearWarnings((err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  async close(): Promise<void> {
    if (this._conn === null) return;

    await new Promise<void>((resolve, reject) => {
      this._conn.close((err: Error | null) => {
        if (err) reject(err);
        else {
          this._conn = null;
          resolve();
        }
      });
    });
  }

  async commit(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this._conn.commit((err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  async createArrayOf(typename: string, objarr: any[]): Promise<any> {
    throw new Error("NOT IMPLEMENTED");
  }

  async createBlob(): Promise<any> {
    throw new Error("NOT IMPLEMENTED");
  }

  async createClob(): Promise<any> {
    throw new Error("NOT IMPLEMENTED");
  }

  async createNClob(): Promise<any> {
    throw new Error("NOT IMPLEMENTED");
  }

  async createSQLXML(): Promise<any> {
    throw new Error("NOT IMPLEMENTED");
  }

  async createStatement(...args: number[]): Promise<Statement> {
    if (!args.every(arg => _.isNumber(arg))) {
      throw new Error("INVALID ARGUMENTS");
    }

    return new Promise<Statement>((resolve, reject) => {
      args.push((err: Error | null, statement: any) => {
        if (err) reject(err);
        else resolve(new Statement(statement));
      });

      this._conn.createStatement.apply(this._conn, args);
    });
  }

  async createStruct(typename: string, attrarr: any[]): Promise<any> {
    throw new Error("NOT IMPLEMENTED");
  }

  async getAutoCommit(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._conn.getAutoCommit((err: Error | null,
