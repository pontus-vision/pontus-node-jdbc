declare module 'jdbc' {
  export interface JDBCConfig {
    url: string;
    drivername: string;
    properties: {
      user: string;
      password: string;
    };
  }

  export interface Connection {
    createStatement(callback: (err: Error | null, statement: Statement) => void): void;
    close(callback: (err: Error | null) => void): void;
  }

  export interface Statement {
    executeQuery(sql: string, callback: (err: Error | null, resultSet: ResultSet) => void): void;
  }

  export interface ResultSet {
    toObjArray(callback: (err: Error | null, results: object[]) => void): void;
    close(callback: (err: Error | null) => void): void;
  }

  export interface ConnectionObject {
    conn: Connection;
  }

  export default class JDBC {
    constructor(config: JDBCConfig);

    initialize(callback: (err: Error | null) => void): void;
    reserve(callback: (err: Error | null, connObj: ConnectionObject) => void): ConnectionObject;
    release(connObj: ConnectionObject, callback: (err: Error | null) => void): void;
  }
}

declare module 'jdbc/lib/jinst' {
  export function isJvmCreated(): boolean;
  export function addOption(option: string): void;
  export function setupClasspath(paths: string[]): void;
}

