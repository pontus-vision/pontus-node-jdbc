import { v4 as uuidv4 } from "uuid";
import * as console from "console";
import Jinst from "./jinst.js";
import dm from "./drivermanager.js";
import Connection from "./connection.js";


interface KeepAliveConfig {
  enabled: boolean;
  interval: number; // in milliseconds
  query: string;
}

interface LoggingConfig {
  level: string; // e.g., 'error', 'info', 'debug', etc.
}

export interface PoolConfig {
  url: string;
  properties: Record<string, any>; // key-value pairs for connection properties
  user?: string; // optional
  password?: string; // optional
  drivername?: string; // optional
  minpoolsize?: number; // default: 1
  maxpoolsize?: number; // default: 1
  keepalive?: KeepAliveConfig; // default: { interval: 60000, query: "select 1", enabled: false }
  maxidle?: number | null; // optional, but impacts keepalive
  logging?: LoggingConfig; // default: { level: "error" }
}


const java = Jinst.getInstance();

if (!Jinst.getInstance().isJvmCreated()) {
  Jinst.getInstance().addOption("-Xrs");
}

const keepalive = (conn: any, query: string): void => {
  conn.createStatement((err: Error, statement: any) => {
    if (err) return console.error(err);
    statement.execute(query, (err: Error, result: any) => {
      if (err) return console.error(err);
      console.debug(`${new Date().toUTCString()} - Keep-Alive`);
    });
  });
};

const addConnection = async (
  url: string,
  props: any,
  ka: { enabled: boolean; interval: number; query: string },
  maxIdle: number | null
): Promise<any> => {
  return new Promise((resolve, reject) => {
    dm.getConnection(url, props)
      .then((conn: any) => {
        const connobj: any = {
          uuid: uuidv4(),
          conn: new Connection(conn),
          keepalive: ka.enabled
            ? setInterval(() => keepalive(conn, ka.query), ka.interval)
            : false,
        };

        if (maxIdle) {
          connobj.lastIdle = new Date().getTime();
        }

        resolve(connobj);
      })
      .catch((err: Error) => {
        reject(err);
      });
  });
};

const addConnectionSync = (
  url: string,
  props: any,
  ka: { enabled: boolean; interval: number; query: string },
  maxIdle: number | null
) => {
  const conn = dm.getConnectionSync(url, props);
  const connobj: {
    uuid: string;
    conn: Connection;
    keepalive: boolean | NodeJS.Timeout;
    lastIdle?: number;
  } = {
    uuid: uuidv4(),
    conn: new Connection(conn),
    keepalive: ka.enabled
      ? setInterval(keepalive, ka.interval, conn, ka.query)
      : false,
  };

  if (maxIdle) {
    connobj["lastIdle"] = new Date().getTime();
  }

  return connobj;
};

class Pool {
  private _url: string;
  private _props: any;
  private _drivername: string;
  private _minpoolsize: number;
  private _maxpoolsize: number;
  private _keepalive: { interval: number; query: string; enabled: boolean };
  private _maxidle: number | null;
  private _logging: { level: string };
  private _pool: any[] = [];
  private _reserved: any[] = [];

  constructor(config: any) {
    this._url = config.url;
    this._props = (() => {
      const Properties = java.import("java.util.Properties");
      const properties = new Properties();

      for (const name in config.properties) {
        properties.putSync(name, config.properties[name]);
      }

      if (config.user && properties.getPropertySync("user") === null) {
        properties.putSync("user", config.user);
      }

      if (config.password && properties.getPropertySync("password") === null) {
        properties.putSync("password", config.password);
      }

      return properties;
    })();
    this._drivername = config.drivername || "";
    this._minpoolsize = config.minpoolsize || 1;
    this._maxpoolsize = config.maxpoolsize || 1;
    this._keepalive = config.keepalive || {
      interval: 60000,
      query: "select 1",
      enabled: false,
    };
    this._maxidle =
      !this._keepalive.enabled && config.maxidle ? config.maxidle : null;
    this._logging = config.logging || { level: "error" };
  }

  async status(): Promise<any> {
    const status: any = {};
    status.available = this._pool.length;
    status.reserved = this._reserved.length;
    status.pool = this.connStatus([], this._pool);
    status.rpool = this.connStatus([], this._reserved);
    return status;
  }

  private connStatus(acc: any[], pool: any[]): any[] {
    return pool.reduce((conns, connobj) => {
        const conn = connobj.conn;
        const closed = conn.isClosedSync();
        const readonly = conn.isReadOnlySync();
        const valid = conn.isValidSync(1000);
        
        conns.push({
            uuid: connobj.uuid,
            closed,
            readonly,
            valid,
        });
        
        return conns;
    }, acc);
}

private async _addConnectionsOnInitialize(): Promise<void> {
    const conns = await Promise.all(
        Array.from({ length: this._minpoolsize }, () =>
            addConnection(this._url, this._props, this._keepalive, this._maxidle)
        )
    );
    this._pool.push(...conns);
}




  async initialize() {
    try {
      // Initialize the driver
      if (this._drivername) {
        const driver = await new Promise((resolve, reject) => {
          resolve(java.newInstance(
            this._drivername
          ));
        });

        // Use the registerDriver method that returns a promise
        await dm.registerDriver(driver);
      }

      // Add connections after initialization
      await this._addConnectionsOnInitialize();
      Jinst.getInstance().events.emit("initialized");
    } catch (err) {
      console.error(err);
      throw err; // Rethrow the error for handling in the calling code
    }
  }

  async reserve(): Promise<{[key: string]: any,conn: Connection}> {
    this._closeIdleConnections();

    let conn = null;
    if (this._pool.length > 0) {
      conn = this._pool.shift();
      if (conn.lastIdle) {
        conn.lastIdle = new Date().getTime();
      }
      this._reserved.unshift(conn);
    } else if (this._reserved.length < this._maxpoolsize) {
      try {
        conn = addConnectionSync(
          this._url,
          this._props,
          this._keepalive,
          this._maxidle
        );
        this._reserved.unshift(conn);
      } catch (err) {
        console.error(err);
        conn = null;
        throw err;
      }
    }

    if (!conn) {
      conn = addConnectionSync(
        this._url,
        this._props,
        this._keepalive,
        this._maxidle
      );
    }

    return conn;
  }

  private _closeIdleConnections(): void {
    if (!this._maxidle) {
      return;
    }

    this.closeIdleConnectionsInArray(this._pool, this._maxidle);
    this.closeIdleConnectionsInArray(this._reserved, this._maxidle);
  }

  private closeIdleConnectionsInArray(array: any[], maxIdle: number): void {
    const time = new Date().getTime();
    const maxLastIdle = time - maxIdle;

    for (let i = array.length - 1; i >= 0; i--) {
      const conn = array[i];
      if (typeof conn === "object" && conn.conn !== null) {
        if (conn.lastIdle < maxLastIdle) {
          conn.conn.close((err: Error) => {});
          array.splice(i, 1);
        }
      }
    }
  }

  async release(conn: any): Promise<void> {
    if (typeof conn === "object") {
        const uuid = conn.uuid;

        // Use native filter instead of Lodash's reject
        this._reserved = this._reserved.filter(reservedConn => reservedConn.uuid !== uuid);

        if (conn.lastIdle) {
            conn.lastIdle = Date.now(); // Using Date.now() for better performance
        }
        
        this._pool.unshift(conn); // Add the connection back to the pool
    } else {
        throw new Error("INVALID CONNECTION");
    }
}

  async purge(): Promise<void> {
    const conns = [...this._pool, ...this._reserved];

    await Promise.all(
      conns.map((conn) => {
        if (typeof conn === "object" && conn.conn !== null) {
          return new Promise<void>((resolve) => {
            conn.conn.close(() => resolve());
          });
        } else {
          return Promise.resolve();
        }
      })
    );

    this._pool = [];
    this._reserved = [];
  }
}

export default Pool;
