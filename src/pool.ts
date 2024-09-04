/* jshint node: true */
"use strict";

import _ from 'lodash';
import { v4 as uuidv4 } from 'uuid';
import jinst from "./jinst";
import dm from './drivermanager';
import {Connection }from './connection';
import winston from 'winston';

const java = jinst.getInstance();

if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
}

async function keepalive(conn: any, query: string) {
  const statement = await conn.createStatement();
  await statement.execute(query);
  winston.silly("%s - Keep-Alive", new Date().toUTCString());
}

async function addConnection(url: string, props: any, ka: any, maxIdle: any): Promise<any> {
  try {
    const conn = await dm.getConnection(url, props);
    const connObj = {
      uuid: uuidv4(),
      conn: new Connection(conn),
      keepalive: ka.enabled ? setInterval(() => keepalive(conn, ka.query), ka.interval) : false
    };

    if (maxIdle) {
      connObj.lastIdle = new Date().getTime();
    }

    return connObj;
  } catch (err) {
    throw err;
  }
}

function addConnectionSync(url: string, props: any, ka: any, maxIdle: any): any {
  const conn = dm.getConnectionSync(url, props);
  const connObj = {
    uuid: uuidv4(),
    conn: new Connection(conn),
    keepalive: ka.enabled ? setInterval(() => keepalive(conn, ka.query), ka.interval) : false
  };

  if (maxIdle) {
    connObj.lastIdle = new Date().getTime();
  }

  return connObj;
}

class Pool {
  private _url: string;
  private _props: any;
  private _drivername: string;
  private _minpoolsize: number;
  private _maxpoolsize: number;
  private _keepalive: any;
  private _maxidle: number | null;
  private _logging: any;
  private _pool: any[];
  private _reserved: any[];

  constructor(config: any) {
    this._url = config.url;
    this._props = (() => {
      const Properties = java.import('java.util.Properties');
      const properties = new Properties();

      for (const name in config.properties) {
        properties.putSync(name, config.properties[name]);
      }

      if (config.user && _.isNil(properties.getPropertySync('user'))) {
        properties.putSync('user', config.user);
      }

      if (config.password && _.isNil(properties.getPropertySync('password'))) {
        properties.putSync('password', config.password);
      }

      return properties;
    })(config);

    this._drivername = config.drivername ? config.drivername : '';
    this._minpoolsize = config.minpoolsize ? config.minpoolsize : 1;
    this._maxpoolsize = config.maxpoolsize ? config.maxpoolsize : 1;
    this._keepalive = config.keepalive ? config.keepalive : {
      interval: 60000,
      query: 'select 1',
      enabled: false
    };
    this._maxidle = (!this._keepalive.enabled && config.maxidle) || null;
    this._logging = config.logging ? config.logging : {
      level: 'error'
    };
    this._pool = [];
    this._reserved = [];
  }

  private connStatus(acc: any, pool: any[]): any {
    return _.reduce(pool, (conns, connObj) => {
      const conn = connObj.conn;
      conns.push({
        uuid: connObj.uuid,
        closed: conn.isClosedSync(),
        readonly: conn.isReadOnlySync(),
        valid: conn.isValidSync(1000)
      });
      return conns;
    }, acc);
  }

  async status(): Promise<any> {
    const status = {
      available: this._pool.length,
      reserved: this._reserved.length,
      pool: this.connStatus([], this._pool),
      rpool: this.connStatus([], this._reserved)
    };

    return status;
  }

  private async _addConnectionsOnInitialize(): Promise<void> {
    const conns = await Promise.all(
      Array.from({ length: this._minpoolsize }, () => addConnection(this._url, this._props, this._keepalive, this._maxidle))
    );
    
    _.each(conns, (conn) => {
      this._pool.push(conn);
    });
  }

  async initialize(): Promise<void> {
    winston.level = this._logging.level;

    if (this._drivername) {
      const driver = await java.newInstance(this._drivername);
      await dm.registerDriver(driver);
      await this._addConnectionsOnInitialize();
    } else {
      await this._addConnectionsOnInitialize();
    }

    jinst.events.emit('initialized');
  }

  async reserve(): Promise<any> {
    this._closeIdleConnections();
    let conn: any = null;

    if (this._pool.length > 0) {
      conn = this._pool.shift();

      if (conn.lastIdle) {
        conn.lastIdle = new Date().getTime();
      }

      this._reserved.unshift(conn);
    } else if (this._reserved.length < this._maxpoolsize) {
      try {
        conn = addConnectionSync(this._url, this._props, this._keepalive, this._maxidle);
        this._reserved.unshift(conn);
      } catch (err) {
        winston.error(err);
        throw err;
      }
    }

    if (!conn) {
      throw new Error("No more pool connections available");
    }

    return conn;
  }

  private _closeIdleConnections(): void {
    if (!this._maxidle) {
      return;
    }

    this._closeIdleConnectionsInArray(this._pool, this._maxidle);
    this._closeIdleConnectionsInArray(this._reserved, this._maxidle);
  }

  private _closeIdleConnectionsInArray(array: any[], maxIdle: number): void {
    const time = new Date().getTime();
    const maxLastIdle = time - maxIdle;

    for (let i = array.length - 1; i >= 0; i--) {
      const conn = array[i];
      if (typeof conn === 'object' && conn.conn !== null) {
        if (conn.lastIdle < maxLastIdle) {
          conn.conn.close(() => { });
          array.splice(i, 1);
        }
      }
    }
  }

  async release(conn: any): Promise<void> {
    if (typeof conn === 'object') {
      const uuid = conn.uuid;
      this._reserved = _.reject(this._reserved, (c) => c.uuid === uuid);

      if (conn.lastIdle) {
        conn.lastIdle = new Date().getTime();
      }

      this._pool.unshift(conn);
    } else {
      throw new Error("INVALID CONNECTION");
    }
  }

  async purge(): Promise<void> {
    const conns = this._pool.concat(this._reserved);

    await Promise.all(conns.map(async (conn) => {
      if (typeof conn === 'object' && conn.conn !== null) {
        await conn.conn.close();
      }
    }));

    this._pool = [];
    this._reserved = [];
  }
}

export default Pool;
