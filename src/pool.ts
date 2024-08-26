import _ from 'lodash';
import asyncjs from 'async';
import uuid from 'uuid';
import jinst from "./jinst";
import dm from './drivermanager';
import Connection from './connection';
import winston from 'winston';

const java = jinst.getInstance();

if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
}

const keepalive = async (conn: any, query: string) => {
  try {
    const statement = await conn.createStatement();
    await statement.execute(query);
    winston.silly("%s - Keep-Alive", new Date().toUTCString());
  } catch (err) {
    winston.error(err);
  }
};

const addConnection = async (url: string, props: any, ka: any, maxIdle: number | null, callback: (err: Error | null, conn: any) => void) => {
  try {
    const conn = await dm.getConnection(url, props);
    const connobj = {
      uuid: uuid.v4(),
      conn: new Connection(conn),
      keepalive: ka.enabled ? setInterval(keepalive, ka.interval, conn, ka.query) : false
    };

    if (maxIdle) {
      connobj.lastIdle = new Date().getTime();
    }

    callback(null, connobj);
  } catch (err) {
    callback(err);
  }
};

const addConnectionSync = (url: string, props: any, ka: any, maxIdle: number | null) => {
  const conn = dm.getConnectionSync(url, props);
  const connobj = {
    uuid: uuid.v4(),
    conn: new Connection(conn),
    keepalive: ka.enabled ? setInterval(keepalive, ka.interval, conn, ka.query) : false
  };

  if (maxIdle) {
    connobj.lastIdle = new Date().getTime();
  }

  return connobj;
};

class Pool {
  _url: string;
  _props: any;
  _drivername: string;
  _minpoolsize: number;
  _maxpoolsize: number;
  _keepalive: any;
  _maxidle: number | null;
  _logging: any;
  _pool: any[];
  _reserved: any[];

  constructor(config: any) {
    this._url = config.url;
    this._props = (function (config: any) {
      const Properties = java.import('java.util.Properties');
      const properties = new Properties();

      for(const name in config.properties) {
        properties.putSync(name, config.properties[name]);
      }

      // NOTE: https://docs.oracle.com/javase/7/docs/api/java/util/Properties.html#getProperty(java.lang.String)
      // if property does not exist it returns 'null' in the new java version, so we can use _.isNil to support
      // older versions as well

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

  static connStatus(acc: any[], pool: any[]) {
    return _.reduce(pool, (conns: any[], connobj: any) => {
      const conn = connobj.conn;
      const closed = conn.isClosedSync();
      const readonly = conn.isReadOnlySync();
      const valid = conn.isValidSync(1000);
      conns.push({
        uuid: connobj.uuid,
        closed: closed,
        readonly: readonly,
        valid: valid
      });
      return conns;
    }, acc);
  }

  status(callback: (err: Error | null, status: any) => void) {
    const self = this;
    const status = {};
    status.available = self._pool.length;
    status.reserved = self._reserved.length;
    status.pool = Pool.connStatus([], self._pool);
    status.rpool = Pool.connStatus([], self._reserved);
    callback(null, status);
  }

  async _addConnectionsOnInitialize(callback: (err: Error | null) => void) {
    try {
      const conns = await Promise.all(Array.from({ length: this._minpoolsize }, (_, index) => 
        addConnection(this._url, this._props, this._keepalive, this._maxidle, (err: Error | null, conn: any) => {
          if (err) {
            callback(err);
          } else {
            this._pool.push(conn);
            callback(null);
          }
        })
      ));
    } catch (err) {
      callback(err);
    }
  }

  async initialize(callback: (err: Error | null) => void) {
    const self = this;

    winston.level = this._logging.level;

    // If a drivername is supplied, initialize the via the old method,
    // Class.forName()
    if (this._drivername) {
      try {
        const driver = await java.newInstance(this._drivername);
        await dm.registerDriver(driver);
        await self._addConnectionsOnInitialize(callback);
      } catch (err) {
        callback(err);
      }
    }
    else {
      await self._addConnectionsOnInitialize(callback);
    }

    jinst.events.emit('initialized');
  }

  async reserve(callback: (err: Error | null, conn: any) => void) {
    const self = this;
    let conn = null;
    await self._closeIdleConnections();

    if (self._pool.length > 0 ) {
      conn = self._pool.shift();

      if (conn.lastIdle) {
        conn.lastIdle = new Date().getTime();
      }

      self._reserved.unshift(conn);
    } else if (self._reserved.length < self._maxpoolsize) {
      try {
        conn = addConnectionSync(self._url, self._props, self._keepalive, self._maxidle);
        self._reserved.unshift(conn);
      } catch (err) {
        winston.error(err);
        conn = null;
        return callback(err);
      }
    }

    if (conn === null) {
      callback(new Error("No more pool connections available"));
    } else {
      callback(null, conn);
    }
  }

  _closeIdleConnections() {
    if (! this._maxidle) {
      return;
    }

    const self = this;

    closeIdleConnectionsInArray(self._pool, this._maxidle);
    closeIdleConnectionsInArray(self._reserved, this._maxidle);
  }

  release(conn: any, callback: (err: Error | null) => void) {
    const self = this;
    if (typeof conn === 'object') {
      const uuid = conn.uuid;
      self._reserved = _.reject(self._reserved, function(conn) {
        return conn.uuid === uuid;
      });

      if (conn.lastIdle) {
        conn.lastIdle = new Date().getTime();
      }

      self._pool.unshift(conn);
      return callback(null);
    } else {
      return callback(new Error("INVALID CONNECTION"));
    }
  }

  async purge(callback: (err: Error | null) => void) {
    const self = this;
    const conns = self._pool.concat(self._reserved);

    try {
      await Promise.all(conns.map(conn => {
        if (typeof conn === 'object' && conn.conn !== null) {
          return conn.conn.close();
        }
      }));
      self._pool = [];
      self._reserved = [];

      callback(null);
    } catch (err) {
      callback(err);
    }
  }
}

function closeIdleConnectionsInArray(array: any[], maxIdle: number) {
  const time = new Date().getTime();
  const maxLastIdle = time - maxIdle;

  for (let i = array.length - 1; i >= 0; i--) {
    const conn = array[i];
    if (typeof conn === 'object' && conn.conn !== null) {
      if (conn.lastIdle < maxLastIdle) {
        conn.conn.close();
        array.splice(i, 1);
      }
    }
  }
}

export default Pool;
