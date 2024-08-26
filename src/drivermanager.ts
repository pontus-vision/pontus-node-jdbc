import _ from 'lodash';
import { jinst } from "./jinst";

const java = jinst.getInstance();

const DM = 'java.sql.DriverManager';

interface DriverManager {
  getConnection(url: string, propsoruser?: string | Record<string, any>, password?: string): Promise<any>;
  getConnectionSync(url: string, propsoruser?: string | Record<string, any>, password?: string): any;
  getLoginTimeout(): Promise<number>;
  registerDriver(driver: any): Promise<void>;
  setLoginTimeout(seconds: number): Promise<boolean>;
}

const driverManager: DriverManager = {
  async getConnection(url: string, propsoruser?: string | Record<string, any>, password?: string): Promise<any> {
    const args = [url, propsoruser, password].filter(arg => arg !== undefined);

    const validArgs = args[0] && (
      // propsoruser and password can both be falsey
      !(args[1] || args[2]) ||

      // propsoruser and password can both be strings
      (_.isString(args[1]) && _.isString(args[2])) ||

      // propsoruser can be an object if password is falsey
      (_.isObject(args[1]) && !args[2])
    );

    if (!validArgs) {
      throw new Error("INVALID ARGUMENTS");
    }

    return new Promise((resolve, reject) => {
      args.push((err: Error | null, conn: any) => {
        if (err) reject(err);
        else resolve(conn);
      });

      args.unshift('getConnection');
      args.unshift(DM);

      java.callStaticMethod.apply(java, args);
    });
  },

  getConnectionSync(url: string, propsoruser?: string | Record<string, any>, password?: string): any {
    const args = [url, propsoruser, password].filter(arg => arg !== undefined);

    const validArgs = args[0] && (
      !(args[1] || args[2]) ||
      (_.isString(args[1]) && _.isString(args[2])) ||
      (_.isObject(args[1]) && !args[2])
    );

    if (!validArgs) {
      throw new Error("INVALID ARGUMENTS");
    }

    args.unshift('getConnection');
    args.unshift(DM);

    return java.callStaticMethodSync.apply(java, args);
  },

  async getLoginTimeout(): Promise<number> {
    return new Promise((resolve, reject) => {
      java.callStaticMethod(DM, 'getLoginTimeout', (err: Error | null, seconds: number) => {
        if (err) reject(err);
        else resolve(seconds);
      });
    });
  },

  async registerDriver(driver: any): Promise<void> {
    return new Promise((resolve, reject) => {
      java.callStaticMethod(DM, 'registerDriver', driver, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  },

  async setLoginTimeout(seconds: number): Promise<boolean> {
    return new Promise((resolve, reject) => {
      java.callStaticMethod(DM, 'setLoginTimeout', seconds, (err: Error | null) => {
        if (err) reject(err);
        else resolve(true);
      });
    });
  },
};

export default driverManager;
