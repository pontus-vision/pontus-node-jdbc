import _ from "lodash";
import PreparedStatement from "./preparedstatement.js";

export class CallableStatement extends PreparedStatement {
  private _cs: any;

  constructor(cs: any) {
    super(cs);
    this._cs = cs;
  }

  async getArray(arg1: number | string): Promise<any> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<any>((resolve, reject) => {
        this._cs.getArray(arg1, (err: Error | null, result: any) => {
          if (err) reject(err);
          else resolve(result);
        });
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getBigDecimal(arg1: number | string): Promise<any> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<any>((resolve, reject) => {
        this._cs.getBigDecimal(arg1, (err: Error | null, result: any) => {
          if (err) reject(err);
          else resolve(result);
        });
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getBlob(arg1: number | string): Promise<any> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<any>((resolve, reject) => {
        this._cs.getBlob(arg1, (err: Error | null, result: any) => {
          if (err) reject(err);
          else resolve(result);
        });
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getBoolean(arg1: number | string): Promise<boolean> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<boolean>((resolve, reject) => {
        this._cs.getBoolean(arg1, (err: Error | null, result: boolean) => {
          if (err) reject(err);
          else resolve(result);
        });
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getByte(arg1: number | string): Promise<number> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<number>((resolve, reject) => {
        this._cs.getByte(arg1, (err: Error | null, result: number) => {
          if (err) reject(err);
          else resolve(result);
        });
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getBytes(arg1: number | string): Promise<Buffer> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<Buffer>((resolve, reject) => {
        this._cs.getBytes(arg1, (err: Error | null, result: Buffer) => {
          if (err) reject(err);
          else resolve(result);
        });
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getCharacterStream(arg1: number | string): Promise<any> {
    throw new Error("NOT IMPLEMENTED");
  }

  async getClob(arg1: number | string): Promise<any> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<any>((resolve, reject) => {
        this._cs.getClob(arg1, (err: Error | null, result: any) => {
          if (err) reject(err);
          else resolve(result);
        });
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getDate(arg1: number | string): Promise<Date> {
    // Placeholder implementation
    return new Date();
  }

  async getObject(arg1: number | string): Promise<any> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<any>((resolve, reject) => {
        this._cs.getObject(arg1, (err: Error | null, result: any) => {
          if (err) reject(err);
          else resolve(result);
        });
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getString(arg1: number | string): Promise<string> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<string>((resolve, reject) => {
        this._cs.getString(arg1, (err: Error | null, result: string) => {
          if (err) reject(err);
          else resolve(result);
        });
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getTime(arg1: number | string): Promise<any> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<any>((resolve, reject) => {
        this._cs.getTime(arg1, (err: Error | null, result: any) => {
          if (err) reject(err);
          else resolve(result);
        });
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getTimestamp(arg1: number | string): Promise<any> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<any>((resolve, reject) => {
        this._cs.getTimestamp(arg1, (err: Error | null, result: any) => {
          if (err) reject(err);
          else resolve(result);
        });
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }
}

