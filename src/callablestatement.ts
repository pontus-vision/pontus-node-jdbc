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
        try {
          resolve(this._cs.getArraySync(arg1))
        } catch (error) {
          reject(error)
        }
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getBigDecimal(arg1: number | string): Promise<any> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<any>((resolve, reject) => {
        try {
          resolve(this._cs.getBigDecimalSync(arg1));
        } catch (error) {
          reject(error)
        }
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getBlob(arg1: number | string): Promise<any> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<any>((resolve, reject) => {
        try {
          resolve(this._cs.getBlobSync(arg1))
        } catch (error) {
          reject(error)
        }
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getBoolean(arg1: number | string): Promise<boolean> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<boolean>((resolve, reject) => {
        try {
          resolve(this._cs.getBooleanSync(arg1))
        } catch (error) {
          reject(error)
        }
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getByte(arg1: number | string): Promise<number> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<number>((resolve, reject) => {
        try {
          resolve(this._cs.getByteSync(arg1));
        } catch (error) {
          reject(error)
        }
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getBytes(arg1: number | string): Promise<Buffer> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<Buffer>((resolve, reject) => {
        try {
          resolve(this._cs.getBytesSync(arg1))
        } catch (error) {
          reject(error)
        };
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
        try {
          resolve(this._cs.getClobSync(arg1))
        } catch (error) {
          reject(error)
        }
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

        try {
          resolve(this._cs.getObjectSync(arg1));
          
        } catch (error) {
          reject(error) 
        }
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getString(arg1: number | string): Promise<string> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<string>((resolve, reject) => {
        try {
          resolve(this._cs.getString(arg1));
        } catch (error) {
          reject(error) 
        }
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getTime(arg1: number | string): Promise<any> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<any>((resolve, reject) => {
        try {
          resolve(this._cs.getTime(arg1));
          
        } catch (error) {
          reject(error)
        }
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }

  async getTimestamp(arg1: number | string): Promise<any> {
    if (typeof arg1 === "number" || typeof arg1 === "string") {
      return new Promise<any>((resolve, reject) => {
        try {
        resolve(this._cs.getTimestamp(arg1))  
        } catch (error) {
          reject(error)
        }
        ;
      });
    } else {
      throw new Error("INVALID ARGUMENTS");
    }
  }
}

