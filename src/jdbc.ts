import Pool, { PoolConfig } from "./pool.js";

class JDBC extends Pool {
  constructor(config: PoolConfig) {
    super(config);
  }
}

export default JDBC;
