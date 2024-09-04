import  Pool,{PoolConfig}  from "./pool";

class JDBC extends Pool {
  constructor(config: PoolConfig) {
    super(config);
  }
}

export default JDBC;
