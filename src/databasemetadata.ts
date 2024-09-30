import _ from "lodash";
import ResultSet from "./resultset";
import Connection from "./connection";
import jinst from "./jinst";

const java = jinst.getInstance();

// Assuming these types are defined elsewhere in your codebase
type Callback<T> = (err: Error | null, result: T) => void;
enum RowIdLifetime {
  ROWID_UNSUPPORTED = 0, // The database does not support ROWIDs
  ROWID_VALID_FOREVER = 1, // The ROWID is valid forever
  ROWID_VALID_SESSION = 2, // The ROWID is valid for the duration of the session
  ROWID_VALID_TRANSACTION = 3, // The ROWID is valid for the duration of the transaction
  ROWID_VALID_OTHER = 4, // The ROWID is valid for some other duration
}
interface IDatabaseMetaData {
  getSchemas(
    catalog?: string | null,
    schemaPattern?: string | null
  ): Promise<ResultSet>;
  getTables(
    catalog?: string | null,
    schemaPattern?: string | null,
    tableNamePattern?: string | null,
    types?: string[] | null
  ): Promise<ResultSet>;
  // allProceduresAreCallable: (
  //   callback: (err: Error | null, result: any) => void
  // ) => void;
  // Define more methods here as needed
}

class DatabaseMetaData implements IDatabaseMetaData {
  private _dbm: any;

  constructor(dbm: any) {
    this._dbm = dbm;
  }

  static async initialize() {
    const staticAttrs = [
      "attributeNoNulls",
      "attributeNullable",
      "attributeNullableUnknown",
      "bestRowNotPseudo",
      // Add all other static fields as needed
    ];

    for (const attr of staticAttrs) {
      (DatabaseMetaData as any)[attr] = await java.getStaticFieldValue(
        "java.sql.DatabaseMetaData",
        attr
      );
    }
  }

  async getSchemas(
    catalog: string | null = null,
    schemaPattern: string | null = null
  ): Promise<ResultSet> {
    if (!_.isNull(catalog) && !_.isString(catalog)) {
      throw new Error("INVALID_ARGUMENTS: catalog must be a string or null.");
    }
    if (!_.isNull(schemaPattern) && !_.isString(schemaPattern)) {
      throw new Error(
        "INVALID_ARGUMENTS: schemaPattern must be a string or null."
      );
    }

    return new Promise((resolve, reject) => {
      this._dbm.getSchemas(
        catalog,
        schemaPattern,
        (err: Error, result: any) => {
          if (err) {
            return reject(err);
          }
          resolve(new ResultSet(result));
        }
      );
    });
  }

  async getTables(
    catalog: string | null = null,
    schemaPattern: string | null = null,
    tableNamePattern: string | null = null,
    types: string[] | null = null
  ): Promise<ResultSet> {
    if (!_.isNull(catalog) && !_.isString(catalog)) {
      throw new Error("INVALID_ARGUMENTS: catalog must be a string or null.");
    }
    if (!_.isNull(schemaPattern) && !_.isString(schemaPattern)) {
      throw new Error(
        "INVALID_ARGUMENTS: schemaPattern must be a string or null."
      );
    }
    if (!_.isNull(tableNamePattern) && !_.isString(tableNamePattern)) {
      throw new Error(
        "INVALID_ARGUMENTS: tableNamePattern must be a string or null."
      );
    }
    if (!_.isNull(types) && !_.isArray(types)) {
      throw new Error("INVALID_ARGUMENTS: types must be an array or null.");
    }
    if (_.isArray(types)) {
      for (const type of types) {
        if (!_.isString(type)) {
          throw new Error(
            "INVALID_ARGUMENTS: all elements in types array must be strings."
          );
        }
      }
    }

    return new Promise((resolve, reject) => {
      this._dbm.getTables(
        catalog,
        schemaPattern,
        tableNamePattern,
        types,
        (err: Error, result: any) => {
          if (err) {
            return reject(err);
          }
          resolve(new ResultSet(result));
        }
      );
    });
  }
  async allProceduresAreCallable(): Promise<any> {
    return new Promise((resolve, reject) => {
      this._dbm.allProceduresAreCallable((err: Error | null, result: any) => {
        if (err) {
          return reject(err);
        }
        return resolve(result);
      });
    });
  }

  async allTablesAreSelectable(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.allTablesAreSelectable((err: Error | null, result: boolean) => {
        if (err) {
          return reject(err);
        }
        return resolve(result);
      });
    });
  }

  async autoCommitFailureClosesAllResultSets(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.autoCommitFailureClosesAllResultSets(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          return resolve(result);
        }
      );
    });
  }

  async dataDefinitionCausesTransactionCommit(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.dataDefinitionCausesTransactionCommit(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          return resolve(result);
        }
      );
    });
  }

  async dataDefinitionIgnoredInTransactions(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.dataDefinitionIgnoredInTransactions(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          return resolve(result);
        }
      );
    });
  }

  async deletesAreDetected(type: number): Promise<boolean> {
    return new Promise((resolve, reject) => {
      if (!Number.isInteger(type)) {
        return reject(new Error("INVALID ARGUMENTS"));
      }

      this._dbm.deletesAreDetected(
        type,
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          return resolve(result);
        }
      );
    });
  }

  async doesMaxRowSizeIncludeBlobs(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.doesMaxRowSizeIncludeBlobs(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          return resolve(result);
        }
      );
    });
  }
  // Promisified version of generatedKeyAlwaysReturned
  async generatedKeyAlwaysReturned(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.generatedKeyAlwaysReturned(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  // Promisified version of getAttributes
  async getAttributes(
    catalog: string | null | undefined,
    schemaPattern: string | null | undefined,
    typeNamePattern: string | null | undefined,
    attributeNamePattern: string | null | undefined
  ): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      if (
        (catalog === null ||
          catalog === undefined ||
          typeof catalog === "string") &&
        (schemaPattern === null ||
          schemaPattern === undefined ||
          typeof schemaPattern === "string") &&
        (typeNamePattern === null ||
          typeNamePattern === undefined ||
          typeof typeNamePattern === "string") &&
        (attributeNamePattern === null ||
          attributeNamePattern === undefined ||
          typeof attributeNamePattern === "string")
      ) {
        this._dbm.getAttributes(
          catalog,
          schemaPattern,
          typeNamePattern,
          attributeNamePattern,
          (err: Error | null, result: any) => {
            if (err) {
              return reject(err);
            }
            resolve(new ResultSet(result));
          }
        );
      } else {
        reject(new Error("INVALID ARGUMENTS"));
      }
    });
  }

  // Promisified version of getBestRowIdentifier
  async getBestRowIdentifier(
    catalog: string | null | undefined,
    schema: string | null | undefined,
    table: string,
    scope: number,
    nullable: boolean
  ): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      if (
        (catalog === null ||
          catalog === undefined ||
          typeof catalog === "string") &&
        (schema === null ||
          schema === undefined ||
          typeof schema === "string") &&
        typeof table === "string" &&
        Number.isInteger(scope) &&
        typeof nullable === "boolean"
      ) {
        this._dbm.getBestRowIdentifier(
          catalog,
          schema,
          table,
          scope,
          nullable,
          (err: Error | null, result: any) => {
            if (err) {
              return reject(err);
            }
            resolve(new ResultSet(result));
          }
        );
      } else {
        reject(new Error("INVALID ARGUMENTS"));
      }
    });
  }

  // Promisified version of getCatalogs
  async getCatalogs(): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      this._dbm.getCatalogs((err: Error | null, result: any) => {
        if (err) {
          return reject(err);
        }
        resolve(new ResultSet(result));
      });
    });
  }

  // Promisified version of getCatalogSeparator
  async getCatalogSeparator(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getCatalogSeparator((err: Error | null, result: string) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  // Promisified version of getCatalogTerm
  async getCatalogTerm(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getCatalogTerm((err: Error | null, result: string) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  // Promisified version of getClientInfoProperties
  async getClientInfoProperties(): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      this._dbm.getClientInfoProperties((err: Error | null, result: any) => {
        if (err) {
          return reject(err);
        }
        resolve(new ResultSet(result));
      });
    });
  }

  // Promisified version of getColumnPrivileges
  async getColumnPrivileges(
    catalog: string | null | undefined,
    schema: string | null | undefined,
    table: string,
    columnNamePattern: string | null | undefined
  ): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      if (
        (catalog === null ||
          catalog === undefined ||
          typeof catalog === "string") &&
        (schema === null ||
          schema === undefined ||
          typeof schema === "string") &&
        typeof table === "string" &&
        (columnNamePattern === null ||
          columnNamePattern === undefined ||
          typeof columnNamePattern === "string")
      ) {
        this._dbm.getColumnPrivileges(
          catalog,
          schema,
          table,
          columnNamePattern,
          (err: Error | null, result: any) => {
            if (err) {
              return reject(err);
            }
            resolve(new ResultSet(result));
          }
        );
      } else {
        reject(new Error("INVALID ARGUMENTS"));
      }
    });
  }

  // Promisified version of getColumns
  async getColumns(
    catalog: string | null | undefined,
    schemaPattern: string | null | undefined,
    tableNamePattern: string | null | undefined,
    columnNamePattern: string | null | undefined
  ): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      if (
        (catalog === null ||
          catalog === undefined ||
          typeof catalog === "string") &&
        (schemaPattern === null ||
          schemaPattern === undefined ||
          typeof schemaPattern === "string") &&
        (tableNamePattern === null ||
          tableNamePattern === undefined ||
          typeof tableNamePattern === "string") &&
        (columnNamePattern === null ||
          columnNamePattern === undefined ||
          typeof columnNamePattern === "string")
      ) {
        this._dbm.getColumns(
          catalog,
          schemaPattern,
          tableNamePattern,
          columnNamePattern,
          (err: Error | null, result: any) => {
            if (err) {
              return reject(err);
            }
            resolve(new ResultSet(result));
          }
        );
      } else {
        reject(new Error("INVALID ARGUMENTS"));
      }
    });
  }

  // Promisified version of getConnection
  async getConnection(): Promise<Connection> {
    return new Promise((resolve, reject) => {
      this._dbm.getConnection((err: Error | null, result: any) => {
        if (err) {
          return reject(err);
        }
        resolve(new Connection(result));
      });
    });
  }

  async getCrossReference(
    parentCatalog: string | null | undefined,
    parentSchema: string | null | undefined,
    parentTable: string,
    foreignCatalog: string | null | undefined,
    foreignSchema: string | null | undefined,
    foreignTable: string
  ): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      if (
        (parentCatalog === null || typeof parentCatalog === "string") &&
        (parentSchema === null || typeof parentSchema === "string") &&
        typeof parentTable === "string" &&
        (foreignCatalog === null || typeof foreignCatalog === "string") &&
        (foreignSchema === null || typeof foreignSchema === "string") &&
        typeof foreignTable === "string"
      ) {
        this._dbm.getCrossReference(
          parentCatalog,
          parentSchema,
          parentTable,
          foreignCatalog,
          foreignSchema,
          foreignTable,
          (err: Error | null, result: any) => {
            if (err) {
              return reject(err);
            }
            resolve(new ResultSet(result));
          }
        );
      } else {
        reject(new Error("INVALID ARGUMENTS"));
      }
    });
  }

  // Promisified version of getDatabaseMajorVersion
  async getDatabaseMajorVersion(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getDatabaseMajorVersion((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  // Promisified version of getDatabaseMinorVersion
  async getDatabaseMinorVersion(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getDatabaseMinorVersion((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  // Promisified version of getDatabaseProductName
  async getDatabaseProductName(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getDatabaseProductName((err: Error | null, result: string) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  // Promisified version of getDatabaseProductVersion
  async getDatabaseProductVersion(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getDatabaseProductVersion(
        (err: Error | null, result: string) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  // Promisified version of getDefaultTransactionIsolation
  async getDefaultTransactionIsolation(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getDefaultTransactionIsolation(
        (err: Error | null, result: number) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  // Promisified version of getDriverMajorVersion
  async getDriverMajorVersion(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getDriverMajorVersion((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  // Promisified version of getDriverMinorVersion
  async getDriverMinorVersion(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getDriverMinorVersion((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  // Promisified version of getDriverName
  async getDriverName(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getDriverName((err: Error | null, result: string) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  // Promisified version of getDriverVersion
  async getDriverVersion(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getDriverVersion((err: Error | null, result: string) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves a description of the foreign key columns that reference the given
   * table's primary key columns (the foreign keys exported by a table).
   *
   * @param catalog - A catalog name.
   * @param schema - A schema name.
   * @param table - A table name.
   * @returns A promise that resolves to a ResultSet.
   */
  async getExportedKeys(
    catalog: string | null,
    schema: string | null,
    table: string
  ): Promise<ResultSet> {
    const validParams =
      (catalog === null ||
        catalog === undefined ||
        typeof catalog === "string") &&
      (schema === null || schema === undefined || typeof schema === "string") &&
      typeof table === "string";

    if (!validParams) {
      throw new Error("INVALID ARGUMENTS");
    }

    return new Promise((resolve, reject) => {
      this._dbm.getExportedKeys(
        catalog,
        schema,
        table,
        (err: Error | null, result: any) => {
          if (err) {
            return reject(err);
          }
          resolve(new ResultSet(result));
        }
      );
    });
  }

  /**
   * Retrieves all the "extra" characters that can be used in unquoted identifier
   * names (those beyond a-z, A-Z, 0-9 and _).
   *
   * @returns A promise that resolves to the extra name characters string.
   */
  async getExtraNameCharacters(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getExtraNameCharacters((err: Error | null, result: string) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves a description of the given catalog's system or user function
   * parameters and return type.
   *
   * @param catalog - A catalog name.
   * @param schemaPattern - A schema name pattern.
   * @param functionNamePattern - A function name pattern.
   * @param columnNamePattern - A column name pattern.
   * @returns A promise that resolves to a ResultSet.
   */
  async getFunctionColumns(
    catalog: string | null,
    schemaPattern: string | null,
    functionNamePattern: string | null,
    columnNamePattern: string | null
  ): Promise<ResultSet> {
    const validParams =
      (catalog === null ||
        catalog === undefined ||
        typeof catalog === "string") &&
      (schemaPattern === null ||
        schemaPattern === undefined ||
        typeof schemaPattern === "string") &&
      (functionNamePattern === null ||
        functionNamePattern === undefined ||
        typeof functionNamePattern === "string") &&
      (columnNamePattern === null ||
        columnNamePattern === undefined ||
        typeof columnNamePattern === "string");

    if (!validParams) {
      throw new Error("INVALID ARGUMENTS");
    }

    return new Promise((resolve, reject) => {
      this._dbm.getFunctionColumns(
        catalog,
        schemaPattern,
        functionNamePattern,
        columnNamePattern,
        (err: Error | null, result: any) => {
          if (err) {
            return reject(err);
          }
          resolve(new ResultSet(result));
        }
      );
    });
  }

  /**
   * Retrieves a description of the system and user functions available in the
   * given catalog.
   *
   * @param catalog - A catalog name.
   * @param schemaPattern - A schema name pattern.
   * @param functionNamePattern - A function name pattern.
   * @returns A promise that resolves to a ResultSet.
   */
  async getFunctions(
    catalog: string | null,
    schemaPattern: string | null,
    functionNamePattern: string | null
  ): Promise<ResultSet> {
    const validParams =
      (catalog === null ||
        catalog === undefined ||
        typeof catalog === "string") &&
      (schemaPattern === null ||
        schemaPattern === undefined ||
        typeof schemaPattern === "string") &&
      (functionNamePattern === null ||
        functionNamePattern === undefined ||
        typeof functionNamePattern === "string");

    if (!validParams) {
      throw new Error("INVALID ARGUMENTS");
    }

    return new Promise((resolve, reject) => {
      this._dbm.getFunctions(
        catalog,
        schemaPattern,
        functionNamePattern,
        (err: Error | null, result: any) => {
          if (err) {
            return reject(err);
          }
          resolve(new ResultSet(result));
        }
      );
    });
  }

  /**
   * Retrieves the string used to quote SQL identifiers.
   *
   * @returns A promise that resolves to the identifier quote string.
   */
  async getIdentifierQuoteString(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getIdentifierQuoteString(
        (err: Error | null, result: string) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  /**
   * Retrieves a description of the primary key columns that are referenced by
   * the given table's foreign key columns (the primary keys imported by a table).
   *
   * @param catalog - A catalog name.
   * @param schema - A schema name.
   * @param table - A table name.
   * @returns A promise that resolves to a ResultSet.
   */
  async getImportedKeys(
    catalog: string | null,
    schema: string | null,
    table: string
  ): Promise<ResultSet> {
    const validParams =
      (catalog === null ||
        catalog === undefined ||
        typeof catalog === "string") &&
      (schema === null || schema === undefined || typeof schema === "string") &&
      typeof table === "string";

    if (!validParams) {
      throw new Error("INVALID ARGUMENTS");
    }

    return new Promise((resolve, reject) => {
      this._dbm.getImportedKeys(
        catalog,
        schema,
        table,
        (err: Error | null, result: any) => {
          if (err) {
            return reject(err);
          }
          resolve(new ResultSet(result));
        }
      );
    });
  }

  /**
   * Retrieves a description of the given table's indices and statistics.
   *
   * @param catalog - A catalog name.
   * @param schema - A schema name.
   * @param table - A table name.
   * @param unique - When true, return only indices for unique values.
   * @param approximate - When true, result is allowed to reflect approximate or out-of-date values.
   * @returns A promise that resolves to a ResultSet.
   */
  async getIndexInfo(
    catalog: string | null,
    schema: string | null,
    table: string,
    unique: boolean,
    approximate: boolean
  ): Promise<ResultSet> {
    const validParams =
      (catalog === null ||
        catalog === undefined ||
        typeof catalog === "string") &&
      (schema === null || schema === undefined || typeof schema === "string") &&
      typeof table === "string" &&
      typeof unique === "boolean" &&
      typeof approximate === "boolean";

    if (!validParams) {
      throw new Error("INVALID ARGUMENTS");
    }

    return new Promise((resolve, reject) => {
      this._dbm.getIndexInfo(
        catalog,
        schema,
        table,
        unique,
        approximate,
        (err: Error | null, result: any) => {
          if (err) {
            return reject(err);
          }
          resolve(new ResultSet(result));
        }
      );
    });
  }

  /**
   * Retrieves the major JDBC version number for this driver.
   *
   * @returns A promise that resolves to the JDBC major version number.
   */
  async getJDBCMajorVersion(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getJDBCMajorVersion((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the minor JDBC version number for this driver.
   *
   * @returns A promise that resolves to the JDBC minor version number.
   */
  async getJDBCMinorVersion(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getJDBCMinorVersion((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the maximum number of hex characters this database allows in an
   * inline binary literal.
   *
   * @returns A promise that resolves to the maximum binary literal length.
   */
  async getMaxBinaryLiteralLength(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxBinaryLiteralLength(
        (err: Error | null, result: number) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  /**
   * Retrieves the maximum number of characters that this database allows in a
   * catalog name.
   *
   * @returns A promise that resolves to the maximum catalog name length.
   */
  async getMaxCatalogNameLength(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxCatalogNameLength((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the maximum number of characters this database allows for a
   * character literal.
   *
   * @returns A promise that resolves to the maximum character literal length.
   */
  async getMaxCharLiteralLength(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxCharLiteralLength((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the maximum number of characters this database allows for a column
   * name.
   *
   * @returns A promise that resolves to the maximum column name length.
   */
  async getMaxColumnNameLength(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxColumnNameLength((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the maximum number of columns this database allows in a GROUP BY
   * clause.
   *
   * @returns A promise that resolves to the maximum number of columns in GROUP BY.
   */
  async getMaxColumnsInGroupBy(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxColumnsInGroupBy((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the maximum number of columns this database allows in an index.
   *
   * @returns A promise that resolves to the maximum number of columns in an index.
   */
  async getMaxColumnsInIndex(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxColumnsInIndex((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the maximum number of columns this database allows in an ORDER BY
   * clause.
   *
   * @returns A promise that resolves to the maximum number of columns in ORDER BY.
   */
  async getMaxColumnsInOrderBy(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxColumnsInOrderBy((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the maximum number of columns this database allows in a SELECT
   * list.
   *
   * @returns A promise that resolves to the maximum number of columns in SELECT.
   */
  async getMaxColumnsInSelect(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxColumnsInSelect((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the maximum number of columns this database allows in a table.
   *
   * @returns A promise that resolves to the maximum number of columns in a table.
   */
  async getMaxColumnsInTable(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxColumnsInTable((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the maximum number of concurrent connections to this database that
   * are possible.
   *
   * @returns A promise that resolves to the maximum number of connections.
   */
  async getMaxConnections(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxConnections((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the maximum number of characters that this database allows in a
   * cursor name.
   *
   * @returns A promise that resolves to the maximum cursor name length.
   */
  async getMaxCursorNameLength(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxCursorNameLength((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the maximum number of bytes this database allows for an index,
   * including all of the parts of the index.
   *
   * @returns A promise that resolves to the maximum index length.
   */
  async getMaxIndexLength(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxIndexLength((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the maximum number of characters that this database allows in a
   * procedure name.
   *
   * @returns A promise that resolves to the maximum procedure name length.
   */
  async getMaxProcedureNameLength(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxProcedureNameLength(
        (err: Error | null, result: number) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  /**
   * Retrieves the maximum number of bytes this database allows in a single row.
   *
   * @returns A promise that resolves to the maximum row size.
   */
  async getMaxRowSize(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxRowSize((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the maximum number of characters that this database allows in a
   * schema name.
   *
   * @returns A promise that resolves to the maximum schema name length.
   */
  async getMaxSchemaNameLength(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxSchemaNameLength((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the maximum number of characters this database allows in an SQL
   * statement.
   *
   * @returns A promise that resolves to the maximum SQL statement length.
   */
  async getMaxStatementLength(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxStatementLength((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Retrieves the maximum number of active statements that can be open at the same time.
   *
   * @returns A promise that resolves to the maximum number of statements that can be open at one time.
   */
  async getMaxStatements(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxStatements((err: Error | null, result: number) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }
  /**
   * Retrieves the maximum number of characters allowed in a table name.
   *
   * @returns A promise that resolves to the maximum number of characters allowed for a table name.
   */
  getMaxTableNameLength(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxTableNameLength((err: Error | null, result: number) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Retrieves the maximum number of tables this database allows in a SELECT statement.
   *
   * @returns A promise that resolves to the maximum number of tables allowed in a SELECT statement.
   */
  getMaxTablesInSelect(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxTablesInSelect((err: Error | null, result: number) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Retrieves the maximum number of characters allowed in a user name.
   *
   * @returns A promise that resolves to the maximum number of characters allowed for a user name.
   */
  getMaxUserNameLength(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getMaxUserNameLength((err: Error | null, result: number) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Retrieves a comma-separated list of math functions available with this database.
   *
   * @returns A promise that resolves to the list of math functions supported by this database.
   */
  getNumericFunctions(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getNumericFunctions((err: Error | null, result: string) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Retrieves a description of the given table's primary key columns.
   *
   * @param catalog - A catalog name; must match the catalog name as it is stored in this database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
   * @param schema - A schema name; must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
   * @param table - A table name; must match the table name as it is stored in this database
   * @returns A promise that resolves to a ResultSet describing the primary key columns of the table.
   */
  getPrimaryKeys(
    catalog: string | null,
    schema: string | null,
    table: string
  ): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      const validParams =
        (catalog === null ||
          catalog === undefined ||
          typeof catalog === "string") &&
        (schema === null ||
          schema === undefined ||
          typeof schema === "string") &&
        typeof table === "string";

      if (!validParams) {
        reject(new Error("INVALID ARGUMENTS"));
        return;
      }

      this._dbm.getPrimaryKeys(
        catalog,
        schema,
        table,
        (err: Error, result: any) => {
          if (err) {
            reject(err);
          } else {
            resolve(new ResultSet(result));
          }
        }
      );
    });
  }

  /**
   * Retrieves a description of the given catalog's stored procedure parameter and result columns.
   *
   * @param catalog - A catalog name; must match the catalog name as it is stored in this database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
   * @param schemaPattern - A schema pattern; must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
   * @param procedureNamePattern - A procedure name pattern; must match the procedure name as it is stored in the database; "" retrieves those without a procedure; null means that the procedure name should not be used to narrow the search
   * @param columnNamePattern - A column name pattern; must match the column name as it is stored in the database; "" retrieves those without a column; null means that the column name should not be used to narrow the search
   * @returns A promise that resolves to a ResultSet describing the procedure columns.
   */
  getProcedureColumns(
    catalog: string | null,
    schemaPattern: string | null,
    procedureNamePattern: string | null,
    columnNamePattern: string | null
  ): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      const validParams =
        (catalog === null ||
          catalog === undefined ||
          typeof catalog === "string") &&
        (schemaPattern === null ||
          schemaPattern === undefined ||
          typeof schemaPattern === "string") &&
        (procedureNamePattern === null ||
          procedureNamePattern === undefined ||
          typeof procedureNamePattern === "string") &&
        (columnNamePattern === null ||
          columnNamePattern === undefined ||
          typeof columnNamePattern === "string");

      if (!validParams) {
        reject(new Error("INVALID ARGUMENTS"));
        return;
      }

      this._dbm.getProcedureColumns(
        catalog,
        schemaPattern,
        procedureNamePattern,
        columnNamePattern,
        (err: Error, result: any) => {
          if (err) {
            reject(err);
          } else {
            resolve(new ResultSet(result));
          }
        }
      );
    });
  }

  /**
   * Retrieves a description of the stored procedures available in the given catalog.
   *
   * @param catalog - A catalog name; must match the catalog name as it is stored in this database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
   * @param schemaPattern - A schema pattern; must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
   * @param procedureNamePattern - A procedure name pattern; must match the procedure name as it is stored in the database; "" retrieves those without a procedure; null means that the procedure name should not be used to narrow the search
   * @returns A promise that resolves to a ResultSet describing the procedures.
   */
  getProcedures(
    catalog: string | null,
    schemaPattern: string | null,
    procedureNamePattern: string | null
  ): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      const validParams =
        (catalog === null ||
          catalog === undefined ||
          typeof catalog === "string") &&
        (schemaPattern === null ||
          schemaPattern === undefined ||
          typeof schemaPattern === "string") &&
        (procedureNamePattern === null ||
          procedureNamePattern === undefined ||
          typeof procedureNamePattern === "string");

      if (!validParams) {
        reject(new Error("INVALID ARGUMENTS"));
        return;
      }

      this._dbm.getProcedures(
        catalog,
        schemaPattern,
        procedureNamePattern,
        (err: Error, result: any) => {
          if (err) {
            reject(err);
          } else {
            resolve(new ResultSet(result));
          }
        }
      );
    });
  }

  /**
   * Retrieves the database vendor's preferred term for "procedure".
   *
   * @returns A promise that resolves to the database vendor's preferred term for "procedure".
   */
  getProcedureTerm(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getProcedureTerm((err: Error, result: string) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Retrieves a description of the pseudo or hidden columns available in a given table.
   *
   * @param catalog - A catalog name; must match the catalog name as it is stored in this database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
   * @param schemaPattern - A schema pattern; must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
   * @param tableNamePattern - A table name pattern; must match the table name as it is stored in this database; "" retrieves those without a table; null means that the table name should not be used to narrow the search
   * @param columnNamePattern - A column name pattern; must match the column name as it is stored in the database; "" retrieves those without a column; null means that the column name should not be used to narrow the search
   * @returns A promise that resolves to a ResultSet describing the pseudo columns.
   */
  getPseudoColumns(
    catalog: string | null,
    schemaPattern: string | null,
    tableNamePattern: string | null,
    columnNamePattern: string | null
  ): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      const validParams =
        (catalog === null ||
          catalog === undefined ||
          typeof catalog === "string") &&
        (schemaPattern === null ||
          schemaPattern === undefined ||
          typeof schemaPattern === "string") &&
        (tableNamePattern === null ||
          tableNamePattern === undefined ||
          typeof tableNamePattern === "string") &&
        (columnNamePattern === null ||
          columnNamePattern === undefined ||
          typeof columnNamePattern === "string");

      if (!validParams) {
        reject(new Error("INVALID ARGUMENTS"));
        return;
      }

      this._dbm.getPseudoColumns(
        catalog,
        schemaPattern,
        tableNamePattern,
        columnNamePattern,
        (err: Error, result: any) => {
          if (err) {
            reject(err);
          } else {
            resolve(new ResultSet(result));
          }
        }
      );
    });
  }

  /**
   * Retrieves this database's default holdability for ResultSet objects.
   *
   * @returns A promise that resolves to the database's default holdability for ResultSet objects.
   */
  getResultSetHoldability(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getResultSetHoldability((err: Error, result: number) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Indicates whether this data source supports the SQL ROWID type.
   *
   * @returns A promise that resolves to the RowIdLifetime indicating if the data source supports the SQL ROWID type.
   */
  getRowIdLifetime(): Promise<RowIdLifetime> {
    return new Promise((resolve, reject) => {
      this._dbm.getRowIdLifetime((err: Error, result: RowIdLifetime) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Retrieves the database vendor's preferred term for "schema".
   *
   * @returns A promise that resolves to the database vendor's preferred term for "schema".
   */
  getSchemaTerm(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getSchemaTerm((err: Error, result: string) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Retrieves the string that can be used to escape wildcard characters.
   *
   * @returns A promise that resolves to the string used to escape wildcard characters.
   */
  getSearchStringEscape(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getSearchStringEscape((err: Error, result: string) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Retrieves a comma-separated list of this database's SQL keywords.
   *
   * @returns A promise that resolves to the comma-separated list of SQL keywords.
   */
  getSQLKeywords(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getSQLKeywords((err: Error, result: string) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Retrieves the type of SQLSTATE returned by SQLException.getSQLState.
   *
   * @returns A promise that resolves to the type of SQLSTATE.
   */
  getSQLStateType(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getSQLStateType((err: Error, result: number) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Retrieves a comma-separated list of string functions available in this database.
   *
   * @returns A promise that resolves to the comma-separated list of string functions.
   */
  getStringFunctions(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getStringFunctions((err: Error, result: string) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Retrieves a description of table hierarchies in a particular schema.
   *
   * @param catalog - A catalog name; must match the catalog name as it is stored in this database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
   * @param schemaPattern - A schema pattern; must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
   * @param tableNamePattern - A table name pattern; must match the table name as it is stored in this database; "" retrieves those without a table; null means that the table name should not be used to narrow the search
   * @returns A promise that resolves to a ResultSet describing the table hierarchies.
   */
  getSuperTables(
    catalog: string | null,
    schemaPattern: string | null,
    tableNamePattern: string | null
  ): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      const validParams =
        (catalog === null ||
          catalog === undefined ||
          typeof catalog === "string") &&
        (schemaPattern === null ||
          schemaPattern === undefined ||
          typeof schemaPattern === "string") &&
        (tableNamePattern === null ||
          tableNamePattern === undefined ||
          typeof tableNamePattern === "string");

      if (!validParams) {
        reject(new Error("INVALID ARGUMENTS"));
        return;
      }

      this._dbm.getSuperTables(
        catalog,
        schemaPattern,
        tableNamePattern,
        (err: Error, result: any) => {
          if (err) {
            reject(err);
          } else {
            resolve(new ResultSet(result));
          }
        }
      );
    });
  }

  /**
   * Retrieves a description of user-defined type (UDT) hierarchies in a schema.
   *
   * @param catalog - A catalog name; must match the catalog name as it is stored in this database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
   * @param schemaPattern - A schema pattern; must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
   * @param typeNamePattern - A type name pattern; must match the type name as it is stored in the database; "" retrieves those without a type; null means that the type name should not be used to narrow the search
   * @returns A promise that resolves to a ResultSet describing the UDT hierarchies.
   */
  getSuperTypes(
    catalog: string | null,
    schemaPattern: string | null,
    typeNamePattern: string | null
  ): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      const validParams =
        (catalog === null ||
          catalog === undefined ||
          typeof catalog === "string") &&
        (schemaPattern === null ||
          schemaPattern === undefined ||
          typeof schemaPattern === "string") &&
        (typeNamePattern === null ||
          typeNamePattern === undefined ||
          typeof typeNamePattern === "string");

      if (!validParams) {
        reject(new Error("INVALID ARGUMENTS"));
        return;
      }

      this._dbm.getSuperTypes(
        catalog,
        schemaPattern,
        typeNamePattern,
        (err: Error, result: any) => {
          if (err) {
            reject(err);
          } else {
            resolve(new ResultSet(result));
          }
        }
      );
    });
  }

  /**
   * Retrieves a comma-separated list of system functions available in this database.
   *
   * @returns A promise that resolves to the comma-separated list of system functions.
   */
  getSystemFunctions(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getSystemFunctions((err: Error | null, result: string) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Retrieves a description of the privileges defined for a table.
   *
   * @param catalog - A catalog name; must match the catalog name as it is stored in this database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
   * @param schemaPattern - A schema pattern; must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
   * @param tableNamePattern - A table name pattern; must match the table name as it is stored in this database; "" retrieves those without a table; null means that the table name should not be used to narrow the search
   * @returns A promise that resolves to a ResultSet describing the table privileges.
   */
  getTablePrivileges(
    catalog: string | null,
    schemaPattern: string | null,
    tableNamePattern: string
  ): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      const validParams =
        (catalog === null ||
          catalog === undefined ||
          typeof catalog === "string") &&
        (schemaPattern === null ||
          schemaPattern === undefined ||
          typeof schemaPattern === "string") &&
        typeof tableNamePattern === "string";

      if (!validParams) {
        reject(new Error("INVALID ARGUMENTS"));
        return;
      }

      this._dbm.getTablePrivileges(
        catalog,
        schemaPattern,
        tableNamePattern,
        (err: Error | null, result: any) => {
          if (err) {
            reject(err);
          } else {
            resolve(new ResultSet(result));
          }
        }
      );
    });
  }

  /**
   * Retrieves a description of the table types available in this database.
   *
   * @returns A promise that resolves to a ResultSet describing the table types.
   */
  getTableTypes(): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      this._dbm.getTableTypes((err: Error | null, result: any) => {
        if (err) {
          reject(err);
        } else {
          resolve(new ResultSet(result));
        }
      });
    });
  }

  /**
   * Retrieves a comma-separated list of time and date functions available in this database.
   *
   * @returns A promise that resolves to a comma-separated list of time and date functions.
   */
  getTimeDateFunctions(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getTimeDateFunctions((err: Error | null, result: string) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Retrieves type information for the database.
   *
   * @returns A promise that resolves to a ResultSet describing the type information.
   */
  getTypeInfo(): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      this._dbm.getTypeInfo((err: Error | null, result: any) => {
        if (err) {
          reject(err);
        } else {
          resolve(new ResultSet(result));
        }
      });
    });
  }

  /**
   * Retrieves a description of user-defined types (UDTs) in a schema.
   *
   * @param catalog - A catalog name; must match the catalog name as it is stored in this database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
   * @param schemaPattern - A schema pattern; must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
   * @param typeNamePattern - A type name pattern; must match the type name as it is stored in the database; "" retrieves those without a type; null means that the type name should not be used to narrow the search
   * @param types - An array of type codes; null retrieves all types
   * @returns A promise that resolves to a ResultSet describing the UDTs.
   */
  getUDTs(
    catalog: string | null,
    schemaPattern: string | null,
    typeNamePattern: string | null,
    types: number[] | null
  ): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      let validParams =
        (catalog === null ||
          catalog === undefined ||
          typeof catalog === "string") &&
        (schemaPattern === null ||
          schemaPattern === undefined ||
          typeof schemaPattern === "string") &&
        (typeNamePattern === null ||
          typeNamePattern === undefined ||
          typeof typeNamePattern === "string") &&
        (types === null || types === undefined || Array.isArray(types));

      if (Array.isArray(types)) {
        for (const type of types) {
          if (!Number.isInteger(type)) {
            validParams = false;
            break;
          }
        }
      }

      if (!validParams) {
        reject(new Error("INVALID ARGUMENTS"));
        return;
      }

      this._dbm.getUDTs(
        catalog,
        schemaPattern,
        typeNamePattern,
        types,
        (err: Error | null, result: any) => {
          if (err) {
            reject(err);
          } else {
            resolve(new ResultSet(result));
          }
        }
      );
    });
  }

  /**
   * Retrieves the URL of the database.
   *
   * @returns A promise that resolves to the database URL.
   */
  getURL(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getURL((err: Error | null, result: string) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Retrieves the username associated with the database connection.
   *
   * @returns A promise that resolves to the username.
   */
  getUserName(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._dbm.getUserName((err: Error | null, result: string) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Retrieves version columns for a given table.
   *
   * @param catalog - A catalog name; must match the catalog name as it is stored in this database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
   * @param schema - A schema pattern; must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
   * @param table - A table name; must match the table name as it is stored in this database
   * @returns A promise that resolves to a ResultSet describing the version columns.
   */
  getVersionColumns(
    catalog: string | null,
    schema: string | null,
    table: string
  ): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      const validParams =
        (catalog === null ||
          catalog === undefined ||
          typeof catalog === "string") &&
        (schema === null ||
          schema === undefined ||
          typeof schema === "string") &&
        typeof table === "string";

      if (!validParams) {
        reject(new Error("INVALID ARGUMENTS"));
        return;
      }

      this._dbm.getVersionColumns(
        catalog,
        schema,
        table,
        (err: Error | null, result: any) => {
          if (err) {
            reject(err);
          } else {
            resolve(new ResultSet(result));
          }
        }
      );
    });
  }

  /**
   * Checks if the database detects inserts.
   *
   * @param type - The type of insert to check
   * @returns A promise that resolves to a boolean indicating if inserts are detected.
   */
  insertsAreDetected(type: number): Promise<boolean> {
    return new Promise((resolve, reject) => {
      if (!Number.isInteger(type)) {
        reject(new Error("INVALID ARGUMENTS"));
        return;
      }

      this._dbm.insertsAreDetected(
        type,
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if the catalog is at the start of the URL.
   *
   * @returns A promise that resolves to a boolean indicating if the catalog is at the start.
   */
  isCatalogAtStart(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.isCatalogAtStart((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if the database is read-only.
   *
   * @returns A promise that resolves to a boolean indicating if the database is read-only.
   */
  isReadOnly(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.isReadOnly((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if locators are updated when the database copy is updated.
   *
   * @returns A promise that resolves to a boolean indicating if locators are updated.
   */
  locatorsUpdateCopy(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.locatorsUpdateCopy((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if adding a NULL to a non-NULL value results in NULL.
   *
   * @returns A promise that resolves to a boolean indicating if NULL plus non-NULL is NULL.
   */
  nullPlusNonNullIsNull(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.nullPlusNonNullIsNull((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if NULLs are sorted at the end.
   *
   * @returns A promise that resolves to a boolean indicating if NULLs are sorted at the end.
   */
  nullsAreSortedAtEnd(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.nullsAreSortedAtEnd((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if NULLs are sorted at the start.
   *
   * @returns A promise that resolves to a boolean indicating if NULLs are sorted at the start.
   */
  nullsAreSortedAtStart(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.nullsAreSortedAtStart((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if NULLs are sorted high.
   *
   * @returns A promise that resolves to a boolean indicating if NULLs are sorted high.
   */
  nullsAreSortedHigh(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.nullsAreSortedHigh((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if NULLs are sorted low.
   *
   * @returns A promise that resolves to a boolean indicating if NULLs are sorted low.
   */
  nullsAreSortedLow(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.nullsAreSortedLow((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if other deletes are visible for a specific type.
   *
   * @param type - The type of visibility to check.
   * @returns A promise that resolves to a boolean indicating if other deletes are visible.
   */
  othersDeletesAreVisible(type: number): Promise<boolean> {
    return new Promise((resolve, reject) => {
      if (!Number.isInteger(type)) {
        reject(new Error("INVALID ARGUMENTS"));
        return;
      }

      this._dbm.othersDeletesAreVisible(
        type,
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if other inserts are visible for a specific type.
   *
   * @param type - The type of visibility to check.
   * @returns A promise that resolves to a boolean indicating if other inserts are visible.
   */
  othersInsertsAreVisible(type: number): Promise<boolean> {
    return new Promise((resolve, reject) => {
      if (!Number.isInteger(type)) {
        reject(new Error("INVALID ARGUMENTS"));
        return;
      }

      this._dbm.othersInsertsAreVisible(
        type,
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if other updates are visible for a specific type.
   *
   * @param type - The type of visibility to check.
   * @returns A promise that resolves to a boolean indicating if other updates are visible.
   */
  othersUpdatesAreVisible(type: number): Promise<boolean> {
    return new Promise((resolve, reject) => {
      if (!Number.isInteger(type)) {
        reject(new Error("INVALID ARGUMENTS"));
        return;
      }

      this._dbm.othersUpdatesAreVisible(
        type,
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  ownDeletesAreVisible(type: number): Promise<boolean> {
    if (!_.isInteger(type)) {
      throw new Error("INVALID ARGUMENTS");
    }
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.ownDeletesAreVisible(
        type,
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  ownInsertsAreVisible(type: number): Promise<boolean> {
    if (!_.isInteger(type)) {
      throw new Error("INVALID ARGUMENTS");
    }
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.ownInsertsAreVisible(
        type,
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  ownUpdatesAreVisible(type: number): Promise<boolean> {
    if (!_.isInteger(type)) {
      throw new Error("INVALID ARGUMENTS");
    }
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.ownUpdatesAreVisible(
        type,
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  storesLowerCaseIdentifiers(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.storesLowerCaseIdentifiers(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  storesLowerCaseQuotedIdentifiers(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.storesLowerCaseQuotedIdentifiers(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  storesMixedCaseIdentifiers(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.storesMixedCaseIdentifiers(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  storesMixedCaseQuotedIdentifiers(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.storesMixedCaseQuotedIdentifiers(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  storesUpperCaseIdentifiers(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.storesUpperCaseIdentifiers(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  storesUpperCaseQuotedIdentifiers(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.storesUpperCaseQuotedIdentifiers(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  supportsAlterTableWithAddColumn(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.supportsAlterTableWithAddColumn(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  supportsAlterTableWithDropColumn(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.supportsAlterTableWithDropColumn(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  supportsANSI92EntryLevelSQL(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.supportsANSI92EntryLevelSQL(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  supportsANSI92FullSQL(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.supportsANSI92FullSQL((err: Error | null, result: boolean) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  supportsANSI92IntermediateSQL(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.supportsANSI92IntermediateSQL(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  supportsBatchUpdates(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.supportsBatchUpdates((err: Error | null, result: boolean) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  supportsCatalogsInDataManipulation(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.supportsCatalogsInDataManipulation(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  supportsCatalogsInIndexDefinitions(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.supportsCatalogsInIndexDefinitions(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  supportsCatalogsInPrivilegeDefinitions(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.supportsCatalogsInPrivilegeDefinitions(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  supportsCatalogsInProcedureCalls(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.supportsCatalogsInProcedureCalls(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  supportsCatalogsInTableDefinitions(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.supportsCatalogsInTableDefinitions(
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  supportsColumnAliasing(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.supportsColumnAliasing((err: Error | null, result: boolean) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  supportsConvert(fromType: number, toType: number): Promise<boolean> {
    if (!_.isInteger(fromType) || !_.isInteger(toType)) {
      throw new Error("INVALID ARGUMENTS");
    }
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.supportsConvert(
        fromType,
        toType,
        (err: Error | null, result: boolean) => {
          if (err) {
            return reject(err);
          }
          resolve(result);
        }
      );
    });
  }

  supportsCoreSQLGrammar(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this._dbm.supportsCoreSQLGrammar((err: Error | null, result: boolean) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  /**
   * Checks if correlated subqueries are supported.
   *
   * @returns A promise that resolves to a boolean indicating if correlated subqueries are supported.
   */
  supportsCorrelatedSubqueries(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsCorrelatedSubqueries(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if both data definition and data manipulation transactions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if both data definition and data manipulation transactions are supported.
   */
  supportsDataDefinitionAndDataManipulationTransactions(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsDataDefinitionAndDataManipulationTransactions(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if only data manipulation transactions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if only data manipulation transactions are supported.
   */
  supportsDataManipulationTransactionsOnly(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsDataManipulationTransactionsOnly(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if different table correlation names are supported.
   *
   * @returns A promise that resolves to a boolean indicating if different table correlation names are supported.
   */
  supportsDifferentTableCorrelationNames(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsDifferentTableCorrelationNames(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if expressions in ORDER BY clauses are supported.
   *
   * @returns A promise that resolves to a boolean indicating if expressions in ORDER BY clauses are supported.
   */
  supportsExpressionsInOrderBy(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsExpressionsInOrderBy(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if extended SQL grammar is supported.
   *
   * @returns A promise that resolves to a boolean indicating if extended SQL grammar is supported.
   */
  supportsExtendedSQLGrammar(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsExtendedSQLGrammar(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if full outer joins are supported.
   *
   * @returns A promise that resolves to a boolean indicating if full outer joins are supported.
   */
  supportsFullOuterJoins(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsFullOuterJoins((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if generated keys are supported.
   *
   * @returns A promise that resolves to a boolean indicating if generated keys are supported.
   */
  supportsGetGeneratedKeys(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsGetGeneratedKeys(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if GROUP BY clauses are supported.
   *
   * @returns A promise that resolves to a boolean indicating if GROUP BY clauses are supported.
   */
  supportsGroupBy(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsGroupBy((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if GROUP BY clauses beyond SELECT are supported.
   *
   * @returns A promise that resolves to a boolean indicating if GROUP BY clauses beyond SELECT are supported.
   */
  supportsGroupByBeyondSelect(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsGroupByBeyondSelect(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if GROUP BY clauses with unrelated columns are supported.
   *
   * @returns A promise that resolves to a boolean indicating if GROUP BY clauses with unrelated columns are supported.
   */
  supportsGroupByUnrelated(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsGroupByUnrelated(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if integrity enhancement facility is supported.
   *
   * @returns A promise that resolves to a boolean indicating if integrity enhancement facility is supported.
   */
  supportsIntegrityEnhancementFacility(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsIntegrityEnhancementFacility(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if LIKE escape clause is supported.
   *
   * @returns A promise that resolves to a boolean indicating if LIKE escape clause is supported.
   */
  supportsLikeEscapeClause(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsLikeEscapeClause(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if limited outer joins are supported.
   *
   * @returns A promise that resolves to a boolean indicating if limited outer joins are supported.
   */
  supportsLimitedOuterJoins(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsLimitedOuterJoins(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if minimum SQL grammar is supported.
   *
   * @returns A promise that resolves to a boolean indicating if minimum SQL grammar is supported.
   */
  supportsMinimumSQLGrammar(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsMinimumSQLGrammar(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if mixed case identifiers are supported.
   *
   * @returns A promise that resolves to a boolean indicating if mixed case identifiers are supported.
   */
  supportsMixedCaseIdentifiers(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsMixedCaseIdentifiers(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if mixed case quoted identifiers are supported.
   *
   * @returns A promise that resolves to a boolean indicating if mixed case quoted identifiers are supported.
   */
  supportsMixedCaseQuotedIdentifiers(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsMixedCaseQuotedIdentifiers(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if multiple open results are supported.
   *
   * @returns A promise that resolves to a boolean indicating if multiple open results are supported.
   */
  supportsMultipleOpenResults(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsMultipleOpenResults(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if multiple result sets are supported.
   *
   * @returns A promise that resolves to a boolean indicating if multiple result sets are supported.
   */
  supportsMultipleResultSets(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsMultipleResultSets(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if multiple transactions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if multiple transactions are supported.
   */
  supportsMultipleTransactions(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsMultipleTransactions(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if named parameters are supported.
   *
   * @returns A promise that resolves to a boolean indicating if named parameters are supported.
   */
  supportsNamedParameters(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsNamedParameters(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if non-nullable columns are supported.
   *
   * @returns A promise that resolves to a boolean indicating if non-nullable columns are supported.
   */
  supportsNonNullableColumns(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsNonNullableColumns(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if open cursors across commits are supported.
   *
   * @returns A promise that resolves to a boolean indicating if open cursors across commits are supported.
   */
  supportsOpenCursorsAcrossCommit(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsOpenCursorsAcrossCommit(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if open cursors across rollbacks are supported.
   *
   * @returns A promise that resolves to a boolean indicating if open cursors across rollbacks are supported.
   */
  supportsOpenCursorsAcrossRollback(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsOpenCursorsAcrossRollback(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if open statements across commits are supported.
   *
   * @returns A promise that resolves to a boolean indicating if open statements across commits are supported.
   */
  supportsOpenStatementsAcrossCommit(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsOpenStatementsAcrossCommit(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if open statements across rollbacks are supported.
   *
   * @returns A promise that resolves to a boolean indicating if open statements across rollbacks are supported.
   */
  supportsOpenStatementsAcrossRollback(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsOpenStatementsAcrossRollback(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if ordering by unrelated columns is supported.
   *
   * @returns A promise that resolves to a boolean indicating if ordering by unrelated columns is supported.
   */
  supportsOrderByUnrelated(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsOrderByUnrelated(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if outer joins are supported.
   *
   * @returns A promise that resolves to a boolean indicating if outer joins are supported.
   */
  supportsOuterJoins(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsOuterJoins((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if positioned delete operations are supported.
   *
   * @returns A promise that resolves to a boolean indicating if positioned delete operations are supported.
   */
  supportsPositionedDelete(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsPositionedDelete(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if positioned update operations are supported.
   *
   * @returns A promise that resolves to a boolean indicating if positioned update operations are supported.
   */
  supportsPositionedUpdate(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsPositionedUpdate(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if the result set concurrency is supported.
   *
   * @param type - The result set type.
   * @param concurrency - The concurrency level.
   * @returns A promise that resolves to a boolean indicating if the result set concurrency is supported.
   */
  supportsResultSetConcurrency(
    type: number,
    concurrency: number
  ): Promise<boolean> {
    if (!Number.isInteger(type) || !Number.isInteger(concurrency)) {
      return Promise.reject(new Error("INVALID ARGUMENTS"));
    }

    return new Promise((resolve, reject) => {
      this._dbm.supportsResultSetConcurrency(
        type,
        concurrency,
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if the result set holdability is supported.
   *
   * @param holdability - The holdability level.
   * @returns A promise that resolves to a boolean indicating if the result set holdability is supported.
   */
  supportsResultSetHoldability(holdability: number): Promise<boolean> {
    if (!Number.isInteger(holdability)) {
      return Promise.reject(new Error("INVALID ARGUMENTS"));
    }

    return new Promise((resolve, reject) => {
      this._dbm.supportsResultSetHoldability(
        holdability,
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if the result set type is supported.
   *
   * @param type - The result set type.
   * @returns A promise that resolves to a boolean indicating if the result set type is supported.
   */
  supportsResultSetType(type: number): Promise<boolean> {
    if (!Number.isInteger(type)) {
      return Promise.reject(new Error("INVALID ARGUMENTS"));
    }

    return new Promise((resolve, reject) => {
      this._dbm.supportsResultSetType(
        type,
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if savepoints are supported.
   *
   * @returns A promise that resolves to a boolean indicating if savepoints are supported.
   */
  supportsSavepoints(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsSavepoints((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if schemas in data manipulation are supported.
   *
   * @returns A promise that resolves to a boolean indicating if schemas in data manipulation are supported.
   */
  supportsSchemasInDataManipulation(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsSchemasInDataManipulation(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if schemas in index definitions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if schemas in index definitions are supported.
   */
  supportsSchemasInIndexDefinitions(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsSchemasInIndexDefinitions(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if schemas in privilege definitions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if schemas in privilege definitions are supported.
   */
  supportsSchemasInPrivilegeDefinitions(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsSchemasInPrivilegeDefinitions(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if schemas in procedure calls are supported.
   *
   * @returns A promise that resolves to a boolean indicating if schemas in procedure calls are supported.
   */
  supportsSchemasInProcedureCalls(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsSchemasInProcedureCalls(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if schemas in table definitions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if schemas in table definitions are supported.
   */
  supportsSchemasInTableDefinitions(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsSchemasInTableDefinitions(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if select for update is supported.
   *
   * @returns A promise that resolves to a boolean indicating if select for update is supported.
   */
  supportsSelectForUpdate(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsSelectForUpdate(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if statement pooling is supported.
   *
   * @returns A promise that resolves to a boolean indicating if statement pooling is supported.
   */
  supportsStatementPooling(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsStatementPooling(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if stored functions using call syntax are supported.
   *
   * @returns A promise that resolves to a boolean indicating if stored functions using call syntax are supported.
   */
  supportsStoredFunctionsUsingCallSyntax(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsStoredFunctionsUsingCallSyntax(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if stored procedures are supported.
   *
   * @returns A promise that resolves to a boolean indicating if stored procedures are supported.
   */
  supportsStoredProcedures(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsStoredProcedures(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if subqueries in comparisons are supported.
   *
   * @returns A promise that resolves to a boolean indicating if subqueries in comparisons are supported.
   */
  supportsSubqueriesInComparisons(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsSubqueriesInComparisons(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if subqueries in EXISTS clauses are supported.
   *
   * @returns A promise that resolves to a boolean indicating if subqueries in EXISTS clauses are supported.
   */
  supportsSubqueriesInExists(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsSubqueriesInExists(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if subqueries in IN clauses are supported.
   *
   * @returns A promise that resolves to a boolean indicating if subqueries in IN clauses are supported.
   */
  supportsSubqueriesInIns(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsSubqueriesInIns(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if subqueries in quantified expressions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if subqueries in quantified expressions are supported.
   */
  supportsSubqueriesInQuantifieds(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsSubqueriesInQuantifieds(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if table correlation names are supported.
   *
   * @returns A promise that resolves to a boolean indicating if table correlation names are supported.
   */
  supportsTableCorrelationNames(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsTableCorrelationNames(
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if a specific transaction isolation level is supported.
   *
   * @param level - The isolation level to check.
   * @returns A promise that resolves to a boolean indicating if the transaction isolation level is supported.
   */
  supportsTransactionIsolationLevel(level: number): Promise<boolean> {
    if (!Number.isInteger(level)) {
      return Promise.reject(new Error("INVALID ARGUMENTS"));
    }

    return new Promise((resolve, reject) => {
      this._dbm.supportsTransactionIsolationLevel(
        level,
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if transactions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if transactions are supported.
   */
  supportsTransactions(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsTransactions((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if SQL UNION is supported.
   *
   * @returns A promise that resolves to a boolean indicating if SQL UNION is supported.
   */
  supportsUnion(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsUnion((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if SQL UNION ALL is supported.
   *
   * @returns A promise that resolves to a boolean indicating if SQL UNION ALL is supported.
   */
  supportsUnionAll(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.supportsUnionAll((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if updates are detected by calling the method ResultSet.rowUpdated.
   *
   * @param type - The ResultSet type; one of ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, or ResultSet.TYPE_SCROLL_SENSITIVE
   * @returns A promise that resolves to a boolean indicating if updates are detected by the result set type.
   */
  updatesAreDetected(type: number): Promise<boolean> {
    if (!Number.isInteger(type)) {
      return Promise.reject(new Error("INVALID ARGUMENTS"));
    }

    return new Promise((resolve, reject) => {
      this._dbm.updatesAreDetected(
        type,
        (err: Error | null, result: boolean) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        }
      );
    });
  }

  /**
   * Checks if the database uses a file for each table.
   *
   * @returns A promise that resolves to a boolean indicating if the database uses a file for each table.
   */
  usesLocalFilePerTable(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.usesLocalFilePerTable((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Checks if the database stores tables in local files.
   *
   * @returns A promise that resolves to a boolean indicating if the database stores tables in local files.
   */
  usesLocalFiles(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._dbm.usesLocalFiles((err: Error | null, result: boolean) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }
}

// Initializing static attributes from `java.sql.DatabaseMetaData`
const staticAttrs: string[] = [
  "attributeNoNulls",
  "attributeNullable",
  "attributeNullableUnknown",
  "bestRowNotPseudo",
  "bestRowPseudo",
  "bestRowSession",
  "bestRowTemporary",
  "bestRowTransaction",
  "bestRowUnknown",
  "columnNoNulls",
  "columnNullable",
  "columnNullableUnknown",
  "functionColumnIn",
  "functionColumnInOut",
  "functionColumnOut",
  "functionColumnResult",
  "functionColumnUnknown",
  "functionNoNulls",
  "functionNoTable",
  "functionNullable",
  "functionNullableUnknown",
  "functionResultUnknown",
  "functionReturn",
  "functionReturnsTable",
  "importedKeyCascade",
  "importedKeyInitiallyDeferred",
  "importedKeyInitiallyImmediate",
  "importedKeyNoAction",
  "importedKeyNotDeferrable",
  "importedKeyRestrict",
  "importedKeySetDefault",
  "importedKeySetNull",
  "procedureColumnIn",
  "procedureColumnInOut",
  "procedureColumnOut",
  "procedureColumnResult",
  "procedureColumnReturn",
  "procedureColumnUnknown",
  "procedureNoNulls",
  "procedureNoResult",
  "procedureNullable",
  "procedureNullableUnknown",
  "procedureResultUnknown",
  "procedureReturnsResult",
  "sqlStateSQL",
  "sqlStateSQL99",
  "sqlStateXOpen",
  "tableIndexClustered",
  "tableIndexHashed",
  "tableIndexOther",
  "tableIndexStatistic",
  "typeNoNulls",
  "typeNullable",
  "typeNullableUnknown",
  "typePredBasic",
  "typePredChar",
  "typePredNone",
  "typeSearchable",
  "versionColumnNotPseudo",
  "versionColumnPseudo",
  "versionColumnUnknown",
];

jinst.events.once("initialized", () => {
  // Assuming 'java' is an external library object with a method to get static field values
  staticAttrs.forEach((attr) => {
    (DatabaseMetaData as any)[attr as keyof DatabaseMetaData] =
      java.getStaticFieldValue("java.sql.DatabaseMetaData", attr);
  });
});

export default DatabaseMetaData;
