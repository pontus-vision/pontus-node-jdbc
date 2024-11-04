import ResultSet from "./resultset.js";
import Connection from "./connection.js";
import jinst from "./jinst.js";

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
    if (catalog !== null && typeof catalog !== 'string') {
      throw new Error("INVALID_ARGUMENTS: catalog must be a string or null.");
    }
    if (schemaPattern !== null && typeof schemaPattern !== 'string') {
      throw new Error(
        "INVALID_ARGUMENTS: schemaPattern must be a string or null."
      );
    }

    return new Promise((resolve, reject) => {
      try {
        resolve(this._dbm.getSchemasSync(catalog, schemaPattern));
      } catch (error) {
        reject(reject)
      }
    });
  }

  async getTables(
    catalog: string | null = null,
    schemaPattern: string | null = null,
    tableNamePattern: string | null = null,
    types: string[] | null = null
  ): Promise<ResultSet> {
    if (catalog !== null && typeof catalog !== 'string') {
      throw new Error("INVALID_ARGUMENTS: catalog must be a string or null.");
    }
    if (schemaPattern !== null && typeof schemaPattern !== 'string') {
      throw new Error(
        "INVALID_ARGUMENTS: schemaPattern must be a string or null."
      );
    }
    if (tableNamePattern !== null && typeof tableNamePattern !== 'string') {
      throw new Error(
        "INVALID_ARGUMENTS: tableNamePattern must be a string or null."
      );
    }
    if (types !== null && !Array.isArray(types)) {
      throw new Error("INVALID_ARGUMENTS: types must be an array or null.");
    }
    if (Array.isArray(types)) {
      for (const type of types) {
        if (typeof type !== 'string') {
          throw new Error(
            "INVALID_ARGUMENTS: all elements in types array must be strings."
          );
        }
      }
    }

    return new Promise((resolve, reject) => {
      try {  
        resolve(this._dbm.getTablesSync(catalog, schemaPattern, tableNamePattern, types));
      } catch (error) {
        reject(error)
      }
    });
  }

  async allProceduresAreCallable(): Promise<any> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._dbm.allProceduresAreCallableSync());
      } catch (error) {
        reject(error)
      }
    });
  }

  async allTablesAreSelectable(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._dbm.allTablesAreSelectableSync());
      } catch (error) {
        reject(error);
      }
    });
  }
  
  async autoCommitFailureClosesAllResultSets(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._dbm.autoCommitFailureClosesAllResultSetsSync());
      } catch (error) {
        reject(error);
      }
    });
  }
  
  async dataDefinitionCausesTransactionCommit(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._dbm.dataDefinitionCausesTransactionCommitSync());
      } catch (error) {
        reject(error);
      }
    });
  }
  
  async dataDefinitionIgnoredInTransactions(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._dbm.dataDefinitionIgnoredInTransactionsSync());
      } catch (error) {
        reject(error);
      }
    });
  }
  
  async deletesAreDetected(type: number): Promise<boolean> {
    return new Promise((resolve, reject) => {
      if (!Number.isInteger(type)) {
        return reject(new Error("INVALID ARGUMENTS"));
      }
      try {
        resolve(this._dbm.deletesAreDetectedSync(type));
      } catch (error) {
        reject(error);
      }
    });
  }
  
  async doesMaxRowSizeIncludeBlobs(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._dbm.doesMaxRowSizeIncludeBlobsSync());
      } catch (error) {
        reject(error);
      }
    });
  }
  
  // Promisified version of generatedKeyAlwaysReturned
  async generatedKeyAlwaysReturned(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._dbm.generatedKeyAlwaysReturnedSync());
      } catch (error) {
        reject(error);
      }
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
        (catalog === null || catalog === undefined || typeof catalog === "string") &&
        (schemaPattern === null || schemaPattern === undefined || typeof schemaPattern === "string") &&
        (typeNamePattern === null || typeNamePattern === undefined || typeof typeNamePattern === "string") &&
        (attributeNamePattern === null || attributeNamePattern === undefined || typeof attributeNamePattern === "string")
      ) {
        try {
          resolve(new ResultSet(this._dbm.getAttributesSync(catalog, schemaPattern, typeNamePattern, attributeNamePattern)));
        } catch (error) {
          reject(error);
        }
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
        (catalog === null || catalog === undefined || typeof catalog === "string") &&
        (schema === null || schema === undefined || typeof schema === "string") &&
        typeof table === "string" &&
        Number.isInteger(scope) &&
        typeof nullable === "boolean"
      ) {
        try {
          resolve(new ResultSet(this._dbm.getBestRowIdentifierSync(catalog, schema, table, scope, nullable)));
        } catch (error) {
          reject(error);
        }
      } else {
        reject(new Error("INVALID ARGUMENTS"));
      }
    });
  }
  
  // Promisified version of getCatalogs
  async getCatalogs(): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      try {
        resolve(new ResultSet(this._dbm.getCatalogsSync()));
      } catch (error) {
        reject(error);
      }
    });
  }
  
  // Promisified version of getCatalogSeparator
  async getCatalogSeparator(): Promise<string> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._dbm.getCatalogSeparatorSync());
      } catch (error) {
        reject(error);
      }
    });
  }
  
  // Promisified version of getCatalogTerm
  async getCatalogTerm(): Promise<string> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._dbm.getCatalogTermSync());
      } catch (error) {
        reject(error);
      }
    });
  }
  
  // Promisified version of getClientInfoProperties
  async getClientInfoProperties(): Promise<ResultSet> {
    return new Promise((resolve, reject) => {
      try {
        resolve(new ResultSet(this._dbm.getClientInfoPropertiesSync()));
      } catch (error) {
        reject(error);
      }
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
        (catalog === null || catalog === undefined || typeof catalog === "string") &&
        (schema === null || schema === undefined || typeof schema === "string") &&
        typeof table === "string" &&
        (columnNamePattern === null || columnNamePattern === undefined || typeof columnNamePattern === "string")
      ) {
        try {
          resolve(new ResultSet(this._dbm.getColumnPrivilegesSync(catalog, schema, table, columnNamePattern)));
        } catch (error) {
          reject(error);
        }
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
        (catalog === null || catalog === undefined || typeof catalog === "string") &&
        (schemaPattern === null || schemaPattern === undefined || typeof schemaPattern === "string") &&
        (tableNamePattern === null || tableNamePattern === undefined || typeof tableNamePattern === "string") &&
        (columnNamePattern === null || columnNamePattern === undefined || typeof columnNamePattern === "string")
      ) {
        try {
          resolve(new ResultSet(this._dbm.getColumnsSync(catalog, schemaPattern, tableNamePattern, columnNamePattern)));
        } catch (error) {
          reject(error);
        }
      } else {
        reject(new Error("INVALID ARGUMENTS"));
      }
    });
  }
  
  // Promisified version of getConnection
  async getConnection(): Promise<Connection> {
    return new Promise((resolve, reject) => {
      try {
        resolve(new Connection(this._dbm.getConnectionSync()));
      } catch (error) {
        reject(error);
      }
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
        try {
          resolve(new ResultSet(this._dbm.getCrossReferenceSync(parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable)));
        } catch (error) {
          reject(error);
        }
      } else {
        reject(new Error("INVALID ARGUMENTS"));
      }
    });
  }
  
  // Promisified version of getDatabaseMajorVersion
  async getDatabaseMajorVersion(): Promise<number> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._dbm.getDatabaseMajorVersionSync());
      } catch (error) {
        reject(error);
      }
    });
  }
  
  // Promisified version of getDatabaseMinorVersion
  async getDatabaseMinorVersion(): Promise<number> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._dbm.getDatabaseMinorVersionSync());
      } catch (error) {
        reject(error);
      }
    });
  }
  
  // Promisified version of getDatabaseProductName
  async getDatabaseProductName(): Promise<string> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._dbm.getDatabaseProductNameSync());
      } catch (error) {
        reject(error);
      }
    });
  }
  
  // Promisified version of getDatabaseProductVersion
  async getDatabaseProductVersion(): Promise<string> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._dbm.getDatabaseProductVersionSync());
      } catch (error) {
        reject(error);
      }
    });
  }
  

  // Promisified version of getDefaultTransactionIsolation
  async getDefaultTransactionIsolation(): Promise<number> {
    return new Promise((resolve, reject) => {
      this._dbm.getDefaultTransactionIsolationSync(
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
      this._dbm.getDriverMajorVersionSync((err: Error | null, result: number) => {
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
      this._dbm.getDriverMinorVersionSync((err: Error | null, result: number) => {
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
      this._dbm.getDriverNameSync((err: Error | null, result: string) => {
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
      this._dbm.getDriverVersionSync((err: Error | null, result: string) => {
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
      this._dbm.getExportedKeysSync(
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
      this._dbm.getExtraNameCharactersSync((err: Error | null, result: string) => {
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
      this._dbm.getFunctionColumnsSync(
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
      this._dbm.getFunctionsSync(
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
      this._dbm.getIdentifierQuoteStringSync(
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
      this._dbm.getImportedKeysSync(
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
      this._dbm.getIndexInfoSync(
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
      this._dbm.getJDBCMajorVersionSync((err: Error | null, result: number) => {
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
      this._dbm.getJDBCMinorVersionSync((err: Error | null, result: number) => {
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
      this._dbm.getMaxBinaryLiteralLengthSync(
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
      this._dbm.getMaxCatalogNameLengthSync((err: Error | null, result: number) => {
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
      this._dbm.getMaxCharLiteralLengthSync((err: Error | null, result: number) => {
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
      this._dbm.getMaxColumnNameLengthSync((err: Error | null, result: number) => {
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
      this._dbm.getMaxColumnsInGroupBySync((err: Error | null, result: number) => {
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
      this._dbm.getMaxColumnsInIndexSync((err: Error | null, result: number) => {
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
      this._dbm.getMaxColumnsInOrderBySync((err: Error | null, result: number) => {
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
      this._dbm.getMaxColumnsInSelectSync((err: Error | null, result: number) => {
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
      this._dbm.getMaxColumnsInTableSync((err: Error | null, result: number) => {
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
      this._dbm.getMaxConnectionsSync((err: Error | null, result: number) => {
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
      this._dbm.getMaxCursorNameLengthSync((err: Error | null, result: number) => {
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
      this._dbm.getMaxIndexLengthSync((err: Error | null, result: number) => {
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
      this._dbm.getMaxProcedureNameLengthSync(
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
      this._dbm.getMaxRowSizeSync((err: Error | null, result: number) => {
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
      this._dbm.getMaxSchemaNameLengthSync((err: Error | null, result: number) => {
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
      this._dbm.getMaxStatementLengthSync((err: Error | null, result: number) => {
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
      this._dbm.getMaxTableNameLengthSync((err: Error | null, result: number) => {
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
      this._dbm.getMaxTablesInSelectSync((err: Error | null, result: number) => {
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
      this._dbm.getMaxUserNameLengthSync((err: Error | null, result: number) => {
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
      this._dbm.getNumericFunctionsSync((err: Error | null, result: string) => {
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

      this._dbm.getPrimaryKeysSync(
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

      this._dbm.getProcedureColumnsSync(
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

      this._dbm.getProceduresSync(
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
      this._dbm.getProcedureTermSync((err: Error, result: string) => {
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

      this._dbm.getPseudoColumnsSync(
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
      this._dbm.getResultSetHoldabilitySync((err: Error, result: number) => {
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
      this._dbm.getRowIdLifetimeSync((err: Error, result: RowIdLifetime) => {
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
      this._dbm.getSchemaTermSync((err: Error, result: string) => {
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
      this._dbm.getSearchStringEscapeSync((err: Error, result: string) => {
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
      this._dbm.getSQLKeywordsSync((err: Error, result: string) => {
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
      this._dbm.getSQLStateTypeSync((err: Error, result: number) => {
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
      this._dbm.getStringFunctionsSync((err: Error, result: string) => {
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

      this._dbm.getSuperTablesSync(
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

      this._dbm.getSuperTypesSync(
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
      this._dbm.getSystemFunctionsSync((err: Error | null, result: string) => {
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
async getTablePrivileges(
  catalog: string | null,
  schemaPattern: string | null,
  tableNamePattern: string
): Promise<ResultSet> {
  const validParams =
    (catalog === null ||
      catalog === undefined ||
      typeof catalog === "string") &&
    (schemaPattern === null ||
      schemaPattern === undefined ||
      typeof schemaPattern === "string") &&
    typeof tableNamePattern === "string";

  if (!validParams) {
    throw new Error("INVALID ARGUMENTS");
  }

  try {
    const result = await this._dbm.getTablePrivilegesSync(
      catalog,
      schemaPattern,
      tableNamePattern
    );
    return new ResultSet(result);
  } catch (error) {
    throw error;
  }
}

/**
 * Retrieves a description of the table types available in this database.
 *
 * @returns A promise that resolves to a ResultSet describing the table types.
 */
async getTableTypes(): Promise<ResultSet> {
  try {
    const result = await this._dbm.getTableTypesSync();
    return new ResultSet(result);
  } catch (error) {
    throw error;
  }
}

/**
 * Retrieves a comma-separated list of time and date functions available in this database.
 *
 * @returns A promise that resolves to a comma-separated list of time and date functions.
 */
async getTimeDateFunctions(): Promise<string> {
  try {
    const result = await this._dbm.getTimeDateFunctionsSync();
    return result;
  } catch (error) {
    throw error;
  }
}

/**
 * Retrieves type information for the database.
 *
 * @returns A promise that resolves to a ResultSet describing the type information.
 */
async getTypeInfo(): Promise<ResultSet> {
  try {
    const result = await this._dbm.getTypeInfoSync();
    return new ResultSet(result);
  } catch (error) {
    throw error;
  }
}

/**
 * Retrieves a description of user-defined types (UDTs) in a schema.
 *
 * @param catalog - A catalog name; must match the catalog name as it is stored in this database; "" retrieves those without a catalog; null means that the catalog name should not be used to narrow the search
 * @param schemaPattern - A schema pattern; must match the schema name as it is stored in the database; "" retrieves those without a schema; null means that the schema name should not be used to narrow the search
 * @param typeNamePattern - A type name pattern; must match the type name as it is stored in this database; "" retrieves those without a type; null means that the type name should not be used to narrow the search
 * @param types - An array of type codes; null retrieves all types
 * @returns A promise that resolves to a ResultSet describing the UDTs.
 */
async getUDTs(
  catalog: string | null,
  schemaPattern: string | null,
  typeNamePattern: string | null,
  types: number[] | null
): Promise<ResultSet> {
  const validParams =
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
        throw new Error("INVALID ARGUMENTS");
      }
    }
  }

  if (!validParams) {
    throw new Error("INVALID ARGUMENTS");
  }

  try {
    const result = await this._dbm.getUDTsSync(
      catalog,
      schemaPattern,
      typeNamePattern,
      types
    );
    return new ResultSet(result);
  } catch (error) {
    throw error;
  }
}

/**
 * Retrieves the URL of the database.
 *
 * @returns A promise that resolves to the database URL.
 */
async getURL(): Promise<string> {
  try {
    const result = await this._dbm.getURLSync();
    return result;
  } catch (error) {
    throw error;
  }
}

/**
 * Retrieves the username associated with the database connection.
 *
 * @returns A promise that resolves to the username.
 */
getUserName(): Promise<string> {
  return new Promise((resolve, reject) => {
    try {
      const result = this._dbm.getUserNameSync();
      resolve(result);
    } catch (err) {
      reject(err);
    }
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

    try {
      const result = this._dbm.getVersionColumnsSync(catalog, schema, table);
      resolve(new ResultSet(result));
    } catch (err) {
      reject(err);
    }
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

    try {
      const result = this._dbm.insertsAreDetectedSync(type);
      resolve(result);
    } catch (err) {
      reject(err);
    }
  });
}

/**
 * Checks if the catalog is at the start of the URL.
 *
 * @returns A promise that resolves to a boolean indicating if the catalog is at the start.
 */
isCatalogAtStart(): Promise<boolean> {
  return new Promise((resolve, reject) => {
    try {
      const result = this._dbm.isCatalogAtStartSync();
      resolve(result);
    } catch (err) {
      reject(err);
    }
  });
}

/**
 * Checks if the database is read-only.
 *
 * @returns A promise that resolves to a boolean indicating if the database is read-only.
 */
isReadOnly(): Promise<boolean> {
  return new Promise((resolve, reject) => {
    try {
      const result = this._dbm.isReadOnlySync();
      resolve(result);
    } catch (err) {
      reject(err);
    }
  });
}

/**
 * Checks if locators are updated when the database copy is updated.
 *
 * @returns A promise that resolves to a boolean indicating if locators are updated.
 */
locatorsUpdateCopy(): Promise<boolean> {
  return new Promise((resolve, reject) => {
    try {
      const result = this._dbm.locatorsUpdateCopySync();
      resolve(result);
    } catch (err) {
      reject(err);
    }
  });
}

/**
 * Checks if adding a NULL to a non-NULL value results in NULL.
 *
 * @returns A promise that resolves to a boolean indicating if NULL plus non-NULL is NULL.
 */
nullPlusNonNullIsNull(): Promise<boolean> {
  return new Promise((resolve, reject) => {
    try {
      const result = this._dbm.nullPlusNonNullIsNullSync();
      resolve(result);
    } catch (err) {
      reject(err);
    }
  });
}

/**
 * Checks if NULLs are sorted at the end.
 *
 * @returns A promise that resolves to a boolean indicating if NULLs are sorted at the end.
 */
nullsAreSortedAtEnd(): Promise<boolean> {
  return new Promise((resolve, reject) => {
    try {
      const result = this._dbm.nullsAreSortedAtEndSync();
      resolve(result);
    } catch (err) {
      reject(err);
    }
  });
}

/**
 * Checks if NULLs are sorted at the start.
 *
 * @returns A promise that resolves to a boolean indicating if NULLs are sorted at the start.
 */
nullsAreSortedAtStart(): Promise<boolean> {
  return new Promise((resolve, reject) => {
    try {
      const result = this._dbm.nullsAreSortedAtStartSync();
      resolve(result);
    } catch (err) {
      reject(err);
    }
  });
}

/**
 * Checks if NULLs are sorted high.
 *
 * @returns A promise that resolves to a boolean indicating if NULLs are sorted high.
 */
nullsAreSortedHigh(): Promise<boolean> {
  return new Promise((resolve, reject) => {
    try {
      const result = this._dbm.nullsAreSortedHighSync();
      resolve(result);
    } catch (err) {
      reject(err);
    }
  });
}

/**
 * Checks if NULLs are sorted low.
 *
 * @returns A promise that resolves to a boolean indicating if NULLs are sorted low.
 */
nullsAreSortedLow(): Promise<boolean> {
  return new Promise((resolve, reject) => {
    try {
      const result = this._dbm.nullsAreSortedLowSync();
      resolve(result);
    } catch (err) {
      reject(err);
    }
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

    try {
      const result = this._dbm.othersDeletesAreVisibleSync(type);
      resolve(result);
    } catch (err) {
      reject(err);
    }
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

    try {
      const result = this._dbm.othersInsertsAreVisibleSync(type);
      resolve(result);
    } catch (err) {
      reject(err);
    }
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

    try {
      const result = this._dbm.othersUpdatesAreVisibleSync(type);
      resolve(result);
    } catch (err) {
      reject(err);
    }
  });
}

/**
 * Checks if own deletes are visible for a specific type.
 *
 * @param type - The type of visibility to check.
 * @returns A promise that resolves to a boolean indicating if own deletes are visible.
 */
ownDeletesAreVisible(type: number): Promise<boolean> {
  return new Promise((resolve, reject) => {
    if (typeof type !== 'number') {
      reject(new Error("INVALID ARGUMENTS"));
      return;
    }

    try {
      const result = this._dbm.ownDeletesAreVisibleSync(type);
      resolve(result);
    } catch (err) {
      reject(err);
    }
  });
}


  ownInsertsAreVisible(type: number): Promise<boolean> {
    if (typeof type !== 'number') {
        throw new Error("INVALID ARGUMENTS");
    }
    return new Promise<boolean>((resolve, reject) => {
        try {
            resolve(this._dbm.ownInsertsAreVisibleSync(type));
        } catch (err) {
            reject(err);
        }
    });
}

ownUpdatesAreVisible(type: number): Promise<boolean> {
    if (typeof type !== 'number') {
        throw new Error("INVALID ARGUMENTS");
    }
    return new Promise<boolean>((resolve, reject) => {
        try {
            resolve(this._dbm.ownUpdatesAreVisibleSync(type));
        } catch (err) {
            reject(err);
        }
    });
}

storesLowerCaseIdentifiers(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
        try {
            resolve(this._dbm.storesLowerCaseIdentifiersSync());
        } catch (err) {
            reject(err);
        }
    });
}

storesLowerCaseQuotedIdentifiers(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
        try {
            resolve(this._dbm.storesLowerCaseQuotedIdentifiersSync());
        } catch (err) {
            reject(err);
        }
    });
}

storesMixedCaseIdentifiers(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
        try {
            resolve(this._dbm.storesMixedCaseIdentifiersSync());
        } catch (err) {
            reject(err);
        }
    });
}

storesMixedCaseQuotedIdentifiers(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
        try {
            resolve(this._dbm.storesMixedCaseQuotedIdentifiersSync());
        } catch (err) {
            reject(err);
        }
    });
}

storesUpperCaseIdentifiers(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
        try {
            resolve(this._dbm.storesUpperCaseIdentifiersSync());
        } catch (err) {
            reject(err);
        }
    });
}

storesUpperCaseQuotedIdentifiers(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
        try {
            resolve(this._dbm.storesUpperCaseQuotedIdentifiersSync());
        } catch (err) {
            reject(err);
        }
    });
}

supportsAlterTableWithAddColumn(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
        try {
            resolve(this._dbm.supportsAlterTableWithAddColumnSync());
        } catch (err) {
            reject(err);
        }
    });
}


supportsAlterTableWithDropColumn(): Promise<boolean> {
  return new Promise<boolean>((resolve, reject) => {
      try {
          resolve(this._dbm.supportsAlterTableWithDropColumnSync());
      } catch (err) {
          reject(err);
      }
  });
}

supportsANSI92EntryLevelSQL(): Promise<boolean> {
  return new Promise<boolean>((resolve, reject) => {
      try {
          resolve(this._dbm.supportsANSI92EntryLevelSQLSync());
      } catch (err) {
          reject(err);
      }
  });
}

supportsANSI92FullSQL(): Promise<boolean> {
  return new Promise<boolean>((resolve, reject) => {
      try {
          resolve(this._dbm.supportsANSI92FullSQLSync());
      } catch (err) {
          reject(err);
      }
  });
}

supportsANSI92IntermediateSQL(): Promise<boolean> {
  return new Promise<boolean>((resolve, reject) => {
      try {
          resolve(this._dbm.supportsANSI92IntermediateSQLSync());
      } catch (err) {
          reject(err);
      }
  });
}

supportsBatchUpdates(): Promise<boolean> {
  return new Promise<boolean>((resolve, reject) => {
      try {
          resolve(this._dbm.supportsBatchUpdatesSync());
      } catch (err) {
          reject(err);
      }
  });
}

supportsCatalogsInDataManipulation(): Promise<boolean> {
  return new Promise<boolean>((resolve, reject) => {
      try {
          resolve(this._dbm.supportsCatalogsInDataManipulationSync());
      } catch (err) {
          reject(err);
      }
  });
}

supportsCatalogsInIndexDefinitions(): Promise<boolean> {
  return new Promise<boolean>((resolve, reject) => {
      try {
          resolve(this._dbm.supportsCatalogsInIndexDefinitionsSync());
      } catch (err) {
          reject(err);
      }
  });
}

supportsCatalogsInPrivilegeDefinitions(): Promise<boolean> {
  return new Promise<boolean>((resolve, reject) => {
      try {
          resolve(this._dbm.supportsCatalogsInPrivilegeDefinitionsSync());
      } catch (err) {
          reject(err);
      }
  });
}

supportsCatalogsInProcedureCalls(): Promise<boolean> {
  return new Promise<boolean>((resolve, reject) => {
      try {
          resolve(this._dbm.supportsCatalogsInProcedureCallsSync());
      } catch (err) {
          reject(err);
      }
  });
}

supportsCatalogsInTableDefinitions(): Promise<boolean> {
  return new Promise<boolean>((resolve, reject) => {
      try {
          resolve(this._dbm.supportsCatalogsInTableDefinitionsSync());
      } catch (err) {
          reject(err);
      }
  });
}

supportsColumnAliasing(): Promise<boolean> {
  return new Promise<boolean>((resolve, reject) => {
      try {
          resolve(this._dbm.supportsColumnAliasingSync());
      } catch (err) {
          reject(err);
      }
  });
}


  supportsConvert(fromType: number, toType: number): Promise<boolean> {
    if (typeof fromType !== 'number'  || typeof toType !== 'number') {
      throw new Error("INVALID ARGUMENTS");
    }
    return new Promise<boolean>((resolve, reject) => {
      resolve(this._dbm.supportsConvertSync(fromType, toType));
    });
  }

  supportsCoreSQLGrammar(): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
     resolve(this._dbm.supportsCoreSQLGrammarSync()) 
    })
  }

  /**
   * Checks if correlated subqueries are supported.
   *
   * @returns A promise that resolves to a boolean indicating if correlated subqueries are supported.
   */
  supportsCorrelatedSubqueries(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsCorrelatedSubqueriesSync());
    });
  }

  /**
   * Checks if both data definition and data manipulation transactions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if both data definition and data manipulation transactions are supported.
   */
  supportsDataDefinitionAndDataManipulationTransactions(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsDataDefinitionAndDataManipulationTransactionsSync());
    });
  }

  /**
   * Checks if only data manipulation transactions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if only data manipulation transactions are supported.
   */
  supportsDataManipulationTransactionsOnly(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsDataManipulationTransactionsOnlySync());
    });
  }

  /**
   * Checks if different table correlation names are supported.
   *
   * @returns A promise that resolves to a boolean indicating if different table correlation names are supported.
   */
  supportsDifferentTableCorrelationNames(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsDifferentTableCorrelationNamesSync());
    });
  }

  /**
   * Checks if expressions in ORDER BY clauses are supported.
   *
   * @returns A promise that resolves to a boolean indicating if expressions in ORDER BY clauses are supported.
   */
  supportsExpressionsInOrderBy(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsExpressionsInOrderBySync());
    });
  }

  /**
   * Checks if extended SQL grammar is supported.
   *
   * @returns A promise that resolves to a boolean indicating if extended SQL grammar is supported.
   */
  supportsExtendedSQLGrammar(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsExtendedSQLGrammarSync());
    });
  }

  /**
   * Checks if full outer joins are supported.
   *
   * @returns A promise that resolves to a boolean indicating if full outer joins are supported.
   */
  supportsFullOuterJoins(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsFullOuterJoinsSync());
    });
  }

  /**
   * Checks if generated keys are supported.
   *
   * @returns A promise that resolves to a boolean indicating if generated keys are supported.
   */
  supportsGetGeneratedKeys(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsGetGeneratedKeysSync());
    });
  }

  /**
   * Checks if GROUP BY clauses are supported.
   *
   * @returns A promise that resolves to a boolean indicating if GROUP BY clauses are supported.
   */
  supportsGroupBy(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsGroupBySync());
    });
  }

  /**
   * Checks if GROUP BY clauses beyond SELECT are supported.
   *
   * @returns A promise that resolves to a boolean indicating if GROUP BY clauses beyond SELECT are supported.
   */
  supportsGroupByBeyondSelect(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsGroupByBeyondSelectSync());
    });
  }

  /**
   * Checks if GROUP BY clauses with unrelated columns are supported.
   *
   * @returns A promise that resolves to a boolean indicating if GROUP BY clauses with unrelated columns are supported.
   */
  supportsGroupByUnrelated(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsGroupByUnrelatedSync());
    });
  }

  /**
   * Checks if integrity enhancement facility is supported.
   *
   * @returns A promise that resolves to a boolean indicating if integrity enhancement facility is supported.
   */
  supportsIntegrityEnhancementFacility(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsIntegrityEnhancementFacilitySync());
    });
  }

  /**
   * Checks if LIKE escape clause is supported.
   *
   * @returns A promise that resolves to a boolean indicating if LIKE escape clause is supported.
   */
  supportsLikeEscapeClause(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsLikeEscapeClauseSync());
    });
  }

  /**
   * Checks if limited outer joins are supported.
   *
   * @returns A promise that resolves to a boolean indicating if limited outer joins are supported.
   */
  supportsLimitedOuterJoins(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsLimitedOuterJoinsSync());
    });
  }

  /**
   * Checks if minimum SQL grammar is supported.
   *
   * @returns A promise that resolves to a boolean indicating if minimum SQL grammar is supported.
   */
  supportsMinimumSQLGrammar(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsMinimumSQLGrammarSync());
    });
  }

  /**
   * Checks if mixed case identifiers are supported.
   *
   * @returns A promise that resolves to a boolean indicating if mixed case identifiers are supported.
   */
  supportsMixedCaseIdentifiers(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsMixedCaseIdentifiersSync());
    });
  }

  /**
   * Checks if mixed case quoted identifiers are supported.
   *
   * @returns A promise that resolves to a boolean indicating if mixed case quoted identifiers are supported.
   */
  supportsMixedCaseQuotedIdentifiers(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsMixedCaseQuotedIdentifiersSync());
    });
  }

  /**
   * Checks if multiple open results are supported.
   *
   * @returns A promise that resolves to a boolean indicating if multiple open results are supported.
   */
  supportsMultipleOpenResults(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsMultipleOpenResultsSync());
    });
  }

  /**
   * Checks if multiple result sets are supported.
   *
   * @returns A promise that resolves to a boolean indicating if multiple result sets are supported.
   */
  supportsMultipleResultSets(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsMultipleResultSetsSync());
    });
  }

  /**
   * Checks if multiple transactions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if multiple transactions are supported.
   */
  supportsMultipleTransactions(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsMultipleTransactionsSync());
    });
  }

  /**
   * Checks if named parameters are supported.
   *
   * @returns A promise that resolves to a boolean indicating if named parameters are supported.
   */
  supportsNamedParameters(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsNamedParametersSync());
    });
  }

  /**
   * Checks if non-nullable columns are supported.
   *
   * @returns A promise that resolves to a boolean indicating if non-nullable columns are supported.
   */
  supportsNonNullableColumns(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsNonNullableColumnsSync());
    });
  }

  /**
   * Checks if open cursors across commits are supported.
   *
   * @returns A promise that resolves to a boolean indicating if open cursors across commits are supported.
   */
  supportsOpenCursorsAcrossCommit(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.supportsOpenCursorsAcrossCommitSync());
    });
  }

  /**
   * Checks if open cursors across rollbacks are supported.
   *
   * @returns A promise that resolves to a boolean indicating if open cursors across rollbacks are supported.
   */
  supportsOpenCursorsAcrossRollback(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsOpenCursorsAcrossRollbackSync());
    });
  }
  
  /**
   * Checks if open statements across commits are supported.
   *
   * @returns A promise that resolves to a boolean indicating if open statements across commits are supported.
   */
  supportsOpenStatementsAcrossCommit(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsOpenStatementsAcrossCommitSync());
    });
  }
  
  /**
   * Checks if open statements across rollbacks are supported.
   *
   * @returns A promise that resolves to a boolean indicating if open statements across rollbacks are supported.
   */
  supportsOpenStatementsAcrossRollback(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsOpenStatementsAcrossRollbackSync());
    });
  }
  
  /**
   * Checks if ordering by unrelated columns is supported.
   *
   * @returns A promise that resolves to a boolean indicating if ordering by unrelated columns is supported.
   */
  supportsOrderByUnrelated(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsOrderByUnrelatedSync());
    });
  }
  

  /**
   * Checks if outer joins are supported.
   *
   * @returns A promise that resolves to a boolean indicating if outer joins are supported.
   */
  supportsOuterJoins(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsOuterJoinsSync());
    });
  }
  
  /**
   * Checks if positioned delete operations are supported.
   *
   * @returns A promise that resolves to a boolean indicating if positioned delete operations are supported.
   */
  supportsPositionedDelete(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsPositionedDeleteSync());
    });
  }
  
  /**
   * Checks if positioned update operations are supported.
   *
   * @returns A promise that resolves to a boolean indicating if positioned update operations are supported.
   */
  supportsPositionedUpdate(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsPositionedUpdateSync());
    });
  }
  
  /**
   * Checks if the result set concurrency is supported.
   *
   * @param type - The result set type.
   * @param concurrency - The concurrency level.
   * @returns A promise that resolves to a boolean indicating if the result set concurrency is supported.
   */
  supportsResultSetConcurrency(type: number, concurrency: number): Promise<boolean> {
    if (!Number.isInteger(type) || !Number.isInteger(concurrency)) {
      return Promise.reject(new Error("INVALID ARGUMENTS"));
    }
  
    return new Promise((resolve) => {
      resolve(this._dbm.supportsResultSetConcurrencySync(type, concurrency));
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
  
    return new Promise((resolve) => {
      resolve(this._dbm.supportsResultSetHoldabilitySync(holdability));
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
  
    return new Promise((resolve) => {
      resolve(this._dbm.supportsResultSetTypeSync(type));
    });
  }
  
  /**
   * Checks if savepoints are supported.
   *
   * @returns A promise that resolves to a boolean indicating if savepoints are supported.
   */
  supportsSavepoints(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsSavepointsSync());
    });
  }
  
  /**
   * Checks if schemas in data manipulation are supported.
   *
   * @returns A promise that resolves to a boolean indicating if schemas in data manipulation are supported.
   */
  supportsSchemasInDataManipulation(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsSchemasInDataManipulationSync());
    });
  }
  
  /**
   * Checks if schemas in index definitions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if schemas in index definitions are supported.
   */
  supportsSchemasInIndexDefinitions(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsSchemasInIndexDefinitionsSync());
    });
  }
  
  /**
   * Checks if schemas in privilege definitions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if schemas in privilege definitions are supported.
   */
  supportsSchemasInPrivilegeDefinitions(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsSchemasInPrivilegeDefinitionsSync());
    });
  }
  
  /**
   * Checks if schemas in procedure calls are supported.
   *
   * @returns A promise that resolves to a boolean indicating if schemas in procedure calls are supported.
   */
  supportsSchemasInProcedureCalls(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsSchemasInProcedureCallsSync());
    });
  }
  
  /**
   * Checks if schemas in table definitions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if schemas in table definitions are supported.
   */
  supportsSchemasInTableDefinitions(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsSchemasInTableDefinitionsSync());
    });
  }
  
  /**
   * Checks if select for update is supported.
   *
   * @returns A promise that resolves to a boolean indicating if select for update is supported.
   */
  supportsSelectForUpdate(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsSelectForUpdateSync());
    });
  }
  
  /**
   * Checks if statement pooling is supported.
   *
   * @returns A promise that resolves to a boolean indicating if statement pooling is supported.
   */
  supportsStatementPooling(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsStatementPoolingSync());
    });
  }
  
  /**
   * Checks if stored functions using call syntax are supported.
   *
   * @returns A promise that resolves to a boolean indicating if stored functions using call syntax are supported.
   */
  supportsStoredFunctionsUsingCallSyntax(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsStoredFunctionsUsingCallSyntaxSync());
    });
  }
  
  /**
   * Checks if stored procedures are supported.
   *
   * @returns A promise that resolves to a boolean indicating if stored procedures are supported.
   */
  supportsStoredProcedures(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsStoredProceduresSync());
    });
  }
  
  /**
   * Checks if subqueries in comparisons are supported.
   *
   * @returns A promise that resolves to a boolean indicating if subqueries in comparisons are supported.
   */
  supportsSubqueriesInComparisons(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsSubqueriesInComparisonsSync());
    });
  }
  
  /**
   * Checks if subqueries in EXISTS clauses are supported.
   *
   * @returns A promise that resolves to a boolean indicating if subqueries in EXISTS clauses are supported.
   */
  supportsSubqueriesInExists(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsSubqueriesInExistsSync());
    });
  }
  
  /**
   * Checks if subqueries in IN clauses are supported.
   *
   * @returns A promise that resolves to a boolean indicating if subqueries in IN clauses are supported.
   */
  supportsSubqueriesInIns(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsSubqueriesInInsSync());
    });
  }
  
  /**
   * Checks if subqueries in quantified expressions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if subqueries in quantified expressions are supported.
   */
  supportsSubqueriesInQuantifieds(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsSubqueriesInQuantifiedsSync());
    });
  }
  
  /**
   * Checks if table correlation names are supported.
   *
   * @returns A promise that resolves to a boolean indicating if table correlation names are supported.
   */
  supportsTableCorrelationNames(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsTableCorrelationNamesSync());
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
  
    return new Promise((resolve) => {
      resolve(this._dbm.supportsTransactionIsolationLevelSync(level));
    });
  }
  
  /**
   * Checks if transactions are supported.
   *
   * @returns A promise that resolves to a boolean indicating if transactions are supported.
   */
  supportsTransactions(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsTransactionsSync());
    });
  }
  
  /**
   * Checks if SQL UNION is supported.
   *
   * @returns A promise that resolves to a boolean indicating if SQL UNION is supported.
   */
  supportsUnion(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsUnionSync());
    });
  }
  
  /**
   * Checks if SQL UNION ALL is supported.
   *
   * @returns A promise that resolves to a boolean indicating if SQL UNION ALL is supported.
   */
  supportsUnionAll(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.supportsUnionAllSync());
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
  
    return new Promise((resolve) => {
      resolve(this._dbm.updatesAreDetectedSync(type));
    });
  }
  
  /**
   * Checks if the database uses a file for each table.
   *
   * @returns A promise that resolves to a boolean indicating if the database uses a file for each table.
   */
  usesLocalFilePerTable(): Promise<boolean> {
    return new Promise((resolve) => {
      resolve(this._dbm.usesLocalFilePerTableSync());
    });
  }
  
  /**
   * Checks if the database stores tables in local files.
   *
   * @returns A promise that resolves to a boolean indicating if the database stores tables in local files.
   */
  usesLocalFiles(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      resolve(this._dbm.usesLocalFilesSync())
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
