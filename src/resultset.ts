import jinst from './jinst.js';
import ResultSetMetaData from './resultsetmetadata.js';

const java = jinst.getInstance();

if (!jinst.getInstance().isJvmCreated()) {
  jinst.getInstance().addOption('-Xrs');
}

interface ColumnMetaData {
  label: string; // The column label (name)
  type: number;  // The column type (integer referring to java.sql.Types)
}

class ResultSet {
  private _rs: any;
  private _holdability: string[];
  private _types: string[];

  constructor(rs: any) {
    this._rs = rs;
    this._holdability = (() => {
      const h: string[] = [];
      h[java.getStaticFieldValue('java.sql.ResultSet', 'CLOSE_CURSORS_AT_COMMIT')] = 'CLOSE_CURSORS_AT_COMMIT';
      h[java.getStaticFieldValue('java.sql.ResultSet', 'HOLD_CURSORS_OVER_COMMIT')] = 'HOLD_CURSORS_OVER_COMMIT';
      return h;
    })();

    this._types = (() => {
      const typeNames: string[] = [];

      typeNames[java.getStaticFieldValue('java.sql.Types', 'BIT')] = 'Boolean';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'TINYINT')] = 'Short';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'SMALLINT')] = 'Short';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'INTEGER')] = 'Int';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'BIGINT')] = 'String';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'FLOAT')] = 'Float';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'REAL')] = 'Float';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'DOUBLE')] = 'Double';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'NUMERIC')] = 'BigDecimal';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'DECIMAL')] = 'BigDecimal';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'CHAR')] = 'String';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'VARCHAR')] = 'String';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'LONGVARCHAR')] = 'String';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'DATE')] = 'Date';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'TIME')] = 'Time';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'TIMESTAMP')] = 'Timestamp';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'BOOLEAN')] = 'Boolean';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'NCHAR')] = 'String';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'NVARCHAR')] = 'String';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'LONGNVARCHAR')] = 'String';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'BINARY')] = 'Bytes';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'VARBINARY')] = 'Bytes';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'LONGVARBINARY')] = 'Bytes';
      typeNames[java.getStaticFieldValue('java.sql.Types', 'BLOB')] = 'Bytes';

      return typeNames;
    })();
  }


  async toObjArray(): Promise<any> {
    const result = await this.toObject();
    return result.rows;
  }

  async toObject(): Promise<any> {
    const rs = await this.toObjectIter();
    const rowIter = rs.rows;
    const rows: any[] = [];
    let row = rowIter.next();

    while (!row.done) {
      rows.push(row.value);
      row = rowIter.next();
    }

    rs.rows = rows;
    return rs;
  }

  async toObjectIter(): Promise<any> {
    const rsmd = await this.getMetaData();
    const colsmetadata: { label: string; type: any }[] = [];

    const count = await rsmd.getColumnCount();

    // Use a for loop instead of Lodash's each and range
    for (let i = 1; i <= count; i++) {
        colsmetadata.push({
            label: rsmd._rsmd.getColumnLabelSync(i),
            type: rsmd._rsmd.getColumnTypeSync(i),
        });
    }

    return {
        labels: colsmetadata.map(col => col.label),
        types: colsmetadata.map(col => col.type),
        rows: {
            next: () => {
                try {
                    const nextRow = this._rs.nextSync();
                    if (!nextRow) {
                        return { done: true };
                    }

                    const result: Record<string, any> = {};
                    // Use a for loop instead of Lodash's each and range
                    for (let i = 1; i <= count; i++) {
                        const cmd = colsmetadata[i - 1];
                        const type = this._types[cmd.type] || 'String';
                        const getter = 'get' + (type === 'BigDecimal' ? 'Double' : type) + 'Sync';

                        if (type === 'Date' || type === 'Time' || type === 'Timestamp') {
                            const dateVal = this._rs[getter](cmd.label);
                            result[cmd.label] = dateVal ? dateVal.toString() : null;
                        } else {
                            result[cmd.label] = this._rs[getter](cmd.label);
                        }
                    }

                    return { value: result, done: false };
                } catch (error: any) {
                    throw new Error(error);
                }
            },
        },
    };
}


  async close(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        resolve(this._rs.closeSync())
      } catch (error) {
        reject(error)
      };
    });
  }

  async getMetaData(): Promise<ResultSetMetaData> {
    return new Promise((resolve, reject) => {
      try {
       resolve(new ResultSetMetaData(this._rs?.getMetaDataSync()))
      } catch (error) {
        reject(error)
      };
    });
  }
}

export default ResultSet;
