/* jshint node: true */
"use strict";
import _ from 'lodash';
import jinst from './jinst';
import ResultSetMetaData from './resultsetmetadata';
import winston from 'winston';

const java = jinst.getInstance();

if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
}

class ResultSet {
  _rs: any;
  _holdability: any;
  _types: any;

  constructor(rs: any) {
    this._rs = rs;
    this._holdability = (function () {
      const h = [];

      h[java.getStaticFieldValue('java.sql.ResultSet', 'CLOSE_CURSORS_AT_COMMIT')] = 'CLOSE_CURSORS_AT_COMMIT';
      h[java.getStaticFieldValue('java.sql.ResultSet', 'HOLD_CURSORS_OVER_COMMIT')] = 'HOLD_CURSORS_OVER_COMMIT';

      return h;
    })();
    this._types = (function () {
      const typeNames = [];

      typeNames[java.getStaticFieldValue("java.sql.Types", "BIT")] = "Boolean";
      typeNames[java.getStaticFieldValue("java.sql.Types", "TINYINT")] = "Short";
      typeNames[java.getStaticFieldValue("java.sql.Types", "SMALLINT")] = "Short";
      typeNames[java.getStaticFieldValue("java.sql.Types", "INTEGER")] = "Int";
      typeNames[java.getStaticFieldValue("java.sql.Types", "BIGINT")] = "String";
      typeNames[java.getStaticFieldValue("java.sql.Types", "FLOAT")] = "Float";
      typeNames[java.getStaticFieldValue("java.sql.Types", "REAL")] = "Float";
      typeNames[java.getStaticFieldValue("java.sql.Types", "DOUBLE")] = "Double";
      typeNames[java.getStaticFieldValue("java.sql.Types", "NUMERIC")] = "BigDecimal";
      typeNames[java.getStaticFieldValue("java.sql.Types", "DECIMAL")] = "BigDecimal";
      typeNames[java.getStaticFieldValue("java.sql.Types", "CHAR")] = "String";
      typeNames[java.getStaticFieldValue("java.sql.Types", "VARCHAR")] = "String";
      typeNames[java.getStaticFieldValue("java.sql.Types", "LONGVARCHAR")] = "String";
      typeNames[java.getStaticFieldValue("java.sql.Types", "DATE")] = "Date";
      typeNames[java.getStaticFieldValue("java.sql.Types", "TIME")] = "Time";
      typeNames[java.getStaticFieldValue("java.sql.Types", "TIMESTAMP")] = "Timestamp";
      typeNames[java.getStaticFieldValue("java.sql.Types", "BOOLEAN")] = "Boolean";
      typeNames[java.getStaticFieldValue("java.sql.Types", "NCHAR")] = "String";
      typeNames[java.getStaticFieldValue("java.sql.Types", "NVARCHAR")] = "String";
      typeNames[java.getStaticFieldValue("java.sql.Types", "LONGNVARCHAR")] = "String";
      typeNames[java.getStaticFieldValue("java.sql.Types", "BINARY")] = "Bytes";
      typeNames[java.getStaticFieldValue("java.sql.Types", "VARBINARY")] = "Bytes";
      typeNames[java.getStaticFieldValue("java.sql.Types", "LONGVARBINARY")] = "Bytes";
      typeNames[java.getStaticFieldValue("java.sql.Types", "BLOB")] = "Bytes";

      return typeNames;
    })();
  }

  async toObjArray(callback: (err: Error | null, result: any[]) => void) {
    try {
      const result = await this.toObject();
      callback(null, result.rows);
    } catch (err) {
      callback(err);
    }
  }

  async toObject(callback: (err: Error | null, result: any) => void) {
    try {
      const rs = await this.toObjectIter();
      const rows = [];
      for await (const row of rs.rows) {
        rows.push(row);
      }
      rs.rows = rows;
      callback(null, rs);
    } catch (err) {
      callback(err);
    }
  }

  async toObjectIter(callback: (err: Error | null, rs: any) => void) {
    const self = this;

    try {
      const rsmd = await self.getMetaData();
      const colsmetadata = [];

      const colcount = await rsmd.getColumnCount();

      // Get some column metadata.
      for (let i = 1; i <= colcount; i++) {
        colsmetadata.push({
          label: await rsmd._rsmd.getColumnLabelSync(i),
          type: await rsmd._rsmd.getColumnTypeSync(i)
        });
      }

      callback(null, {
        labels: _.map(colsmetadata, 'label'),
        types: _.map(colsmetadata, 'type'),
        rows: {
          async next() {
            let nextRow = null;
            try {
              nextRow = await self._rs.nextSync(); // this row can lead to Java RuntimeException - sould be cathced.
            }
            catch (error) {
              callback(error);
            }
            if (!nextRow) {
              return {
                done: true
              };
            }

            const result = {};

            // loop through each column
            for (let i = 1; i <= colcount; i++) {
              const cmd = colsmetadata[i - 1];
              const type = self._types[cmd.type] || 'String';
              if (type === 'BigDecimal') type = 'Double';
              const getter = 'get' + type + 'Sync';

              if (type === 'Date' || type === 'Time' || type === 'Timestamp') {
                const dateVal = self._rs[getter](cmd.label);
                result[cmd.label] = dateVal ? dateVal.toString() : null;
              } else {
                // If the column is an integer and is null, set result to null and continue
                if (type === 'Int' && _.isNull(await self._rs.getObjectSync(cmd.label))) {
                  result[cmd.label] = null;
                  continue;
                }

                result[cmd.label] = await self._rs[getter](cmd.label);
              }
            }

            return {
              value: result,
              done: false
            };
          }
        }
      });
    } catch (err) {
      callback(err);
    }
  }

  async close(callback: (err: Error | null) => void) {
    try {
      await this._rs.close();
      callback(null);
    } catch (err) {
      callback(err);
    }
  }

  async getMetaData(callback: (err: Error | null, rsmd: ResultSetMetaData) => void) {
    try {
      const rsmd = await this._rs.getMetaData();
      callback(null, new ResultSetMetaData(rsmd));
    } catch (err) {
      callback(err);
    }
  }
}

export default ResultSet;
