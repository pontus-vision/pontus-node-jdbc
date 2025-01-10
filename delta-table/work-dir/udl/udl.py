from azure.storage.blob import BlobServiceClient
import threading

from pyspark.sql.functions import col, struct, explode_outer, to_json, udf, lit, input_file_name, array, array_union, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, ArrayType
from collections import defaultdict
from faker import Faker

from pyspark.sql import DataFrame
from pyspark import StorageLevel

from pyspark.sql.types import ArrayType, StructType
import json
import yaml
from IPython.display import display
import random

from jsonschema import Draft7Validator
# from json_checker import Checker

from delta import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql import *
from typing import Dict, List
from importlib import metadata
import numpy as np

import time

import datetime
from azure.identity.aio import ClientSecretCredential
from msgraph import GraphServiceClient
from msgraph.generated.sites.item.lists.item.items.items_request_builder import ItemsRequestBuilder
from msgraph.generated.sites.item.lists.lists_request_builder import ListsRequestBuilder
import json
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from datetime import datetime
# import IPython.display as display



class UDL:

    def __init__(self, spark, prefix: str, data_type: str = 'dap'):
        self.spark = spark
        self.prefix = prefix
        self.num_nodes = self.get_number_of_executors()
        self.set_delta_feature_flags()
        self.set_udl_feature_flags()
        self.set_spark_user_defined_functions()
        self.data_type = data_type
        self.dim_facts_conf = self.get_gold_config('dim-facts-config.yaml')
        self.complex_conf = self.get_gold_config('complex-config.yaml')
        self.config_spark()
        self.reset_gold()

    def set_udl_feature_flags(self):
        self.use_app_history = False
        self.use_dedup_raw = True
        self.use_separate_tables = True
        self.suffix = None

    def check_alive(self):
        """Check if connection is alive. ``True`` if alive, ``False`` if not"""
        try:
            get_java_obj = self.spark._jsc.sc().getExecutorMemoryStatus()
            return True
        except Exception:
            return False

    # Get the number of executors
    def get_num_executors(self):
        return int(self.spark.sparkContext._conf.get("spark.executor.instances", "1"))

    # Get the number of cores per executor
    def get_num_cores_per_executor(self):
        return int(self.spark.sparkContext._conf.get("spark.executor.cores", "1"))

    # Get the total number of threads (cores) available
    def get_total_cores(self):
        return self.get_num_executors() * self.get_num_cores_per_executor()

    def get_number_of_executors(self):
        if not self.check_alive():
            raise Exception('Unexpected Error: Spark Session has been killed')
        try:
            return self.spark._jsc.sc().getExecutorMemoryStatus().size()
        except Exception:
            raise Exception('Unknown error')

    # Depending on which version of delta spark we are using, different capabilities can be used.
    # This function sets the use_liquid_clustering flag by comparing the semver of delta spark against
    # version 3.1.0
    def set_delta_feature_flags(self):
        try:
            self.delta_version = metadata.version("delta_spark")
            self.delta_version = np.array(list(map(int, self.delta_version.split('.'))))
            self.three_one_zero = np.array([3, 1, 0])
            self.use_liquid_clustering = (self.delta_version >= self.three_one_zero).all()
            display(f'Delta Spark version {self.delta_version}; use_liquid_clustering = {self.use_liquid_clustering}')
        except:
            pass

    def set_spark_user_defined_functions(self):
        self.validate_json_udf = udf(UDL.json_parse)
        self.spark.udf.register("validate_json_udf", self.validate_json_udf)
        self.anonymise_udf = udf(UDL.anonymise)
        self.spark.udf.register("anonymise_udf", self.anonymise_udf)
        # self.spark.udf.register("lit",lit)
        # self.spark.udf.register("col",col)
        # self.spark.udf.register("array_union",array_union)
        # self.spark.udf.register("array",array)
        # self.spark.udf.register("to_json",to_json)

    def config_spark(self):
        # delta.deletedFileRetentionDuration
        self.spark.conf.set('spark.databricks.delta.deletedFileRetentionDuration', '365 days')
        self.spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled', 'true')
        self.spark.conf.set('spark.databricks.delta.clusteredTable.enableClusteringTablePreview', 'true')
        # set this to increase the default number of columns in delta tables that have statistics collected
        # self.spark.conf.set('spark.databricks.delta.dataSkippingNumIndexedCols', '32')
        self.spark.conf.set("spark.sql.streaming.schemaInference", "true")

    def get_dtype(self, df: DataFrame, colname: str):
        try:
            # data_type = df.schema[colname].dataType
            data_type = df.schema[colname].simpleString().split(':')[1]
            return data_type
        except Exception as e: # pragma: no cover
            print(f"Error getting type - file name is {self.get_df_source(df)} - col is {colname}: {str(e)}")
            return 'string'

    def create_gold_table_dim(self, df: DataFrame, batch_id: int, key: str, config: Dict[str, Any]):
        dimTables = config.get("dim",{})
        for table_name, table_conf in dimTables.items():
            dimTableName = f"udl_gold.dim_{table_name}"
            gold_path = f"{self.prefix}/gold/dim/delta-parquet/dim_{table_name}"
            if not DeltaTable.isDeltaTable(self.spark, gold_path):
                dimTableFactory = DeltaTable.createOrReplace(self.spark) \
                    .tableName(dimTableName) \
                    .location(gold_path)
                type_spec = []
                column_names = []
                for colName, colDef in table_conf.items():
                    column_names.append(colName)
                    if isinstance(colDef, str):
                        # display(f"{dimTableName} - {colName} - simple str {colDef}")
                        if colDef.startswith(f"${key}"):
                            refColName = colDef.replace(f"${key}.", "")
                            refColType = self.get_dtype(df, refColName)
                            dimTableFactory.addColumn(colName, dataType=refColType)
                            type_spec.append(f"{colName} {refColType}")

                    else:
                        refColType = colDef.get("dataType", "STRING")
                        generatedAlwaysAs = colDef.get("generatedAlwaysAs")
                        # display(f"{dimTableName} - {colName} - dataType {refColType} - generatedAlwaysAs {generatedAlwaysAs}")
                        dimTableFactory.addColumn(colName, dataType=refColType, generatedAlwaysAs=generatedAlwaysAs)
                        type_spec.append(f"{colName} {refColType}")
                # LPPM - 2024-04-23
                # NOTE: At the time of this writing, we had to use a combo of sql_str (below) plus the DeltaTable.createOrReplace() because
                #   the following function doesn't exist yet in python:  dimTableFactory.clusterBy(column_names)
                #   and similarly, SQL doesn't seem to support generated columns yet :(
                if self.use_liquid_clustering: 
                    # NOTE: There's a limitation in delta clustering that only allows the first four columns to be indexed.
                    sql_str = f"CREATE EXTERNAL TABLE {dimTableName} ({','.join(type_spec)}) USING DELTA CLUSTER BY ({','.join(column_names[:4])}) LOCATION '{gold_path}'"
                else: # pragma: no cover
                    sql_str = f"CREATE EXTERNAL TABLE {dimTableName} ({','.join(type_spec)}) USING DELTA LOCATION '{gold_path}'"

                # display(f"in create_gold_table() - about to process {dimTableName} {','.join(column_names)}")
                display(f"in create_gold_table() - about to process {sql_str}")
                # dimTableFactory.clusterBy(column_names)
                self.spark.sql(sql_str)
                dimTableFactory.execute()
                display(f"in create_gold_table() - after processing {dimTableName}")
                # except Exception as e:  # pragma: no cover
                #     display(f"in create_gold_table() - Failed to process {dimTableName}; error = {str(e)}")
                #     raise(e)

    def create_gold_table_fact(self, df: DataFrame, batch_id: int, key: str, config: Dict[str, Any]):
        configs = self.dim_facts_conf
        fact_tables = config.get("fact", {})
        for table_name, table_conf in fact_tables.items():
            fact_table_name = f"udl_gold.fact_{table_name}"
            gold_path = f"{self.prefix}/gold/fact/delta-parquet/fact_{table_name}"
            if not DeltaTable.isDeltaTable(self.spark, gold_path):
                type_spec = []
                column_names = []

                factTableFactory = DeltaTable.createOrReplace(self.spark) \
                    .tableName(fact_table_name) \
                    .location(gold_path)

                for colName, colDef in table_conf.items():
                    column_names.append(colName)
                    if isinstance(colDef, str):
                        display(f"{fact_table_name} - {colName} - simple str {colDef}")
                        if colDef.startswith(f"${key}"):
                            display(f"in create_gold_table() - found a col def that starts with ${key}; table: {fact_table_name}; colName: {colName}; coldef: {colDef}")
                            refColName = colDef.replace(f"${key}.", "")
                            refColType = self.get_dtype(df, refColName)
                            factTableFactory.addColumn(colName, dataType=refColType)
                            type_spec.append(f"{colName} {refColType}")

                        elif colDef.startswith("$dim."):
                            display(f"in create_gold_table() - found a col def that starts with $dim; table: {fact_table_name}; colName: {colName}; coldef: {colDef}")
                            refColTuple = colDef.replace("$dim.", "").split('.')
                            refTableName = refColTuple[0]
                            refColName = refColTuple[1]
                            subRefColInfo = configs[key]['dim'][refTableName][refColName]
                            display(f"in create_gold_table() - found a col def that starts with $dim; table: {fact_table_name}; colName: {colName}; coldef: {colDef}; subRefColInfo: {subRefColInfo}")
                            if isinstance(subRefColInfo, str):
                                if subRefColInfo.startswith(f"${key}"):
                                    subRefColInfo = subRefColInfo.replace(f"${key}.", "")
                                    refColType = self.get_dtype(df, subRefColInfo)
                                else:
                                    refColType = self.get_dtype(df, subRefColInfo)
                            else:
                                refColType = subRefColInfo.get("dataType", "STRING")
                            factTableFactory.addColumn(colName, dataType=refColType)
                            type_spec.append(f"{colName} {refColType}")

                    else:
                        display(f"in create_gold_table() - found a non-string definition in {fact_table_name} - {colName} - obj {colDef}")
                        refColType = colDef.get("dataType", "STRING")
                        generatedAlwaysAs = colDef.get("generatedAlwaysAs")
                        if generatedAlwaysAs is not None:
                            factTableFactory.addColumn(colName, dataType=refColType, generatedAlwaysAs=generatedAlwaysAs)
                        else:
                            factTableFactory.addColumn(colName, dataType=refColType)
                        type_spec.append(f"{colName} {refColType}")

                # LPPM - 2024-04-23
                # NOTE: At the time of this writing, we had to use a combo of sql_str (below) plus the DeltaTable.createOrReplace() because
                #   the following function doesn't exist yet in python:  dimTableFactory.clusterBy(column_names)
                #   and similarly, SQL doesn't seem to support generated columns yet :(
                sql_str = None
                if self.use_liquid_clustering: 
                    sql_str = f"CREATE EXTERNAL TABLE {fact_table_name} ({','.join(type_spec)}) USING DELTA CLUSTER BY ({','.join(column_names[:4])}) LOCATION '{gold_path}'"
                else: # pragma: no cover
                    sql_str = f"CREATE EXTERNAL TABLE {fact_table_name} ({','.join(type_spec)}) USING DELTA LOCATION '{gold_path}'"

                # display(f"in create_gold_table() - about to process {dimTableName} {','.join(column_names)}")
                display(f"in create_gold_table() - about to process {sql_str}")
                # dimTableFactory.clusterBy(column_names)
                self.spark.sql(sql_str)
                factTableFactory.execute()
                display(f"in create_gold_table() - after processing {fact_table_name}")

    def create_gold_table(self, df: DataFrame, batch_id: int, key: str, config: Dict[str, Any]):

        if (config is None):  # pragma: no cover
            return 
        display(f"in create_gold_table() - found {key} config to process - file name is {self.get_df_source(df)}")

        self.create_gold_table_dim(df, batch_id, key, config)

        self.create_gold_table_fact(df, batch_id, key, config)

    @staticmethod
    def get_dim_table_conf(table_conf: Dict[str, Any], key: str, df: DataFrame, source: str = "source",
                           target: str = "target") -> Dict[str, Any]:
        retVal = {}
        colsToKeep = []
        mergeClauses = []
        valuesClause = {}

        for colName, colDef in table_conf.items():
            if isinstance(colDef, str):
                if colDef.startswith(f"${key}"):
                    refColName = colDef.replace(f"${key}.", "")
                    colsToKeep.append(refColName)
                    mergeClauses.append(f"{target}.{colName} = {source}.{colName}")
                    valuesClause.update({colName: f"{source}.{colName}"})
                elif colDef.startswith("$dim."):
                    refColName = colDef.replace(f"$dim.", "")
            elif isinstance(colDef,dict):
                if (
                    colDef.get('expression') is not None and 
                    colDef.get('generatedAlwaysAs') is None
                ):
                    mergeClauses.append(f"{target}.{colName} = {source}.{colName}")
                    expr = colDef.get('expression')
                    valuesClause.update({colName: f"{source}.{colName}"})

                    colsToKeep.extend(colDef.get('refCols',[]))

        retVal.update({"colsToKeep": list(set(colsToKeep))})
        retVal.update({"mergeClauses": " and ".join(mergeClauses)})
        retVal.update({"valuesClause": valuesClause})
        retVal.update({"target": target})
        retVal.update({"source": source})
        display(str(retVal))
        return retVal

    def get_fact_table_conf(self, key: str, table_name: str, source: str = "source",
                            target: str = "target") -> Dict[str, Any]:
        
        configs = self.dim_facts_conf
        retVal = {}
        colsToKeep = []
        mergeClauses = []
        searchClause = {}
        table_conf = configs[key]["fact"][table_name]
        dimTableConf = configs[key]["dim"]

        for colName, colDef in table_conf.items():
            if isinstance(colDef, str):
                if colDef.startswith(f"${key}"):
                    refColName = colDef.replace(f"${key}.", "")
                    colsToKeep.append(refColName)
                    # mergeClauses.append(f"{target}.{colName} = {source}.{refColName}")
                    # valuesClause.update({colName:f"{source}.{refColName}"})
                elif colDef.startswith("$dim."):
                    searchClause[colName] = {}
                    refColName = colDef.replace(f"$dim.", "")
                    factColRef = colDef.split('.')
                    if (len(factColRef) != 3):
                        raise Exception(f"Invalid number of items in fact definition")
                    dimTableName = factColRef[1]
                    dimColName = factColRef[2]

                    dimColRefDef = dimTableConf.get(dimTableName,{}).get(dimColName)
                    if dimColRefDef is None:
                        raise Exception(
                            f"Invalid reference from fact table {table_name} - {colDef} ")

                    if isinstance(dimColRefDef, str):
                        if (dimColRefDef.startswith(f"${key}.")):
                            refColNameToKeep = dimColRefDef.replace(f"${key}.", "")
                            colsToKeep.append(refColNameToKeep)
                            searchClause[colName][refColNameToKeep] = f"{dimTableName}.{dimColRef}"
                    elif isinstance(dimColRefDef, dict):
                        dimColRefs = dimColRefDef.get("refCols",dimTableConf[dimTableName].keys())
                        try:
                            dimColRefs = dimColRefs.remove(dimColName)
                        except:
                            pass

                        if dimColRefDef.get('generatedAlwaysAs') is not None:
                            for dimColRef in dimColRefs:
                                localDimColRef = dimTableConf[dimTableName][dimColRef]
                                if isinstance(localDimColRef, str):
                                    if (localDimColRef.startswith(f"${key}.")):
                                        refColNameToKeep = localDimColRef.replace(f"${key}.", "")
                                        colsToKeep.append(refColNameToKeep)
                                        searchClause[colName][refColNameToKeep] = f"{dimTableName}.{dimColRef}"
                                elif ( 
                                    isinstance(localDimColRef,dict)
                                ):
                                    if (
                                        localDimColRef.get('expression') is not None and 
                                        localDimColRef.get('generatedAlwaysAs') is None 
                                    ):
                                        refColNamesToKeep =  localDimColRef.get('refCols',[])
                                        colsToKeep.extend(refColNamesToKeep)  
                                        expression = localDimColRef.get('expression')
                                        for refColNameToKeep in refColNamesToKeep:
                                            expression = expression.replace(refColNameToKeep, f"{target}.{refColNameToKeep}")
                                        searchClause[colName][dimColRef] = f'expr({source}.{dimColRef} == ({expression}))'
                                        ## TODO: WE MUST ADD the expression here somehow.

        retVal.update({"colsToKeep": list(set(colsToKeep))})
        retVal.update({"searchClause": searchClause})
        retVal.update({"target": target})
        retVal.update({"source": source})

        display(str(retVal))
        return retVal

    def populate_dim_table(self, df: DataFrame, batch_id: int, key: str, config: Dict[str, Any]):

        if (config is None):  # pragma: no cover
            return

        display(f"in populate_dim_table() - found {key} config to process - file name is {self.get_df_source(df)}")

        configs = self.dim_facts_conf
        dfDict = {}
        dimTables = config.get("dim",{})
        for table_name, table_conf in dimTables.items():
            dimTableName = f"udl_gold.dim_{table_name}"
            tableConfDict = UDL.get_dim_table_conf(table_conf, key,df)
            colsToKeep = tableConfDict.get("colsToKeep")
            if colsToKeep is not None:
                dimDf = df[colsToKeep]

            # complexRefCols = []
            for colName, colDef in table_conf.items():
                if isinstance(colDef, str):
                    display(f"{dimTableName} - {colName} - simple str {colDef}")
                    if colDef.startswith(f"${key}"):
                        refColName = colDef.replace(f"${key}.", "")
                        # refColType = get_dtype(df,refColName)
                        if (refColName != colName):
                            dimDf = dimDf.withColumn(colName, dimDf[refColName])
                elif isinstance(colDef,dict):
                    generatedAlwaysAs  = colDef.get('generatedAlwaysAs')
                    expression = colDef.get('expression')
                    if (
                        generatedAlwaysAs is None and
                        expression is not None and
                        isinstance(expression,str) 
                    ):
                        display(f"%%%%%%%%%%%%%%%%%%%%%%% in populate_dim_table() - adding expression {colName} - {expression}")
                        dimDf = dimDf.withColumn(colName, expr(expression))
                        # complexRefCols = list(set(complexRefCols.extend(colDef.get('refCols',[]))))
            if colsToKeep is not None:
                for colToDrop in colsToKeep:
                    display(f"%%%%%%%%%%%%%%%%%%%%%%% in populate_dim_table() - dropping  {colToDrop} from {dimTableName}")
                    dimDf = dimDf.drop(colToDrop)

            dimDf = dimDf.distinct()
            dimDf.show(4,False)
            dimDeltaParquetPath = f"{self.prefix}/gold/dim/delta-parquet/dim_{table_name}"

            for i in range(5):
                try:
                    deltaTable = DeltaTable.forPath(self.spark, dimDeltaParquetPath)
                    deltaTable.alias(tableConfDict.get("target","target")) \
                        .merge(dimDf.alias(tableConfDict.get("source")), tableConfDict.get("mergeClauses",{})) \
                        .whenNotMatchedInsert(values=tableConfDict.get("valuesClause",[])) \
                        .execute()

                    self.spark.sql(f"COMMENT ON TABLE delta.`{dimDeltaParquetPath}` IS '{json.dumps(self.lineage)}'")
                    if (self.use_liquid_clustering):
                        self.spark.sql(f"OPTIMIZE  delta.`{dimDeltaParquetPath}`")
                    break;
                except Exception as e:  # pragma: no cover
                    display(f"$$$$$$$$$$$$$$$$$$$$ in populate_dim_table() - reprocessing {key} - dim_{table_name} {str(e)}")
                    continue;

    def populate_fact_table(self, df: DataFrame, batch_id: int, key: str, config: Dict[str, Any]):
        if (config is None): # pragma: no cover
            return
        display(f"in populate_fact_table() - found {key} config to process - file name is {self.get_df_source(df)}")
        dfDict = {}
        fact_tables = config.get("fact",{})

        for table_name, table_conf in fact_tables.items():
            fact_table_name = f"udl_gold.fact_{table_name}"

            factTableConfDict = self.get_fact_table_conf(key, table_name)
            colsToKeep = factTableConfDict.get("colsToKeep")
            if colsToKeep is not None:
                factDf = df[colsToKeep]

            for colName, colDef in table_conf.items():
                if isinstance(colDef, str):
                    # "{'colsToKeep': ['applicationId', 'data__printDestination', 'data__priority', 'serviceName'], 'searchClause': {'print_destination_id': {'data__printDestination': 'print_destination.print_destination', 'data__priority': 'print_destination.priority'}, 'service_name_id': {'serviceName': 'service_name.service_name'}}}"

                    if colDef.startswith(f"${key}"):
                        refColName = colDef.replace(f"${key}.", "")
                        # refColType = get_dtype(df,refColName)
                        if (refColName != colName):
                            factDf = factDf.withColumn(colName, df[refColName])
                            factDf = factDf.drop(refColName)
                            if colsToKeep is not None:
                                colsToKeep.remove(refColName)
                    elif colDef.startswith("$dim."):
                        factColRef = colDef.split('.')
                        if (len(factColRef) != 3):
                            raise Exception(f"Invalid number of items in fact definition")
                        dimTableName = factColRef[1]
                        dimColName = factColRef[2]

                        searchClause = factTableConfDict.get("searchClause", {})
                        colsInScope = searchClause.get(colName)

                        dimDf = self.spark.read.format('delta').load(f"{self.prefix}/gold/dim/delta-parquet/dim_{dimTableName}")
                        dimFilterCriteria = []

                        dimCols = [dimColName]

                        normal_ref = True
                        cols_to_remove = []
                        for searchColName, searchColDef in colsInScope.items():
                            if not searchColDef.startswith('expr('):
                                dimCol = searchColDef.split('.')[1]
                                dimCols.append(dimCol)
                                if colsToKeep is not None:
                                    colsToKeep.append(dimCol)
                                dimFilterCriteria.append(factDf[searchColName] == dimDf[dimCol])
                            else:
                                normal_ref = False
                                expression = searchColDef.replace('expr(', '')
                                expression = UDL.replace_last(expression,')', '');
                                expression = expression.strip('"').strip("'")
                                dimFilterCriteria.append(expr(expression))
                                cols_to_remove.append(searchColName)
                                


                        ### TODO: Somewhere here, we need to cater for the expression
                        if normal_ref:
                            dimDf = dimDf[dimCols].withColumnRenamed(dimColName, colName)
                            
                        dimDf.show(5,False)

                        factDf = factDf.alias("target").join(dimDf.alias("source"), dimFilterCriteria, 'inner')
                        if not normal_ref:
                            factDf = factDf.withColumnRenamed(dimColName,colName)
                            for col_to_remove in cols_to_remove:
                                factDf = factDf.drop(col_to_remove)

                elif isinstance(colDef,dict):
                    generatedAlwaysAs = colDef.get('generatedAlwaysAs')
                    expression = colDef.get('expression')
                    if (
                        generatedAlwaysAs is None and
                        expression is not None and
                        isinstance(expression,str) 
                    ):
                        factDf = factDf.withColumn(colName, expr(expression))


            
            if colsToKeep is not None:
                for colToDrop in colsToKeep:
                    factDf = factDf.drop(colToDrop)
                    
            factDf = factDf.distinct()

            factDeltaTableName = f'{self.prefix}/gold/fact/delta-parquet/fact_{table_name}';

            display(f"in populate_fact_table(); updating {str(factDf)}" )
            factDf.show(5)
            self.save_df_with_retry(factDf, factDeltaTableName)
            if (self.use_liquid_clustering):
                self.spark.sql(f"OPTIMIZE  delta.`{factDeltaTableName}`")

    
    def get_gold_config(self, file_name:str) -> Dict[str, Any]:
        df = self.spark.read \
            .text(f'{self.prefix}/{file_name}', wholetext=True)
        conf_str = df.select("*").collect()[0][0]
        conf = yaml.safe_load(conf_str) # yaml_object will be a list or a dict
        # json.dump(yaml_object, json_out)

        # conf = json.loads(conf_str)
        return conf


    
    def get_json_schema_dict(self, csv_location: str) -> Dict[str, str]:
        json_schema_df = self.spark.read.option("delimiter", "|") \
            .option("header", True) \
            .option("quote", "'") \
            .csv(csv_location) \
            .select("EventName", "EventVersion", "EventSchema").collect()

        ret_val = {}

        for row in json_schema_df:
            name = row["EventName"]
            ver = row["EventVersion"]
            schema = row["EventSchema"]

            # Draft202012Validator.check_schema(json.loads(schema))
            # Draft7Validator.check_schema(json.loads(schema))
            # No output means the schema is valid, otherwise `SchemaError` will be raised.

            ret_val[name + ver] = schema

        return ret_val
        
    @staticmethod
    def anonymise(faker_type: str, val: str):
        ret_val = ""
        if not hasattr(UDL.anonymise, faker_type):
            faker = Faker()
            Faker.seed(4321)
            exec(f"""UDL.anonymise.{faker_type} = defaultdict(faker.{faker_type})""", { "faker": faker, "defaultdict": defaultdict, "UDL": UDL})
        ret_val = eval(f"UDL.anonymise.{faker_type}['{val}']")
        display(f'anonymising ${faker_type} on {val} becomes {ret_val}')
        return ret_val
                
       

    
    @staticmethod
    def json_parse(json_str: str, schema: str, event_name: str, event_version: str):
        ret_val = ""
        try:
            if not hasattr(UDL.json_parse, "valMap"):
                UDL.json_parse.valMap = {}
            json_obj = json.loads(json_str)
            key = f"{event_name}{event_version}"
            validator = UDL.json_parse.valMap.get(key)
            if not validator:
                # Create an instance of the validator using a valid schema.
                validator = Draft7Validator(schema=json.loads(schema), 
                                            format_checker = None)

                UDL.json_parse.valMap[key] = validator

            if validator is None: 
                return f"Cannot Find JSON Schema for {event_name} version {event_version}"

            elif validator.is_valid(json_obj):
                return "Valid Schema"
            else:
                ret_val = "InvalidSchema:\n"
                for error in sorted(validator.iter_errors(json_obj), key=str):
                    ret_val = ret_val + error.message + '\n'

                return ret_val
        except Exception as e: # pragma: no cover
            return f"ERROR VALIDATING: ret_val = {ret_val}; \nerror = {str(e)}"

    @staticmethod
    # Adapted from https://gist.github.com/shreyasms17/96f74e45d862f8f1dce0532442cc95b2
    def flatten_df(df: DataFrame) -> DataFrame:
        """
        Description:
        This function executes the core autoflattening operation
        :param df: [type: pyspark.sql.dataframe.DataFrame] dataframe to be used for flattening
        :return df: DataFrame containing flattened records
        """
        # gets all fields of StructType or ArrayType in the nested_fields dictionary
        nested_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, ArrayType) or isinstance(field.dataType, StructType)
        ])

        # repeat until all nested_fields i.e. belonging to StructType or ArrayType are covered
        while nested_fields:
            # if there are any elements in the nested_fields dictionary
            if nested_fields:
                # get a column
                column_name = list(nested_fields.keys())[0]
                # if field belongs to a StructType, all child fields inside it are accessed
                # and are aliased with complete path to every child field
                if isinstance(nested_fields[column_name], StructType):
                    unnested = [col(column_name + '.' + child).alias(column_name + '__' + child) for child in
                                [n.name for n in nested_fields[column_name]]] # type: ignore
                    df = df.select("*", *unnested).drop(column_name)
                # else, if the field belongs to an ArrayType, an explode_outer is done
                elif isinstance(nested_fields[column_name], ArrayType):
                    df = df.withColumn(column_name, explode_outer(column_name))

            # Now that df is updated, gets all fields of StructType and ArrayType in a fresh nested_fields dictionary
            nested_fields = dict([
                (field.name, field.dataType)
                for field in df.schema.fields
                if isinstance(field.dataType, ArrayType) or isinstance(field.dataType, StructType)
            ])

        return df

    @staticmethod
    def get_df_source(df: DataFrame) -> str:
        fileName = ""
        try:
            fileName = df.select('_metadata.file_path').collect()[0][0]
        except  Exception as e:
            try:
                tmpDf = df.withColumn("sourcefile", input_file_name())
                fileName = tmpDf.select("sourcefile").collect()[0][0]
            except  Exception as e2:
                display(f"!!!! in get_df_source() - failed to get the name for df {df}; error = {str(e2)}")

        return fileName

    @staticmethod
    def parse_name_ver(json_df: DataFrame) -> Dict[str, str]:
        file_name = UDL.get_df_source(json_df)
        # file_name = file_name.replace(prefix, '')
        # file_name = file_name.replace("file:","")
        file_name_parsed_split = file_name.split('/')
        file_name_parsed = file_name_parsed_split[-2]
        ver = file_name_parsed[file_name_parsed.rfind("_") + 1:]
        name = file_name_parsed[:file_name_parsed.rfind("_")]
        ret_val = {"ver": ver, "name": name}

        return ret_val

    def process_batch_dim_facts(self,df: DataFrame, batch_id: int, key: str):

        conf = self.dim_facts_conf.get(key, None)
        if conf is None:  # pragma: no cover
            display(f"in process_batch_dim_facts() could not find conf for {key}; exiting")
            return

        else:
            display(f"in process_batch_dim_facts() - found config for {key}")
            self.create_gold_table(df, batch_id, key, conf) # type: ignore
            self.populate_dim_table(df, batch_id, key, conf) # type: ignore
            self.populate_fact_table(df, batch_id, key, conf) # type: ignore

    def save_df_with_retry(
        self,
        df: DataFrame,
        path: str,
        format_str: str = 'delta',
        mode: str = 'append',
        use_dedup_raw_override: bool = False,
        add_delta_comment: bool = True,
        batch_id: int = None
    ):
        last_error = ""
        commit_msg = str(json.dumps(self.lineage))
        for i in range(5):
            try:
                if (self.use_dedup_raw or use_dedup_raw_override): # pragma: no cover
                    try:
                        orig_df = (self.spark.read
                                   .option("inferSchema", 'true')
                                   .format(format_str)
                                   .load(path))
                        all_columns = (df.columns)  # Get all columns from df
                        orig_df = orig_df.dropDuplicates(all_columns)
                        delta_df = df.dropDuplicates(all_columns)
                        # Perform an inner join on all columns to find duplicates 
                        delta_df_dups = orig_df.join(delta_df, all_columns, "inner")
                        # remove the duplicates from the incoming batch
                        df = delta_df.exceptAll(delta_df_dups)
                    except Exception as e:
                        display(f"Ignoring error {str(e)}")
                        pass
                df.write \
                    .format(format_str) \
                    .option("mergeSchema", "true") \
                    .option('spark.databricks.delta.commitInfo.userMetadata', commit_msg) \
                    .option('userMetadata', commit_msg) \
                    .mode(mode) \
                    .save(path)
                if format_str == 'delta' and add_delta_comment:
                    self.spark.sql(f"COMMENT ON TABLE delta.`{path}` IS '{commit_msg}'")
                break
            except Exception as e: # pragma: no cover
                last_error = str(e)
                display(f"!!!!!retrying ({i}) write operation to {path}; err = {last_error}")
        if (last_error != ""):  # pragma: no cover
            raise Exception(last_error)

    def process_batch_flattened(self, json_df: DataFrame, batch_id: int, data_path: str, name: str, ver: str, is_valid: bool):
        # display(f"in process_batch_flattened() - processing {name} - ver {ver} - df: {json_df}")

        flattened_df = UDL.flatten_df(json_df)
        # display(
        #     f"in process_batch_flattened() - processed {name}_{ver} - num flattened rows = {flattened_df.rdd.count()}; num unflattened rows = {json_df.rdd.count()} ")

        key = f"{name}_{ver}{'' if is_valid else '-invalid'}"
        # def get_table_path(self, medallion_layer:str, data_type:str, table_format:str, name:str, ver:str)-> str:

        output_path = self.get_table_path_key(False,'silver','flattened','delta-parquet',key )
        #f'{self.prefix}/silver/flattened/delta-parquet/{key}'

        self.save_df_with_retry(json_df, data_path)
        json_df.unpersist()
        self.save_df_with_retry(flattened_df, output_path)
        flattened_df.unpersist()

    def get_table_path(self, add_suffix: bool, medallion_layer:str, data_type:str, table_format:str, name:str, ver:str)-> str:
        return self.get_table_path_key(add_suffix,medallion_layer,data_type,table_format,f"{name}_{ver}")

    def get_table_path_key(self, add_suffix: bool, medallion_layer:str, data_type:str, table_format:str, key:str)-> str:
        if add_suffix and self.suffix is not None:
            return f'{self.prefix}/{medallion_layer}/{data_type}/{table_format}/{self.suffix}/{key}'
        else:
            return f'{self.prefix}/{medallion_layer}/{data_type}/{table_format}/{key}'

    def process_batch_json_split(self, json_df: DataFrame, batch_id:int, jsonSchemaDict: Dict[str, str]):

        parsed_name_ver = UDL.parse_name_ver(json_df)

        ver = parsed_name_ver["ver"]
        name = parsed_name_ver["name"]
        display(
            f"in process_batch_json_split() - pre-validation  name = {name}, ver = {ver},   batch_id = {batch_id} df_path = {UDL.get_df_source(json_df)}")
        json_schema_str = jsonSchemaDict.get(f"{name}{ver}")
        df = json_df.drop("EventType")
        df = df.drop("EventVersion")
        if json_schema_str is None: # pragma: no cover
            df = df.withColumn("validated", F.lit("cannot find schema"))
        else:
            display(f"in process_batch_json_split() - Found {name}{ver} schema {json_schema_str[:25]}")
            df = df.withColumn("validated", self.validate_json_udf(F.col("raw_json"), F.lit(json_schema_str),F.lit(name),F.lit(ver)))
        data_path = self.get_table_path(self.use_separate_tables, 'bronze', 'split', 'delta-parquet', name, ver)
        valid_df = df.drop("raw_json")
        df.unpersist()
        valid_df = valid_df.filter(col("validated") == 'Valid Schema')
        display(
            f"in process_batch_json_split - processing  name = {name}_{ver}  data_path = {data_path};  batch_id = {batch_id} df_path = {self.get_df_source(valid_df)}")
        if valid_df.rdd.count() > 0:
            # valid_df.select("validated").show(5)
            self.process_batch_flattened(valid_df, batch_id, data_path, name, ver, True)
            valid_df.unpersist()
        else:
            invalid_df = df.filter(col("validated") != 'Valid Schema')
            display(
                f"!!!!!!! in process_batch_json_split - processing  name = {name}_{ver} - FOUND INVALID ENTRIES: invalid_df.rdd.count() = {invalid_df.rdd.count()}")
            invalid_df.select("validated").show(5, False)
            invalid_df = invalid_df.drop("raw_json")
            self.process_batch_flattened(invalid_df, batch_id, f"{data_path}-invalid", name, ver, False)
            invalid_df.unpersist()

    def process_batch_raw(self, batch_df: DataFrame, batch_id: int):

        display('in process_batch_raw')
        unique_names = batch_df.select("EventType", "EventVersion").distinct().collect()

        queries = {}
        jsonSchemaDict = self.get_json_schema_dict(f'{self.prefix}/{self.data_type}-schema.csv')
        counter = 0
        for row in unique_names:
            name = row["EventType"]
            ver = row["EventVersion"]

            filtered_df = batch_df \
                .filter(col("EventType") == name) \
                .filter(col("EventVersion") == ver)

            # Write the filtered DataFrame to a Delta Parquet file
            output_path =  self.get_table_path(self.use_separate_tables,'bronze', 'split', 'json', name, ver)

            display(f'in process_batch_raw - processing {name}, {ver} - writing {output_path}')
            self.save_df_with_retry(filtered_df, output_path,"json", "append")

            # display(f'in process_batch_raw - processed {name}, {ver} -> num lines = {filtered_df.rdd.count()}')
            filtered_df.unpersist()

            # display(f'in process_batch_raw - processing {name}, {ver} - creating readStream for {output_path}')

            stream_df = self.spark.readStream \
                .option("inferSchema", "true") \
                .option("path", output_path) \
                .format("json") \
                .load()
            # display(f'in process_batch_raw - processing {name}, {ver} - after readStream for {output_path}')
            queries[f"{name}_{ver}"] = stream_df \
                .writeStream \
                .option("checkpointLocation", f"{output_path}/checkpoint") \
                .format("json") \
                .foreachBatch(lambda df, bid: self.process_batch_json_split(df, bid, jsonSchemaDict)) \
                .outputMode("append") \
                .trigger(availableNow=True) \
                .start()
            counter += 1
            if counter % (4*self.num_nodes) == 0:
                display (f"In process_batch_raw() - before waiting for queries to finish (intra-batch) - {counter}")
                for key, query in queries.items():
                    query.awaitTermination()
                queries = {}
                display (f"In process_batch_raw() - after for queries to finish (intra-batch) - {counter} ")

        display (f"In process_batch_raw() - before waiting for the last queries to finish (post-batch) - {counter}")

        for key, query in queries.items():
            query.awaitTermination()
         
        display (f"In process_batch_raw() - after waiting for the last queries to finish (post-batch) - {counter}")

    @staticmethod
    def replace_last(string, old, new):
        old_idx = string.rfind(old)
        return string[:old_idx] + new + string[old_idx+len(old):]

    def start_raw_batch(self, checkpoint_location, raw_file_name:str, suffix='dapEvent.json' ):

        display(f"in start_raw_batch; processing {raw_file_name}")

        raw_file_path = UDL.replace_last(raw_file_name, suffix, '')
        df = (self.spark.readStream
            .option("multiLine", "false")
            .option("inferSchema", "true")
            .format("json")
            .load(raw_file_path))

        df = df.withColumn('raw_json', to_json(struct(col("*"))))

        # Create another data frame with a new column 'EventType' based on the name.
        # This is needed because the file will have multiple event types in it.
        df = df.withColumn("EventType", df["name"])

        # todo: also get the event version from the data>version field and add that as another column (EventVersion)
        df = df.withColumn("EventVersion", df["data"]["version"])

        raw_query = (df.writeStream
            .option("checkpointLocation", checkpoint_location)
            .outputMode("append")
            .foreachBatch(self.process_batch_raw)
            .trigger(availableNow=True)
            .start())

        raw_query.awaitTermination()

    def reset_gold(self):

        db_name = "udl_gold"
        # spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        db_name = "udl_silver"

        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    def process_gold_complex_stream_batch (self, df:DataFrame, batch_id:int,table_name:str, table_conf: Dict[str,Any], gold_path: str )-> DataFrame:
        prefix = self.prefix
        df2 = df
        df2.createOrReplaceTempView("updates")
        upsert_sql_str = None
        upsert_sql = table_conf.get("upsert_sql")
        if upsert_sql is not None:
            upsert_sql_str = eval(f"str(f'''{upsert_sql}''')")
        if upsert_sql_str is not None:
            df2 = self.spark.sql(upsert_sql_str)
        return df2

    def process_gold_complex_stream(self,table_name:str, table_conf: Dict[str,Any] ):
        prefix = self.prefix
        gold_path = f"{prefix}/gold/complex/delta-parquet/{table_name}"

        source_sql = table_conf.get("source_sql")

        if source_sql is not None:
            source_sql_eval = eval(f"str(f'''{source_sql}''')")

        source_delta_file_suffix = table_conf.get("source_delta_file_suffix")
        if source_delta_file_suffix is not None:
            source_delta_file_path = eval(f"str(f'''{source_delta_file_suffix}''')")
            
        if not DeltaTable.isDeltaTable(self.spark, source_delta_file_path):
            return
        
        df = self.spark.sql(f"{source_sql_eval}")

        if not DeltaTable.isDeltaTable(self.spark, gold_path):
            self.save_df_with_retry(df, gold_path,'delta', 'append')



        checkpoint_location = None
        checkpoint_location_suffix = table_conf.get("checkpoint_location_suffix")
        if checkpoint_location_suffix is not None:
            checkpoint_location_suffix_eval = eval(f"str(f'''{checkpoint_location_suffix}''')")
            checkpoint_location = f"{self.prefix}/{checkpoint_location_suffix_eval}"
        
        self.process_gold_complex_stream_batch(df, 1, table_name, table_conf, gold_path)
        
        # raw_query = (df.writeStream \
        #     .option("checkpointLocation", checkpoint_location)
        #     .format("delta")
        #     .outputMode("update") 
        #     .foreachBatch(lambda df, bid: self.process_gold_complex_stream_batch(df,bid,table_name, table_conf, gold_path)) 
        #     .trigger(availableNow=True) 
        #     .start()
        # )
        # raw_query.awaitTermination()

    def process_gold_complex(self):
        complex_conf = self.complex_conf
        prefix = self.prefix
        display (f'in process_gold_complex(); config = {str(complex_conf)}')
        for table_name, table_conf in complex_conf.items():
            display(f"in process_gold_complex(); processing {table_name}")
            print(f"in process_gold_complex(); processing {table_name}")       
            self.process_gold_complex_stream(table_name, table_conf)

    def process_gold(self):

        dim_facts_conf = self.dim_facts_conf

        for key, val in dim_facts_conf.items():
            # path = f"{self.prefix}/silver/flattened/delta-parquet/{key}"
            path = self.get_table_path_key(False,'silver','flattened','delta-parquet', key)
            if DeltaTable.isDeltaTable(self.spark, path):
                display(f"In process_gold(); ingesting {path}")
                try:
                    df = self.spark.readStream \
                        .option("path", path) \
                        .option("inferSchema", "true") \
                        .format("delta") \
                        .load()
                    raw_query = df.writeStream \
                        .option("checkpointLocation", f"{path}/checkpoint") \
                        .outputMode("append") \
                        .foreachBatch(lambda data_frame,batch_id: self.process_batch_dim_facts(data_frame,batch_id, key)) \
                        .trigger(availableNow=True) \
                        .start()
                    raw_query.awaitTermination()
                except Exception as e: # pragma: no cover
                    display(f'in process_gold() - failed to process {path}: {str(e)}')
                    raise (e)
            else:
                display (f"in process_gold() - Skipping processing of {path}; delta table does not exist or path is not a delta table")
        self.process_gold_complex() 

    def get_blob_url(self,container_name: str, storage_account_name: str, blob_name: str):  # pragma: no cover
        return f'wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{blob_name}'
    
    def get_blobs(self, sas_token:str, storage_account_name: str, raw_prefix: str, container_name: str)-> List[str]:  # pragma: no cover
        self.blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=sas_token)

        # List the blobs in the container (with a wildcard)
        blobs = self.blob_service_client.get_container_client(container_name).list_blob_names(name_starts_with=raw_prefix)
        return blobs
    
    def move_blobs(self,container_name: str, blob_name: str, new_name: str):  # pragma: no cover
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        display(f'@@@  @ moving  {blob_name} to {new_name}')

        new_blob_client = self.blob_service_client.get_blob_client(container_name, new_name)

        # Copy the blob to the new name
        new_blob_client.start_copy_from_url(blob_client.url)

        # Delete the original blob

        blob_client.delete_blob()

    def process_silver(self, sas_token: str, storage_account_name: str, container_name: str, raw_prefix: str, raw_suffix: str, new_prefix: str, max_count: int = 5 ):

        self.spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
        self.spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
        self.spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)
        self.spark.conf.set(f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net", sas_token)
        # wildcard = 'prysm/recorder/stg/dapEvent.json/SBI.DAP.SQL2022/2023/';

        # raw_suffix ='dapEvent.json'
        # new_prefix = 'udl/recorder/stg/dapEvent.json/SBI.DAP.SQL2022/2023/';

        # List the blobs in the container (with a wildcard)
        blobs = self.get_blobs(sas_token, storage_account_name, raw_prefix, container_name)
        print(f"in UDL.process_silver() - got blobs {str(blobs)} - raw_suffix is {raw_suffix}")

        # Print the names of the blobs
        counter = 0
        for blob in blobs:
            print(f"in UDL.process_silver() - processing blob {blob}")
            display(f"in UDL.process_silver() - processing blob {blob}")
            blob_name = str(blob)
            if blob_name.endswith(raw_suffix):
                display(f'processing {blob_name} ({counter+1}/{max_count})')
                print(f'processing {blob_name} ({counter+1}/{max_count})')
                new_name = blob_name.replace(raw_prefix,new_prefix)
                bronze_raw = self.get_blob_url(container_name, storage_account_name, blob_name)
                bronze_raw_processed = self.get_blob_url(container_name, storage_account_name, new_name)

                self.lineage = {
                    "bronze_raw": bronze_raw,
                    "bronze_raw_processed": bronze_raw_processed
                }
                self.suffix = blob_name
                self.start_raw_batch(f'{self.prefix}/bronze/raw/checkpoint/{blob_name}', bronze_raw, raw_suffix)

                self.move_blobs(container_name, blob_name, new_name)
                counter = counter + 1
                if counter >= max_count:
                    display(f'STOPPED MICRO BATCH PROCESSING - REACHED {max_count} files')
                    break
            else:  # pragma: no cover
                print(f"in UDL.process_silver() - NOT processing blob {blob}")
                display(f"in UDL.process_silver() - NOT processing blob {blob}")

    def process(self, sas_token: str, storage_account_name: str, container_name: str, raw_prefix: str, raw_suffix: str, new_prefix: str, max_count: int = 5 ):
        self.process_silver(sas_token, storage_account_name, container_name, raw_prefix, raw_suffix, new_prefix, max_count )
        self.process_gold()




class UDL_Qmatic(UDL): # pragma: no cover
    def __init__(self, spark, prefix: str, data_type: str = 'qmatic'):
        super().__init__(spark, prefix, data_type)

    def parse_qmatic_name_ver(self,dlsr_df: DataFrame) -> str:
        file_name = UDL.get_df_source(dlsr_df)

        file_name_parsed_split = file_name.split('/')
        file_name_parsed = file_name_parsed_split[-1]
        name = UDL.replace_last(file_name_parsed,'.json','')
        ret_val = name

        return ret_val


    def start_raw_batch(self, checkpoint_location:str, raw_file_name:str, suffix='json' ):

        display(f"in start_raw_batch; processing {raw_file_name}")
        self.lineage = {
            "bronze_raw": raw_file_name,
            # "bronze_raw_processed": bronze_raw_processed
        }

        df = (self.spark.read 
          .option("multiLine", "false")
          .option("inferSchema", "true") 
          .format('json') 
          .load(raw_file_name)
        )
        
        json_file_name = UDL.get_df_source(df)
        qmatic_type = self.parse_qmatic_name_ver(df)
            # def get_table_path(self, medallion_layer:str, data_type:str, table_format:str, name:str, ver:str)-> str:

        json_file_path = self.get_table_path(self.use_separate_tables,'bronze','raw','qmatic/json',qmatic_type,'1')
        display(f"in start_raw_batch; processing {raw_file_name} - qmatic_type = {qmatic_type}; json_file_path = {json_file_path}")

        self.save_df_with_retry(df, json_file_path,'json', 'append')
        df.unpersist()

        df2 = self.spark.readStream \
            .option("multiLine", "false") \
            .option("inferSchema", "true") \
            .format("json") \
            .load(json_file_path)
        

        df2 = df2.withColumn('raw_json', to_json(struct(col("*"))))
        
        # Create another data frame with a new column 'EventType' based on the name.
        # This is needed because the file will have multiple event types in it.
        df2 = df2.withColumn("EventType", F.lit(qmatic_type))
        # Qmatic does not have different Event versions; we just hardwire it to '1'
        df2 = df2.withColumn("EventVersion", F.lit(1))
        raw_query = df2.writeStream \
            .option("checkpointLocation", checkpoint_location) \
            .outputMode("append") \
            .foreachBatch(self.process_batch_raw) \
            .trigger(availableNow=True) \
            .start()
        raw_query.awaitTermination()



class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)


class UDL_Sharepoint(UDL):
    def __init__(
        self,
        spark,
        prefix: str,
        tenant_id: str,
        app_client_id: str,
        client_secret_val: str,
        tenant_name: str,
        site_name: str,
        data_type: str = 'sharepoint'
    ):
        super().__init__(spark, prefix, data_type)
        self.use_app_history = False
        self.use_dedup_raw = False
        self.tenant_id = tenant_id
        self.app_client_id = app_client_id
        self.client_secret_val = client_secret_val
        self.tenant_name = tenant_name
        self.site_name = site_name
        # Build client
        self.credential = ClientSecretCredential(
            self.tenant_id,
            self.app_client_id,
            self.client_secret_val
        )
        self.scopes = ['https://graph.microsoft.com/.default']
        self.client = GraphServiceClient(
            credentials=self.credential,
            scopes=self.scopes
        )

    def set_udl_feature_flags(self):
        self.use_app_history = False
        self.app_history_data_path = f'{self.prefix}/silver/split/delta-parquet/app-history'
        self.use_dedup_raw = True
        self.use_separate_tables = True
        self.suffix = None

    async def get_site_id(self):
        self.web_url = f"https://{self.tenant_name}.sharepoint.com/sites/{self.site_name}"
        sites = await self.client.sites.get()
        for site in sites.value:
            if site.web_url == self.web_url:
                return site.id
        return None

    async def get_list_id_by_display_name(
        self,
        site_id: str,
        list_display_name: str
    ):
        query_params = ListsRequestBuilder.ListsRequestBuilderGetQueryParameters(
            # Only request specific properties
            select=['displayName', 'id'],
            filter=f"displayName eq '{list_display_name}'"
        )
        request_config = ListsRequestBuilder.ListsRequestBuilderGetRequestConfiguration(
            query_parameters=query_params
        )
        lists = await self.client.sites.by_site_id(site_id).lists.get(request_config)
        if lists is not None and len(lists.value) == 1:
            return lists.value[0].id
        return None

    async def get_list_data(
        self,
        site_id: str,
        list_id: str,
        list_data_last_url: str
    ):
        query_params = ItemsRequestBuilder.ItemsRequestBuilderGetQueryParameters(
        )
        request_config = ItemsRequestBuilder.ItemsRequestBuilderGetRequestConfiguration(
            query_parameters=query_params
        )
        if list_data_last_url is None:
            list_items = await (
              self.client.sites.by_site_id(site_id)
                  .lists.by_list_id(list_id)
                  .items.delta.get(request_config)
            )
            if list_items is not None and list_items.value:
                return list_items
        else:
            list_items = await (
              self.client.sites.by_site_id(site_id)
                  .lists.by_list_id(list_id)
                  .items.with_url(list_data_last_url).delta.get(request_config)
            )
            return list_items
        return None

    def get_last_url_path(self, list_name: str):
        return f"{self.prefix}/sharepoint/{list_name}"

    def get_last_url(self, list_name: str) -> str:
        try:
            df = self.spark.read.json(self.get_last_url_path(list_name))
            last_url = df.select("last_url").collect()[0][0]
            return last_url
        except Exception as e:
            return None

    def set_last_url(self, list_name: str, last_url: str):
        rdd = self.spark.sparkContext.parallelize([json.dumps({"last_url": last_url})])
        df = self.spark.read.json(rdd)
        self.save_df_with_retry(
            df,
            self.get_last_url_path(list_name),
            'json',
            'overwrite'
        )

    async def list_data_to_dataframe(
        self,
        list_name: str,
    ) -> DataFrame:
        site_id = await (self.get_site_id())
        # site_id = 'pontusvisioncom.sharepoint.com,46df1d00-55a1-4708-b778-60ce8cdfa73b,0ae06f7b-17ae-4674-b4db-8ad50bfbf134'
        list_id = await self.get_list_id_by_display_name(site_id, list_name)
        self.lineage = {
            "bronze_raw": f"{self.web_url}/Lists/{list_name}",
            # "bronze_raw_processed": bronze_raw_processed
        }
        list_data_last_url = self.get_last_url(list_name)
        dta = []
        while True:
            list_data = await self.get_list_data(site_id, list_id, list_data_last_url)
            if list_data.odata_next_link is not None:
                list_data_last_url = list_data.odata_next_link
            elif list_data.odata_delta_link is not None:
                list_data_last_url = list_data.odata_delta_link
            for item in list_data.value:
                dta.append(
                    json.dumps(
                        item.fields.additional_data,
                        cls=DatetimeEncoder
                    )
                )
            if len(list_data.value) == 0:
                break
        rdd = self.spark.sparkContext.parallelize(dta)
        df = self.spark.read.json(rdd)
        self.set_last_url(list_name,list_data_last_url)
        return df

    async def start_raw_batch(
        self,
        checkpoint_location: str,
        list_name: str,
        suffix: str = 'json'
    ):
        display(f"in start_raw_batch; processing {list_name}")
        df = await self.list_data_to_dataframe(list_name)

        df = df.withColumn('raw_json', F.to_json(F.struct(F.col("*"))))

        # Create another data frame with a new column 'EventType' based on the name.
        # This is needed because the file will have multiple event types in it.
        df = df.withColumn("EventType", F.lit(list_name))

        # Sharepoint does not have different Event versions; we just hardwire it to '1'
        df = df.withColumn("EventVersion", F.lit(1))

        df.show(5, True)

        # Get the current date
        current_date = datetime.now()
        # Format the date as "YYYY-MM-DD"
        self.suffix = current_date.strftime("%Y/%m/%d/%H:%M:%S")

        json_file_path = self.get_table_path(
            self.use_separate_tables,
            'bronze', 'split', 'json', list_name, "1")
        display(f"In start_raw_batch(); json file name is {json_file_path}; list_name = {list_name}")

        self.save_df_with_retry(df, json_file_path, 'json', 'append')

        df.unpersist()

        df = self.spark.readStream \
            .option("multiLine", "false") \
            .option("inferSchema", "true") \
            .format("json") \
            .load(json_file_path)

        if (self.use_dedup_raw): # pragma: no cover
            df = df.dropDuplicates()

        raw_query = (
            df.writeStream
              .option("checkpointLocation",  f"{json_file_path}_checkpoint")
              .outputMode("append")
              .foreachBatch(self.process_batch_raw)
              .trigger(availableNow=True)
              .start())
        raw_query.awaitTermination()

    async def process(self, list_name: str, not_used_1: str, not_used_2: str, not_used_3: str, not_used_4: str, not_used_5: str, max_count: int = 500):
        await self.start_raw_batch('', list_name)
        self.process_gold()