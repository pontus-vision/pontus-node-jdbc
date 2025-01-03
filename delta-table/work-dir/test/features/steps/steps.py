import json
import yaml
from behave import given, then, when
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, like
from IPython.display import display
import sys
import os
import shutil
from typing import List

# Get the absolute path to the 'udl' directory
udl_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..','udl'))

# Add the 'udl' directory to the Python path
sys.path.append(udl_dir)


from udl import UDL, UDL_Qmatic


class UDL_DAP_MOCK(UDL):
    def __init__(self, spark, prefix: str, data_type: str = 'dap'):
        super().__init__(spark, prefix, data_type)

    def get_blob_url(self,
                     container_name: str,
                     storage_account_name: str,
                     blob_name: str):
        return f'{blob_name}'

    def get_blobs(self,
                  sas_token: str,
                  storage_account_name: str,
                  raw_prefix: str,
                  container_name: str) -> List[str]:  # pragma: no cover
        return [f"{raw_prefix}/dapEvent.json"]

    def move_blobs(self, container_name, blob_name, new_name):
        pass


class UDL_Qmatic_MOCK(UDL_Qmatic):
    def __init__(self, spark, prefix: str, context):
        super().__init__(spark, prefix, 'qmatic')
        self.context = context

    def get_blob_url(self,
                     container_name: str,
                     storage_account_name: str,
                     blob_name: str):
        return f'{blob_name}'

    def get_blobs(self, sas_token:str,
                  storage_account_name: str,
                  raw_prefix: str, container_name: str) -> List[str]:
        return [f"{os.path.join(self.context.qmatic_list_path_prefix,self.context.qmatic_list_path_suffix)}"]

    def move_blobs(self, container_name, blob_name, new_name):
        pass


def cleanup(context, data_prefix):
    try:
        display("in cleanup() - dropping DB if it exists")
        context.spark.sql("DROP DATABASE IF EXISTS udl_gold CASCADE")
        print(f"in cleanup() - removing folder {os.path.join(data_prefix, 'bronze')}")
        shutil.rmtree(f"{os.path.join(data_prefix, 'bronze')}")

        display(f"in cleanup() - removing folder {os.path.join(data_prefix, 'silver')}")
        shutil.rmtree(f"{os.path.join(data_prefix, 'silver')}")

        display(f"in cleanup() - removing folder {os.path.join(data_prefix, 'gold')}")
        shutil.rmtree(f"{os.path.join(data_prefix, 'gold')}")

        display("in cleanup() - removed all folders")

    except Exception as e:
        print(f"in cleanup() - error cleaning up: {str(e)}")
        display(f"in cleanup() - error cleaning up: {str(e)}")
        pass
    try:
        display(f"in step cleanup() - removing folder {os.path.join(data_prefix, 'dap')}")
        shutil.rmtree(f"{os.path.join(data_prefix, 'dap')}")
    except Exception as e:
        print(f"in cleanup() - error cleaning up: {str(e)}")
        display(f"in cleanup() - error cleaning up: {str(e)}")
        pass


@given('a call to UDL.anonymise() using "{faker_type}" against "{val}" I expect "{exp}"')
def call_udl_anonymise(context, faker_type: str, val: str, exp: str):
    try:
        actual = UDL.anonymise(faker_type, val)
        assert actual == exp, f"Unexpected result <{actual}>; expected <{exp}>"
    except Exception as e:
        actual = str(e)
        assert actual == exp, f"Unexpected result <{actual}>; expected <{exp}>" 


@given('a DAP UDL object with the following data prefix "{data_prefix}"')
def create_dap_udl_application_history_object(context, data_prefix: str):
    context.data_prefix = data_prefix
    cleanup(context, data_prefix)
    context.dap_udl = UDL_DAP_MOCK(context.spark, data_prefix)
    display(f"in step create_dap_udl_object() - created dap_udl with data prefix {data_prefix}")


@given('a Qmatic UDL object with the following data prefix "{data_prefix}"')
def create_qmatic_udl_object(context, data_prefix):
    context.qmatic_data_prefix = data_prefix
    cleanup(context, data_prefix)
    context.qmatic_udl = UDL_Qmatic_MOCK(context.spark,data_prefix,context)
    display(f"in step create_qmatic_udl_object() - created qmatic_udl with data prefix {data_prefix}")


@given('a JSON object')
def create_json_object(context):
    # Parse the JSON data from the table
    context.json_text = context.text
    try:
        context.json_data = json.loads(context.text)
        json_rdd = context.spark.sparkContext.parallelize([context.text])
        context.df = context.spark.read.option("multiline", "true").option("inferSchema", 'true').json(json_rdd)
        context.df.show()
    except Exception as e:
        pass


@given('a list of JSON objects in each line written to path prefix "{path}"')
def create_json_object_list(context,path):
    # Parse the JSON data from the table
    context.json_list_text = context.text
    context.json_list_path_suffix = 'dapEvent.json'
    context.json_list_path_prefix = os.path.join(path,'pre-process')

    full_path = os.path.join(context.json_list_path_prefix,context.json_list_path_suffix)
    if not os.path.exists(context.json_list_path_prefix):
        os.makedirs(os.path.join(context.json_list_path_prefix))
    if os.path.exists(full_path):
        # delete the file
        os.remove(full_path)

    with open(full_path, "w+") as file:
        file.write(context.json_list_text)


@given('a list of qmatic JSON objects in each line written to path prefix "{path}" file name "{file_name}"')
def create_json_object_list_with_file(context, path: str, file_name: str):
    # Parse the JSON data from the table
    context.qmatic_list_text = context.text
    context.qmatic_list_path_suffix = file_name
    context.qmatic_list_path_prefix = os.path.join(path,'pre-process')

    full_path = os.path.join(context.qmatic_list_path_prefix,context.qmatic_list_path_suffix)
    if not os.path.exists(context.qmatic_list_path_prefix):
        os.makedirs(os.path.join(context.qmatic_list_path_prefix))
    if os.path.exists(full_path):
        # delete the file
        os.remove(full_path)
    with open(full_path, "w+") as file:
        file.write(context.qmatic_list_text)


@given('a list of CSV entries written to path prefix "{path}" file name "{file_name}"')
def create_csv_file(context, path, file_name):
    # Parse the JSON data from the table
    context.csv_list_text = context.text
    context.csv_list_path_suffix = file_name
    context.csv_list_path_prefix = os.path.join(path, 'pre-process')
    full_path = os.path.join(context.csv_list_path_prefix, context.csv_list_path_suffix)
    try:
        os.makedirs(os.path.join(context.csv_list_path_prefix))
    except:
        pass
    with open(full_path, "w+") as file:
        file.write(context.csv_list_text)


@when('I call the UDL.json_parse() function with the JSON object, event name "{event_name}" event version "{event_ver}"')
def call_udl_json_parse(context, event_name, event_ver):
    jsonSchemaDict = context.dap_udl.get_json_schema_dict(f'{context.dap_udl.prefix}/{context.dap_udl.data_type}-schema.csv')
    json_schema_str = jsonSchemaDict.get(f"{event_name}{event_ver}")
    context.json_parse_res = UDL.json_parse(context.json_text, json_schema_str, event_name, event_ver)


@when('I override the UDL dim_facts_conf with the following config')
def override_udl_dim_facts_conf(context):
    dim_facts_config = json.loads(context.text)
    try:
        if (context.dap_udl is not None):
            context.dap_udl.dim_facts_conf = dim_facts_config
    except:
        pass


@when('I override the UDL dim_facts_conf with the following yaml config')
def override_udl_dim_facts_conf_yaml(context):
    dim_facts_config = yaml.safe_load(context.text)
    try:
        if (context.dap_udl is not None):
            context.dap_udl.dim_facts_conf = dim_facts_config
    except:
        pass


@when('I override the UDL complex_conf with the following yaml config')
def override_udl_dim_facts_conf_complex(context):
    complex_conf = yaml.safe_load(context.text)
    try:
        if (context.dap_udl is not None):
            context.dap_udl.complex_conf.update(complex_conf)
    except:
        pass
    try:
        if (context.qmatic_udl is not None):
            context.qmatic_udl.complex_conf.update(complex_conf)
    except:
        pass


@when('I call the DAP UDL.process_batch_raw() function with the context.df')
def call_dap_process_batch_raw(context ):
    df = context.df
    df = df.withColumn('raw_json', to_json(struct(col("*"))))
    # Create another data frame with a new column 'EventType' based on the name.
    # This is needed because the file will have multiple event types in it.
    df = df.withColumn("EventType", df["name"])
    # todo: also get the event version from the data>version field and add that as another column (EventVersion)
    df = df.withColumn("EventVersion", df["data"]["version"])
    context.dap_udl.lineage = {'bronze_layer': 'test-001'}
    context.dap_udl.process_batch_raw(df, 0)


@when('I call the DAP UDL.process_gold() function')
def call_dap_process_gold(context):
    context.dap_udl.reset_gold()
    context.dap_udl.process_gold()


@when('I read a flattened delta parquet file for "{event_type}" version "{event_ver}"')
def read_flattened_object(context, event_type: str, event_ver: str):
    key = f"{event_type}_{event_ver}"
    flattened_path = f'{context.data_prefix}/silver/flattened/delta-parquet/{key}'
    context.flattened_df = context.spark.read.format('delta').load(flattened_path)


@when('I read an invalid flattened delta parquet file for "{event_type}" version "{event_ver}"')
def read_flattened_object_invalid(context,event_type,event_ver):
    key = f"{event_type}_{event_ver}-invalid"
    flattened_path = f'{context.data_prefix}/silver/flattened/delta-parquet/{key}'
    context.flattened_df = context.spark.read.format('delta').load(flattened_path)


@when('I run a sql query')
def run_sql_query(context):
    context.query_res = context.spark.sql(context.text).collect()


@when('I call the get_dtype() function in dap_udl for column "{col_name}"')
def call_dap_get_dtype(context, col_name):
    try:
        context.col_data_type = context.dap_udl.get_dtype(context.df, col_name)
    except Exception as e:
        context.col_data_type = str(e)


@when('I flatten the object')
def flatten_json_object(context):
    context.flattened_df = UDL.flatten_df(context.df)


@then('the expected event parse result is')
def check_data_type(context):
    actual = str(context.json_parse_res).replace('\n', '\\n')
    expect = str(context.text)
    assert actual == expect, f"Unexpected event parse result <{actual}>; expected <{expect}>"


@when('I call the dap_udl.process() method')
def call_dap_udl_process(context):
    raw_suffix = context.json_list_path_suffix
    raw_prefix = context.json_list_path_prefix
    new_prefix = context.json_list_path_prefix.replace('/pre','/post')
    context.dap_udl.process('sas_token', 'storage_acct', 'container_name', raw_prefix, raw_suffix, new_prefix, 1)


@when('I call the dap_udl.process_silver() method')
def call_dap_udl_process_silver(context):
    raw_suffix = context.json_list_path_suffix
    raw_prefix = context.json_list_path_prefix
    new_prefix = context.json_list_path_prefix.replace('/pre','/post')
    context.dap_udl.process_silver('sas_token', 'storage_acct', 'container_name', raw_prefix, raw_suffix, new_prefix, 1)


@when('I call the qmatic_udl.process() method')
def call_qmatic_udl_process(context):
    raw_suffix = context.qmatic_list_path_suffix
    raw_prefix = context.qmatic_list_path_prefix
    new_prefix = context.qmatic_list_path_prefix.replace('/pre','/post')
    context.qmatic_udl.process('sas_token', 'storage_acct', 'container_name', raw_prefix, raw_suffix, new_prefix, 1)


@when('I call the qmatic_udl.process_silver() method')
def call_qmatic_udl_process_silver(context):
    raw_suffix = context.qmatic_list_path_suffix
    raw_prefix = context.qmatic_list_path_prefix
    new_prefix = context.qmatic_list_path_prefix.replace('/pre','/post')
    context.qmatic_udl.process_silver('sas_token', 'storage_acct', 'container_name', raw_prefix, raw_suffix, new_prefix, 1)


@when('I call the qmatic_udl.process_gold() method')
def call_qmatic_udl_process_gold(context):
    context.qmatic_udl.process_gold()


@then('I call the udl.get_fact_table_conf() method with key "{key}" tableName "{table_name}"  and expect the following output')
def call_udl_get_fact_table_conf(context, key, table_name):
    exp_res = json.loads(context.text)
    actual_json = None
    try:
        if (context.dap_udl is not None):
            actual_json = context.dap_udl.get_fact_table_conf(key,table_name)
    except:
        pass
    expected = json.dumps(exp_res)
    actual = json.dumps(actual_json)
    assert (expected) == (actual), f"expected <{expected}>, actual <{actual}>"


@then('I get the time machine history on table "{table_name}" and expect the following output')
def call_describe_history(context, table_name):
    actual = context.spark.sql( \
         f"DESCRIBE HISTORY {table_name}") \
        .select('version', 'operationParameters.properties') \
        .filter('operationParameters.properties is not NULL') \
        .toPandas()\
        .to_markdown(index=False)
    expected = context.text
    assert (expected) == (actual), f"expected <{expected}>, actual <{actual}>"


@then('the expected col data type is "{col_data_type}"')
def check_data_type_col_type(context, col_data_type):
    actual = str(context.col_data_type)
    expect = col_data_type
    assert actual == expect, f"Unexpected col data type <{actual}>; expected <{expect}>"


# Then the expected schema is provided
@then('the expected flattened schema is "{schema}"')
def check_expected_schema(context, schema):
    context.expected_schema = schema
    actual_schema = str(context.flattened_df.schema)
    assert actual_schema == context.expected_schema, f"Unexpected schema: {actual_schema}"


@then('the expected flattened columns are "{schema}"')
def check_expected_columns(context, schema):
    context.expected_cols = schema
    actual_cols = str(context.flattened_df.columns)
    assert actual_cols == context.expected_cols, f"Unexpected cols: {actual_cols}"


@then('the expected query result is')
def check_expected_query_result(context):
    actual_res = str(str(context.query_res).replace('\n','').replace('  ', ' '))
    exp_res = str(str(context.text).strip().replace('\n','').replace('  ', ' '))
    assert exp_res == actual_res, f"Unexpected result: <{actual_res}>; expected <{exp_res}>"


# Then the expected data is provided
@then('the expected data is')
def set_expected_data(context):
    # table_rdd = context.spark.sparkContext.parallelize(context.table)
    table_from_feature = context.table
    df = context.flattened_df
    # Show the DataFrames (for demonstration purposes)
    print("Original DataFrame:")
    df.show()
    print(f"Table DataFrame:\n{str(table_from_feature)}")
    row_counter = 0
    for row_dict in table_from_feature:
        # Extract column names and values dynamically
        # row_values = [(col_name, row_dict.get(col_name)) for col_name in row_dict]
        print(f"row = {row_dict}")
        col_counter = 0
        # Filter the DataFrame based on row values
        for col_name in row_dict.headings:
            exp_val = str(row_dict.cells[col_counter])
            actual_val = str(df.collect()[row_counter][col_counter]).replace('\n','\\n')
            print(f"actual_val [{row_counter}, {col_counter}] = <{actual_val}>")
            print(f"exp val = <{exp_val}>")
            col_counter = col_counter + 1
            assert exp_val == actual_val, f"Unexpected val: <{actual_val}>, expected <{exp_val}>"
        row_counter = row_counter + 1