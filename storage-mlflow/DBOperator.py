import sys
sys.path.append('./')

from utils.support_query import QueryTemplate
from core.configuration import Settings
from contextlib import closing
import sqlalchemy as sql
from sqlalchemy import inspect
from datetime import datetime
import polars as pl
from functools import reduce
import operator
import os


class SQLOperators:
    def __init__(self, conn_id:str, settings:Settings):
        try:
            self.settings = settings
            self.__dbconn = sql.create_engine(f"{self.settings.DB_URL}/{conn_id}")
            self.__inspector = inspect(self.__dbconn)
            with self.__dbconn.connect() as conn:
                conn.execute(sql.text('SELECT 1'))
        except:
            raise Exception(f"====> Can't connect to '{conn_id}' database with host: {self.settings.DB_URL}")
    
    def get_dataframe_from_database(self, query) -> pl.DataFrame:
        try:
            df = pl.read_database(query=query, connection=self.__dbconn, infer_schema_length=1000)
        except Exception as ex:
            df = pl.DataFrame()
            raise Exception(str(ex))    
        return df
    
    def execute_query(self, query):
        with self.__dbconn.begin() as cur:
            try:
                cur.execute(query)
            except:
                cur.close()
                raise Exception(f"====> Can't execute query: {query}")
    
    def create_modified_date(self, df:pl.DataFrame):
        try:
            temp_df = df.with_columns(
                modifiedDate = datetime.now()
            )
        except Exception as ex:
            raise ValueError(str(ex))
        return temp_df
    
    def get_latest_fetching_time(self, table_name, schema, finished_time, dag_name):
        audit = self.get_dataframe_from_database(query=f'''SELECT * FROM {schema}."{table_name}"''')
        latest_time = audit.filter(
            pl.col('status').str.to_uppercase() == 'SUCCESS',
            pl.col('dag') == dag_name
        ).select(
            pl.col(finished_time).max()
        ).to_dicts()[0][finished_time]
        return latest_time
    
    def upsert_dataframe_table(self, table_name:str, schema:str, data:list, columns:list, conflict_column:tuple=None, arrjson:list=[], chunk_size=10000):
        query = QueryTemplate(table_name, schema).create_query_upsert(columns, conflict_column, arrjson)
        with self.__dbconn.begin() as cur:
            try:
                for i in range(0, len(data), chunk_size):
                    partitioned_data = data[i:i+chunk_size]
                    num_records = len(partitioned_data)
                    cur.execute(sql.text(query), partitioned_data)
                    # cur.commit()
                    print(f"merged or updated {num_records} records")
            except Exception as ex:
                cur.close()
                raise Exception(f"====> Can't execute {query} - {str(ex)}")
            
    def insert_dataframe_table_nonconflict(self, table_name:str, schema:str, data:list, columns:list, conflict_column:tuple=None, arrjson:list=[] ,chunk_size:int=10000):
        query = QueryTemplate(table_name, schema).create_query_insert_nonconflict(columns, conflict_column, arrjson)
        try:
            with closing(self.__dbconn.connect()) as cur:
                for i in range(0, len(data), chunk_size):
                    partitioned_data = data[i:i+chunk_size]
                    num_records = len(partitioned_data)
                    cur.execute(sql.text(query), partitioned_data)
                    cur.commit()
                    print(f"merged or updated {num_records} records")
        except Exception as ex:
            raise Exception(f"====> Can't execute {query} - {str(ex)}")
        
    def insert_dataframe_table(self, table_name:str, schema:str, data:list, columns:list, arrjson:list=[] ,chunk_size:int=10000):
        query = QueryTemplate(table_name, schema).create_query_insert(columns, arrjson)
        try:
            with closing(self.__dbconn.connect()) as cur:
                for i in range(0, len(data), chunk_size):
                    partitioned_data = data[i:i+chunk_size]
                    num_records = len(partitioned_data)
                    cur.execute(sql.text(query), partitioned_data)
                    cur.commit()
                    print(f"merged or updated {num_records} records")
        except Exception as ex:
            raise Exception(f"====> Can't execute {query} - {str(ex)}")
        
    def update_dataframe_table(self, table_name:str, schema:str, data:list, columns:list, where_columns:list, arrjson:list=[] ,chunk_size:int=10000):
        query = QueryTemplate(table_name, schema).create_query_update(columns, where_columns, arrjson)
        try:
            with closing(self.__dbconn.connect()) as cur:
                for i in range(0, len(data), chunk_size):
                    partitioned_data = data[i:i+chunk_size]
                    num_records = len(partitioned_data)
                    cur.execute(sql.text(query), partitioned_data)
                    cur.commit()
                    print(f"merged or updated {num_records} records")
        except Exception as ex:
            raise Exception(f"====> Can't execute {query} - {str(ex)}")
    
    def delete_records_in_table(self, table_name, key_field, values):
        query = QueryTemplate(table_name).create_delete_query(key_field, values)
        try:
            with closing(self.mysql_conn) as conn:
                if self.mysqlhook.supports_autocommit:
                    self.mysqlhook.set_autocommit(conn, False)
                conn.commit()
                with closing(conn.cursor()) as cur:
                    lst = []
                    for cell in values:
                       lst.append(self.mysqlhook._serialize_cell(cell, conn))
                    del_values = tuple(lst)
                    cur.execute(query, del_values)
                    num_records = len(values)
                    print(f"deleted {num_records} records")
                    conn.commit()
                conn.commit()
        except:
            raise Exception(f"====> Can't execute {query}")
        
    def append_data_into_table(self, df, table_name, schema, dtype=None, primary_key:str=None, chunksize:int=10000):
        method = "replace" if not self.__inspector.has_table(table_name, schema) else "append"
        try:
            df.to_pandas(use_pyarrow_extension_array=True).to_sql(
                name=table_name,
                schema=schema,
                con=self.__dbconn,
                if_exists=method,
                index=False,
                dtype=dtype,
                chunksize=chunksize
            )
            if method == 'replace':
                with self.__dbconn.connect() as conn:
                    conn.execute(sql.text(f'''
                        ALTER TABLE {table_name}
                        ADD CONSTRAINT {table_name}_pkey PRIMARY KEY ({primary_key})                
                    '''))
        except Exception as ex:
            raise Exception(str(ex))
        return df
    
    def remove_table_if_exists(self, table_name):
        try:
            with self.__dbconn.connect() as cur:
                remove_table = f'''DROP TABLE IF EXISTS "{table_name}";'''
                cur.execute(sql.text(remove_table))
                cur.commit()
        except Exception as ex:
            raise Exception(f"====> Can't remove table: {table_name} - {str(ex)}")
    
    def truncate_table(self, table_name, schema):
        try:
            with self.__dbconn.connect() as cur:
                cur.execute(sql.text(f'''truncate table {schema}."{table_name}" CASCADE'''))
                cur.commit()
        except Exception as ex:
            raise Exception(f"====> Can't truncate data from: {table_name} - {str(ex)}")
    
    def dump_table_into_path(self, table_name):
        try:
            priv = self.mysqlhook.get_first("SELECT @@global.secure_file_priv")
            if priv and priv[0]:
                tbl_name = list(table_name)[0]
                file_name = tbl_name.replace(".", "__")
                self.mysqlhook.bulk_dump(tbl_name, os.path.join(priv[0], f"{file_name}.txt"))
            else:
                raise PermissionError("missing priviledge")
        except:
            raise Exception(f"Can't dump {table_name}")
            
    def load_data_into_table(self, table_name):
        try:
            priv = self.mysqlhook.get_first("SELECT @@global.secure_file_priv")
            if priv and priv[0]:
                file_path = os.path.join(priv[0], "TABLES.txt")
                load_data_into_tbl = f"LOAD DATA INFILE '{file_path}' INTO TABLE {table_name};"
                cur = self.mysql_conn.cursor()
                cur.execute(load_data_into_tbl)
                self.mysql_conn.commit()
            else:
                raise PermissionError("missing priviledge")
        except:
            raise Exception(f"Can't load {table_name}")
            
            
            
class SCDOperators(SQLOperators):
    def __init__(self, conn_id: str, settings: Settings):
        super().__init__(conn_id, settings)
    
    def update_audit(self, message:str, success_status:str, dag_name:str):
        # Nếu cả hai flags đều True
        if success_status=='FAILED':
            info = f'Error some task {message}'
        else:
            info = f'All tasks are successfull: {message}'
        
        query = f"""
                UPDATE public."DimAudit" 
                SET
                    finished_at = NOW(),
                    information = {info},
                    status = {success_status}
                WHERE dag='{dag_name}' AND (SELECT MAX(start_at) FROM "DimAudit")
            """
        self.execute_query(query)
        
        if success_status != 'FAILED':
            print("Both flags are True. Proceeding to the next task.")
        else:
            print("One of the flags is False. Stopping pipeline.")
            raise Exception("Stopping pipeline due to one False flag.")
        
    def write_logs(self, flags, success_status='RUNNING'):
        all_flags_true = True
        for flag in flags:
            if flag==None:
                all_flags_true=False
                break
        message = ""
        for flag in flags:
            if flag!=None:
                message += flag['task'] + ", " 
        
        if all_flags_true:
            SCDOperators.update_audit(status=success_status)
        else:
            SCDOperators.update_audit(status='FAILED')
    
    def filter_newest(self, df, timecol, dag_name):
        latest_time = self.get_latest_fetching_time('DimAudit','public','finished_at', dag_name)
        df = df.with_columns(
                pl.col(timecol).cast(pl.Datetime).alias(f'{timecol}_time')
            ).filter(
                pl.col(f'{timecol}_time') > latest_time
            ).drop(f'{timecol}_time')
        return df
    
    def scd2_changed_rows(self, table_name:str, schema:str, dim_df:pl.DataFrame, stg_df:pl.DataFrame, scd2_columns:list):
        ## TODO: Find updated data (included changed and new data)
        changed_df = dim_df.join(other=stg_df, on='uuid', how='right', suffix='_new')
        
        ## TODO: Find object ids from updated datasets above -> update rowIsCurrent and rowEndDate columns in Dimension
        changed_df = changed_df.filter(
            pl.col(scd2_columns[1]) == True    
        ).with_columns(
            pl.lit(False).alias('rowIsCurrent'),
            pl.lit(datetime.now()).alias('rowEndDate')
        ).select(scd2_columns)
        
        ## TODO: Update scd type-2 timing columns
        self.update_dataframe_table(table_name, schema, changed_df.to_dicts(), ['rowIsCurrent', 'rowEndDate'], ['id'])
        
        return changed_df
    
    def scd1_updated_rows(self, joined_df:pl.DataFrame, columns:list, excepted_columns:list) -> pl.DataFrame:
        # Kết hợp các điều kiện bằng toán tử OR (|)
        conditions = [
            (pl.col(col).is_null() & pl.col(f"{col}_new").is_not_null()) |
            (pl.col(col) != pl.col(f"{col}_new")) for col in columns if col not in excepted_columns
        ]
        combined_condition = reduce(operator.or_, conditions)
        # Update the newest values to each column
        updated_df = joined_df.filter(
            combined_condition
        ).select([
            pl.col(excepted_columns[0]),
            *[pl.col(f"{col}_new").alias(col) for col in columns if col not in excepted_columns]
        ])
        return updated_df

    def scd1_new_rows(self, joined_df:pl.DataFrame, columns:list, excepted_columns:list) -> pl.DataFrame:
        # Kết hợp các điều kiện bằng toán tử OR (|)
        conditions = [
            pl.col(col).is_null() for col in columns if col not in excepted_columns
        ]
        combined_condition = reduce(operator.and_, conditions)
        # Update the newest values to each column
        inserted_df = joined_df.filter(
            combined_condition
        ).select([
            pl.col(excepted_columns[0]),
            *[pl.col(f"{col}_new").alias(col) for col in columns if col not in excepted_columns]
        ])
        return inserted_df
    
    def scd1_find_data(self, dim_df, stgDim_df, special_colums:list):
        if dim_df.shape[0] <= 1:
            inserted_df = stgDim_df
            updated_df = pl.DataFrame()
        else:
            joined_df = dim_df.join(other=stgDim_df, on=special_colums[0], how='right', suffix='_new')
            # Find brand-new rows needed to be inserted
            inserted_df = self.scd1_new_rows(joined_df, dim_df.columns, special_colums)
            # Find updated rows
            updated_df = self.scd1_updated_rows(joined_df, dim_df.columns, special_colums)
            updated_df = updated_df.join(other=inserted_df, on=special_colums[0], how='anti')
            
        return updated_df, inserted_df
    
    def insert_to_database(self, mode:str, table_name:str, schema:str, columns:list, data:list=None, conflict_columns:list=None, arrjson:list=[]):
        if mode == 'stage':
        # write data to warehouse database
            self.insert_dataframe_table_nonconflict(table_name, schema, data, columns, conflict_columns, arrjson)
        elif mode == 'scd-type1':
            self.upsert_dataframe_table(table_name, schema, data, columns, conflict_columns, arrjson)
        else:
            raise KeyError('mode key is not correct - There are "scd-type1","scd-type2"')