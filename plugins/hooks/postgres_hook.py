from typing import List, Tuple, Dict, Any, Union, Optional
from datetime import date
import logging
import json

from airflow.sdk.bases.hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from plugins.utils.constants import SQL_TYPE_MAP

class PostgresDataHook(BaseHook):
    def __init__(self, postgres_conn_id: str = "postgres_default"):
        super().__init__()
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.table_keys = {}

    def create_table_from_record(
            self, 
            table_name: str, 
            record: dict, 
            primary_keys: Optional[List[str]] = None
        ):
        columns = []
        keys = []

        # Normalize and map all record fields
        for key, value in record.items():
            key = key.lower().strip()
            sql_type = SQL_TYPE_MAP.get(type(value), "TEXT")
            columns.append(f"{key} {sql_type}")
            keys.append(key)

        # Add system field run_date (only as column, not user keys)
        if "run_date" not in record:
            columns.append("run_date DATE NOT NULL")

        # Save table keys (only user fields, NOT run_date)
        if table_name not in self.table_keys:
            self.table_keys[table_name] = keys

        # Primary key setup
        pk = ", ".join(primary_keys) if primary_keys else None
        pk_sql = f", PRIMARY KEY ({pk})" if pk else ""

        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)}{pk_sql});"

        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(create_sql)
            conn.commit()
            logging.info(f"Table '{table_name}' created or already exists.")
        except Exception as e:
            conn.rollback()
            logging.error(f"Failed to create table {table_name}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def create_table_if_not_exists(
            self,
            table_name: str,
            columns: Tuple[str],
            row: Tuple[Any],
            primary_keys: List[str]
        ):
        # Define SQL request
        pk = ", ".join(primary_keys) if primary_keys else None
        pk_sql = f", PRIMARY KEY ({pk})" if pk else ""
        columns_sql = []
        for column, value in zip(columns, row):
            sql_type = SQL_TYPE_MAP.get(type(value), "TEXT")
            columns_sql.append(f"{column} {sql_type}")

        request_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_sql)}{pk_sql});"

        # Create table
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(request_sql)
            conn.commit()
            logging.info(f"Table '{table_name}' created or already exists.")
        except Exception as e:
            conn.rollback()
            logging.error(f"Failed to create table {table_name}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def insert(
        self,
        table: str,
        records: Union[dict, List[dict]],
        run_date: Optional[date] = None
    ) -> None:
            
        if not isinstance(records, list):
            records = [records]

        insert_records = []

        for record in records:

            # Normalize user-provided keys
            normalized = {k.lower().strip(): v for k, v in record.items()}

            r = {}

            # Ensure all expected columns exist
            for col in self.table_keys[table]:
                value = normalized.get(col)

                if isinstance(value, (dict, list)):
                    value = json.dumps(value)

                r[col] = value

            # Add run_date
            r["run_date"] = run_date
            insert_records.append(r)

        if not insert_records:
            return

        # SQL 
        keys = self.table_keys[table] + ["run_date"]
        columns = ", ".join(keys)
        placeholders = ", ".join([f"%({k})s" for k in keys])
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        conn = self.hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.executemany(sql, insert_records)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"Failed to insert into {table}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def insert_norm_rows(
            self,
            table_name: str,
            rows: List[Tuple],
            columns: Tuple[str]
        ):
        self.hook.insert_rows(
            table=table_name,
            rows=rows,
            target_fields=columns,
            commit_every=0, # Commits all rows in one transaction
            # For performance with large inserts, especially if using a compatible psycopg2 version:
            # fast_executemany=True 
        )

    def find(
        self,
        table: str,
        run_date: Optional[date] = None,
        where: Optional[str] = None,
        params: Optional[dict] = None
    ) -> List[Dict[str, Any]]:
        """
        Select records from table optionally filtered by run_date and additional WHERE clause.
        """
        sql = f"SELECT * FROM {table}"
        conditions = []

        if run_date:
            conditions.append("run_date = %(run_date)s")

        if where:
            conditions.append(where)

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        final_params = {"run_date": run_date} if run_date else {}
        if params:
            final_params.update(params)

        conn = self.hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql, final_params)

        columns = [desc[0].lower().strip() for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        result = []
        for row in rows:
            record = {}
            for k, v in zip(columns, row):
                if isinstance(v, str):
                    try:
                        parsed = json.loads(v)
                        record[k] = parsed
                    except (json.JSONDecodeError, TypeError):
                        record[k] = v
                else:
                    record[k] = v
            result.append(record)

        return result

    def delete(
        self,
        table: str,
        run_date: Optional[date] = None,
        where: Optional[str] = None,
        params: Optional[dict] = None
    ) -> int:
        """
        Delete records from table filtered by run_date and optional WHERE clause.
        """
        sql = f"DELETE FROM {table}"
        conditions = []

        if run_date:
            conditions.append("run_date = %(run_date)s")
        if where:
            conditions.append(where)

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        final_params = {"run_date": run_date} if run_date else {}
        if params:
            final_params.update(params)

        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, final_params)
            deleted_count = cursor.rowcount
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"Failed to delete from {table}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

        return deleted_count
