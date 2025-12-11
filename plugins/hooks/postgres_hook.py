from typing import List, Dict, Any, Union, Optional
from datetime import date
import logging
import json

from airflow.sdk.bases.hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

class PostgresDataHook(BaseHook):
    def __init__(self, postgres_conn_id: str = "postgres_default"):
        super().__init__()
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def create_table_from_record(
            self, 
            table_name: str, 
            record: dict, 
            primary_keys: Optional[List[str]] = None
    ):
        sql_type_map = {
            int: "INTEGER",
            float: "FLOAT",
            str: "TEXT",
            bool: "BOOLEAN",
            dict: "JSONB",
            list: "JSONB",
            type(None): "TEXT",
        }

        columns = []
        for key, value in record.items():
            sql_type = sql_type_map.get(type(value), "TEXT")
            columns.append(f"{key.lower().strip()} {sql_type}")

        # Ensure run_date column exists for partitioning
        if "run_date" not in record:
            columns.append("run_date DATE NOT NULL")

        # Primary key
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
            r = {
                k.lower().strip(): json.dumps(v) if isinstance(v, (dict, list)) else v
                for k, v in record.items()
            }
            r["run_date"] = run_date
            insert_records.append(r)

        if not insert_records:
            return

        keys = insert_records[0].keys()
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
