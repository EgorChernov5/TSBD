from typing import List, Tuple, Dict, Any, Union, Optional, Iterable
from datetime import date
import logging
import json
from psycopg2.extras import execute_values
import pandas as pd

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
            primary_keys: List[str],
            surrogate_key: Optional[str] = None
        ):
        # Define SQL request
        pk = ", ".join(primary_keys) if primary_keys else None
        pk_sql = f", PRIMARY KEY ({pk})" if surrogate_key is None else ""
        columns_sql = [f"{surrogate_key} INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY"] if surrogate_key is not None else []
        for column, value in zip(columns, row):
            sql_type = SQL_TYPE_MAP.get(type(value), "TEXT")
            if (column in primary_keys) and (surrogate_key is not None):
                sql_type += ' NOT NULL UNIQUE'

            columns_sql.append(f"{column} {sql_type}")
                
        # columns_sql.append("run_date DATE NOT NULL")  # add system field run_date
        request_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_sql)}{pk_sql});"
        print(request_sql)

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

    def insert_scd1_data(
        self,
        df: pd.DataFrame,
        table_name: str,
        pk_cols: Iterable[str],
        schema: str = "public"
    ) -> None:
        """
        Insert only NEW SCD1 rows
        """

        if df.empty:
            self.log.info("No rows to insert for SCD1 in %s", table_name)
            return

        cols = list(df.columns)

        conn = self.hook.get_conn()
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {schema}.{table_name} (
                        {", ".join(cols)}
                    )
                    VALUES %s
                    """,
                    [tuple(row[c] for c in cols) for _, row in df.iterrows()]
                )

            conn.commit()
            self.log.info("Inserted %s new rows into %s", len(df), table_name)
        except Exception:
            conn.rollback()
            self.log.exception("Insert new SCD1 data failed")
            raise
        finally:
            conn.close()
    
    def insert_scd2_data(
        self,
        df: pd.DataFrame,
        table_name: str,
        change_ts,
        schema: str = "public"
    ) -> None:
        """
        Insert only NEW SCD2 rows
        """

        if df.empty:
            self.log.info("No rows to insert for SCD2 in %s", table_name)
            return

        cols = list(df.columns)

        conn = self.hook.get_conn()
        try:
            with conn.cursor() as cur:
                values = [tuple(row[c] for c in cols) for _, row in df.iterrows()]

                execute_values(
                    cur,
                    f"""
                    INSERT INTO {schema}.{table_name} (
                        {", ".join(cols)},
                        start_date,
                        end_date,
                        is_current
                    )
                    VALUES %s
                    """,
                    [
                        (*row, change_ts, None, True)
                        for row in values
                    ]
                )

            conn.commit()
            self.log.info("Inserted %s new rows into %s", len(df), table_name)
        except Exception:
            conn.rollback()
            self.log.exception("Insert new SCD2 data failed")
            raise
        finally:
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
    
    def find_changes(self, table_name, business_key_cols, tracked_cols, unique_bk_set):
        # sql = """
        # SELECT
        #     s.CustomerBK,
        #     s.Name,
        #     s.City
        # FROM StagingCustomer s
        # LEFT JOIN DimCustomer d
        #     ON s.CustomerBK = d.CustomerBK
        #     AND d.IsCurrent = 'Y'
        # WHERE
        #     d.CustomerSK IS NULL  -- Новые клиенты
        #     OR (
        #         d.CustomerSK IS NOT NULL
        #         AND (
        #             ISNULL(s.Name, '') != ISNULL(d.Name, '')
        #             OR ISNULL(s.City, '') != ISNULL(d.City, '')
        #         )
        #     );
        # """

        # === 2. Получаем текущие активные версии из БД ===
        bk_col_list = ', '.join(business_key_cols)
        tracked_col_list = ', '.join(tracked_cols)

        placeholders = ', '.join(['%s'] * len(business_key_cols))
        in_clause = ' OR '.join([f"({bk_col_list}) = ({placeholders})"] * len(unique_bk_set))

        # TODO: change query
        query = f"""
            SELECT {bk_col_list}, {tracked_col_list}, customer_sk, version
            FROM {table_name}
            WHERE is_current = true
            AND ({in_clause})
        """

        # "Выпрямляем" параметры для запроса: каждый business key — кортеж аргументов
        query_params = []
        for bk_tuple in unique_bk_set:
            query_params.extend(bk_tuple)

        conn = self.hook.get_conn()
        with conn.cursor() as cur:
            cur.execute(query, query_params)
            current_rows = cur.fetchall()

            return current_rows, bk_col_list

    def update_scd1_data(
        self,
        df: pd.DataFrame,
        table_name: str,
        pk_cols: Iterable[str],
        schema: str = "public"
    ) -> None:
        """
        Update existing rows in SCD Type 1 table.
        - df: DataFrame с новыми значениями для обновления
        - table_name: имя таблицы
        - pk_cols: составной ключ для поиска строк
        """

        if df.empty:
            self.log.info("No rows to update for SCD1 in %s", table_name)
            return

        pk_cols = list(pk_cols)
        data_cols = [c for c in df.columns if c not in pk_cols]

        if not data_cols:
            self.log.warning("No columns to update for SCD1 in %s", table_name)
            return

        conn = self.hook.get_conn()

        try:
            with conn.cursor() as cur:
                # Формируем SET выражение
                set_expr = ", ".join([f"{c} = %s" for c in data_cols])
                where_expr = " AND ".join([f"{c} = %s" for c in pk_cols])

                sql = f"""
                    UPDATE {schema}.{table_name}
                    SET {set_expr}
                    WHERE {where_expr}
                """

                # Подготавливаем данные для execute_values
                values_list = [
                    [row[c] for c in data_cols] + [row[c] for c in pk_cols]
                    for _, row in df.iterrows()
                ]

                # Выполняем пакетное обновление
                execute_values(cur, sql, values_list, template=None, page_size=100)

            conn.commit()
            self.log.info("SCD1 update completed: %s rows", len(df))

        except Exception:
            conn.rollback()
            self.log.exception("SCD1 update failed")
            raise

        finally:
            conn.close()

    def update_scd2_data(
        self,
        df: pd.DataFrame,
        table_name: str,
        pk_cols: Iterable[str],
        change_ts,
        schema: str = "public"
    ) -> None:
        """
        Close old versions and insert new versions for CHANGED rows
        """

        if df.empty:
            self.log.info("No rows to update for SCD2 in %s", table_name)
            return

        pk_cols = list(pk_cols)
        data_cols = [c for c in df.columns if c not in pk_cols]

        conn = self.hook.get_conn()

        try:
            with conn.cursor() as cur:

                # 1️⃣ temp table
                cur.execute(f"""
                    CREATE TEMP TABLE tmp_changed
                    (LIKE {schema}.{table_name} INCLUDING DEFAULTS)
                    ON COMMIT DROP;
                """)

                # 2️⃣ load df → temp
                values = [
                    tuple(row[c] for c in pk_cols + data_cols)
                    for _, row in df.iterrows()
                ]

                execute_values(
                    cur,
                    f"""
                    INSERT INTO tmp_changed (
                        {", ".join(pk_cols + data_cols)}
                    )
                    VALUES %s
                    """,
                    values
                )

                # 3️⃣ close old rows
                join_cond = " AND ".join(
                    [f"t.{c} = s.{c}" for c in pk_cols]
                )

                cur.execute(f"""
                    UPDATE {schema}.{table_name} t
                    SET
                        end_date = %s,
                        is_current = FALSE
                    FROM tmp_changed s
                    WHERE
                        {join_cond}
                        AND t.is_current = TRUE;
                """, (change_ts,))

                # 4️⃣ insert new versions
                cur.execute(f"""
                    INSERT INTO {schema}.{table_name} (
                        {", ".join(pk_cols + data_cols)},
                        start_date,
                        end_date,
                        is_current
                    )
                    SELECT
                        {", ".join(pk_cols + data_cols)},
                        %s,
                        NULL,
                        TRUE
                    FROM tmp_changed;
                """, (change_ts,))

            conn.commit()
            self.log.info(
                "Updated %s changed rows in %s",
                len(df),
                table_name
            )

        except Exception:
            conn.rollback()
            self.log.exception("Update SCD data failed")
            raise

        finally:
            conn.close()

    def update_sqd(
            self,
            table_name,
            business_key_cols,
            tracked_cols,
            bk_col_list,
            current_by_bk,
            changed_or_new,
            today,
            end_date_sentinel
        ):
        # === 4. Закрываем текущие версии (UPDATE) ===
        bk_to_close = [
            tuple(rec[col] for col in business_key_cols)
            for rec, _ in changed_or_new
            if tuple(rec[col] for col in business_key_cols) in current_by_bk
        ]

        conn = self.hook.get_conn()
        if bk_to_close:
            placeholders = ', '.join(['%s'] * len(business_key_cols))
            in_clause = ' OR '.join([f"({bk_col_list}) = ({placeholders})"] * len(bk_to_close))
            update_query = f"""
                UPDATE {table_name}
                SET end_date = %s, is_current = false
                WHERE is_current = true AND ({in_clause})
            """
            update_params = [today]
            for bk in bk_to_close:
                update_params.extend(bk)

            with conn.cursor() as cur:
                cur.execute(update_query, update_params)

        # === 5. Вставляем новые версии (INSERT) ===
        insert_cols = list(business_key_cols) + list(tracked_cols) + ['start_date', 'end_date', 'is_current', 'version']
        insert_col_str = ', '.join(insert_cols)
        placeholders = ', '.join(['%s'] * len(insert_cols))
        insert_query = f"""
            INSERT INTO {table_name} ({insert_col_str})
            VALUES ({placeholders})
        """

        insert_values = []
        for rec, version in changed_or_new:
            row = (
                *[rec[col] for col in business_key_cols],
                *[rec[col] for col in tracked_cols],
                today,
                end_date_sentinel,
                True,
                version
            )
            insert_values.append(row)

        with conn.cursor() as cur:
            cur.executemany(insert_query, insert_values)

        conn.commit()

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
