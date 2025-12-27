from datetime import date, datetime, timezone
import pandas as pd
import numpy as np
from decimal import Decimal


SQL_TYPE_MAP = {
    int: "INTEGER",
    float: "FLOAT",
    str: "TEXT",
    bool: "BOOLEAN",
    dict: "JSONB",
    list: "JSONB",
    type(None): "TEXT"
}
END_DATE = datetime(9999, 12, 31, 0, 0, 0, tzinfo=timezone.utc)


def map_python_to_sql_type(value):
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return "TIMESTAMPTZ"
        else:
            return "TIMESTAMP"

    # NaN / NaT
    if value is None or pd.isna(value):
        return "TEXT"

    # bool (numpy.bool_ тоже ловится)
    if isinstance(value, (bool, np.bool_)):
        return "BOOLEAN"

    # int
    if isinstance(value, (int, np.integer)):
        return "INTEGER"

    # float
    if isinstance(value, (float, np.floating)):
        return "REAL"

    # Decimal
    if isinstance(value, Decimal):
        return "NUMERIC"

    # datetime / pandas Timestamp
    if isinstance(value, (datetime, pd.Timestamp)):
        return "TIMESTAMP"

    # date
    if isinstance(value, date):
        return "DATE"

    # bytes
    if isinstance(value, (bytes, bytearray)):
        return "BLOB"

    # str и всё остальное
    return "TEXT"
