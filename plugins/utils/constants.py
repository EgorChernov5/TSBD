from datetime import date, datetime, timezone


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

    return SQL_TYPE_MAP.get(type(value), "TEXT")