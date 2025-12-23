from datetime import date, datetime


SQL_TYPE_MAP = {
    int: "INTEGER",
    float: "FLOAT",
    str: "TEXT",
    bool: "BOOLEAN",
    dict: "JSONB",
    list: "JSONB",
    type(None): "TEXT"
}


def map_python_to_sql_type(value):
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return "TIMESTAMPTZ"
        else:
            return "TIMESTAMP"

    return SQL_TYPE_MAP.get(type(value), "TEXT")