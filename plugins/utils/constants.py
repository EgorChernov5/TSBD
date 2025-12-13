from datetime import date


SQL_TYPE_MAP = {
    int: "INTEGER",
    float: "FLOAT",
    str: "TEXT",
    bool: "BOOLEAN",
    dict: "JSONB",
    list: "JSONB",
    type(None): "TEXT",
    date: "DATE NOT NULL"
}
