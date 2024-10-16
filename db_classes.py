from datetime import datetime
from enum import Enum
from uuid import uuid4


class Type(Enum):
    ID = 'ID'
    INT = 'INT'
    REAL = 'REAL'
    CHAR = 'CHAR'
    STRING = 'STRING'
    DATE = 'DATE'
    DATEINVL = 'DATEINVL'


class Field:
    def __init__(self, name: str, ftype: Type):
        if ftype not in Type.__members__.values():
            raise ValueError(f"Type {ftype} is not supported.")
        self.name = name
        self.ftype = ftype


class Schema:
    def __init__(self, fields: list[Field]):
        self.fields: list[Field] = fields


class Row:
    def __init__(self, schema: Schema, values: list):
        self.schema = schema
        self.values = values

    def update(self, new_values):
        self.values = new_values

    def __str__(self):
        return str(self.values)


class Table:
    def __init__(self, name: str, schema: Schema):
        self.name = name
        self.schema = schema
        self.rows: dict[str, Row] = {}

    def insert_row(self, row_data, _id=None):
        if len(row_data) != len(self.schema.fields):
            raise ValueError("Row data does not match table schema.")
        self.validate(row_data)
        row = Row(self.schema, row_data)
        if _id is None:
            _id = str(uuid4())
        self.rows[_id] = row

    def update_row(self, row_index, new_data):
        if row_index not in self.rows.keys():
            raise IndexError("Row index out of range.")
        self.rows[row_index].update(new_data)

    def delete_row(self, row_index):
        if row_index not in self.rows.keys():
            raise IndexError("Row index out of range.")
        del self.rows[row_index]

    def display(self):
        for row in self.rows:
            print(row)

    def validate(self, row_data):
        try:
            for col_data, col in zip(self.schema.fields, row_data):
                match col_data.ftype:
                    case Type.INT:
                        int(col)
                    case Type.REAL:
                        float(col)
                    case Type.CHAR:
                        assert len(col) == 1, 'CHAR type supports only symbols'
                    case Type.STRING:
                        pass
                    case Type.DATE:
                        datetime.strptime(col, '%Y.%m.%d')
                    case Type.DATEINVL:
                        dates = col.split('-')
                        assert len(dates) == 2
                        datetime.strptime(col[0], '%Y.%m.%d')
                        datetime.strptime(col[1], '%Y.%m.%d')
        except Exception as e:
            raise TypeError(e)


class Database:
    def __init__(self, name) -> None:
        self.name: str = name
        self.tables: dict[str, Table] = {}

    def create_table(self, name, schema):
        if name in self.tables:
            raise ValueError(f"Table {name} already exists.")
        self.tables[name] = Table(name, schema)

    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]
        else:
            raise ValueError(f"Table {name} does not exist.")

    def get_table(self, name):
        if name in self.tables:
            return self.tables[name]
        raise ValueError(f"Table {name} not found.")
