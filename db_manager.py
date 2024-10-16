import os
import csv
from collections import defaultdict

from db_classes import Schema, Database, Type, Field


DB_FOLDER_PATH = 'db'


class AutoCreateDict(defaultdict):
    def __missing__(self, key):
        self[key] = Database(key)
        return self[key]


class DbManager:
    def __init__(self, db_folder_path=DB_FOLDER_PATH) -> None:
        self.db_folder_path = db_folder_path
        self.databases: dict[str, Database] = AutoCreateDict()

    def load(self):
        csv_files = [f for f in os.listdir(self.db_folder_path) if f.endswith('.csv')]

        for file_name in csv_files:
            pure_name = file_name.split('.')[0]
            split_name = pure_name.split('-')
            types, headers, rows = self._fetch_table_data(split_name[0], split_name[1])
            self.databases[split_name[0]].create_table(
                split_name[1],
                Schema([Field(val[0], Type(val[1])) for val in zip(headers[1:], types[1:])]),
            )
            for row in rows:
                self.databases[split_name[0]].tables[split_name[1]].insert_row(row[1:], row[0])

    def create_database(self, db_name):
        self.databases[db_name] = Database(db_name)

    def drop_database(self, db_name):
        del self.databases[db_name]
        file_paths = [
            f"{self.db_folder_path}/{f}"
            for f in os.listdir(self.db_folder_path)
            if f.endswith(".csv") and f.split("-")[0] == db_name
        ]
        for file_path in file_paths:
            if os.path.exists(file_path):
                os.remove(file_path)

    def create_table(self, db_name, table_name, columns: str):
        fields = []
        for column in columns.split(','):
            col_data = column.split(':')
            if len(col_data) != 2:
                raise ValueError("Wrong Columns definition")
            if col_data[1] not in Type._member_names_:
                raise ValueError("Wrong Columns definition")
            fields.append(Field(name=col_data[0], ftype=Type(col_data[1])))
        self.databases[db_name].create_table(table_name, Schema(fields))
        self._save_table_data(db_name, table_name)

    def delete_table(self, db_name, table_name):
        del self.databases[db_name].tables[table_name]
        file_path = f'{self.db_folder_path}/{db_name}-{table_name}.csv'
        if os.path.exists(file_path):
            os.remove(file_path)

    def update_row(self, db_name, table_name, _id, values):
        self.databases[db_name].tables[table_name].update_row(_id, values)
        self._save_table_data(db_name, table_name)

    def insert_row(self, db_name, table_name, values):
        _id = self.databases[db_name].tables[table_name].insert_row(values)
        self._save_table_data(db_name, table_name)
        return _id

    def delete_row(self, db_name, table_name, _id):
        self.databases[db_name].tables[table_name].delete_row(_id)
        self._save_table_data(db_name, table_name)

    def delete_repeated(self, db_name, table_name):
        unique_rows = defaultdict(list)
        duplicates_to_delete = []

        for key, row in self.databases[db_name].tables[table_name].rows.items():
            row_values = tuple(row.values)
            if row_values not in unique_rows:
                unique_rows[row_values].append(key)
            else:
                duplicates_to_delete.append(key)

        for duplicate_key in duplicates_to_delete:
            self.delete_row(db_name, table_name, duplicate_key)

        return len(duplicates_to_delete)

    def fetch_databases_and_tables(self):
        databases = defaultdict(list)

        csv_files = [f for f in os.listdir(self.db_folder_path) if f.endswith('.csv')]

        for file_name in csv_files:
            pure_name = file_name.split('.')[0]
            split_name = pure_name.split('-')
            databases[split_name[0]].append(split_name[1])

        return databases

    def get_table_data(self, db_name, table_name):
        if db_name in self.databases:
            if table := self.databases[db_name].tables.get(table_name, None):
                return (
                    [Type.ID.value] + [field.ftype.value for field in table.schema.fields],
                    ['id'] + [field.name for field in table.schema.fields],
                    table.rows,
                )

    def _fetch_table_data(self, db_name, table_name):
        with open(f'{self.db_folder_path}/{db_name}-{table_name}.csv', mode='r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            types = next(csv_reader)
            headers = next(csv_reader)
            rows = [row for row in csv_reader]
            return types, headers, rows

    def _save_table_data(self, db_name, table_name):
        with open(f'{self.db_folder_path}/{db_name}-{table_name}.csv', mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            types = [Type.ID]
            headers = ['id']
            for field in self.databases[db_name].tables[table_name].schema.fields:
                types.append(field.ftype.value)
                headers.append(field.name)
            writer.writerow(types)
            writer.writerow(headers)
            for key, value in self.databases[db_name].tables[table_name].rows.items():
                writer.writerow([key] + value.values)
