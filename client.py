import socket
import json


class DbClient:
    def __init__(self) -> None:
        self.host = '127.0.0.1'
        self.port = 9001
        self.client = None

    def run(self):
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.connect((self.host, self.port))

    def create_database(self, db_name):
        self.client.send(json.dumps({"action": "create_database", "data": {"db_name": db_name}}).encode('utf-8'))
        response = json.loads(self.client.recv(4096).decode('utf-8'))
        return response

    def drop_database(self, db_name):
        self.client.send(json.dumps({"action": "drop_database", "data": {"db_name": db_name}}).encode('utf-8'))
        response = json.loads(self.client.recv(4096).decode('utf-8'))
        return response

    def create_table(self, db_name, table_name, columns: str):
        self.client.send(json.dumps({"action": "create_table", "data": {"db_name": db_name, "table_name": table_name, "columns": columns}}).encode('utf-8'))
        response = json.loads(self.client.recv(4096).decode('utf-8'))
        return response

    def delete_table(self, db_name, table_name):
        self.client.send(json.dumps({"action": "delete_table", "data": {"db_name": db_name, "table_name": table_name}}).encode('utf-8'))
        response = json.loads(self.client.recv(4096).decode('utf-8'))
        return response

    def update_row(self, db_name, table_name, _id, values):
        self.client.send(json.dumps({"action": "update_row", "data": {"db_name": db_name, "table_name": table_name, "values": values, "_id": _id}}).encode('utf-8'))
        response = json.loads(self.client.recv(4096).decode('utf-8'))
        return response

    def insert_row(self, db_name, table_name, values):
        self.client.send(json.dumps({"action": "insert_row", "data": {"db_name": db_name, "table_name": table_name, "values": values}}).encode('utf-8'))
        response = json.loads(self.client.recv(4096).decode('utf-8'))
        return response

    def delete_row(self, db_name, table_name, _id):
        self.client.send(json.dumps({"action": "delete_row", "data": {"db_name": db_name, "table_name": table_name, "_id": _id}}).encode('utf-8'))
        response = json.loads(self.client.recv(4096).decode('utf-8'))
        return response

    def delete_repeated(self, db_name, table_name):
        self.client.send(json.dumps({"action": "delete_repeated", "data": {"db_name": db_name, "table_name": table_name}}).encode('utf-8'))
        response = json.loads(self.client.recv(4096).decode('utf-8'))
        return response["num"]

    def fetch_databases_and_tables(self):
        self.client.send(json.dumps({"action": "fetch_databases_and_tables", "data": {}}).encode('utf-8'))
        response = json.loads(self.client.recv(4096).decode('utf-8'))
        return response['databases']

    def get_table_data(self, db_name, table_name):
        self.client.send(json.dumps({"action": "get_table_data", "data": {"db_name": db_name, "table_name": table_name}}).encode('utf-8'))
        response = json.loads(self.client.recv(4096).decode('utf-8'))
        return response["types"], response["columns"], response["rows"]

    def _fetch_table_data(self, db_name, table_name):
        self.client.send(json.dumps({"action": "fetch_table_data", "data": {"db_name": db_name, "table_name": table_name}}).encode('utf-8'))
        response = json.loads(self.client.recv(4096).decode('utf-8'))
        return response["types"], response["columns"], response["rows"]
