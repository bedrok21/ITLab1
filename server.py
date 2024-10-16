import socket
import threading
import json
from db_manager import DbManager


class DbServer:
    def __init__(self) -> None:
        self.dbm: DbManager = DbManager()
        self.host = '127.0.0.1'
        self.port = 9001

    def handle_client(self, client_socket: socket.socket):
        while True:
            try:
                request = client_socket.recv(1024).decode('utf-8')
                if not request:
                    break

                request_data = json.loads(request)
                action = request_data.get("action")
                data: dict = request_data.get("data")

                if action == "select_table":
                    db_name, table_name = data.get("db_name", None), data.get("table_name", None)
                    if None not in (db_name, table_name):
                        _, columns, rows = self.dbm.get_table_data(db_name, table_name)
                        response = {"columns": columns, "rows": [[key]+row.values for key, row in rows.items()]}

                elif action == "insert_row":
                    db_name, table_name, values = data.get("db_name", None), data.get("table_name", None), data.get("values", None)
                    if None not in (db_name, table_name, values):
                        self.dbm.insert_row(db_name, table_name, values)
                        response = {"status": "Record inserted"}

                elif action == "update_row":
                    db_name, table_name, values, _id = data.get("db_name", None), data.get("table_name", None), data.get("values", None), data.get("_id", None)
                    if None not in (db_name, table_name, values, _id):
                        self.dbm.update_row(db_name, table_name, _id, values)
                        response = {"status": "Record updated"}

                elif action == "delete_row":
                    db_name, table_name, _id = data.get("db_name", None), data.get("table_name", None), data.get("_id", None)
                    if None not in (db_name, table_name, _id):
                        self.dbm.delete_row(db_name, table_name, _id)
                        response = {"status": "Record deleted"}

                elif action == "delete_table":
                    db_name, table_name = data.get("db_name", None), data.get("table_name", None)
                    if None not in (db_name, table_name):
                        self.dbm.delete_table(db_name, table_name)
                    response = {"status": "Record deleted"}

                elif action == "drop_database":
                    db_name = data.get("db_name", None)
                    if db_name:
                        self.dbm.drop_database(db_name)
                    response = {"status": "Database deleted"}

                elif action == "create_table":
                    db_name, table_name, columns = data.get("db_name", None), data.get("table_name", None), data.get("columns", None)
                    if None not in (db_name, table_name, columns):
                        self.dbm.create_table(db_name, table_name, columns)
                    response = {"status": "Table created"}

                elif action == "create_database":
                    db_name = data.get("db_name", None)
                    if db_name:
                        self.dbm.create_database(db_name)
                    response = {"status": "Datbase created"}

                elif action == 'fetch_table_data':
                    db_name, table_name = data.get("db_name", None), data.get("table_name", None)
                    if None not in (db_name, table_name):
                        types, headers, rows = self.dbm._fetch_table_data(db_name, table_name)
                        response = {"types": types, "headers": headers, "rows": rows}

                elif action == 'get_table_data':
                    db_name, table_name = data.get("db_name", None), data.get("table_name", None)
                    if None not in (db_name, table_name):
                        types, columns, rows = self.dbm.get_table_data(db_name, table_name)
                        response = {"types": types, "columns": columns, "rows": rows}

                elif action == 'fetch_databases_and_tables':
                    databases = self.dbm.fetch_databases_and_tables()
                    response = {"databases": databases}

                elif action == 'delete_repeated':
                    db_name, table_name = data.get("db_name", None), data.get("table_name", None)
                    if None not in (db_name, table_name):
                        num = self.dbm.delete_repeated(db_name, table_name)
                        response = {"num": num}

                else:
                    response = {"error": "Unknown action"}

                client_socket.send(json.dumps(response).encode('utf-8'))

            except Exception as e:
                client_socket.send(json.dumps({"error": str(e)}).encode('utf-8'))
                break

        client_socket.close()

    def start_server(self):
        self.dbm.load()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"Server is listening on port {self.port}")

        while True:
            client_socket, addr = server.accept()
            print(f"Accepted connection from {addr}")

            client_handler = threading.Thread(target=self.handle_client, args=(client_socket,))
            client_handler.start()


if __name__ == "__main__":
    server = DbServer()
    server.start_server()
