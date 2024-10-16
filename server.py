import socket
import threading
import json
from db_manager import DbManager


def handle_client(client_socket, dbm: DbManager):
    while True:
        try:
            request = client_socket.recv(1024).decode('utf-8')
            if not request:
                break

            request_data = json.loads(request)
            action = request_data.get("action")
            data = request_data.get("data")

            if action == "select":
                db_name, table_name = data.get("db_name"), data.get("table_name")
                _, columns, rows = dbm.get_table_data(db_name, table_name)
                response = {"columns": columns, "rows": [[key]+row.values for key, row in rows.items()]}

            elif action == "insert":
                db_name, table_name, values = data.get("db_name"), data.get("table_name"), data.get("values")
                dbm.insert_row(db_name, table_name, values)
                response = {"status": "Record inserted"}

            elif action == "delete":
                db_name, table_name, key = data.get("db_name"), data.get("table_name"), data.get("key")
                dbm.delete_row(db_name, table_name, key)
                response = {"status": "Record deleted"}

            else:
                response = {"error": "Unknown action"}

            client_socket.send(json.dumps(response).encode('utf-8'))

        except Exception as e:
            client_socket.send(json.dumps({"error": str(e)}).encode('utf-8'))
            break

    client_socket.close()


def start_server():
    dbm = DbManager()
    dbm.load()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", 9999))
    server.listen(5)
    print("Server is listening on port 9999")

    while True:
        client_socket, addr = server.accept()
        print(f"Accepted connection from {addr}")

        client_handler = threading.Thread(target=handle_client, args=(client_socket, dbm))
        client_handler.start()


if __name__ == "__main__":
    start_server()
