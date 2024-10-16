import time
import tkinter as tk
from tkinter import ttk, messagebox
from db_manager import DbManager
from client import DbClient


def load_table_data(db_name, table_name):
    global dbm, selected_db_name, selected_table_name, main_table
    selected_db_name = db_name
    selected_table_name = table_name

    try:
        _, columns, rows = dbm.get_table_data(db_name, table_name)
        main_table.delete(*main_table.get_children())
        main_table["columns"] = columns
        main_table["show"] = "headings"
        for col in main_table["columns"]:
            main_table.heading(col, text=col)
            main_table.column(col, anchor="center", width=100)
        for key, row in rows.items():
            main_table.insert("", "end", values=[key] + row)

        create_add_fields()

    except Exception as e:
        messagebox.showerror("Database Error", f"Error loading data from {table_name}: {str(e)}")


def clear_main_table():
    main_table.delete(*main_table.get_children())


def on_tree_select(event):
    global selected_db_name
    selected_item = tree.selection()
    if selected_item:
        item = tree.item(selected_item[0])
        parent_item = tree.parent(selected_item[0])

        if parent_item:
            db_name = tree.item(parent_item)["text"]
            table_name = item["text"]
            load_table_data(db_name, table_name)
        else:
            selected_db_name = item["text"]


def on_table_select(event):
    global entries, selected_db_name, selected_table_name
    selected_item = main_table.selection()
    if selected_item:
        item = main_table.item(selected_item[0])
        selected_row = item['values']

        entries.clear()
        for widget in edit_frame.winfo_children():
            widget.destroy()

        for i, value in enumerate(selected_row[1:]):
            label = ttk.Label(edit_frame, text=f"{main_table['columns'][i]}:")
            label.grid(row=i, column=0, padx=5, pady=5, sticky='e')
            entry = ttk.Entry(edit_frame)
            entry.insert(0, value)
            entry.grid(row=i, column=1, padx=5, pady=5, sticky='ew')
            entries.append(entry)

        save_button = ttk.Button(edit_frame, text="Save Changes", command=save_changes)
        save_button.grid(row=len(selected_row), column=0, columnspan=2, padx=20, pady=10, sticky="ew")

        delete_button = ttk.Button(edit_frame, text="Delete Selected Record", command=delete_record)
        delete_button.grid(row=len(selected_row) + 1, column=0, columnspan=2, padx=20, pady=10, sticky="ew")

    edit_frame.grid_columnconfigure(0, weight=1)
    edit_frame.grid_columnconfigure(1, weight=1)


def save_changes():
    updated_values = [entry.get() for entry in entries]
    try:
        dbm.update_row(
            selected_db_name,
            selected_table_name,
            main_table.item(main_table.selection()[0])["values"][0],
            updated_values,
        )
        load_table_data(selected_db_name, selected_table_name)

        messagebox.showinfo("Success", "Record updated successfully!")

    except Exception as e:
        messagebox.showerror("Database Error", f"Error updating record: {str(e)}")


def add_record():
    new_values = [entry.get() for entry in add_entries]

    if any(not val for val in new_values):
        messagebox.showerror("Input Error", "Please fill in all fields before adding a record.")
        return

    try:
        dbm.insert_row(selected_db_name, selected_table_name, new_values)
        load_table_data(selected_db_name, selected_table_name)
        messagebox.showinfo("Success", "Record added successfully!")
    except Exception as e:
        messagebox.showerror("Database Error", f"Error adding record: {str(e)}")


def delete_record():
    selected_item = main_table.selection()
    if not selected_item:
        messagebox.showerror("Error", "Please select a record to delete.")
        return

    item = main_table.item(selected_item[0])
    primary_key_value = item['values'][0]

    try:
        dbm.delete_row(selected_db_name, selected_table_name, primary_key_value)        
        load_table_data(selected_db_name, selected_table_name)
        messagebox.showinfo("Success", "Record deleted successfully!")
    except Exception as e:
        messagebox.showerror("Database Error", f"Error deleting record: {str(e)}")


def create_add_fields():
    global add_entries
    add_entries.clear()
    for widget in add_record_frame.winfo_children():
        widget.destroy()

    for i, col in enumerate(main_table["columns"][1:]):
        label = ttk.Label(add_record_frame, text=f"{col}:")
        label.grid(row=i, column=0, padx=5, pady=5, sticky='e')
        entry = ttk.Entry(add_record_frame)
        entry.grid(row=i, column=1, padx=5, pady=5)
        add_entries.append(entry)

    add_button = ttk.Button(add_record_frame, text="Add New Record", command=add_record)
    add_button.grid(row=len(main_table["columns"]), column=0, columnspan=2, pady=10)

def create_database():
    db_name = new_db_entry.get()
    if not db_name:
        messagebox.showerror("Input Error", "Please enter a name for the new database.")
        return

    try:
        dbm.create_database(db_name)
        tree.insert("", "end", text=db_name, open=True)
        new_db_entry.delete(0, 'end')
        messagebox.showinfo("Success", f"Database '{db_name}' created successfully!")
    except Exception as e:
        messagebox.showerror("Database Error", f"Error creating database: {str(e)}")

def create_table():
    table_name = new_table_entry.get()
    column_definitions = new_table_columns_entry.get()

    if not selected_db_name:
        messagebox.showerror("Selection Error", "Please select a database first.")
        return

    if not table_name or not column_definitions:
        messagebox.showerror("Input Error", "Please enter both table name and column definitions.")
        return

    try:
        dbm.create_table(selected_db_name, table_name, column_definitions)
        parent_item = None
        for item in tree.get_children():
            if tree.item(item, "text") == selected_db_name:
                parent_item = item
                break
        if parent_item:
            tree.insert(parent_item, "end", text=table_name)
        new_table_entry.delete(0, 'end')
        new_table_columns_entry.delete(0, 'end')
        messagebox.showinfo("Success", f"Table '{table_name}' created successfully!")
    except Exception as e:
        messagebox.showerror("Database Error", f"Error creating table: {str(e)}")

def delete_database():
    global selected_db_name
    if not selected_db_name:
        messagebox.showerror("Selection Error", "Please select a database to delete.")
        return

    confirm = messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete the database '{selected_db_name}'?")
    if not confirm:
        return

    try:
        dbm.drop_database(selected_db_name)
        for item in tree.get_children():
            if tree.item(item, "text") == selected_db_name:
                tree.delete(item)
                break
        clear_main_table()
        selected_db_name = None
        messagebox.showinfo("Success", "Database deleted successfully!")
    except Exception as e:
        messagebox.showerror("Database Error", f"Error deleting database: {str(e)}")


def delete_table():
    selected_item = tree.selection()
    if not selected_item:
        messagebox.showerror("Selection Error", "Please select a table to delete.")
        return

    item = tree.item(selected_item[0])
    parent_item = tree.parent(selected_item[0])

    if not parent_item:
        messagebox.showerror("Selection Error", "Please select a table, not a database.")
        return

    db_name = tree.item(parent_item)["text"]
    table_name = item["text"]

    confirm = messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete the table '{table_name}' from '{db_name}'?")
    if not confirm:
        return

    try:
        dbm.delete_table(db_name, table_name)
        tree.delete(selected_item[0])
        clear_main_table()
        messagebox.showinfo("Success", "Table deleted successfully!")
    except Exception as e:
        messagebox.showerror("Database Error", f"Error deleting table: {str(e)}")


def delete_duplicate_rows():
    """Delete duplicate rows from the currently selected table."""
    global dbm, selected_db_name, selected_table_name, main_table

    if not selected_db_name or not selected_table_name:
        messagebox.showerror("Error", "Please select a table first.")
        return

    try:
        deleted_duplicates = dbm.delete_repeated(selected_db_name, selected_table_name)
        load_table_data(selected_db_name, selected_table_name)

        if deleted_duplicates:
            messagebox.showinfo("Success", f"Deleted {deleted_duplicates} duplicate rows.")
        else:
            messagebox.showinfo("Info", "No duplicate rows found.")

    except Exception as e:
        messagebox.showerror("Database Error", f"Error deleting duplicates: {str(e)}")


def run_gui(mode):
    global dbm, tree, main_table, entries, add_entries, add_record_frame, edit_frame, new_db_entry, new_table_entry, new_table_columns_entry

    root = tk.Tk()
    root.title("Database and Table Viewer with Editing")

    side_panel = ttk.Frame(root, width=300)
    side_panel.pack(side="left", fill="y")

    tree = ttk.Treeview(side_panel)
    tree.heading("#0", text="Databases", anchor="w")
    tree.grid(row=0, column=0, columnspan=3, padx=10, pady=10, sticky="nsew")

    side_panel.grid_rowconfigure(0, weight=1)
    side_panel.grid_columnconfigure(0, weight=1)

    create_db_frame = ttk.Frame(side_panel)
    create_db_frame.grid(row=1, column=0, columnspan=3, padx=10, pady=5, sticky="ew")

    ttk.Label(create_db_frame, text="New Database:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
    new_db_entry = ttk.Entry(create_db_frame, width=15) 
    new_db_entry.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
    create_db_button = ttk.Button(create_db_frame, text="Create", command=create_database)
    create_db_button.grid(row=0, column=2, padx=5, pady=5)

    create_table_frame = ttk.Frame(side_panel)
    create_table_frame.grid(row=2, column=0, columnspan=3, padx=10, pady=5, sticky="ew")

    ttk.Label(create_table_frame, text="New Table:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
    new_table_entry = ttk.Entry(create_table_frame, width=15)
    new_table_entry.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
    create_table_button = ttk.Button(create_table_frame, text="Create", command=create_table)
    create_table_button.grid(row=0, column=2, padx=5, pady=5)

    ttk.Label(create_table_frame, text="Columns:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
    new_table_columns_entry = ttk.Entry(create_table_frame, width=15)
    new_table_columns_entry.grid(row=1, column=1, padx=5, pady=5, sticky="ew")

    delete_db_button = ttk.Button(side_panel, text="Delete Database", command=delete_database)
    delete_db_button.grid(row=3, column=0, columnspan=3, padx=10, pady=5, sticky="ew")

    delete_table_button = ttk.Button(side_panel, text="Delete Table", command=delete_table)
    delete_table_button.grid(row=4, column=0, columnspan=3, padx=10, pady=5, sticky="ew")

    delete_duplicates_button = ttk.Button(side_panel, text="Delete Duplicates", command=delete_duplicate_rows)
    delete_duplicates_button.grid(row=5, column=0, columnspan=3, padx=10, pady=5, sticky="ew")

    main_panel = ttk.Frame(root)
    main_panel.pack(side="left", fill="both", expand=True)

    main_table = ttk.Treeview(main_panel)
    main_table.pack(fill="both", expand=True)

    edit_frame = ttk.Frame(root)
    edit_frame.pack(side="right", fill="y")

    add_record_frame = ttk.Frame(root)
    add_record_frame.pack(side="bottom", fill="x", pady=10)

    entries = []
    add_entries = []

    main_table.bind("<<TreeviewSelect>>", on_table_select)

    create_add_fields()

    match mode:
        case 'd':
            dbm = DbManager()
            dbm.load()
        case 'c':
            dbm = DbClient()
            dbm.run()

    databases = dbm.fetch_databases_and_tables()
    for db_name, tables in databases.items():
        db_item = tree.insert("", "end", text=db_name, open=True)
        for table in tables:
            tree.insert(db_item, "end", text=table)

    tree.bind("<<TreeviewSelect>>", on_tree_select)

    root.mainloop()
