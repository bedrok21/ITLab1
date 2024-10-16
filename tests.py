import unittest

from db_classes import Database, Type, Field, Schema
from db_manager import DbManager


class TestDbManager(unittest.TestCase):
    def setUp(self):
        self.db_manager = DbManager()
        self.db_manager.databases['test_db'] = Database('test_db')

        fields = [Field(name="name", ftype=Type.STRING), Field(name="age", ftype=Type.INT)]
        schema = Schema(fields)
        self.db_manager.databases['test_db'].create_table('test_table', schema)

    def test_insert_row(self):
        _id = self.db_manager.insert_row('test_db', 'test_table', ['John', 25])
        table = self.db_manager.databases['test_db'].tables['test_table']
        self.assertEqual(len(table.rows), 1)
        self.assertEqual(table.rows[_id].values, ['John', 25])

    def test_delete_row(self):
        _id1 = self.db_manager.insert_row('test_db', 'test_table', ['John', 25])
        _id2 = self.db_manager.insert_row('test_db', 'test_table', ['Jane', 30])

        self.db_manager.delete_row('test_db', 'test_table', _id1)
        table = self.db_manager.databases['test_db'].tables['test_table']
        self.assertEqual(len(table.rows), 1)
        self.assertNotIn(_id1, table.rows)
        self.assertIn(_id2, table.rows)

    def test_delete_repeated(self):
        self.db_manager.insert_row('test_db', 'test_table', ['John', 25])
        self.db_manager.insert_row('test_db', 'test_table', ['John', 25])
        self.db_manager.insert_row('test_db', 'test_table', ['Jane', 30])

        deleted_count = self.db_manager.delete_repeated('test_db', 'test_table')

        table = self.db_manager.databases['test_db'].tables['test_table']
        self.assertEqual(deleted_count, 1)
        self.assertEqual(len(table.rows), 2)


if __name__ == '__main__':
    unittest.main()
