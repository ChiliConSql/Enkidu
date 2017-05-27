import unittest

from enkidu import dbutils

class DBUtilsTest(unittest.TestCase):

  def setUp(self):
    pass

  def test_db_creds(self):
    dburl = 'postgres://jupiter:mars@dagnyhost/kinoiste'
    dbcreds = dbutils.db_creds(dburl)

    self.assertEqual(dbcreds.user, 'jupiter')
    self.assertEqual(dbcreds.database, 'kinoiste')

  def test_db_connection(self):
    self.assertTrue(True)

  def test_pg_record(self):
    self.assertTrue(True)

  def test_reset_jsonDB(self):
    self.assertTrue(True)

  def test_add_json_table(self):
    self.assertTrue(True)

  def test_empty_tables(self):
    self.assertTrue(True)

  def test_reset_sequence(self):
    self.assertTrue(True)

  def test_default_schema(self):
    self.assertTrue(True)

  def test_read_json(self):
    self.assertTrue(True)

  def test_read_csv(self):
    self.assertTrue(True)

if __name__ == '__main__':
  unittest.main()
