#TODO - replace some functions with dbutils methods
import os, subprocess
import psycopg2

from cement.core.controller import CementBaseController, expose
from cement.core import output

from enkidu import dbutils

class DBController(CementBaseController):
  class Meta:
    label = 'dbwizz'
    description = 'Manage my DB operations'
    config_defaults = dict(
        admin_role='postgres',
        host=None,
        port=5432,
        dbname=None
        )
    arguments = [
        (['-A', '--admin-role'], dict(action='store', help='the admin user name')),
        (['-S', '--schemas'], dict(action='store', help='the schemas being built')),
        (['-T', '--tables'], dict(action='store', help='the tables being played')),
        (['-D', '--database'], dict(action='store', help='the database name')),
        ]

  def _db_connection(self):
    dbcreds = dbutils.db_creds(
        'postgres://{user}:{pwd}@{host}:{port}/{database}'.format(
          self.app.config.get('user'),
          self.app.config.get('password'),
          self.app.config.get('host'),
          self.app.config.get('port'),
          self.app.config.get('database')
        )
      )
    connection = dbutils.db_connection(dbcreds)
    return connection

  def _exec_sql(self, sql):
    subprocess.call([
          'psql',
          '--host', self.app.config.get('dbwizz','host'),
          '--port', self.app.config.get('dbwizz','port'),
          '--username', self.app.config.get('dbwizz','dbname'),
          '-c', sql
          ]
          )

  def _exec_file(self, fname):
    subprocess.call([
          'psql',
          '--host', self.app.config.get('dbwizz','host'),
          '--port', self.app.config.get('dbwizz','port'),
          '--username', self.app.config.get('dbwizz','dbname'),
          ], stdin=fname
          )

  "pass in the list of files, directory name and extension"
  def _exec_batch(self, dname, fnames, ext='sql'):
    for fname in [os.path.join(dname, f + '.' + ext) for f in fnames]:
      with open(fname, 'r') as sql:
        self._exec_file(sql)

  @expose(help='create the initial database')
  def create_db(self):
    dbname = self.app.config.get('dbwizz','dbname')
    admin = os.getenv(
        'PGADMIN',
        self.app.pargs.admin_role or self.app.config.get('controller.dbwizz', 'admin_role')
        )
    subprocess.call([
        'dropdb',
        '--host', self.app.config.get('dbwizz','host'),
        '--port', self.app.config.get('dbwizz','port'),
        '-U', admin,
        dbname
        ])
    subprocess.call([
        'dropuser',
        '--host', self.app.config.get('dbwizz','host'),
        '--port', self.app.config.get('dbwizz','port'),
        '-U', admin,
        dbname
        ])
    subprocess.call([
        'createuser',
        '--host', self.app.config.get('dbwizz','host'),
        '--port', self.app.config.get('dbwizz','port'),
        '-U', admin,
        dbname, '-s'
        ])
    subprocess.call([
        'createdb',
        '--host', self.app.config.get('dbwizz','host'),
        '--port', self.app.config.get('dbwizz','port'),
        '-U', admin,
        dbname, '-O', dbname
        ])
    sqldir = self.app.config.get('dbwizz','sqldir')
    self._exec_batch(sqldir, ['schema'])

  @expose(help='build the schema')
  def build_schema(self):
    tabs = self.app.pargs.schemas.split()
    sqldir = self.app.config.get('dbwizz','sqldir')
    self._exec_batch(sqldir, tabs)

  @expose(help='build a test schema')
  def build_test(self):
    sqldir = self.app.config.get('dbwizz','sqldir')
    self._exec_file(os.path.join(sqldir, 'galileo.sql'))

  @expose(help='add real data')
  def add_data(self):
    tabs = self.app.pargs.tables.split()
    datadir = self.app.config.get('dbwizz','datadir')
    self._exec_batch(datadir, tabs)

  def create_res(self, resname, **args):
    "create a resource in DB"
    pass

  def read_res(self, resname, **args):
    "read a resource from DB"
    pass

  def update_res(self, resname, **args):
    "update a resource in DB"
    pass

  def delete_res(self, resname, **args):
    "delete a resource from DB"
    pass

