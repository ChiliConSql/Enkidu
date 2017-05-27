from cement.core.controller import CementBaseController, expose
from cement.core import output
from configparser import NoOptionError

import os.path
from enkidu import dbutils
from tinydb import TinyDB

class TestController(CementBaseController):
  class Meta:
    label = 'galileo'
    description = 'Manage test chores'
    config_defaults = dict(
        json_db='testdb.json',
        basedir='test'
        )
    #TODO - replace with positional args
    arguments = [
        (['--tablist'], dict(action='store', help='the tables being mocked'))
        ]

  def _datadir(self):
    try:
      return self.app.config.get('galileo', 'datadir')
    except NoOptionError:
      return os.path.join(
        self.app.config.get('galileo', 'basedir'),
        'data'
        )

  @expose(help='''
  populate the tinydb file with our json test data
    - pass in a table list to populate --tablist
  ''')
  def populate_jsonDB(self):
    dest_path = self.app.config.get('galileo', 'json_db')
    dest_db = TinyDB(dest_path)
    for res in self.app.pargs.tablist.split():
      jsonres = dbutils.read_json(self._datadir(), res)
      dbutils.add_json_table(dest_db, res, jsonres)
