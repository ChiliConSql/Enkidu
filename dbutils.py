import os.path, json, csv
from munch import Munch
import psycopg2, psycopg2.extras
from urllib.parse import urlparse

def read_json(datadir, jres):
  with open(os.path.join(datadir, jres + '.json')) as jfile:
    return json.loads(jfile.read())

def read_csv(self, mod_name, delimiter='\t', sfx='.csv'):
  with open(os.path.join(self.datadir, mod_name + sfx)) as dfile:
    recreader = csv.DictReader(dfile, delimiter=delimiter)
    for row in recreader:
      yield row

def db_creds(dburl):
  """
  return an object with db params from dburl
  """
  dbcreds = Munch()
  parsed = urlparse(dburl)
  dbcreds.user = parsed.username
  dbcreds.password = parsed.password
  dbcreds.hostname = parsed.hostname
  dbcreds.port = str(parsed.port)
  dbcreds.database = parsed.path[1:]
  # dbcreds.database = parsed.path.strip('/')
  return dbcreds

def dsn_creds(dbconn):
  "pass in a psycopg conn object and return the connection creds"
  dbcreds = Munch()
  dsn = dbconn.dsn
  params = dsn.split()
  for k,v in [param.split('=') for param in params]:
    dbcreds[k] = v
  dbcreds.hostname = dbcreds.pop('host')
  dbcreds.database = dbcreds.pop('dbname')
  return dbcreds

def db_connection(dbcreds):
  """ pass in a dbcreds object with the following props
  database, user, password, host, port
  """
  conn = psycopg2.connect(
      database=dbcreds.database,
      user=dbcreds.user,
      password=dbcreds.password,
      host=dbcreds.hostname,
      port=dbcreds.port,
      )
  conn.set_session(autocommit=True)

  return conn

def pg_record(conn, tabname, idfld, recid):
  sql = '''
    select * from {}
    where {} = %s
    '''.format(tabname, idfld)
  with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
    cur.execute(sql, (recid,))
    if cur.rowcount:
      return cur.fetchone()
    else:
      raise Exception('No records found %s' % tabname)

def reset_jsonDB(jdb):
  jdb.purge_tables()
  jdb.purge()

def add_json_table(jsonDB, resname, jsonres):
  json_tbl = jsonDB.table(resname)
  for rec in jsonres['VALID_DATA']:
    rec['test_cat'] = 'VALID_DATA'
    json_tbl.insert(rec)
  try:
    invalid_recs = jsonres['INVALID_DATA']
    for inv_type in invalid_recs.keys():
      rec = invalid_recs[inv_type]
      rec['test_cat'] = inv_type
      json_tbl.insert(rec)
  except KeyError:
    pass

def empty_tables(dbconn, schemas):
  dbcreds = dsn_creds(dbconn)
  app_tables_query = """
    SELECT  table_schema, table_name
    FROM    information_schema.tables
    WHERE   table_schema in %(schem)s
      AND   table_catalog = %(dbname)s
      AND   table_type = 'BASE TABLE'
      AND   table_name != 'schema_version';
    """
  with dbconn.cursor() as cursor:
    cursor.execute(
        app_tables_query,
        {'schem':schemas, 'dbname':dbcreds.database}
        )
    tables = [(r[0],r[1]) for r in cursor.fetchall()]
    for s,t in tables:
      query = 'TRUNCATE TABLE {0}.{1} CASCADE;'.format(s,t)
      cursor.execute(query)

def reset_sequences(db):
  app_seq_query = """
  SELECT  sequence_schema, sequence_name
  FROM    information_schema.sequences
  WHERE   sequence_schema in('jupiter','machine')
    AND   sequence_catalog = '{0}';""".format(database)

  cursor = db.cursor()
  cursor.execute(app_seq_query)
  seqs = [(r[0],r[1]) for r in cursor.fetchall()]
  for s,t in seqs:
    query = "SELECT pg_catalog.setval('{0}.{1}', 1, false)".format(s,t)
    cursor.execute(query)
    db.commit()
  cursor.close()

def default_schema(db):
  sql = "select current_schema()"
  with db.cursor() as cur:
    cur.execute(sql)
    schema = cur.fetchone()[0]
  return schema

