from pymongo import Connection
from pymongo.master_slave_connection import MasterSlaveConnection
from pymongo.errors import ConnectionFailure

import settings

__all__ = ['ConnectionError', 'connect']

MONGODB_DATABASES = getattr(settings, 'MONGODB_DATABASES', {})

_connection = None
_db = None
_db_name = MONGODB_DATABASES.get('COLLECTION', None)
_db_username = MONGODB_DATABASES.get('USERNAME', None)
_db_password = MONGODB_DATABASES.get('PASSWORD', None)

class ConnectionError(Exception):
	pass

def _connection_settings(server, is_slave=False):
	settings = {
		'host':server.get('HOST', 'localhost'),
		'port':int(server.get('PORT', 27017)),
		'pool_size':server.get('POOL_SIZE', None),
		'timeout':server.get('TIMEOUT', None),
		'network_timeout':server.get('NETWORK_TIMEOUT', None),
	}
	
	if not 'slave_okay' in server and is_slave:
		settings['slave_okay'] = True
	else:
		settings['slave_okay'] = server.get('SLAVE_OKAY', False)
	
	return settings

def _get_connection():
	global _connection
	# Connect to the database if not already connected
	if _connection is None:
		try:
			if 'master' in MONGODB_DATABASES:
				master = Connection(**_connection_settings(MONGODB_DATABASES['master']))
				slaves = []
				for slave in MONGODB_DATABASES.get('slaves', []):
					try:
						slaves.append(Connection(**_connection_settings(slave, True)))
					except ConnectionFailure:
						pass
				_connection = MasterSlaveConnection(master, slaves)
			else:
				_connection = Connection(**_connection_settings(MONGODB_DATABASES.get('default', {})))
		except:
			raise ConnectionError('Cannot connect to the database')
	return _connection

def _get_db():
	global _db, _connection
	# Connect if not already connected
	if _connection is None:
		_connection = _get_connection()

	if _db is None:
		# _db_name will be None if the user hasn't called connect()
		if _db_name is None:
			raise ConnectionError('Not connected to the database')

		# Get DB from current connection and authenticate if necessary
		_db = _connection[_db_name]
		if _db_username and _db_password:
			_db.authenticate(_db_username, _db_password)
	return _db

def connect(db, username=None, password=None, **kwargs):
	"""Connect to the database specified by the 'db' argument. Connection 
	settings may be provided here as well if the database is not running on
	the default port on localhost. If authentication is needed, provide
	username and password arguments as well.
	"""
	MONGODB_DATABASES.update(kwargs)
	_db_name = db
	_db_username = username
	_db_password = password
	return _get_db()