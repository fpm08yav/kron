import os
# Debug
DEBUG = True

# DATABASE
# DB_HOST = '172.16.1.134'
DB_HOST = 'analyticspg'
# DB_HOST = '127.0.0.1'
DB_PORT = 5432
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
DB_NAME = 'cubes_app'

# DIRECTORIES
PARENT_PATH = '/home/yansen-a/analytics/local_cubes'
TEMP_PATH = os.path.join(PARENT_PATH, 'temp')
SQL_PATH = os.path.join(PARENT_PATH, 'sql')
LOGGING_PATH = os.path.join(PARENT_PATH, 'logs')
LOG_CONF_PATH = os.path.join(PARENT_PATH, 'log_conf')

# PARENT_PATH_SERVER = '/home/yansen/cubes/cubes/cubes'
PARENT_PATH_SERVER = '/home/playrix/cubes_tools'
IMPORT_CSV_PATH = os.path.join(PARENT_PATH_SERVER, 'import_csv')
IMPORTED_CSV_PATH = os.path.join(PARENT_PATH_SERVER, 'imported_csv')

LOCAL_CUBES = '/home/yansen-a/analytics/analytics_cubes_tools'

# SERVER
# S_USER = 'yansen'
# S_HOST = 'webanalytics'
# S_PASSWORD = 'yansenpower'

S_USER = 'playrix'
S_HOST = '172.16.5.28'
S_PASSWORD = '123456'
