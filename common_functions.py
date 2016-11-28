#! coding=utf-8
import os
import logging
import socket
import impala
import time
import calendar
import thrift
import psycopg2

from datetime import datetime, timedelta
from impala.dbapi import connect
from paramiko import Transport, SFTPClient
from subprocess import Popen, PIPE


from config import *

LOCAL_TMP_DIR = '/home/yansen-a/analytics'
PATH_LOCK_FILE = '/home/yansen-a/analytics/lock'
DATE_FORMAT = '%Y-%m-%d'

SEGMENT_STEP = 25
NUMBER_ATTEMPT = 10
TIME_SLEEP = 10 * 60
BEGIN_HOUR = 1452729600     # unix_timestamp('2016-01-14 00:00:00')


# def get_select_values_sql(list_values, list_names):
#     return 'select * from (values %(header)s%(other)s) as list' % {
#         'header': '(%s)' % ', '.join(
#             map(lambda i, j: '%s as %s' % (
#                 i if isinstance(i, int) else "'%s'" % i, j
#             ),
#                 list_values[0], list_names
#             )),
#         'other': ', %s' % ', '.join(
#             map(
#                 lambda i: '%s' % str(tuple(i)),
#                 list_values[1:]
#             )
#         ) if list_values[1:] else ''
#     }


# def get_values_sql(list_values, list_names):
#     return '%(header)s%(other)s' % {
#         'header': '(%s)' % ', '.join(
#             map(lambda i, j: '%s as %s' % (
#                 i if isinstance(i, int) else "'%s'" % i, j
#             ),
#                 list_values[0], list_names
#             )),
#         'other': ', %s' % ', '.join(
#             map(
#                 lambda i: '%s' % str(tuple(i)),
#                 list_values[1:]
#             )
#         ) if list_values[1:] else ''
#     }


def execute_pg(sql):
    conn = psycopg2.connect(host='analyticspg',
                            port=5432,
                            user='postgres',
                            password='postgres',
                            database='cubes')
    with conn.cursor() as cursor:
        cursor.execute(sql)
        conn.commit()


def fetchall_pg(sql):
    conn = psycopg2.connect(host=DB_HOST,
                            port=DB_PORT,
                            user=DB_USER,
                            password=DB_PASSWORD,
                            database='cubes')
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def execute(sql):
    conn = connect(host='172.16.5.22', protocol='beeswax', port=21000)
    with conn.cursor() as cursor:
        cursor.execute("set compression_codec=gzip")
        cursor.execute(sql)


def fetchall(sql):
    attempt = 0
    complete = False
    while attempt < NUMBER_ATTEMPT and not complete:
        try:
            conn = connect(host='172.16.5.22', protocol='beeswax', port=21000)
            with conn.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()
        except (socket.timeout, impala.error.DisconnectedError,
                impala.error.OperationalError, impala.error.RPCError,
                thrift.transport.TTransport.TTransportException) as msg:
            attempt += 1
            # logging.info(type(msg))
            # logging.info(msg.message)
            # logging.info('SLEEP: %s' % (
            #     type(msg)
            # ))
            logging.info('SLEEP: %s' % msg)
            time.sleep(TIME_SLEEP)
        else:
            complete = True


def get_last_day_seg_table(table):
    conn = connect(host='172.16.5.22', protocol='beeswax', port=21000)
    with conn.cursor() as cursor:
        cursor.execute('show partitions %s;' % table)
        partitions = cursor.fetchall()
        partition_list = map(
            lambda partition: int(partition[0]),
            filter(
                lambda partition: partition[0].isdigit(), partitions
            )
        )
        day = datetime.fromtimestamp(max(partition_list)) - timedelta(hours=3)
        return day.strftime(DATE_FORMAT)


def get_last_days_seg_tables(app):
    partitions = fetchall('show partitions seg_users_%s_pq;' % app)
    partition_list = map(
        lambda partition: int(partition[0]),
        filter(
            lambda partition: partition[0].isdigit(), partitions
        )
    )
    partition_list.sort()
    seg_users_lh = partition_list[-1]

    partitions = fetchall('show partitions seg_players_%s_pq;' % app)
    partition_list = map(
        lambda partition: int(partition[0]),
        filter(
            lambda partition: partition[0].isdigit(), partitions
        )
    )
    partition_list.sort()
    seg_players_lh = partition_list[-1]
    return seg_users_lh, seg_players_lh


def get_last_day(file):
    path_file = os.path.join(LOCAL_TMP_DIR, file)
    if not os.path.exists(path_file):
        raise Exception("File %s not found" % path_file)
    if not os.access(path_file, os.R_OK):
        raise Exception("File %s is closed for reading" % path_file)

    with open(path_file, 'r') as f:
        line = f.readline()
        # if key not in line:
        #     line = f.readline()
        if line == '':
            raise Exception("File %s is empty" % path_file)
        else:
            # last_day_str = line.lstrip(key + ':').strip()
            # return last_day_str
            return line.strip()
    return None


def union_files(end_file, path_files, temp_path_files, headers=None, drop_file=False):
    list_file = os.listdir(temp_path_files)
    if list_file:
        list_file.sort()
        with open(os.path.join(path_files, end_file), 'w') as f_w:
            if headers:
                f_w.write(headers)
            for file_name in list_file:
                with open(os.path.join(temp_path_files, file_name), 'r') as f:
                    line_count = range(
                        sum(1 for l in open(os.path.join(
                            temp_path_files, file_name), 'r')))
                    for i in line_count:
                        line = f.readline()
                        f_w.write(line)
        if drop_file:
            for file_name in list_file:
                os.remove(file_name)


def push_last_day(file, last_day):
    path_file = os.path.join(LOCAL_TMP_DIR, file)
    if not os.path.exists(path_file):
        raise Exception("File %s not found" % path_file)
    if not os.access(path_file, os.W_OK):
        raise Exception("File %s is closed for writing" % path_file)

    with open(path_file, 'w') as f:
        f.write(str(last_day))


def exist_process(ipid):
    try:
        os.getpgid(ipid)
    except OSError:
        return False
    else:
        return True


def try_lock(lock_file):
    process_ext = False
    ipid_str = ''
    if not os.path.exists(PATH_LOCK_FILE):
        logging.info("Folder %s not found" % PATH_LOCK_FILE)
    elif os.path.exists(os.path.join(PATH_LOCK_FILE, lock_file)):
        with open(os.path.join(PATH_LOCK_FILE, lock_file), 'r') as f:
            ipid_str = f.readline()
            if ipid_str.isdigit():
                process_ext = exist_process(int(ipid_str))
    if (not os.path.exists(os.path.join(PATH_LOCK_FILE, lock_file))
        or not process_ext):
        with open(os.path.join(PATH_LOCK_FILE, lock_file), 'w') as f:
            ipid_str = str(os.getpid())
            f.write(ipid_str)
        logging.info(("File %s created. ipid = %s" % (
            os.path.join(PATH_LOCK_FILE, lock_file), ipid_str)))
        return False
    elif process_ext:
        logging.info(u'Process with ipid = %s exists' % ipid_str)
    return True


def delete_lock(lock_file):
    if not os.path.exists(PATH_LOCK_FILE):
        logging.info("Folder %s not found" % PATH_LOCK_FILE)
    elif not os.path.exists(os.path.join(PATH_LOCK_FILE, lock_file)):
        logging.info("File %s not found" % (os.path.join(PATH_LOCK_FILE, lock_file)))
    elif os.path.exists(os.path.join(PATH_LOCK_FILE, lock_file)):
        os.remove(os.path.join(PATH_LOCK_FILE, lock_file))
        logging.info("File %s deleted" % (os.path.join(PATH_LOCK_FILE, lock_file)))


def get_list_day(first_day, last_day):
    day = first_day
    day_list = [
        day + timedelta(days=i) for i in range((last_day-first_day).days)]
    return day_list


def check_day_csv_file(path, days):
    result = list()
    for day in days:
        path_file = os.path.join(path, '%s.csv' % day.strftime('%Y-%m-%d'))
        if not os.path.exists(path_file) or not os.path.isfile(path_file):
            result.append(day)
    return result


def get_full_list_hour(table):
    hours = fetchall('show partitions %s;' % table)
    hour_list = map(lambda hour: int(hour[0]) if hour[0].isdigit() else None, hours)[:-1]
    result = list(set(hour_list))
    result.sort()
    return result


def add_breaket(el):
    el = "(" + "'%s'" % el + ")"
    return el


def create_levels(max_level):
    level_list = range(max_level)
    lds = map(add_breaket, level_list)
    for el in lds:
        if el == lds[0]:
            el = el[:-1] + " AS id)"
            lds[0] = el
    return ", ".join(lds)


def levels_table(max_level):
    table = """
        select
            *
        from
            (values %s) as levels
    """ % create_levels(max_level)
    return table


def create_chains(max_chain):
    chain_list = range(max_chain)
    cds = map(add_breaket, chain_list)
    for el in cds:
        if el == cds[0]:
            el = el[:-1] + " AS id)"
            cds[0] = el
    return ", ".join(cds)


def chains_table(max_chain):
    table = """
        select
            *
        from
            (values %s) as chains
    """ % create_chains(max_chain)
    return table


def create_values(ab_name):
    ids = map(add_breaket, ab_name)
    for el in ids:
        if el == ids[0]:
            el = el[:-1] + " AS ab_names)"
            ids[0] = el
    return ", ".join(ids)


def ids_table(ab_name):
    if ab_name == [] or ab_name == ['--']:
        val = "('--' AS ab_names)"
    else:
        val = create_values(ab_name)
    table = """
        (
        SELECT
            *
        FROM
            (VALUES %s) AS ids
       )
    """ % val
    return table


def get_ab_values(app, hour):
    create_ab_table(app)
    ab_names = []
    sql = """
    SELECT DISTINCT
        regexp_extract(payload, "ABGroups\\":\\"([^\\"]*)", 1) as ab_group
    FROM
        all_events_%(app)s_pq AS p
    WHERE
        p.hour >= unix_timestamp(to_date('%(hour)s'))
        and p.hour < unix_timestamp(days_add(to_date('%(hour)s'), 1))
        and p.event_type = 'event'
        and (p.parameters ilike '%%\\"Level.Complete\\"%%'
            or p.parameters ilike '%%\\"Level.Failed\\"%%')
        AND payload LIKE '%%ABGroups%%'
    """ % {'hour': hour, 'app': app}
    # logging.info(sql)
    res_sql = fetchall(sql)
    for elem in res_sql:
        for el in elem:
            ab = el.split(', ')
            ab_names += ab
    if [''] in res_sql:
        return map(lambda x: '--' if x == '' else x, list(set(ab_names)))
    else:
        return list(set(ab_names))


def get_list_hour(table, first_hour, last_hour):
    partitions = fetchall('show partitions %s;' % table)
    hours = map(
        lambda hour: int(hour[0]),
        filter(
            lambda hour:
                hour[0].isdigit() and
                int(hour[0]) >= first_hour and
                int(hour[0]) < last_hour,
            partitions
        )
    )
    hours.sort()
    return list(set(hours))


def get_drop_partition_sql(table, hour):
    return """
        alter table %s drop partition(hour = %s)
    """ % (table, hour)


def execute_file(sql, filename=None):
    conn = connect(host='172.16.5.22', protocol='beeswax', port=21000)
    with conn.cursor() as cursor:
        cursor.execute(sql)
        if not filename:
            return cursor.fetchall()
        else:
            open(filename, 'w').close()
            block_size = 1000000
            block = cursor.fetchmany(size=block_size)
            with open(filename, 'a') as f:
                while block:
                    for row in block:
                        f.write('%s\n' % ';'.join(row))
                    block = cursor.fetchmany(size=block_size)


def put_headers(filename=None, headers=[]):
    with open(filename, 'w') as f:
        f.write('%s\n' % ';'.join(headers))


def get_max_level(app):
    partitions = fetchall('show partitions seg_players_%s_pq;' % app)
    last_hour = int(partitions[-2][0])
    sql = """
        select
            max(level)
        from
            seg_players_%(app)s_pq
        where
            hour = %(hour)s
    """ % {'app': app, 'hour': last_hour}
    return int(fetchall(sql)[0][0]) + 1


def get_difference_days_list(
        first_table, second_table, gte=None, lte=None):
    first_hour = None
    last_hour = None

    partitions_f = get_full_list_hour(first_table)
    partitions_s = get_full_list_hour(second_table)

    if gte:
        first_hour = int(time.mktime((
            datetime.strptime(gte, '%Y-%m-%d') + timedelta(hours=3)
        ).timetuple()))
    if lte:
        last_hour = int(time.mktime((
            datetime.strptime(lte, '%Y-%m-%d') + timedelta(hours=3)
        ).timetuple()))
    if partitions_f == []:
        first_hour = None

    if not first_hour and not last_hour:
        difference_days = sorted(set(partitions_s) - set(partitions_f))
    else:
        difference_days = partitions_s
        if first_hour:
            difference_days = map(
                lambda hour: hour,
                filter(
                    lambda hour: hour >= first_hour, difference_days
                ))
        if last_hour:
            difference_days = map(
                lambda hour: hour,
                filter(
                    lambda hour: hour <= last_hour, difference_days
                ))

    result = []
    for day in difference_days:
        for i in range(len(partitions_s) - 1):
            if day == partitions_s[i]:
                result.append([partitions_s[i], partitions_s[i + 1]])
    result.reverse()
    return result


def get_difference_days(first_table, second_table):
    result = list()
    partitions = fetchall('show partitions %s;' % first_table)
    first_table_p = map(
        lambda hour: int(hour[0]),
        filter(
            lambda hour: hour[0].isdigit(), partitions
        )
    )
    first_table_p.sort()

    partitions = fetchall('show partitions %s;' % second_table)
    second_table_p = map(
        lambda hour: int(hour[0]),
        filter(
            lambda hour: hour[0].isdigit(), partitions
        )
    )
    second_table_p.sort()

    for i in range(len(second_table_p) - 1):
        hours = list()
        for hour in first_table_p:
            if hour >= second_table_p[i] and hour < second_table_p[i + 1]:
                hours.append(hour)
        if len(hours) < 24:
            result.append([second_table_p[i], second_table_p[i + 1]])
    result.reverse()
    for hour1, hour2 in result:
        day1 = datetime.fromtimestamp(hour1) - timedelta(hours=3)
        day2 = datetime.fromtimestamp(hour2) - timedelta(hours=3)
        logging.info((day1.strftime('%Y-%m-%d'), day2.strftime('%Y-%m-%d')))
    return result


def create_ab_table(app):
    sql = '''
    create table if not exists
        ab_group_%(app)s_pq
        (
        ab_names varchar(255)
        )
    partitioned by (hour int)
    stored as parquet
    ''' % {'app': app}
    execute(sql)


def update_ab_table(app, hour):
    hour = datetime.strftime(hour, '%Y-%m-%d')
    table = 'ab_group_%(app)s_pq' % {'app': app}
    insert_sql = ids_table(get_ab_values(app, hour))
    sql = '''
        insert into
        %(table)s
        partition (hour = cast(unix_timestamp('%(hour)s') as int))
        select
            cast(ab_names as varchar(255)) as ab_names
        from
            %(insert_sql)s as ab
        ''' % {'table': table,
               'hour': hour,
               'insert_sql': insert_sql}
    exists_hours = get_full_list_hour(table)

    hour = datetime.strptime(hour, '%Y-%m-%d')
    hour = int(calendar.timegm(hour.timetuple()))
    if hour not in exists_hours:
        execute(sql)
    else:
        pass


def sftp_exists(sftp, path):
    """os.path.exists for paramiko's SCP object"""
    try:
        sftp.stat(path)
    except IOError, e:
        if 'No such file' in str(e):
            return False
        raise
    else:
        return True


def recreate_cube(cube_name):
    cmd = "python %(local_cubes)s/manage_cubes.py --host='%(host)s' recreate %(cube_name)s" % {
            'local_cubes': LOCAL_CUBES,
            'cube_name': cube_name,
            'host': DB_HOST
        }
    print cmd
    proc = Popen(
        cmd,
        shell=True,
        stdout=PIPE, stderr=PIPE
    )
    proc.wait()
    res = proc.communicate()
    if proc.returncode:
        print(res[1])
    else:
        print('recreate cube %s' % cube_name)


def put(localpath, remotepath):
    trans = Transport((S_HOST, 22))
    trans.connect(username=S_USER, password=S_PASSWORD)
    sftp = SFTPClient.from_transport(trans)
    temp_remotepath = remotepath + '.temp'
    sftp.put(localpath, temp_remotepath)
    if sftp_exists(sftp, remotepath):
        sftp.remove(remotepath)
    sftp.rename(temp_remotepath, remotepath)
    trans.close()


def put_with_check(localpath, remotepath, file_name, imported=None):
    logging.info(imported)
    trans = Transport((S_HOST, 22))
    trans.connect(username=S_USER, password=S_PASSWORD)
    sftp = SFTPClient.from_transport(trans)
    if not sftp_exists(sftp, imported):
        temp_remotepath = remotepath + '.temp'
        logging.info((localpath, temp_remotepath))
        sftp.put(localpath, temp_remotepath)
        if sftp_exists(sftp, remotepath):
            sftp.remove(remotepath)
        sftp.rename(temp_remotepath, remotepath)
    else:
        logging.info('File %s alredy exists.' % file_name)
    trans.close()


def put_with_recreate(localpath, remotepath, file_name, cube_name):
    trans = Transport((S_HOST, 22))
    trans.connect(username=S_USER, password=S_PASSWORD)
    sftp = SFTPClient.from_transport(trans)
    temp_remotepath = remotepath + '.temp'
    sftp.put(localpath, temp_remotepath)
    recreate_cube(cube_name)
    if sftp_exists(sftp, remotepath):
        sftp.remove(remotepath)
    sftp.rename(temp_remotepath, remotepath)
    trans.close()


def put_with_sql(localpath, remotepath, file_name, sql, result_sql):
    trans = Transport((S_HOST, 22))
    trans.connect(username=S_USER, password=S_PASSWORD)
    sftp = SFTPClient.from_transport(trans)
    temp_remotepath = remotepath + '.temp'
    if sftp_exists(sftp, result_sql):
        sftp.remove(result_sql)
    sftp.put(sql, result_sql)
    sftp.put(localpath, temp_remotepath)
    if sftp_exists(sftp, remotepath):
        sftp.remove(remotepath)
    sftp.rename(temp_remotepath, remotepath)
    trans.close()


def create_flag(cube_name):
    trans = Transport((S_HOST, 22))
    trans.connect(username=S_USER, password=S_PASSWORD)
    sftp = SFTPClient.from_transport(trans)
    flag_import = os.path.join(IMPORT_CSV_PATH, cube_name, 'in_progress.flag')
    flag_temp = os.path.join(TEMP_PATH, 'in_progress.flag')
    with sftp.open(flag_import, 'w') as f:
        pass
    # sftp.put(flag_import, flag_temp)
    trans.close()


def drop_flag(cube_name):
    trans = Transport((S_HOST, 22))
    trans.connect(username=S_USER, password=S_PASSWORD)
    sftp = SFTPClient.from_transport(trans)
    flag = os.path.join(IMPORT_CSV_PATH, cube_name, 'in_progress.flag')
    sftp.remove(flag)
    trans.close()


def relocation_file(cube_name, file_name, sql=None, sql_name=None,
                    check_exist=False, recreate=False, from_path=None):

    # Создаем Flag, который блокирует загрузку данных из этой папки
    create_flag(cube_name)

    filename = file_name + '.csv'
    if not from_path:
        from_path = os.path.join(TEMP_PATH, cube_name, filename)
    import_csv_path = os.path.join(IMPORT_CSV_PATH, cube_name, filename)
    imported_csv_path = os.path.join(IMPORTED_CSV_PATH, cube_name, filename)
    logging.info('COPY: from %s to %s' % (from_path, import_csv_path))
    if not check_exist and not recreate and not sql:
        put(from_path, import_csv_path)
    elif check_exist and not recreate and not sql:
        put_with_check(from_path, import_csv_path, filename,
                       imported=imported_csv_path)
    elif recreate and not check_exist and not sql:
        put_with_recreate(from_path, import_csv_path, filename, cube_name)
    elif sql and not recreate and not check_exist:
        result_sql = os.path.join(IMPORT_CSV_PATH, cube_name, sql_name)
        put_with_sql(from_path, import_csv_path, filename, sql, result_sql)
    else:
        logging.error('Invalid combination of keys.')

    drop_flag(cube_name)


def create_sql_file(sql_name, file_name, cube_name, app_table=None):
    sql = ''
    with file(os.path.join(SQL_PATH, '%s.sql' % sql_name), 'r') as f:
        for line in f:
            sql += line
    return sql % {'file_name': os.path.join(
        IMPORT_CSV_PATH, cube_name, file_name
    ), 'cube_name': cube_name, 'app_table': app_table}


def get_difference_month_list(gte, lte):
        month_list = []
        date = gte.replace(day=1)
        while date < lte:
            month_list.append(
                int(time.mktime(
                        (date + timedelta(hours=3)).timetuple()
                )))
            days_in_month = calendar.monthrange(date.year, date.month)[1]
            date += timedelta(days=days_in_month)
        return month_list


# def get_max_level(app, seg_hour):
#     sql = """
#         SELECT
#             max(level)
#         from
#             seg_players_%(app)s_pq
#         where
#             hour = %(seg_hour)s
#     """ % {'app': app, 'seg_hour': seg_hour}
#     return int(fetchall(sql)[0][0])


def get_segments_levels(max_level):
    result = list()
    i = 1
    while i < max_level:
        result.append([i, i + SEGMENT_STEP - 1, '%04d-%04d' % (
            i, i + SEGMENT_STEP - 1)
        ])
        i += SEGMENT_STEP
    return result


def get_values_sql(list_values, list_names):
    return 'select * from (values %(header)s%(other)s) as list' % {
        'header': '(%s)' % ', '.join(
            map(
                    lambda i, j: '%s as %s' % (
                        i if isinstance(i, int) else "'%s'" % i, j
                    ),
                    list_values[0], list_names
            )),
        'other': ', %s' % ', '.join(
                map(
                    lambda i: '%s' % str(tuple(i)),
                    list_values[1:]
                    )
            ) if list_values[1:] else ''
    }


class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """
    def __init__(self, logger, log_level=logging.ERROR):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass
