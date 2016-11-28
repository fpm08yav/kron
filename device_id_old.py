# -*- coding: utf-8 -*-
"""
Created on Mon Jan 25 18:38:49 2016

@author: kopylov-a
"""
# +++++++++++++
import os
# +++++++++++++
from impala.dbapi import connect
from datetime import datetime
from datetime import timedelta
from common_functions import try_lock, delete_lock

APPS = [3444, 3426]
# APPS = [3426]
# +++++++++++++
PARENT_PATH = '/home/yansen-a/device_id'
LOCK_FILE = 'device_id.lock'
# +++++++++++++


def execute_sql(sql):
    con = connect(host = '172.16.5.22',protocol = 'beeswax', port = 21000)
    with con:
        cur = con.cursor()
        cur.execute("set compression_codec=gzip")
        cur.execute(sql)
        return cur.fetchall()


# +++++++++++++++++++++
def get_hours(table):
    hours = []
    sql = "show partitions %s" % table
    inf_t = execute_sql(sql)

    for h in inf_t:
        if not h[0] == "Total":
            hours += [int(h[0])]

    hours = sorted(hours)
    return hours


def get_list_loaded_hours(path):
    h_list = []
    if os.path.exists(path):
        with open(path, 'r') as f:
            h_list = map(
                lambda hour: int(hour),
                filter(
                    lambda hour: line.rstrip('\n').isdigit(),
                    [line for line in f]))
    else:
        open(path, 'w').close()
    return h_list


def put_list_loaded_hours(path, h_list):
    if not isinstance(h_list, list) and isinstance(h_list, int):
        h_list = [h_list]
    if isinstance(h_list, list):
        with open(path, 'a') as f:
            for hour in h_list:
                if isinstance(hour, int):
                    f.write('%s\n' % hour)
                else:
                    return False
        return True
    return False


def get_new_hour(parent_table, h_file):
    table_h_list = get_hours(parent_table)
    file_h_list = get_list_loaded_hours(h_file)
    return sorted(set(table_h_list) - set(file_h_list), reverse=True)

# +++++++++++++++++++++


sql_DT = '''
DROP TABLE IF EXISTS %s
'''

# execute_sql(sql_DT %'segments_3426_pq')
sql_CT = '''
    CREATE TABLE IF NOT EXISTS
        %(table_name)s
        %(table_columns)s
    STORED AS PARQUET
'''

sql_IT = '''
INSERT INTO
    %(table_name)s
    %(SQL_insert)s
'''

sql_S = """
    SELECT
        event_user
    FROM
        all_events_%(app)s_pq AS ae
    WHERE
        ae.hour = %(h)s
        AND ae.event_type = 'user'
        AND ae.parameters ILIKE '%%deviceId%%'
    limit 1
"""


table_params = ['device_id_%(app)s_pq',
                """
                (
                event_user VARCHAR(255),
                device_id VARCHAR(255),
                first_event_time BIGINT,
                last_event_time BIGINT
                )
                """]

table_params_time = ['device_id_%(app)s_pq_time',
                    """
                    (
                    event_user VARCHAR(255),
                    device_id VARCHAR(255),
                    first_event_time BIGINT,
                    last_event_time BIGINT
                    )
                    """,
                '''
                    SELECT
                        event_user,
                        CAST(device_id AS VARCHAR(255)) AS device_id,
                        MIN(first_event_time) AS first_event_time,
                        MAX(last_event_time) AS last_event_time
                    FROM
                        (
                            (
                            SELECT
                                ae.event_user,
                                REGEXP_EXTRACT(
                                    ae.parameters,
                                    '"deviceId":"([^/"]*?)"',1) AS device_id,
                                event_time AS first_event_time,
                                event_time AS last_event_time
                            FROM
                                all_events_%(app)s_pq AS ae
                            WHERE
                                ae.event_type = 'user'
                                AND UPPER(ae.parameters) LIKE '%%DEVICEID%%'
                                --AND ae.hour >= %(h)s
                                --AND ae.hour < %(h)s + 1*24*60*60
                                AND ae.hour = %(h)s
                            )
                            UNION ALL
                            (
                            SELECT
                                *
                            FROM
                                device_id_%(app)s_pq AS di
                            )
                        ) AS ae_di
                    GROUP BY
                        event_user,
                        device_id
                ''']

if not try_lock(LOCK_FILE):
    for app in APPS:
        # +++++++++++++++++
        h_file = os.path.join(PARENT_PATH, 'device_id_%s_pq.txt' % app)
        hour_list = get_new_hour('all_events_%s_pq' % app, h_file)
        print len(hour_list)
        for hour in hour_list:
            print app, hour
            res = execute_sql(sql_S % {'h': hour, 'app': app})
            if res:
                execute_sql(sql_CT % {'table_name': table_params[0] % {'app': app},
                                      'table_columns': table_params[1]})

                execute_sql(sql_CT % {
                    'table_name': table_params_time[0] % {'app': app},
                    'table_columns': table_params_time[1]})

                execute_sql(sql_IT % {
                    'table_name': table_params_time[0] % {'app': app},
                    'SQL_insert': table_params_time[2] % {'h': hour, 'app': app}})

                execute_sql(
                    """ALTER TABLE
                        device_id_%(app)s_pq
                    RENAME TO device_id_%(app)s_pq_old""" % {'app': app})
                execute_sql(
                    """ALTER TABLE
                        device_id_%(app)s_pq_time
                    RENAME TO device_id_%(app)s_pq""" % {'app': app})
                execute_sql(
                    'DROP TABLE IF EXISTS device_id_%(app)s_pq_old' % {'app': app})
            put_list_loaded_hours(h_file, hour)
    delete_lock(LOCK_FILE)
