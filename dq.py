from datetime import datetime, timedelta
from impala.dbapi import connect
import psycopg2
import itertools
import os

from common_functions import get_full_list_hour, create_sql_file, relocation_file

APPS = ['3789', '3790', '3791', '3858']
# APPS = ['3791']
CUBES = ['cube_distribution']
# CUBES = ['cube_two', 'cube_distribution', 'cube_conv_drop',
#          'retention_fishdom', 'retention_township']

PATH_FILE = '/home/yansen-a/analytics/logs'

# MIN_DAY = {'3789': '2016-06-04'}
MIN_DAY = {
    # '3444': '2016-01-14',
    # '3426': '2016-02-04',
    # '758':  '2015-12-23',
    '3789': '2016-06-04',
    '3790': '2016-07-07',
    '3791': '2016-08-24',
    # '3928': '2016-06-27',
    '3858': '2016-07-01'
}
CUBE_APP = {
    # '3444': 'cube_distribution',
    # '3426': 'cube_distribution_fd_an',
    '3789': 'cube_distribution_gs_cu',
    '3790': 'cube_distribution_gs_an_cu',
    '3791': 'cube_distribution_gs_am_cu',
    '3858': 'cube_distribution_gs_fb_cu',
    # '3928': 'cube_distribution_zm'
}


def get_last_seg(apps):
    conn = connect(host='172.16.5.22', protocol='beeswax', port=21000)
    cursor = conn.cursor()

    for app in apps:
        sql = """
            select min(hour), max(hour)
            from seg_players_%(app)s_pq
            """ % {'app': app}
        cursor.execute(sql)
        row = cursor.fetchone()
        print '=============seg_players', app
        print 'start', datetime.fromtimestamp(int(row[0])).strftime("%Y-%m-%d")
        print 'finish', datetime.fromtimestamp(int(row[1])).strftime("%Y-%m-%d")

        sql = """
            select min(hour), max(hour)
            from seg_users_%(app)s_pq
            """ % {'app': app}
        cursor.execute(sql)
        row = cursor.fetchone()
        print '=============seg_users', app
        print 'start', datetime.fromtimestamp(int(row[0])).strftime("%Y-%m-%d")
        print 'finish', datetime.fromtimestamp(int(row[1])).strftime("%Y-%m-%d")


def get_last_cubes(cubes):
    conn = psycopg2.connect(host='172.16.1.64',
                            port=5432,
                            user='postgres',
                            password='postgres',
                            database='cubes')
    cursor = conn.cursor()
    for cube in cubes:
        sql = """
            select min(cube_date), max(cube_date)
            from %(cube)s
            """ % {'cube': cube}

        cursor.execute(sql)
        row = cursor.fetchone()
        print '=============', cube
        print 'start', row[0]
        print 'finish', row[1]
        # print 'start', datetime.fromtimestamp(int(row[0])).strftime("%Y-%m-%d")
        # print 'finish', datetime.fromtimestamp(int(row[1])).strftime("%Y-%m-%d")


def imp_test(app):
    # conn = connect(host='172.16.5.22', port=21050)
    conn = connect(host='172.16.5.22', protocol='beeswax', port=21000)
    cursor = conn.cursor()

    app_fs = MIN_DAY[app]   # '2016-01-14'
    if app in ['3789', '3858', '3790', '3791']:
        levels_version = 'nvl(levels_version, "NOT_INSTALLED")'
    else:
        levels_version = 'nvl(levels_version, -1)'
    last_seg_date = (
        datetime.fromtimestamp(
            get_full_list_hour('seg_players_%s_pq' % app)[-1]
        ) - timedelta(hours=3)
    ).strftime('%Y-%m-%d')
    print last_seg_date
    levels_sql = """
                    select distinct
                        quest
                    from
                        seg_players_%(app)s_pq
                    where
                        hour = unix_timestamp('%(last_seg_date)s')
                        and quest is not null
                """ % {'last_seg_date': last_seg_date, 'app': app}
    cursor.execute(levels_sql)
    levels = zip(*cursor.fetchall())[0]

    params_sql = """
            select
                to_date(cast(sp.first_session/1000 as timestamp)) as day,
                payer,
                app_version,
                levels_version,
                (case
                    when
                        device_region is null
                    then
                        "--"
                    when
                        LENGTH(device_region) != 2
                    then
                        "--"
                    else
                        device_region
                end) as device_region,
                count(1) as cnt
            from
                (
                select
                    event_user,
                    first_session,
                    quest,
                    decode(payer, 1, 'Payers', 0, 'Non payers') as payer,
                    app_version,
                    %(levels_version)s as levels_version,
                    hour
                from
                    seg_players_%(app)s_pq
                where
                    hour = unix_timestamp('%(last_seg_date)s')
                    and first_session is not null
                    and quest is not null
                    and first_session >= unix_timestamp('%(app_fs)s') * 1000
                    and first_session < unix_timestamp('%(last_seg_date)s') * 1000
                ) as sp
            left join
                (
                select
                    event_user,
                    max(device_region) as device_region
                from
                    seg_users_%(app)s_pq
                where
                    hour = unix_timestamp('%(last_seg_date)s')
                group by
                    event_user
                ) as su
            on
                sp.event_user = su.event_user
            group by
                day,
                payer,
                app_version,
                levels_version,
                device_region
            order by
                day,
                payer,
                app_version,
                levels_version,
                device_region
        """ % {'last_seg_date': last_seg_date,
               'app_fs': app_fs, 'app': app, 'levels_version': levels_version}
    # print params_sql
    cursor.execute(params_sql)
    params = cursor.fetchall()

    params_dict = {}
    for row in params:
        params_dict[tuple(row[:-1])] = row[-1]

    print len(params_dict)

    sql = """
            select
                to_date(cast(sp.first_session/1000 as timestamp)) as day,
                payer,
                app_version,
                levels_version,
                (case
                    when
                        device_region is null
                    then
                        "--"
                    when
                        LENGTH(device_region) != 2
                    then
                        "--"
                    else
                        device_region
                end) as device_region,
                quest,
                count(1) as cnt
            from
                (
                select
                    event_user,
                    first_session,
                    quest,
                    decode(payer, 1, 'Payers', 0, 'Non payers') as payer,
                    app_version,
                    %(levels_version)s as levels_version,
                    hour
                from
                    seg_players_%(app)s_pq
                where
                    hour = unix_timestamp('%(last_seg_date)s')
                    and first_session is not null
                    and quest is not null
                    and first_session >= unix_timestamp('%(app_fs)s') * 1000
                    and first_session < unix_timestamp('%(last_seg_date)s') * 1000
                ) as sp
             left join
                (
                select
                    event_user,
                    max(device_region) as device_region
                from
                    seg_users_%(app)s_pq
                where
                    hour = unix_timestamp('%(last_seg_date)s')
                group by event_user
                ) as su
            on
                sp.event_user = su.event_user
            group by
                day,
                payer,
                app_version,
                levels_version,
                device_region,
                quest
            order by
                day,
                payer,
                app_version,
                levels_version,
                device_region,
                quest
        """ % {'last_seg_date': last_seg_date,
               'app_fs': app_fs, 'app': app, 'levels_version': levels_version}
    cursor.execute(sql)
    res = cursor.fetchall()
    d = {}
    for row in res:
        d[tuple(row[:-1])] = row[-1]
        # print row

    print len(d)

    # path_file = '/home/yansen-a/analytics/logs'
    file_name = 'dist_cu_%s' % app
    # with open('/home/ivanov-i/prj/gs_metagame/dist_%s.csv' % app, 'w') as f:
    with open(os.path.join(PATH_FILE, file_name + '.csv'), 'w') as f:
        f.write('first_session;payer;app_version;levels_version;device_region;quest;count_user;sum_user\n')
        for level in levels:
            for param, cnt in params_dict.items():
                if param + tuple([level]) in d:
                    val = str(d[param + tuple([level])])
                else:
                    val = '0'
                f.write(';'.join(param + tuple([level, val, cnt])) + '\n')

    sql_name = 'dist_cu_%s.sql' % app
    sql_file_name = os.path.join(PATH_FILE, sql_name)
    if app in ['3789', '3790', '3791', '3858']:
        sql_file = create_sql_file('distribution_gs_cu', file_name,  CUBE_APP[app])
    with file(sql_file_name, 'w') as f:
        f.write(sql_file)
    # print (CUBE_APP[app], file_name,
    #                 sql_file_name, sql_name,
    #                 os.path.join(PATH_FILE, '%s.csv' % file_name))
    relocation_file(CUBE_APP[app], file_name,
                    sql=sql_file_name, sql_name=sql_name,
                    from_path=os.path.join(PATH_FILE, '%s.csv' % file_name))

if __name__ == '__main__':
    # get_last_seg(APPS)
    # get_last_cubes(CUBES)
    for app in APPS:
        print app
        imp_test(app)
    print 'FINISH'
    # d = {row : 0 for row in itertools.product([1,2],[3,4,5], ['a','b'])}
    # print d
    # print tuple(['a','b']) + tuple([1])
