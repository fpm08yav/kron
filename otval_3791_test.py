import socket
import impala
import time
import logging
import logging.config
import sys
import os
import calendar
import psycopg2

from datetime import datetime, timedelta
from impala.dbapi import connect
from common_functions import (
    try_lock, delete_lock, get_last_day, push_last_day, StreamToLogger,
    relocation_file
)


LOCK_FILE = 'conversion_otval2.lock'
LOGGING_PATH = '/home/yansen-a/analytics/logs'

NUMBER_ATTEMPT = 10
TIME_SLEEP = 20 * 60

# APPS = ['3444', '3790', '3928', '3858', '3606']
APPS = ['3606']


MIN_DAY = {
    '3444': '2016-01-14',
    '3426': '2016-02-04',
    '758': '2015-12-23',
    '3789': '2016-06-04',
    '3928': '2016-06-27',
    '3858': '2016-07-01',
    '3790': '2016-07-07',
    '3791': '2016-08-24',
    '3606': '2016-06-12'
}

CUBE_APP = {
    # '3444': 'cube_drop_fd',
    # '3426': 'cube_drop_fd_an',
    '3606': 'cube_drop_fd_am_t',
    # '3789': 'cube_drop_gs',
    # '3790': 'cube_drop_gs_an',
    # '3791': 'cube_drop_gs_am_t',
    # '3858': 'cube_drop_gs_fb',
    # '3928': 'cube_drop_zm'
}


P_APP = {
    '3426': 'fd_an',
    '3606': 'fd_am'
}


def execute(sql):
    conn = connect(host='172.16.5.22', protocol='beeswax', port=21000)
    with conn.cursor() as cursor:
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
                impala.error.OperationalError, impala.error.RPCError) as msg:
            attempt += 1
            logger.info('SLEEP: %s' % msg)
            time.sleep(TIME_SLEEP)
        else:
            complete = True


def fetchall_post(sql):
    conn = psycopg2.connect(host='172.16.5.28',
                            port=5432,
                            user='postgres',
                            password='postgres',
                            database='cubes')
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def ab_group_table():
    return """
        select
            *
        from
            (values %s) as ab_group
    """ % get_values(list_element('ab_group'))


def app_version_table():
    return """
        select
            *
        from
            (values %s) as app_version
    """ % get_values(list_element('app_version', app))


def add_breakets(el):
    s = ""
    for i in el:
        s += "(%s, cast('%s' as varchar(255)), cast('%s' as varchar(255))), " % (
            i[0], i[1], i[1])
    return s[:-2]


def get_values(ab_group_list):
    return '(%(id)s as id, "%(name)s" as name, "%(name)s" as description)%(other)s' % {
        'id': ab_group_list[0][1],
        'name': ab_group_list[0][0],
        'other': ', %s' % ','.join(
            map(
                lambda i: '(%s, "%s", "%s")' % (i[1], i[0], i[0]),
                ab_group_list[1:])) if ab_group_list[1:] else ''
    }


def get_element_sql(name, app):
    return """
        SELECT
            name,
            id
        from
            %s_%s
    """ % (name, P_APP[app])


def list_element(name, app):
    return fetchall_post(get_element_sql(name, app))


def get_full_list_hour(table):
    hours = fetchall('show partitions %s;' % table)
    return map(lambda hour: int(hour[0]) if hour[0].isdigit() else None, hours)[:-1]


def get_list_hour(max_hour, delta_days):
    date = max_hour
    hours = []
    for i in range(delta_days):
        for j in range(24):
            hours.append(date.replace(hour=date.hour+j).replace(day=date.day+i))
    return hours


def add_breaket_str(el):
    el = "(" + "'%s'" % el + ")"
    return el


def add_breaket(el):
    el = "(" + "%s" % el + ")"
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
    """ % {'hour': last_hour, 'app': app}
    # logger.info(sql)
    return int(fetchall(sql)[0][0])


def get_hours(table):
    hours = []
    sql = "show partitions %s" % table
    inf_t = fetchall(sql)

    for h in inf_t:
        if not h[0] == "Total":
            hours += [int(h[0])]

    hours = sorted(hours)
    return hours


def create_values(ab_name):
    ids = map(add_breaket_str, ab_name)
    for el in ids:
        if el == ids[0]:
            el = el[:-1] + " AS ab_names)"
            ids[0] = el
    return ", ".join(ids)


def ids_table(ab_name):
    # logger.info(ab_name)
    if ab_name == [] or ab_name == ['--']:
        val = '("--" AS ab_names)'
    else:
        val = create_values(ab_name)
    # logger.info(val)
    table = """
        (
        SELECT
            *
        FROM
            (VALUES %s) AS ids
       )
    """ % val
    # logger.info(table)
    return table


def create_ab_table(app):
    sql_CT = '''
    CREATE TABLE IF NOT EXISTS
        ab_group_%(app)s_pq
        (
        ab_names VARCHAR(255)
        )
    PARTITIONED BY (hour INT)
    STORED AS PARQUET
    ''' %{'app':app}
    execute(sql_CT)


def get_ab_values(app,hour):
    ab_names = []
    sql = """
    SELECT DISTINCT
        regexp_extract(payload, "ABGroups\\":\\"([^\\"]*)", 1) as ab_group
    FROM
        all_events_%(app)s_pq AS p
    WHERE
        hour >= unix_timestamp(days_add(to_date('%(hour)s'), -1))
        and hour < unix_timestamp('%(hour)s')
        and p.event_type = 'event'
        and (p.parameters ilike '%%\\"Level.Complete\\"%%'
            or p.parameters ilike '%%\\"Level.Failed\\"%%')
        AND payload LIKE '%%ABGroups%%'
    """ % {'hour': hour,
            'app': app}
    # logger.info(sql)
    res_sql = fetchall(sql)
    # print res_sql
    for elem in res_sql:
        for el in elem:
            ab = el.split(', ')
            ab_names += ab
    if [''] in res_sql:
        return map(lambda x: '--' if x == '' else x, list(set(ab_names)))
    else:
        return list(set(ab_names))


def update_ab_table(app, hour):
    hour = datetime.strftime(hour, '%Y-%m-%d')
    table = 'ab_group_%(app)s_pq' % {'app':app}
    insert_sql = ids_table(get_ab_values(app, hour))
    sql_IT = '''
        INSERT INTO
        %(table)s
        PARTITION (hour = CAST(unix_timestamp('%(hour)s') AS INT))
        SELECT
            CAST(ab_names AS VARCHAR(255)) AS ab_names
        FROM
            %(insert_sql)s AS ab
        ''' % {'table':table,
                'hour':hour,
                'insert_sql':insert_sql}
    exists_hours = get_hours(table)

    hour = datetime.strptime(hour, '%Y-%m-%d')
    hour = int(calendar.timegm(hour.timetuple()))

    if hour not in exists_hours:
        execute(sql_IT)
    else:
        pass


def get_dumps_sql():
    sql = """
    WITH
    ab AS
        (
        SELECT
            ae.event_user,
            ids.ab_names
        FROM
            (
            SELECT DISTINCT
                event_user,
                regexp_extract(payload, 'ABGroups\\":\\"([^\\"]*)', 1) AS ab_names
            FROM
                all_events_%(app)s_pq
            WHERE
                hour >= unix_timestamp(days_add(to_date('%(next_date)s'), -1))
                AND hour < unix_timestamp('%(next_date)s')
                AND event_type = 'event'
                AND payload LIKE '%%ABGroups%%'
            ) AS ae,
            (
            SELECT
                ab_names
            FROM
                ab_group_%(app)s_pq
            WHERE
                hour = unix_timestamp('%(next_date)s')
            ) AS ids
        WHERE
            decode(ae.ab_names, '', '--', ae.ab_names) rlike concat('(^|,)', ids.ab_names, '($|,)')
        )

        SELECT
            to_date(days_add(to_date('%(next_date)s'), -1)),
            level,
            decode(payer, 1, 'Payers', 0, 'Non payers') as payer,
            app_version,
            %(levels_version)s,
            device_region,
            ab_group,
            SUM(DECODE(alive,0,1,0)) AS dumps_not_alive_cnt,
            SUM(DECODE(alive,1,1,0)) + SUM(DECODE(alive,0,1,0)) AS dumps_all_cnt
        FROM
        (
            SELECT
                event_user,
                s_level,
                l.id as level,
                e_level,
                f_level,
                (
                CASE
                    WHEN f_level = l.id AND activ = 0 THEN 0
                    ELSE 1
                END
                ) AS alive,
                payer,
                app_version,
                levels_version,
                device_region,
                ab_group
            FROM
                (
                SELECT DISTINCT
                    e.event_user,
                    NVL(s.s_level,1) AS s_level,
                    e.e_level,
                    f.f_level,
                    e.e_level - NVL(s.s_level,1) AS delta,
                    f.activ,
                    e.payer,
                    e.app_version,
                    e.levels_version,
                    e.device_region,
                    e.ab_group/*,
                    f.level_conv*/
                FROM
                    (
                    SELECT
                        sp.event_user,
                        sp.level AS e_level,
                        sp.payer,
                        sp.app_version,
                        sp.levels_version,
                        (CASE
                            WHEN
                                su.device_region IS NULL
                            THEN
                                "--"
                            WHEN
                                LENGTH(su.device_region) != 2
                            THEN
                                "--"
                            ELSE
                                su.device_region
                        END) as device_region,
                        NVL(ab.ab_names, '--') AS ab_group
                    FROM
                        seg_players_%(app)s_pq sp
                    LEFT JOIN
                        (
                            %(device_region)s
                        ) AS su
                        ON
                            sp.event_user = su.event_user
                    LEFT JOIN
                        ab
                    ON
                        sp.event_user = ab.event_user
                    WHERE
                        sp.hour = unix_timestamp('%(next_date)s')
                        AND sp.first_session IS NOT NULL
                        AND sp.level IS NOT NULL
                    ) AS e
                LEFT JOIN
                    (
                    SELECT
                        event_user,
                        level AS s_level
                    FROM
                        seg_players_%(app)s_pq
                    WHERE
                        hour = unix_timestamp(days_add(to_date('%(next_date)s'), -1))
                        --AND first_session IS NOT NULL
                    ) AS s
                ON
                    e.event_user = s.event_user
                LEFT JOIN
                    (
                    SELECT
                        sp.event_user,
                        sp.level AS f_level,
                        (
                        CASE
                            --WHEN ((sp.hour + 3*3600)*1000 - sp.last_active)/(24*3600*1000) > 7 THEN 0
                            WHEN (sp.hour*1000 - sp.last_active)/(24*3600*1000) > 7 THEN 0
                            ELSE 1
                        END
                        ) AS activ/*,
                        level_conv*/

                    FROM
                        seg_players_%(app)s_pq sp
                    WHERE
                        /*sp.hour = %(seg_date)s*/
                        sp.hour = unix_timestamp(days_add(to_date('%(next_date)s'), 7)
                        AND first_session IS NOT NULL
                    ) AS f
                ON
                    f.event_user = e.event_user
                ) AS a
            INNER JOIN
                (
                    SELECT
                        row_number() over (
                            order by event_user
                        ) as id
                    from
                        seg_players_3444_pq
                    where
                        hour = unix_timestamp(days_add(to_date(from_unixtime(%(seg_date)s)), -5))
                    limit
                        %(max_level)s
                ) l
            ON
                l.id > a.s_level
                AND l.id <= a.e_level
            WHERE
                a.delta > 0
            ORDER BY
                event_user,
                level
        ) AS b
        GROUP BY
            days_add(to_date('%(next_date)s'), -1),
            level,
            payer,
            app_version,
            levels_version,
            device_region,
            ab_group
    """
    return sql


def get_dumps_an_sql():
    sql = """
        WITH
        ab AS
            (
            SELECT
                ae.event_user,
                ids.ab_names
            FROM
                (
                SELECT DISTINCT
                    event_user,
                    regexp_extract(payload, "ABGroups\\":\\"([^\\"]*)", 1) AS ab_names
                FROM
                    all_events_%(app)s_pq
                WHERE
                    hour >= unix_timestamp(days_add(to_date('%(next_date)s'), -1))
                    AND hour < unix_timestamp('%(next_date)s')
                    AND event_type = 'event'
                    AND payload LIKE '%%ABGroups%%'
                ) AS ae,
                (
                SELECT
                    ab_names
                FROM
                    ab_group_%(app)s_pq
                WHERE
                    hour = unix_timestamp('%(next_date)s')
                ) AS ids
            WHERE
                decode(ae.ab_names, '', '--', ae.ab_names) rlike concat('(^|,)', ids.ab_names, '($|,)')
            )

            SELECT
                to_date(days_add(to_date('%(next_date)s'), -1)),
                level,
                decode(payer, 1, 'Payers', 0, 'Non payers') as payer,
                app.id as app_version,
                %(levels_version)s,
                device_region,
                ab_group,
                SUM(DECODE(alive,0,1,0)) AS dumps_not_alive_cnt,
                SUM(DECODE(alive,1,1,0)) + SUM(DECODE(alive,0,1,0)) AS dumps_all_cnt/*,
                SUM(DECODE(conv_true,1,1,0)) AS conv_cnt,
                SUM(DECODE(conv_true,1,1,0)) + SUM(DECODE(conv_true,0,1,0)) AS all_conv_cnt*/
            FROM
            (
                SELECT
                    event_user,
                    s_level,
                    l.id as level,
                    e_level,
                    f_level,
                    --level_conv,
                    (
                    CASE
                        WHEN f_level = l.id AND activ = 0 THEN 0
                        ELSE 1
                    END
                    ) AS alive,
                    payer,
                    /*(CASE
                        WHEN level_conv = l.id THEN 1
                        WHEN level_conv < l.id THEN NULL
                        WHEN f_level = e_level AND activ = 1 AND level_conv IS NULL THEN NULL
                        ELSE 0
                    END) AS conv_true,*/
                    app_version,
                    levels_version,
                    device_region,
                    ab_group
                FROM
                    (
                    SELECT DISTINCT
                        e.event_user,
                        NVL(s.s_level,1) AS s_level,
                        e.e_level,
                        f.f_level,
                        e.e_level - NVL(s.s_level,1) AS delta,
                        f.activ,
                        e.payer,
                        e.app_version,
                        e.levels_version,
                        e.device_region,
                        e.ab_group
                    FROM
                        (
                        SELECT
                            sp.event_user,
                            sp.level AS e_level,
                            sp.payer,
                            sp.app_version,
                            sp.levels_version,
                            (CASE
                                WHEN
                                    su.device_region IS NULL
                                THEN
                                    "--"
                                WHEN
                                    LENGTH(su.device_region) != 2
                                THEN
                                    "--"
                                ELSE
                                    su.device_region
                            END) as device_region,
                            NVL(ab.ab_names, '--') AS ab_group
                        FROM
                            seg_players_%(app)s_pq sp
                        LEFT JOIN
                            (
                                %(device_region)s
                            ) AS su
                            ON
                                sp.event_user = su.event_user
                        LEFT JOIN
                            ab
                        ON
                            sp.event_user = ab.event_user
                        WHERE
                            sp.hour = unix_timestamp('%(next_date)s')
                            AND sp.first_session IS NOT NULL
                            AND sp.level IS NOT NULL
                        ) AS e
                    LEFT JOIN
                        (
                        SELECT
                            event_user,
                            level AS s_level
                        FROM
                            seg_players_%(app)s_pq
                        WHERE
                            hour = unix_timestamp(days_add(to_date('%(next_date)s'), -1))
                            --AND first_session IS NOT NULL
                        ) AS s
                    ON
                        e.event_user = s.event_user
                    LEFT JOIN
                        (
                        SELECT
                            sp.event_user,
                            sp.level AS f_level,
                            (
                            CASE
                                --WHEN ((sp.hour + 3*3600)*1000 - sp.last_active)/(24*3600*1000) > 7 THEN 0
                                WHEN (sp.hour*1000 - sp.last_active)/(24*3600*1000) > 7 THEN 0
                                ELSE 1
                            END
                            ) AS activ

                        FROM
                            seg_players_%(app)s_pq sp
                        WHERE
                            /*sp.hour = %(seg_date)s*/
                            sp.hour = unix_timestamp(days_add(to_date('%(next_date)s'), 7))
                            AND first_session IS NOT NULL
                        ) AS f
                    ON
                        f.event_user = e.event_user
                    ) AS a
                INNER JOIN
                    (
                        SELECT
                            row_number() over (
                                order by event_user
                            ) as id
                        from
                            seg_players_3444_pq
                        where
                            hour = unix_timestamp(days_add(to_date(from_unixtime(%(seg_date)s)), -5))
                        limit
                            %(max_level)s
                    ) l
                ON
                    l.id > a.s_level
                    AND l.id <= a.e_level
                WHERE
                    a.delta > 0
                ORDER BY
                    event_user,
                    level
            ) AS b,
            (%(app_version)s) as app
            where
                app_version = cast(app.name as varchar(255))
            GROUP BY
                days_add(to_date('%(next_date)s'), -1),
                level,
                payer,
                app_version,
                levels_version,
                device_region,
                ab_group
        """
    return sql
# app = '3444'
# create_ab_table('3444')
# start = datetime(2016, 1, 15, 0, 0)
# end = get_full_list_hour('seg_players_3444_pq')[-1]
# end = datetime.utcfromtimestamp(end)
# while start <= end:
#     update_ab_table(app, start)
#     print start
#     start += timedelta(days=1)


if __name__ == '__main__':
    if not try_lock(LOCK_FILE):
        loggin_file = 'log_conf.conf'
        logging.config.fileConfig(os.path.join(LOGGING_PATH, loggin_file))
        logger = logging.getLogger('conversion_otval')
        log_stderr = logging.getLogger('conversion_otval_error')
        sl = StreamToLogger(log_stderr, logging.ERROR)
        sys.stderr = sl
        logger.info('START!')

        for app in APPS:
            logger.info('Start for %s!' % app)
            if app in ['3789', '3858', '3790', '3791']:
                levels_version = 'nvl(levels_version, "NOT_INSTALLED")'
            else:
                levels_version = 'nvl(levels_version, -1)'
            create_ab_table(app)
            path_file = '/home/yansen-a/analytics/logs'
            file_name = 'conversion_otval_%s_test' % app
            date_format = '%Y-%m-%d'

            start = datetime.strptime(MIN_DAY[app], '%Y-%m-%d')
            # app_fs = datetime.strptime(MIN_DAY[app], '%Y-%m-%d')
            seg_players = get_full_list_hour('seg_players_%s_pq' % app)[-1]
            seg_users = get_full_list_hour('seg_players_%s_pq' % app)[-1]
            if seg_players != seg_users:
                logger.info(
                    'ERROR: seg_players (%s) lte seg_users(%s)' % (
                        seg_players, seg_users))
            else:
                seg_date = seg_users
                end = datetime.fromtimestamp(seg_date) - timedelta(
                    days=7, hours=3)
                last_day = datetime.strptime(
                    get_last_day('conversion_otval_%s_t.txt' % app), date_format)
                if end > last_day:
                    count_days = (end - start).days
                    # levels = levels_table(get_max_level(app))
                    max_level = get_max_level(app) + 1
                    i = 0
                    # open(file_name, 'w').close()
                    with open(
                        os.path.join(path_file, file_name + '.csv'),
                            'w') as f:
                        f.write('day;level;payer;app_version;levels_version;device_region;ab_group;dumps_not_alive_cnt;dumps_all_cnt;conv_cnt;all_conv_cnt\n')
                    while start <= end:
                        logger.info('|%s| %s - %s [%s]' % (
                            app, start, end, count_days - i))
                       # ab_table = ids_table(get_ab_values(app,start))
                        update_ab_table(app, start)
                        if app in ['3606', '3790', '3791']:
                            device_region = """
                                SELECT
                                    s.event_user as event_user,
                                    s2.device_region as device_region
                                FROM
                                    (
                                        SELECT
                                            event_user,
                                            MAX(device_region) AS device_region
                                        FROM
                                            seg_users_%(app)s_pq
                                        WHERE
                                            hour = unix_timestamp('%(seg_date)s')
                                        GROUP BY
                                            event_user
                                    ) s
                                left join
                                    (   select
                                            alpha2 as device_region,
                                            alpha3
                                        from
                                            device_region
                                    ) s2
                                on
                                    s.device_region = s2.alpha3
                            """
                        else:
                            device_region = """
                                SELECT
                                    event_user,
                                    MAX(device_region) AS device_region
                                FROM
                                    seg_users_%(app)s_pq
                                WHERE
                                    hour = unix_timestamp('%(seg_date)s')
                                GROUP BY
                                    event_user
                            """
                        if app in ['3426', '3606']:
                            sql = get_dumps_an_sql() % {
                                'app_version': app_version_table(),
                                'seg_date': seg_date,
                                'next_date': start,
                                'max_level': max_level,
                                'levels_version': levels_version,
                                'app': app,
                                'device_region': device_region % {
                                    'app': app,
                                    'seg_date': seg_date,
                                    }
                            }
                        else:
                            sql = get_dumps_sql() % {
                                'seg_date': seg_date,
                                'next_date': start,
                                'max_level': max_level,
                                'levels_version': levels_version,
                                'app': app,
                                'device_region': device_region % {
                                    'app': app,
                                    'seg_date': seg_date,
                                    }
                            }
                        # logger.info(sql)
                        res = fetchall(sql)
                        # logging.info(res)
                        with open(os.path.join(path_file, file_name + '.csv'), 'a') as f:
                            for row in res:
                                f.write('%s\n' % ';'.join(row))
                        start += timedelta(days=1)
                        i += 1
                    push_last_day(
                        'conversion_otval_%s_t.txt' % app,
                        datetime.strftime(end, date_format))
            relocation_file(CUBE_APP[app], file_name,
                            recreate=True,
                            from_path=os.path.join(path_file, file_name + '.csv'))
        # else:
        #     logger.info('ERROR: end_day (%s) lte last_day(%s)' % (
        #         end, last_day
        #     ))
        delete_lock(LOCK_FILE)
        logger.info('END!')
