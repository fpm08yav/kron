#! coding=utf-8
import socket
import logging
import logging.config
import sys
import os
import time
import impala
import psycopg2

from datetime import datetime, timedelta
from impala.dbapi import connect

from common_functions import (get_last_day, get_last_day_seg_table, try_lock,
                              delete_lock, push_last_day, fetchall,
                              get_last_days_seg_tables, StreamToLogger,
                              get_full_list_hour, relocation_file)


LOCK_FILE = 'retention1.lock'
LOGGING_PATH = '/home/yansen-a/analytics/logs/'
RESULT_PATH = '/home/yansen-a/analytics/local_cubes/temp/retention_var_id/'

NUMBER_ATTEMPT = 10
TIME_SLEEP = 10*60
STEP = 2

MIN_DAY = {
    # '758':  '2015-12-23',
    '3444': '2016-07-31',
    # '3444': '2016-08-20',
    # '3789': '2016-06-04',
    # '3790': '2016-07-07',
    # '3426': '2016-02-04',
    '3426': '2016-08-06',
    # '3928': '2016-06-27',
    # '3858': '2016-06-27'
}

CUBE_APP = {
    # '3606': 'retention_fd_am_dev_id',
    '3426': 'retention_fd_an_dev_id',
    '3444': 'retention_fd_dev_id',
    # '3790': 'retention_gs_an_var',
    # '3858': 'retention_gs_fb_var',
    # '3789': 'retention_gs_var',
    # '758':  'retention_ts_var',
    # '3928': 'retention_zm_var'
}


# APP_LIST = ['3444']
APP_LIST = ['3444', '3426']


def execute(sql, filename=None, headers=[]):
    if not filename:
        attempt = 0
        complete = False
        while attempt < NUMBER_ATTEMPT and not complete:
            try:
                conn = connect(host='172.16.5.22', protocol='beeswax', port=21000)
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    result = cursor.fetchall()
            except socket.timeout, impala.error.DisconnectedError:
                attempt += 1
                logger.info('SLEEP')
                time.sleep(TIME_SLEEP)
            else:
                complete = True
        return result
    else:
        conn = connect(host='172.16.5.22', protocol='beeswax', port=21000)
        cursor = conn.cursor()
        cursor.execute(sql)
        block_size = 1000000
        block = cursor.fetchmany(size=block_size)
        with open(filename, 'w') as f:
            if headers:
                f.write('%s\n' % ';'.join(headers))
            while block:
                for row in block:
                    # row[0] = md5.md5(row[0]).hexdigest()
                    f.write('%s\n' % ';'.join(row))
                block = cursor.fetchmany(size=block_size)


def execute_many(sql, sh, eh):
    conn = connect(host='172.16.5.22', protocol='beeswax', port=21000)
    cursor = conn.cursor()
    date_format = '%Y-%m-%d'

    start = datetime.strptime(sh, date_format)
    end = datetime.strptime(eh, date_format)

    i = 0
    while start < end:
        one_sql = sql % {'sch': datetime.strftime(start, date_format),
                         'ech': datetime.strftime(start + timedelta(days=1), date_format),
                         'eh': datetime.strftime(end, date_format)}
        cursor.execute(one_sql)
        res = cursor.fetchall()

        with open('cohorts_ts5.csv', 'a') as f:
            for row in res:
                f.write('%s;%s\n' % (str(i), ';'.join(row)))
        start += timedelta(days=1)
        i += 1


def get_select_sql(app, next_date, seg_date):
    if app in ['3606', '3790']:
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

    sql = """
        with ids as
            (
            select
                p.event_user,
                p.first_session,
                unix_timestamp(to_date(from_unixtime(round(p.first_session/1000)))) as coh_day,
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
                payers.payer,
                DECODE(pub.id,NULL,1,pub.id) AS publisher,
                --DECODE(pub.id,NULL,0,1) AS organic
                (case
                    when pub.id is NULL then 0
                    when pub.id = 1 then 0
                    when pub.id = 162 then 2
                    else 1
                end) as organic
            from
                seg_players_%(app)s_pq p
                left join
                    (
                        %(device_region)s
                    ) AS su
                on
                    p.event_user = su.event_user
                left join
                    (
                    select
                        event_user,
                        payer
                    from
                        seg_players_%(app)s_pq
                    where
                        hour = unix_timestamp('%(next_date)s')
                        and first_session is not null
                        and round(first_session/1000) < unix_timestamp('%(next_date)s') - 48*3600
                        and round(first_session/1000) >= unix_timestamp('%(min_day)s')
                        and round(first_session/1000) < unix_timestamp('%(next_date)s')
                    ) as payers
                on
                    payers.event_user = p.event_user
                LEFT JOIN
                (
                    SELECT
                        d.event_user,
                        m.publisher
                        --if(m.device_id is null, '--', m.publisher) as publisher
                    FROM
                        (
                        select distinct
                            device_id,
                            publisher
                        from
                        (
                            select
                                device_id,
                                first_value(publisher) over (partition by device_id order by type_install desc, publisher) as publisher
                            from
                                (
                                    select distinct
                                        device_id,
                                        publisher,
                                        'publisher' as type_install
                                    from
                                    (
                                        select
                                            upper(device_id) AS device_id,
                                            first_value(publisher) over (partition by device_id order by type_install, publisher) as publisher
                                        from
                                            mat_device_id_%(app)s_pq
                                        WHERE
                                            type_install in ('install', 'tf')
                                    ) publisher

                                    union all
                                    ------not_publisher--------
                                        select distinct
                                            upper(device_id) as device_id,
                                            '' as publisher,
                                            'not publisher' as type_install
                                        from
                                            mat_device_id_%(app)s_pq
                                        where
                                            type_install not in ('install', 'tf')
                                            --and publisher = ''
                                    ---------------------------

                                    /*union all
                                    -----------unknow----------
                                        select distinct
                                            upper(device_id) as device_id,
                                            '--' as publisher,
                                            '--' as type_install
                                        from
                                            mat_device_id_%(app)s_pq
                                        where
                                            type_install not in ('install', 'tf')
                                            and publisher != ''
                                    ---------------------------*/
                                ) m
                        ) m2
                    ) AS m
                    RIGHT JOIN
                        device_id_%(app)s_pq AS d
                    ON
                        upper(d.device_id) = m.device_id
                    WHERE
                        first_event_time = (
                                            SELECT
                                                MIN(first_event_time)
                                            FROM
                                                device_id_%(app)s_pq AS d0
                                            WHERE
                                                d0.event_user = d.event_user
                                            )
                    ) AS mat
                ON
                    mat.event_user = p.event_user
                left join
                (%(publishers)s) as pub
                on pub.name = mat.publisher
            where
                p.hour = unix_timestamp('%(seg_date)s') and
                p.first_session is not null and
                round(p.first_session/1000) < unix_timestamp('%(next_date)s') - 48*3600
                and round(p.first_session/1000) >= unix_timestamp('%(min_day)s')
                and round(p.first_session/1000) < unix_timestamp('%(next_date)s')
                /*and u.hour = unix_timestamp('%(seg_date)s')
                and u.event_user = p.event_user*/
            )

        select
            to_date(from_unixtime(coh_day)) as coh_day,
            n,
            device_region,
            decode(s.payer, 1, 'Payers', 'Non payers') as payer,
            sum(active) as sum_active,
            round(variance(active), 2) as variance,
            count(active) as count_active,
            publisher,
            organic
        from
            (
            select
                ids.event_user,
                ids.coh_day,
                floor((unix_timestamp('%(next_date)s') - 48*3600 - ids.coh_day)/(24*3600)) as n,
                ids.device_region,
                ids.payer,
                decode(max(zeroifnull(e2.event_time)), 0, 0, 1) as active,
                ids.publisher,
                ids.organic
            from
                ids
            left join
                (
                select
                    e.event_user,
                    e.event_time
                from
                    ids
                inner join
                    (
                    select
                        event_user, event_time
                    from
                        all_events_%(app)s_pq
                    where
                        hour >= unix_timestamp('%(next_date)s') - 48*3600 and
                        hour < unix_timestamp('%(next_date)s') and
                        event_type = 'session_start'
                    ) e
                    on ids.event_user = e.event_user
                where
                    (e.event_time - ids.first_session)/1000 >= unix_timestamp('%(next_date)s') - 48*3600 - ids.coh_day and
                    (e.event_time - ids.first_session)/1000 < unix_timestamp('%(next_date)s') - 24*3600 - ids.coh_day
                ) e2
                on ids.event_user = e2.event_user
            group by
                ids.event_user,
                ids.coh_day,
                n,
                ids.device_region,
                ids.payer,
                ids.publisher,
                ids.organic
            ) s
        group by
            coh_day,
            n,
            device_region,
            payer,
            publisher,
            organic
        order by
            coh_day,
            n,
            device_region,
            payer
    """ % {
        'app': app,
        'next_date': next_date,
        'seg_date': seg_date,
        'min_day': MIN_DAY[app],
        'device_region': device_region % {
            'app': app,
            'seg_date': seg_date,
            },
        'publishers': publisher_table()
    }
    # print sql
    return sql


def execute_post(sql):
    conn = psycopg2.connect(
                            # host='localhost',
                            host='analyticspg',
                            # host='172.16.5.28',
                            port=5432,
                            user='postgres',
                            password='postgres',
                            database='cubes')
    with conn.cursor() as cursor:
        cursor.execute(sql)
        conn.commit()


def fetchall_post(sql):
    conn = psycopg2.connect(
                            # host='localhost',
                            host='analyticspg',
                            # host='172.16.5.28',
                            port=5432,
                            user='postgres',
                            password='postgres',
                            database='cubes')
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def add_breakets(el):
    s = ""
    for i in el:
        s += "(%s, cast('%s' as varchar(255)), cast('%s' as varchar(255))), " % (
            i[0], i[1], i[1])
    return s[:-2]


def get_values(element_list):
    return '(%(id)s as id, "%(name)s" as name, "%(name)s" as description)%(other)s' % {
        'id': element_list[0][1],
        'name': element_list[0][0],
        'other': ', %s' % ','.join(
            map(
                lambda i: '(%s, "%s", "%s")' % (i[1], i[0], i[0]),
                element_list[1:])) if element_list[1:] else ''
    }


def get_element_sql(name):
    return """
        SELECT
            name,
            id
        from
            %s
    """ % name


def get_list_element(name):
    return fetchall_post(get_element_sql(name))


def get_new_publisher_sql():
    elements = get_list_element('publisher')
    if elements:
        return """
            SELECT
                new_id,
                name
            from
                (
                SELECT
                    row_number() over (order by id, name) as new_id,
                    name,
                    id as old_id
                from
                    (
                    SELECT
                        id,
                        pq.name
                    from
                        (values %(elements)s) el
                    right join
                    (
                        SELECT
                            if(publisher = '', '--', publisher) as name
                        from
                            mat_device_id
                        group by
                            publisher
                    ) pq
                    on el.name = pq.name
                ) s
            ) s2
            where old_id is null
        """ % {
            'elements': get_values(elements)
        }
    else:
        return """
             SELECT
                row_number() over (order by name) as new_id,
                name
            from
                (
                    SELECT
                        publisher as name
                    from
                        mat_device_id
                    group by
                            publisher
                ) s
        """


def get_new_element(name, gte_hour=None, lt_hour=None):
    if name == 'publisher':
        return fetchall(get_new_publisher_sql())


def get_insert_table_sql(name, gte_hour=None, lt_hour=None):
    new_element = add_breakets(get_new_element(name, gte_hour, lt_hour))

    if new_element:
        return """
            INSERT INTO
                %(table_name)s
            (id, name, description)
            VALUES
            %(new_element)s;
        """ % {
            'new_element': new_element,
            'table_name': name}
    return None


def get_create_table_sql(name):
    return """
        CREATE TABLE IF NOT EXISTS %s
        (
          id smallint,
          name varchar(255),
          description varchar(255)
        )
    """ % name


def try_create_table(name):
    logger.info('START create table')
    execute_post(get_create_table_sql(name))
    logger.info('END create table')
    logger.info('START update table')
    try_update_table(name)
    logger.info('END update table')


def try_update_table(name, gte_hour=None, lt_hour=None):
    new_element = get_insert_table_sql(name, gte_hour, lt_hour)
    if new_element:
        execute_post(new_element)


def publisher_table():
    return """
        select
            *
        from
            (values %s) as publisher
    """ % get_values(get_list_element('publisher'))


def make_csv():
    sql_3444 = """
        with
        /*payers as
            (select
                event_user,
                1 as is_payer
            from
                all_events_%(app)s_pq
            where
                hour <= unix_timestamp('%(next_date)s')
                and event_type = 'iap'
            group by event_user),*/

        ids as
            (
            select
                p.event_user,
                p.first_session,
                unix_timestamp(to_date(from_unixtime(round(p.first_session/1000 -3*3600)))) as coh_day,
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
                payers.payer
            from
                seg_players_%(app)s_pq p
            left join
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
                ) AS su
            on
                p.event_user = su.event_user
            left join
                (
                select
                    event_user,
                    payer
                from
                    seg_players_%(app)s_pq
                where
                    hour = unix_timestamp('%(next_date)s')
                    and first_session is not null
                    and round(first_session/1000 - 3*3600) < unix_timestamp('%(next_date)s') - 48*3600
                    and round(first_session/1000 - 3*3600) >= unix_timestamp('2016-01-14')
                    and round(first_session/1000 - 3*3600) < unix_timestamp('%(next_date)s')
                ) as payers
            on
                payers.event_user = p.event_user
            where
                p.hour = unix_timestamp('%(seg_date)s') and
                p.first_session is not null and
                round(p.first_session/1000 - 3*3600) < unix_timestamp('%(next_date)s') - 48*3600
                and round(p.first_session/1000 - 3*3600) >= unix_timestamp('2016-01-14')
                and round(p.first_session/1000 - 3*3600) < unix_timestamp('%(next_date)s')
                /*and u.hour = unix_timestamp('%(seg_date)s')
                and u.event_user = p.event_user*/
            )

        select
            to_date(from_unixtime(coh_day)) as coh_day,
            n,
            device_region,
            decode(s.payer, 1, 'Payers', 'Non payers') as payer,
            sum(active) as sum_active,
            count(active) as count_active
        from
            (
            select
                ids.event_user,
                ids.coh_day,
                floor((unix_timestamp('%(next_date)s') - 48*3600 - ids.coh_day)/(24*3600)) as n,
                ids.device_region,
                ids.payer,
                decode(max(zeroifnull(e2.event_time)), 0, 0, 1) as active
            from
                ids
            left join
                (
                select
                    e.event_user,
                    e.event_time
                from
                    ids
                inner join
                    (
                    select
                        event_user, event_time
                    from
                        all_events_%(app)s_pq
                    where
                        hour >= unix_timestamp('%(next_date)s') - 48*3600 and
                        hour < unix_timestamp('%(next_date)s') and
                        event_type = 'session_start'
                    ) e
                    on ids.event_user = e.event_user
                where
                    (e.event_time - ids.first_session)/1000 >= unix_timestamp('%(next_date)s') - 48*3600 - ids.coh_day and
                    (e.event_time - ids.first_session)/1000 < unix_timestamp('%(next_date)s') - 24*3600 - ids.coh_day
                ) e2
                on ids.event_user = e2.event_user
            group by
                ids.event_user,
                ids.coh_day,
                n,
                ids.device_region,
                ids.payer
            ) s
        group by
            coh_day,
            n,
            device_region,
            payer
        order by
            coh_day,
            n,
            device_region,
            payer
    """

    sql_758 = """
        with
        /*payers as
            (select
                event_user,
                1 as is_payer
            from
                all_events_%(app)s_pq
            where
                hour <= unix_timestamp('%(next_date)s')
                and event_type = 'iap'
            group by event_user),*/

        ids as
            (
            select
                p.event_user,
                p.first_session,
                unix_timestamp(to_date(from_unixtime(round(p.first_session/1000 -3*3600)))) as coh_day,
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
                payers.payer
            from
                seg_players_%(app)s_pq p
            left join
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
                ) AS su
            on
                p.event_user = su.event_user
            left join
                (
                select
                    event_user,
                    payer
                from
                    seg_players_%(app)s_pq
                where
                    hour = unix_timestamp('%(next_date)s')
                    and first_session is not null
                    and round(first_session/1000 - 3*3600) < unix_timestamp('%(next_date)s') - 48*3600
                    and round(first_session/1000 - 3*3600) >= unix_timestamp('2015-12-23')
                    and round(first_session/1000 - 3*3600) < unix_timestamp('%(next_date)s')
                ) as payers
            on
                payers.event_user = p.event_user
            where
                p.hour = unix_timestamp('%(seg_date)s') and
                p.first_session is not null and
                round(p.first_session/1000 - 3*3600) < unix_timestamp('%(next_date)s') - 48*3600
                and round(p.first_session/1000 - 3*3600) >= unix_timestamp('2015-12-23')
                and round(p.first_session/1000 - 3*3600) < unix_timestamp('%(next_date)s')
                /*and u.hour = unix_timestamp('%(seg_date)s')
                and u.event_user = p.event_user*/
            )

        select
            to_date(from_unixtime(coh_day)) as coh_day,
            n,
            device_region,
            decode(s.payer, 1, 'Payers', 'Non payers') as payer,
            sum(active) as sum_active,
            count(active) as count_active
        from
            (
            select
                ids.event_user,
                ids.coh_day,
                floor((unix_timestamp('%(next_date)s') - 48*3600 - ids.coh_day)/(24*3600)) as n,
                ids.device_region,
                ids.payer,
                decode(max(zeroifnull(e2.event_time)), 0, 0, 1) as active
            from
                ids
            left join
                (
                select
                    e.event_user,
                    e.event_time
                from
                    ids
                inner join
                    (
                    select
                        event_user, event_time
                    from
                        all_events_%(app)s_pq
                    where
                        hour >= unix_timestamp('%(next_date)s') - 48*3600 and
                        hour < unix_timestamp('%(next_date)s') and
                        event_type = 'session_start'
                    ) e
                    on ids.event_user = e.event_user
                where
                    (e.event_time - ids.first_session)/1000 >= unix_timestamp('%(next_date)s') - 48*3600 - ids.coh_day and
                    (e.event_time - ids.first_session)/1000 < unix_timestamp('%(next_date)s') - 24*3600 - ids.coh_day
                ) e2
                on ids.event_user = e2.event_user
            group by
                ids.event_user,
                ids.coh_day,
                n,
                ids.device_region,
                ids.payer
            ) s
        group by
            coh_day,
            n,
            device_region,
            payer
        order by
            coh_day,
            n,
            device_region,
            payer
    """

    for app in APP_LIST:
        logger.info('\n')
        logger.info('Start retention for %s' % app)
        list_seg_day = get_last_days_seg_tables(app)
        if min(list_seg_day) != max(list_seg_day):
            hour_u = datetime.utcfromtimestamp(
                get_full_list_hour('seg_users_%s_pq' % app)[-1])
            hour_p = datetime.utcfromtimestamp(
                get_full_list_hour('seg_players_%s_pq' % app)[-1])
            logger.error(
                """Last hour seg_users_%(app)s_pq (%(hour_u)s)
                    not equal last hour seg_players_%(app)s_pq (%(hour_p)s)
                """ % {
                    'app': app,
                    'hour_u': hour_u.strftime('%Y-%m-%d'),
                    'hour_p': hour_p.strftime('%Y-%m-%d')})
        else:
            seg_date = get_last_day_seg_table('seg_players_%s_pq' % app)
            logger.info('Last hour seg_tables = |%s|' % seg_date)
            next_date = get_last_day(os.path.join(RESULT_PATH, 'retention_%s.txt' % app))
            if not next_date:
                next_date = MIN_DAY[app]
            logger.info('Next_date = |%s|' % seg_date)

            date_format = '%Y-%m-%d'
            start = datetime.strptime(next_date, date_format) + timedelta(days=1)
            end = datetime.strptime(seg_date, date_format)# - timedelta(days=2)
            i = 0
            logger.info('start: %s||end: %s' % (start, end))
            while start <= end:
                logger.info('app-%s |%s| %s - %s' % (
                    app, (end - start).days, start, end))
                next_date = datetime.strftime(start, date_format)
                sql = get_select_sql(app, next_date, seg_date)
                # logger.info(sql)
                res = execute(sql)
                file_name = 'retention_%s_%s' % (
                    app, datetime.strftime(start, '%Y_%m_%d')
                )
                path_file = os.path.join(RESULT_PATH, file_name + '.csv')
                with open(path_file, 'w') as f:
                    f.write('coh_day;n;device_region;payer;sum_active;variance;count_active;publisher;organic\n')
                    for row in res:
                        f.write('%s\n' % ';'.join(row))
                relocation_file(CUBE_APP[app], file_name,
                                from_path=path_file)
                push_last_day(os.path.join(RESULT_PATH, 'retention_%s.txt' % app),
                              datetime.strftime(start, date_format))
                start += timedelta(days=1)
                i += 1
        logger.info('End retention for %s' % app)


if __name__ == '__main__':
    if not try_lock(LOCK_FILE):
        loggin_file = 'log_conf.conf'
        logging.config.fileConfig(os.path.join(LOGGING_PATH, loggin_file))
        logger = logging.getLogger('retention')
        sl = StreamToLogger(logger, logging.ERROR)
        sys.stderr = sl
        # logger = logging.getLogger()
        logger.info('START!')
        try_create_table('publisher')
        make_csv()
        delete_lock(LOCK_FILE)
        logger.info('END!')
    # make_text('t0.csv', 't0.txt')
