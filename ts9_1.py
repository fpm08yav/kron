#! coding=utf-8
import socket
import logging
import logging.config
import sys
import os
import time
import impala
import thrift

from datetime import datetime, timedelta
from impala.dbapi import connect

from common_functions import (get_last_day, get_last_day_seg_table, try_lock,
                              delete_lock, push_last_day,
                              get_last_days_seg_tables, StreamToLogger,
                              get_full_list_hour, relocation_file)


LOCK_FILE = 'retention.lock'
LOGGING_PATH = '/home/yansen-a/analytics/logs'
RESULT_PATH = '/home/yansen-a/analytics/local_cubes/temp/retention_var/'

NUMBER_ATTEMPT = 10
TIME_SLEEP = 10 * 60
STEP = 2

MIN_DAY = {
    '758': '2015-12-23',
    '3444': '2016-01-14',
    '3789': '2016-06-04',
    '3790': '2016-07-07',
    '3791': '2016-08-24',
    '3426': '2016-02-04',
    '3606': '2016-06-13',
    '3928': '2016-06-27',
    '3858': '2016-06-27'
}

CUBE_APP = {
    '758': 'retention_ts_var',
    '3606': 'retention_fd_am_var',
    '3426': 'retention_fd_an_var',
    '3444': 'retention_fd_var',
    '3790': 'retention_gs_an_var',
    '3791': 'retention_gs_am_var',
    '3858': 'retention_gs_fb_var',
    '3789': 'retention_gs_var',
    '3928': 'retention_zm_var'
}


# APP_LIST = ['3791']
APP_LIST = [
    '3790', '3791', '3858', '3928', '3789', '3606', '3444', '3426', '758']


def execute(sql, filename=None, headers=[]):
    if not filename:
        attempt = 0
        complete = False
        while attempt < NUMBER_ATTEMPT and not complete:
            try:
                conn = connect(
                    host='172.16.5.22', protocol='beeswax', port=21000)
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    result = cursor.fetchall()
            except (socket.timeout, impala.error.DisconnectedError,
                    impala.error.OperationalError, impala.error.RPCError,
                    thrift.transport.TTransport.TTransportException):
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
                payers.payer
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
    """ % {
        'app': app,
        'next_date': next_date,
        'seg_date': seg_date,
        'min_day': MIN_DAY[app],
        'device_region': device_region % {
            'app': app,
            'seg_date': seg_date,
            }
    }
    return sql


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
            next_date = get_last_day(
                os.path.join(RESULT_PATH, 'retention_%s.txt' % app))
            if not next_date:
                next_date = MIN_DAY[app]
            logger.info('Next_date = |%s|' % seg_date)

            date_format = '%Y-%m-%d'
            start = datetime.strptime(next_date, date_format) + timedelta(days=1)
            end = datetime.strptime(seg_date, date_format)# - timedelta(days=2)
            i = 0
            logger.info('start: %s||end: %s' % (start, end))
            while start <= end:
                logger.info('app-%s |%s| %s - %s' % (app, (end-start).days, start, end))
                next_date = datetime.strftime(start, date_format)
                sql = get_select_sql(app, next_date, seg_date)
                # logger.info(sql)
                res = execute(sql)
                file_name = 'retention_%s_%s' % (
                        app, datetime.strftime(start, '%Y_%m_%d')
                    )
                path_file = os.path.join(RESULT_PATH, file_name + '.csv')
                with open(path_file, 'w') as f:
                    f.write('coh_day;n;device_region;payer;sum_active;count_active\n')
                    for row in res:
                        f.write('%s\n' % ';'.join(row))
                relocation_file(CUBE_APP[app], file_name,
                                check_exist=True,
                                from_path=path_file)
                push_last_day(
                    os.path.join(RESULT_PATH, 'retention_%s.txt' % app),
                              datetime.strftime(start, date_format))
                start += timedelta(days=1)
                i += 1
                # start = end + timedelta(days=1)
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
        make_csv()
        delete_lock(LOCK_FILE)
        logger.info('END!')
    # make_text('t0.csv', 't0.txt')
