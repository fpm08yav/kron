#! coding=utf-8
import socket
import logging
import logging.config
import sys
import os
import time
import impala
import psycopg2
import calendar

from datetime import datetime, timedelta
from impala.dbapi import connect
from config import *

from common_functions import (
    try_lock, delete_lock, execute, fetchall, relocation_file,
    get_full_list_hour, StreamToLogger, #get_difference_days_list,
    get_difference_month_list, put_headers)


LOGGIN_FILE = 'ad.conf'
LOCK_FILE = 'ad.lock'
RESULT_PATH = os.path.join(TEMP_PATH, 'ad')


MIN_DAY = {
    '758':  '2015-12-23',
    '1170': '2016-07-01',
    '3444': '2016-01-01',
    '3789': '2016-06-04',
    '3790': '2016-07-07',
    '3426': '2016-02-04',
    '3928': '2016-06-27',
    '3858': '2016-06-27'
}

CUBE_APP = {
    # '3606': 'retention_fd_am_dev_id',
    # '3426': 'cube_ad_fd_an',
    # '3444': 'cube_ad_fd',
    # '758': 'cube_ad_ts',
    '1170': 'cube_ad_ts_an',
    # '3790': 'retention_gs_an_var',
    # '3858': 'retention_gs_fb_var',
    # '3789': 'retention_gs_var',
    # '758':  'retention_ts_var',
    # '3928': 'retention_zm_var'
}

# Drilldown on cnt
VIDEO_AD_CNT_LIST = ['1', '3', '10', '30']


# APP_LIST = ['3444', '3426']

date_format = '%Y-%m-%d'


def fetchall_post(sql):
    conn = psycopg2.connect(
                            # host='localhost',
                            host='analyticspg',
                            port=5432,
                            user='postgres',
                            password='postgres',
                            database='cubes')
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def execute_post(sql):
    conn = psycopg2.connect(
                            # host='localhost',
                            host='analyticspg',
                            port=5432,
                            user='postgres',
                            password='postgres',
                            database='cubes')
    with conn.cursor() as cursor:
        cursor.execute(sql)
        conn.commit()


# %% SQL
def get_select_ad_sql(start_day, last_day, app):
    return """
        WITH ad AS (
            SELECT
                ad_show,
                count(1) as cnt
            from
            (
            select
                event_user,
                sum(count_ad) as ad_show
            from
                video_ad_%(app)s_pq
                where hour >= %(start_day)s
                and hour < %(last_day)s
                group by event_user
            ) s
            group by ad_show
        ),
        list as ((%(ad_cnt)s)),
        mau as(
            select
                count(1) as cnt
            from
                seg_players_%(app)s_pq
            where
                hour = %(last_day)s
                and last_active div 1000 >= %(start_day)s
                and last_active div 1000 < %(last_day)s
        ),
        b_show as (
            select
                count(distinct event_user) as cnt
            from
                all_events_%(app)s_pq
            where
                hour >= %(start_day)s
                and hour < %(last_day)s
                and event_type = 'event'
                AND parameters LIKE '%%VideoAd.ButtonShow%%'
        )

        SELECT
            to_date(from_unixtime(%(last_day)s)) as day,
            list.id as vide_cnt,
            nvl(sum(if(ad_show >= list.id, ad.cnt, 0)), 0) as cnt,
            mau.cnt,
            b_show.cnt
        from
            list,
            ad,
            mau,
            b_show
        group by
            mau.cnt, list.id, b_show.cnt
        having
            cnt > 0
    """ % {
        'start_day': start_day,
        'last_day': last_day,
        'app': app,
        'ad_cnt': get_ad_cnt()}


def get_select_ad_ts_sql(start_day, last_day, app):
    return """
        WITH ad AS (
            SELECT
                ad_show,
                count(1) as cnt
            from
            (
            select
                event_user,
                sum(count_ad) as ad_show
            from
                video_ad_%(app)s_pq
                where hour >= %(start_day)s
                and hour < %(last_day)s
                group by event_user
            ) s
            group by ad_show
        ),
        list as ((%(ad_cnt)s)),
        mau as(
            select
                count(1) as cnt
            from
                seg_players_%(app)s_pq
            where
                hour = %(last_day)s
                and last_active div 1000 >= %(start_day)s
                and last_active div 1000 < %(last_day)s
        ),
        b_show as (
            select
                count(distinct event_user) as cnt
            from
                all_events_%(app)s_pq
            where
                hour >= %(start_day)s
                and hour < %(last_day)s
                and event_type = 'event'
                AND (parameters LIKE '%%VideoAd.VideoAvailable%%' OR parameters LIKE '%%VideoAd.VideoNotAvailable%%')
        )

        SELECT
            to_date(from_unixtime(%(last_day)s)) as day,
            list.id as vide_cnt,
            nvl(sum(if(ad_show >= list.id, ad.cnt, 0)), 0) as cnt,
            mau.cnt,
            b_show.cnt
        from
            list,
            ad,
            mau,
            b_show
        group by
            mau.cnt, list.id, b_show.cnt
        having
            cnt > 0
    """ % {
        'start_day': start_day,
        'last_day': last_day,
        'app': app,
        'ad_cnt': get_ad_cnt()}


def get_select_ad_ts_an_sql(start_day, last_day, app):
    return """
        WITH ad AS (
            SELECT
                ad_show,
                count(1) as cnt
            from
            (
            select
                event_user,
                sum(count_ad) as ad_show
            from
                video_ad_%(app)s_pq
                where hour >= %(start_day)s
                and hour < %(last_day)s
                group by event_user
            ) s
            group by ad_show
        ),
        list as ((%(ad_cnt)s)),
        mau as(
            select
                count(distinct event_user) as cnt
            from
                all_events_%(app)s_pq
            where
                hour >= %(start_day)s
                and hour < %(last_day)s
        ),
        b_show as (
            select
                count(distinct event_user) as cnt
            from
                all_events_%(app)s_pq
            where
                hour >= %(start_day)s
                and hour < %(last_day)s
                and event_type = 'event'
                AND (parameters LIKE '%%VideoAd.VideoAvailable%%' OR parameters LIKE '%%VideoAd.VideoNotAvailable%%')
        )

        SELECT
            to_date(from_unixtime(%(last_day)s)) as day,
            list.id as vide_cnt,
            nvl(sum(if(ad_show >= list.id, ad.cnt, 0)), 0) as cnt,
            mau.cnt,
            b_show.cnt
        from
            list,
            ad,
            mau,
            b_show
        group by
            mau.cnt, list.id, b_show.cnt
        having
            cnt > 0
    """ % {
        'start_day': start_day,
        'last_day': last_day,
        'app': app,
        'ad_cnt': get_ad_cnt()}


def get_select_last_day_ad_sql(app):
    return """
        SELECT
            max(cube_date)
        from
            %(table)s
    """ % {'table': CUBE_APP[app]}


def get_create_table_sql_p(table):
    return """
        CREATE TABLE IF NOT EXISTS %(table)s
        (
            month VARCHAR(255),
            cube_date date NOT NULL,
            count_ad int,
            count_user bigint,
            mau int,
            b_show int
        )
        TABLESPACE pg_default;
        GRANT SELECT ON TABLE %(table)s TO cubes;
    """ % {'table': table}


def get_create_table_sql_i(app):
    return """
        CREATE TABLE IF NOT EXISTS video_ad_%(app)s_pq
        (
            event_user VARCHAR(255),
            count_ad BIGINT
        )
        partitioned by (hour INT)
        stored as parquet;
    """ % {'app': app}


def get_update_sql_i(app, first_hour, second_hour):
    return """
    INSERT into
        video_ad_%(app)s_pq (
            event_user, count_ad
        )
        partition (hour=%(first_hour)s)

        SELECT
            event_user,
            count(1) as count_ad
        FROM
            all_events_%(app)s_pq
        WHERE
            hour >= %(first_hour)s
            AND hour < %(second_hour)s
            AND event_type = 'event'
            AND parameters LIKE '%%VideoAd.Show%%'
        GROUP BY
            event_user
    """ % {'app': app, 'first_hour': first_hour, 'second_hour': second_hour}


def get_drop_partition_i(app, first_hour):
    return """
        ALTER TABLE
            video_ad_%(app)s_pq
        DROP IF EXISTS PARTITION (hour = %(first_hour)s)

    """ % {'app': app, 'first_hour': first_hour}


# %%

def get_ad_cnt():
    return 'values (%(id)s as id)%(other)s' % {
        'id': VIDEO_AD_CNT_LIST[0],
        'other': ', (%s)' % '),('.join(VIDEO_AD_CNT_LIST[1:])
    }


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


def execute_file(sql, filename, first_row=None):
    conn = connect(host='172.16.5.22', protocol='beeswax', port=21000)
    with conn.cursor() as cursor:
        cursor.execute(sql)
        put_headers(filename, ['month', 'cube_date', 'count_ad', 'count_user', 'mau'])
        # open(filename, 'w').close()
        block_size = 1000000
        block = cursor.fetchmany(size=block_size)
        with open(filename, 'a') as f:
            while block:
                for row in block:
                    f.write('%s\n' % (u'%s;%s' % (first_row, ';'.join(row))))
                block = cursor.fetchmany(size=block_size)


def create_csv(app, start_day, last_day, parent_path):
    start_day_str = datetime.utcfromtimestamp(start_day)
    last_day_str = datetime.utcfromtimestamp(last_day)
    csv_name = os.path.join(
        parent_path, '%s.csv' % last_day_str.strftime('%Y-%m-%d'))
    logging.warning('start: %s||end: %s' % (
        start_day_str.strftime('%Y-%m-%d'), last_day_str.strftime('%Y-%m-%d'))
    )
    if app == '758':
        sql = get_select_ad_ts_sql(start_day, last_day, app)
    elif app == '1170':
        sql = get_select_ad_ts_an_sql(start_day, last_day, app)
    else:
        sql = get_select_ad_sql(start_day, last_day, app)
    # logging.warning(sql)
    month_year = start_day_str.strftime('%m.%Y')
    execute_file(sql, csv_name, first_row=month_year)
    return last_day_str.strftime('%Y-%m-%d')


def update_table_in_postgres(app):
    logging.warning('Start update %s' % CUBE_APP[app])
    # узнаем у Pstgresql последний загруженный день (начало месяца)
    execute_post(get_create_table_sql_p(CUBE_APP[app]))
    last_day_p = fetchall_post(get_select_last_day_ad_sql(app))
    # if last_day_p[0] and last_day_p[0][0]:
    #     first_day = datetime.combine(last_day_p[0][0], datetime.min.time())
    # else:
    first_day = datetime.strptime(MIN_DAY[app], '%Y-%m-%d')
    # узнаем у импалы последний день
    last_day = datetime.utcfromtimestamp(
        get_full_list_hour('video_ad_%s_pq' % app)[-1])
    # получаем список месяцев лежащих в этом интервапе
    month_list = get_difference_month_list(first_day, last_day)

    if len(month_list) > 1:
        path_file = os.path.join(TEMP_PATH, CUBE_APP[app])

        for i in range(len(month_list) - 1):
            start_day = month_list[i]
            end_day = month_list[i + 1]
            file_name = create_csv(app, start_day, end_day, path_file)
            logging.warning(file_name)
            relocation_file(CUBE_APP[app], file_name)
    else:
        logging.warning('Update %s is not required' % CUBE_APP[app])
    logging.warning('End update %s' % CUBE_APP[app])


def update_table_in_impala(app):
    logging.warning('Start update video_ad_%s_pq' % app)

    execute(get_create_table_sql_i(app))
    if app == '1170':
        difference_days = get_difference_days_list(
            'video_ad_%s_pq' % app,
            'seg_players_758_pq',
            gte=MIN_DAY[app],
            # lte='2016-10-01'
        )
    else:
        difference_days = get_difference_days_list(
            'video_ad_%s_pq' % app,
            'seg_players_%s_pq' % app,
            # gte='2015-01-01',
            # lte='2016-10-01'
        )

    for hour1, hour2 in difference_days:
        day1 = datetime.fromtimestamp(hour1) - timedelta(hours=3)
        day2 = datetime.fromtimestamp(hour2) - timedelta(hours=3)
        logging.warning((day1.strftime('%Y-%m-%d'), day2.strftime('%Y-%m-%d')))
        execute(get_drop_partition_i(app, hour1))
        execute(get_update_sql_i(app, hour1, hour2))

    logging.warning('End update video_ad_%s_pq' % app)


def main():
    if not try_lock(LOCK_FILE):
        logging.warning('\nSTART')
        for app in CUBE_APP.keys():
            logging.warning('Start ad for %s' % app)

            # update_table_in_impala(app)
            update_table_in_postgres(app)

            logging.warning('End ad for %s' % app)
    logging.warning('End')
    delete_lock(LOCK_FILE)


if __name__ == '__main__':
    logging.config.fileConfig(os.path.join(LOG_CONF_PATH, LOGGIN_FILE))
    logging = logging.getLogger()
    sl = StreamToLogger(logging)
    sys.stderr = sl

    main()
