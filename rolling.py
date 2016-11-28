# -*- coding: utf-8 -*-
import pandas as pd
import os
import sys
import csv
import calendar
import impala
import thrift
import socket
import itertools
import logging
import logging.config

from impala.dbapi import connect
from datetime import datetime
from common_functions import (try_lock, delete_lock, get_last_day,
                              push_last_day, StreamToLogger,
                              get_full_list_hour,
                              execute, fetchall, get_difference_days_list,
                              relocation_file)

LOCK_FILE = 'retention.lock'
LOGGING_PATH = '/home/yansen-a/analytics/logs'
RESULT_PATH = '/home/yansen-a/analytics/local_cubes/temp/retention_rol/'

CUBE_APP = {
    '3606': 'retention_fd_am_rol',
    '3426': 'retention_fd_an_rol',
    '3444': 'retention_fd_rol',
    '3790': 'retention_gs_an_rol',
    '3858': 'retention_gs_fb_rol',
    '3789': 'retention_gs_rol',
    '758':  'retention_ts_rol',
    '3928': 'retention_zm_rol'
}


def execute_sql(sql):
    con = connect(host='172.16.5.22', protocol='beeswax', port=21000)
    with con:
        cur = con.cursor()
        cur.execute(sql)
        return cur.fetchall()


def execute_sql_with_exept(sql):
    i = 0
    can_fail = 5
    complete_status = False
    while i <= can_fail and not complete_status:
        try:
            res = execute_sql(sql)
            complete_status = True
            i = 0
        except (socket.timeout, impala.error.DisconnectedError,
                impala.error.OperationalError, impala.error.RPCError,
                thrift.transport.TTransport.TTransportException) as msg:
            i += 1
            print 'problem with impala, wait 10 minutes:', msg
            time.sleep(10*60)
            res = [[], []]
    return res


def get_select_rolling_sql():
    return """
        SELECT
            to_date(from_unixtime(p.first_session div 1000)) AS first_session,
            --u.device_region,
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
            decode(p.payer, 1, 'Payers', 'Non payers') as payer,
            --p.payer,
            ((p.last_active - p.first_session) div 1000) div (24*60*60) AS days,
            COUNT(1) AS cnt
        FROM
            seg_players_%(app)s_pq p
            left join
                (
                    %(device_region)s
                ) AS su
            on
                p.event_user = su.event_user
        WHERE
            --p.event_user = u.event_user
            p.hour = %(seg_hour)s
            --AND u.hour = %(seg_hour)s
            AND p.first_session IS NOT NULL
            AND p.last_active IS NOT NULL
            /*AND u.device_region =  (
                                    SELECT
                                        MAX(u1.device_region)
                                    FROM
                                        seg_users_%(app)s_pq u1
                                    WHERE
                                        u1.event_user = u.event_user
                                        AND u1.hour = %(seg_hour)s
                                    )*/
        GROUP BY
            to_date(from_unixtime(p.first_session div 1000)),
            ((p.last_active - p.first_session) div 1000) div (24*60*60),
            su.device_region,
            p.payer
        ORDER BY
            to_date(from_unixtime(p.first_session div 1000))
    """

    # return """
    #     SELECT
    #         to_date(from_unixtime(p.first_session div 1000)) AS first_session,
    #         --u.device_region,
    #         (CASE
    #             WHEN
    #                 u.device_region IS NULL
    #             THEN
    #                 "--"
    #             WHEN
    #                 LENGTH(u.device_region) != 2
    #             THEN
    #                 "--"
    #             ELSE
    #                 u.device_region
    #         END) as device_region,
    #         decode(p.payer, 1, 'Payers', 'Non payers') as payer,
    #         --p.payer,
    #         ((p.last_active - p.first_session) div 1000) div (24*60*60) AS days,
    #         COUNT(1) AS cnt
    #     FROM
    #         seg_players_%(app)s_pq p,
    #         seg_users_%(app)s_pq u
    #     WHERE
    #         p.event_user = u.event_user
    #         AND p.hour = %(seg_hour)s
    #         AND u.hour = %(seg_hour)s
    #         AND p.first_session IS NOT NULL
    #         AND p.last_active IS NOT NULL
    #         AND u.device_region =  (
    #                                 SELECT
    #                                     MAX(u1.device_region)
    #                                 FROM
    #                                     seg_users_%(app)s_pq u1
    #                                 WHERE
    #                                     u1.event_user = u.event_user
    #                                     AND u1.hour = %(seg_hour)s
    #                                 )
    #     GROUP BY
    #         to_date(from_unixtime(p.first_session div 1000)),
    #         ((p.last_active - p.first_session) div 1000) div (24*60*60),
    #         u.device_region,
    #         p.payer
    #     ORDER BY
    #         to_date(from_unixtime(p.first_session div 1000))
    # """


def main():
    for app in CUBE_APP.keys():
        logger.info('\n')
        logger.info('Start rolling retention for %s' % app)

        seg_hour_int = get_full_list_hour('seg_players_%s_pq' % app)[-1]
        if app in ['3606', '3790']:
            device_region = """
                SELECT
                    s.event_user as event_user,
                    UPPER(s2.device_region) as device_region
                FROM
                    (
                        SELECT
                            event_user,
                            MAX(device_region) AS device_region
                        FROM
                            seg_users_%(app)s_pq
                        WHERE
                            hour = %(seg_hour)s
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
                    UPPER(MAX(device_region)) AS device_region
                FROM
                    seg_users_%(app)s_pq
                WHERE
                    hour = %(seg_hour)s
                GROUP BY
                    event_user
            """
        sql = get_select_rolling_sql() % {
            'app': app,
            'seg_hour': seg_hour_int,
            'device_region': device_region % {
                'app': app,
                'seg_hour': seg_hour_int
            }
        }

        # seg_hour = datetime.strptime('2016-09-19', '%Y-%m-%d')
        seg_hour = datetime.utcfromtimestamp(seg_hour_int)

        # заполняем временный файл
        file_name_temp = './ROLLING.csv'
        with open(file_name_temp, 'w') as f:
            text = execute_sql(sql)
            for row in text:
                f.write(';'.join(row) + '\n')

        # на основе его заполняем словарь, где ключ это
        # first_session, device_region, payer, а значение
        # DayN и количество игроков вернувшихся в этот день
        d = {}
        with open(file_name_temp, 'r') as f:
            reader = csv.reader(f, delimiter=';')
            for row in reader:
                dict_key = tuple(row[:3])
                day_n = int(row[3])
                day_cnt = int(row[4])
                if dict_key in d:
                    d[dict_key] += [[day_n, day_cnt]]
                else:
                    d[dict_key] = [[day_n, day_cnt]]

        file_name = 'retention_%s' % (app)
        path_file = os.path.join(RESULT_PATH, file_name + '.csv')

        with open(path_file, 'w') as f:
            f.write('coh_day;device_region;payer;n;sum_active;count_active\n')
            for key, vals in d.items():
                max_n = (seg_hour - datetime.strptime(key[0], '%Y-%m-%d')).days

                days_list, cnt_list = zip(*vals)
                res = list(sum(map(lambda day, day_cnt, max_n: pd.Series([day_cnt]*(day + 1) + [0]*(max_n - day - 1)),
                               days_list, cnt_list, itertools.repeat(max_n, len(days_list)))))

                all_cnt = res[0]
                for idx, val in enumerate(res[1:]):
                    f.write(';'.join(key) + ';%s;%s;%s\n' % (idx + 1, val, all_cnt))
        relocation_file(CUBE_APP[app], file_name, recreate=True, from_path=path_file)
        logger.info('End rolling retention for %s' % app)

if __name__ == '__main__':
    loggin_file = 'log_conf.conf'
    logging.config.fileConfig(os.path.join(LOGGING_PATH, loggin_file))
    logger = logging.getLogger('retention')
    sl = StreamToLogger(logger, logging.ERROR)
    sys.stderr = sl
    logger.info('START!')
    main()
    delete_lock(LOCK_FILE)
    logger.info('END!')
