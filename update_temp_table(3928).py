#! coding=utf-8
import multiprocessing
import impala
import time
import logging
import logging.config
import sys
import os
import socket
import calendar

from impala.dbapi import connect
from datetime import datetime, timedelta
from common_functions import (StreamToLogger, try_lock, delete_lock,
                              get_last_day, push_last_day,
                              get_last_days_seg_tables,
                              execute, fetchall, execute_file,
                              create_levels, levels_table, create_chains,
                              chains_table, create_values, ids_table,
                              get_ab_values, get_max_level,
                              get_difference_days_list, get_list_hour,
                              get_drop_partition_sql, put_headers,
                              create_ab_table, update_ab_table,
                              get_full_list_hour, relocation_file)

LOGGING_PATH = '/home/yansen-a/analytics/logs'
LOCK_FILE = 'update_temp_table.lock'
BEGIN_HOUR = 1452729600     # unix_timestamp('2016-01-14 00:00:00')
NUM_WORKERS = 3
TABLE_WIN_FAIL = 'win_fail_3928_pq_test_s'
TABLE_BOOSTS = 'boosts_3928_pq_test_s'


PARENT_PATH = '/home/yansen-a/analytics/local_cubes'

BACKUP_PATH = os.path.join(PARENT_PATH, 'backup')
IMPORT_CSV_PATH = os.path.join(PARENT_PATH, 'import_csv')
LOCK_PATH = os.path.join(PARENT_PATH, 'lock')
LOG_CONF_PATH = os.path.join(PARENT_PATH, 'log_conf')
LOGS_PATH = os.path.join(PARENT_PATH, 'logs')
TEMP_PATH = os.path.join(PARENT_PATH, 'temp')

LOGGIN_FILE = 'update_temp_table.conf'


NUMBER_ATTEMPT = 10
TIME_SLEEP = 10 * 60
STEP = 2


def get_insert_boosts_sql(hour, app):
    # ab_table = ids_table(get_ab_values(app, hour))
    next_day_date = (
        datetime.fromtimestamp(hour) - timedelta(hours=3)
    ).replace(hour=0) + timedelta(days=1)
    next_day = int(calendar.timegm(next_day_date.timetuple()))
    sql = """
        INSERT OVERWRITE %(table)s (
            app_version, level_version, level, ab_group, device_region, payer,
            chain, boost, boost_count
        )
        partition (hour=%(hour)s)

        WITH ab AS (
            SELECT DISTINCT
                p.event_user,
                g.ab_names
            FROM
                all_events_3928_pq p,
                ab_group_3928_pq g
            WHERE
                p.hour = %(hour)s
                AND p.event_type = 'event'
                AND p.payload LIKE '%%ABGroups%%'
                AND g.hour = %(next_day)s
                AND decode(
                    regexp_extract(payload, 'ABGroups\\":\\"([^\\"]*)', 1) ,
                    '',
                    '--',
                    regexp_extract(payload, 'ABGroups\\":\\"([^\\"]*)', 1)
                ) rlike concat('(^|,)', g.ab_names, '($|,)')
        ),
        payload as
        (
            select
                p.event_id,
                p.event_user,
                nvl(cast(regexp_extract(
                    payload,
                    'LevelChain\\":\\"(\\\\d+)', 1) as int), 0) as chain,
                cast(regexp_extract(
                    payload,
                    'level\\":\\"(\\\\d+)', 1) as int) as level,
                nvl(cast(regexp_extract(
                    payload,
                    'LevelsVersion\\":\\"(\\\\d+)', 1) as int),
                    -1) as level_version,
                nvl(ab.ab_names, '--') as ab_group,
                payload
            from
                all_events_3928_pq p left join ab
            on
                ab.event_user = p.event_user
            where
                p.hour = %(hour)s
                and p.event_type = 'event'
                and (
                    p.parameters ilike '%%Level.Complete%%' or
                    p.parameters ilike '%%Level.Failed%%')
        ),
        countries as (
            SELECT
                event_user,
                MAX(device_region) AS device_region
            FROM
                seg_users_3928_pq
            WHERE
                hour = %(next_day)s
            GROUP BY
                event_user
        )

        select
            app_version,
            level_version,
            level_number,
            cast(ab_group as VARCHAR(255)),
            cast(device_region as VARCHAR(255)),
            payer,
            chain_number,
            cast(boost as VARCHAR(255)),
            boost_count
        from
            (
                select
                    from_unixtime(%(hour)s, 'yyyy') as year,
                    from_unixtime(%(hour)s, 'MM') as month,
                    from_unixtime(%(hour)s, 'dd') as day,
                    c.app_version as app_version,
                    p.level_version,
                    p.chain as chain_number,
                    p.level as level_number,
                    p.ab_group,
                    s.device_region as device_region,
                    c.payer as payer,
                    boosts.name as boost,
                    sum(nvl(cast(regexp_extract(
                        p.payload,
                        concat(boosts.name, '\\":\\"(\\\\d+)'),
                        1) as int), 0)) as boost_count
                from
                    payload p,
                    seg_players_3928_pq c,
                    (values
                        ('BoostsBombs' as name),
                        ('BoostsDynamiteAndLightning'),
                        ('BoostsShovel'),
                        ('BoostsLightning')/*,
                        ('BoostsHammer'),
                        ('BoostsReshuffle'),
                        ('SquidSaves'),
                        ('TimebombSaves')*/
                    ) as boosts
                    left join
                    countries AS s
                    ON c.event_user = s.event_user
                where
                    c.hour = %(next_day)s
                    and c.first_session is not Null
                    and c.level is not null
                    and c.event_user = p.event_user
                group by
                    year, month, day, app_version, level_version, chain_number,
                    level_number, ab_group, device_region, payer, boost
                union all
                (
                    select
                        from_unixtime(%(hour)s, 'yyyy') as year,
                        from_unixtime(%(hour)s, 'MM') as month,
                        from_unixtime(%(hour)s, 'dd') as day,
                        c.app_version as app_version,
                        p.level_version,
                        p.chain as chain_number,
                        p.level as level_number,
                        p.ab_group,
                        s.device_region as device_region,
                        c.payer as payer,
                        concat(
                            'AdditionalMoves', '_', boost_number.num) as boost,
                        sum(
                            if(
                                nvl(cast(regexp_extract(
                                    p.payload,
                                    'AdditionalMoves\\":\\"(\\\\d+)',
                                    1) as int),
                                0) >= cast(boost_number.num as int),
                            1,0)) as boost_count
                    from
                        payload p,
                        (%(boost_number)s) boost_number,
                        seg_players_3928_pq c
                        left join
                        countries AS s
                        ON c.event_user = s.event_user
                    where
                        c.hour = %(next_day)s
                        and c.first_session is not Null
                        and c.level is not null
                        and c.event_user = p.event_user
                    group by
                        year, month, day, app_version, level_version,
                        chain_number, level_number, ab_group, device_region, payer, boost
                )
                union all
                (
                    select
                        from_unixtime(%(hour)s, 'yyyy') as year,
                        from_unixtime(%(hour)s, 'MM') as month,
                        from_unixtime(%(hour)s, 'dd') as day,
                        c.app_version as app_version,
                        p.level_version,
                        p.chain as chain_number,
                        p.level as level_number,
                        p.ab_group,
                        s.device_region as device_region,
                        c.payer as payer,
                        concat(
                            'AdditionalTime', '_', boost_number.num) as boost,
                        sum(
                            if(
                                nvl(cast(regexp_extract(
                                    p.payload,
                                    'AdditionalTime\\":\\"(\\\\d+)',
                                    1) as int),
                                0) >= cast(boost_number.num as int),
                            1, 0)) as boost_count
                    from
                        payload p,
                        (%(boost_number)s) boost_number,
                        seg_players_3928_pq c
                        left join
                        countries AS s
                        ON c.event_user = s.event_user
                    where
                        c.hour = %(next_day)s
                        and c.first_session is not Null
                        and c.level is not null
                        and c.event_user = p.event_user
                    group by
                        year, month, day, app_version, level_version,
                        chain_number, level_number, ab_group, device_region,
                        payer, boost
                )
            ) s
    """ % {
        'table': TABLE_BOOSTS,
        'hour': hour,
        'additional': 'values(("AdditionalMoves" as name), ("AdditionalTime"))',
        'boost_number': 'values(("1" as num), ("2"), ("3"), ("4"), ("5"), ("6"), ("7"))',
        'next_day': next_day
    }
    return sql


def get_insert_win_fail_sql(hour, app):
    # ab_table = ids_table(get_ab_values(app, hour))
    next_day_date = (
        datetime.fromtimestamp(hour) - timedelta(hours=3)
    ).replace(hour=0) + timedelta(days=1)
    next_day = int(calendar.timegm(next_day_date.timetuple()))
    sql = """
        INSERT OVERWRITE %(table)s (
            app_version, level_version, ab_group, device_region, level,
            chain, payer, count_user, win, fail, free_cash, real_cash,
            win_without_boosts, value, variance
        )
        partition (hour=%(hour)s)

        WITH ab AS (
            SELECT DISTINCT
                p.event_user,
                g.ab_names
            FROM
                all_events_3928_pq p,
                ab_group_3928_pq g
            WHERE
                p.hour = %(hour)s
                AND p.event_type = 'event'
                AND p.payload LIKE '%%ABGroups%%'
                AND g.hour = %(next_day)s
                AND decode(
                    regexp_extract(payload, 'ABGroups\\":\\"([^\\"]*)', 1) ,
                    '',
                    '--',
                    regexp_extract(payload, 'ABGroups\\":\\"([^\\"]*)', 1)
                ) rlike concat('(^|,)', g.ab_names, '($|,)')
        ),

        payload as
        (
            select
                p.event_id,
                p.event_user,
                p.chain,
                p.level,
                p.level_version,
                p.ab_group,
                p.event_name,
                p.reason,
                sum(nvl(p.free_cash, 0)) as free_cash,
                sum(nvl(p.real_cash, 0)) as real_cash,
                (
                    sum(nvl(p.boost_count, 0)) +
                    sum(nvl(p.at_count, 0)) +
                    sum(nvl(p.am_count, 0))
                ) as all_boosts
            from
                (
                    select
                        p.event_id,
                        p.event_user,
                        decode(
                            instr(parameters, 'Level.Failed'), 0,
                            decode(instr(parameters, 'Chain.Failed'), 0, 1, 2),
                            2) as event_name,
                        nvl(cast(regexp_extract(
                            payload,
                            'LevelChain\\":\\"(\\\\d+)',
                            1) as int), 0) as chain,
                        cast(regexp_extract(
                            payload,
                            'level\\":\\"(\\\\d+)', 1) as int) as level,
                        cast(regexp_extract(
                            payload,
                            'FreeCashBalance\\":(\\\\d+)',
                            1) as int) as free_cash,
                        cast(regexp_extract(
                            payload,
                            'RealCashBalance\\":(\\\\d+)',
                            1) as int) as real_cash,
                        cast(regexp_extract(
                            payload,
                            'Boosts\\":\\"(\\\\d+)', 1) as int) as boost_count,
                        cast(regexp_extract(
                            payload,
                            'AdditionalTimes\\":\\"(\\\\d+)',
                            1) as int) as at_count,
                        cast(regexp_extract(
                            payload,
                            'AdditionalMoves\\":\\"(\\\\d+)',
                            1) as int) as am_count,
                        nvl(cast(regexp_extract(
                            payload,
                            'LevelsVersion\\":\\"(\\\\d+)',
                            1) as int), -1) as level_version,
                        nvl(ab.ab_names, '--') as ab_group,
                        regexp_extract(
                        payload, 'Reason\\":\\"(\\\\w+)', 1) as reason
                    from
                        all_events_3928_pq p left join ab
                    on
                        p.event_user = ab.event_user
                    where
                        p.hour = %(hour)s
                        and p.event_type = 'event'
                        and (
                            p.parameters ilike '%%Level.Complete%%' or
                            p.parameters ilike '%%Level.Failed%%')
                ) p
            group by p.event_id, p.event_user, p.chain, p.level,
                p.level_version, p.ab_group, p.event_name, p.reason
        )
        select
            app_version, level_version, ab_group,
            device_region, level_number,
            chain_number, payer,
            cast(sum(count_user) as INT)  as count_user,
            sum(win_count) as win_count,
            sum(fail_count) as fail_count,
            /*win_count, fail_count,*/
            sum(free_cash) as free_cash,
            sum(real_cash) as real_cash,
            sum(win_without_boosts) as win_without_boosts,
            round(sum(df), 2) as value,
            round(variance(df), 2) as variance
        from(
            select
                from_unixtime(%(hour)s, 'yyyy') as year,
                from_unixtime(%(hour)s, 'MM') as month,
                from_unixtime(%(hour)s, 'dd') as day,
                c.app_version as app_version,
                p.level_version,
                p.chain as chain_number,
                p.level as level_number,
                cast(p.ab_group as VARCHAR(255)) as ab_group,
                cast(s.device_region as VARCHAR(255)) as device_region,
                c.payer as payer,
                p.event_user as event_user,
                count(distinct p.event_user) as count_user,
                sum(decode(p.event_name, 1, 1, 0)) as win_count,
                /*sum(decode(p.event_name, 2, decode(p.reason, 'Fail', 1, 0), 0)) as fail_count,*/
                sum(decode(p.event_name, 2, 1, 0)) as fail_count,
                sum(nvl(p.free_cash, 0)) * 100 as free_cash,
                sum(nvl(p.real_cash, 0)) * 100 as real_cash,
                sum(decode(p.event_name, 1, decode(p.all_boosts, 0, 1, 0), 0)) as win_without_boosts,
                zeroifnull(sum(decode(p.event_name, 2, decode(p.reason, 'Fail', 1, 0), 0))/(
                    nullif(sum(decode(p.event_name, 1, 1, 0)) + sum(decode(p.event_name, 2, decode(p.reason, 'Fail', 1, 0), 0)), 0)
                )) df
            from
                payload p,
                seg_players_3928_pq c
                left join
                (
                    SELECT
                        event_user,
                        MAX(device_region) AS device_region
                    FROM
                        seg_users_3928_pq
                    WHERE
                        hour = %(next_day)s
                    GROUP BY
                        event_user
                ) AS s
                ON c.event_user = s.event_user
            where
                c.hour = %(next_day)s
                and c.last_active is not Null
                /*and c.cash_balance < 5000*/
                and c.first_session is not Null
                and c.level is not null
                and c.event_user = p.event_user
                and p.reason not in ('ExitButton', 'BySystem')
            group by
                year, month, day, c.app_version, level_version,
                chain_number, level_number, ab_group, s.device_region,
                c.payer, p.event_user
            /*order by
                chain_number, level_number, ab_group, app_version,
                level_version, device_region, payer, event_user*/
        ) s
        group by
            day, app_version, level_version, chain_number, level_number,
            ab_group, device_region, payer
        /*order by
            chain_number, level_number, ab_group, app_version, level_version,
            device_region, payer*/

    """ % {
        'table': TABLE_WIN_FAIL,
        'hour': hour,
        'next_day': next_day
    }
    return sql


def get_cube_create_sql(first_hour, last_hour, max_level, max_chain):
    sql = """
        SELECT
            day, app_version, level_version, device_region,
            level_number, chain_number, payer, win_count, fail_count,
            count_user, boost, boost_count, passed,
            win_without_boosts_count, decode(ab_group, '', '--', ab_group),
            total_monetization, value, variance
        from
        (
            select
                to_date(from_unixtime(%(second_hour)s)) as day,
                w.app_version as app_version,
                w.level_version as level_version,
                w.ab_group,
                (CASE
                    WHEN
                        w.device_region IS NULL
                    THEN
                        "--"
                    WHEN
                        LENGTH(w.device_region) != 2
                    THEN
                        "--"
                    ELSE
                        w.device_region
                END) as device_region,
                w.level as level_number,
                w.chain as chain_number,
                decode(w.payer, 1, 'Payers', 0, 'Non payers') as payer,
                sum(w.win) as win_count,
                sum(w.fail) as fail_count,
                sum(w.count_user) as count_user,
                null as boost,
                null as boost_count,
                p.prev_win as passed,
                sum(w.free_cash) as free_cash,
                sum(w.real_cash) as real_cash,
                sum(w.win_without_boosts) as win_without_boosts_count,
                null as total_monetization,
                round(sum(w.value), 2) as value,
                round(sum(w.variance), 2) as variance
            from
                (%(levels)s) levels,
                (%(chains)s) chains,
                (
                    select
                        s.app_version,
                        s.level_version,
                        s.ab_group,
                        s.chain,
                        s.level,
                        s.win,
                        lag(s.win, 1) over (
                            partition by s.app_version, s.level_version, s.chain
                            order by s.year, s.month, s.day,
                                s.app_version, s.level_version, s.ab_group,
                                s.chain, s.level
                        ) as prev_win
                    from
                    (
                        select
                            from_unixtime(%(second_hour)s, 'yyyy') as year,
                            from_unixtime(%(second_hour)s, 'MM') as month,
                            from_unixtime(%(second_hour)s, 'dd') as day,
                            app_version as app_version,
                            level_version,
                            ab_group,
                            chain,
                            level,
                            sum(win) as win
                        from
                            %(table_win_fail)s
                        where
                            hour >= %(second_hour)s
                            and hour < %(first_hour)s
                        group by year, month, day, app_version, level_version, ab_group, chain, level
                     ) s
                ) p,
                %(table_win_fail)s w
            where
                w.hour >= %(second_hour)s
                and w.hour < %(first_hour)s
                and w.level = cast(levels.id as int)
                and w.chain = cast(chains.id as int)

                and w.level = p.level
                and w.chain = p.chain
                and w.level_version = p.level_version
                and w.app_version = p.app_version
                and w.ab_group = p.ab_group
            group by
                day, app_version, w.level_version, w.ab_group, device_region,
                w.chain, w.level, payer, p.prev_win

            /*union
                all
            select
                to_date(from_unixtime(%(second_hour)s)) as day,
                b.app_version as app_version,
                b.level_version as level_version,
                b.ab_group,
                (CASE
                    WHEN
                        b.device_region IS NULL
                    THEN
                        "--"
                    WHEN
                        LENGTH(b.device_region) != 2
                    THEN
                        "--"
                    ELSE
                        b.device_region
                END) as device_region,
                b.level as level_number,
                b.chain as chain_number,
                decode(b.payer, 1, 'Payers', 0, 'Non payers') as payer,
                null as win_count,
                null as fail_count,
                null as count_user,
                boost as boost,
                sum(boost_count) as boost_count,
                w.prev_win as passed,
                0 as free_cash,
                0 as real_cash,
                null as win_without_boosts_count,
                case boost
                    when 'BoostsBombs' then sum(boost_count) * 1300
                    when 'BoostsDynamiteAndLightning' then sum(boost_count) * 1900
                    when 'BoostsShovel' then sum(boost_count) * 1900
                    when 'BoostsLightning' then sum(boost_count) * 900
                    when 'SquidSaves' then sum(boost_count) * 18
                    when 'TimebombSaves' then sum(boost_count) * 18
                    when 'BoostsReshuffle' then sum(boost_count) * 19
                    when 'BoostsHammer' then sum(boost_count) * 19
                    when 'AdditionalTime_7' then sum(boost_count) * 3800
                    when 'AdditionalTime_6' then sum(boost_count) * 3800
                    when 'AdditionalTime_5' then sum(boost_count) * 3300
                    when 'AdditionalTime_4' then sum(boost_count) * 2800
                    when 'AdditionalTime_3' then sum(boost_count) * 2000
                    when 'AdditionalTime_2' then sum(boost_count) * 1200
                    when 'AdditionalTime_1' then sum(boost_count) * 900
                    when 'AdditionalMoves_7' then sum(boost_count) * 3800
                    when 'AdditionalMoves_6' then sum(boost_count) * 3800
                    when 'AdditionalMoves_5' then sum(boost_count) * 3300
                    when 'AdditionalMoves_4' then sum(boost_count) * 2800
                    when 'AdditionalMoves_3' then sum(boost_count) * 2000
                    when 'AdditionalMoves_2' then sum(boost_count) * 1200
                    when 'AdditionalMoves_1' then sum(boost_count) * 900
                end as total_monetization,
                null as value,
                null as variance
            from
                %(table_boosts)s b,
                (%(levels)s) levels,
                (%(chains)s) chains,
                (
                    select
                        s.app_version,
                        s.level_version,
                        s.ab_group,
                        s.device_region,
                        s.payer,
                        s.chain,
                        s.level,
                        s.win,
                        lag(s.win, 1) over (
                            partition by s.app_version, s.level_version,
                                s.ab_group, s.device_region, s.payer, s.chain
                            order by s.year, s.month, s.day,
                                s.app_version, s.level_version, s.ab_group,
                                s.device_region, s.payer, s.chain, s.level
                        ) as prev_win
                    from
                    (
                        select
                            from_unixtime(%(second_hour)s, 'yyyy') as year,
                            from_unixtime(%(second_hour)s, 'MM') as month,
                            from_unixtime(%(second_hour)s, 'dd') as day,
                            w.app_version as app_version,
                            w.level_version,
                            w.ab_group,
                            w.device_region,
                            w.payer,
                            w.chain,
                            w.level,
                            sum(w.win) as win
                        from
                            %(table_win_fail)s w
                        where
                            w.hour >= %(second_hour)s
                            and w.hour < %(first_hour)s
                        group by
                            year, month, day, app_version, level_version, ab_group,
                            w.device_region, w.payer,
                            chain,
                            w.level
                     ) s
                ) w
            where
                b.hour >= %(second_hour)s
                and b.hour < %(first_hour)s
                and b.level = cast(levels.id as int)
                and b.chain = cast(chains.id as int)
                /*and b.boost_count > 0

                and b.level = w.level
                and b.chain = w.chain
                and b.level_version = w.level_version
                and b.app_version = w.app_version
                and b.ab_group = w.ab_group
                and b.device_region = w.device_region
                and b.payer = w.payer
            group by
                day, app_version, b.level_version, b.ab_group, device_region,
                b.chain, b.level, payer, boost,
                prev_win
            order by
                b.app_version, b.level, b.chain, boost*/
        ) s
        order by
            day, chain_number, level_number, app_version,
            level_version, device_region, payer, win_count
        """ % {'table_win_fail': TABLE_WIN_FAIL,
               'table_boosts': TABLE_BOOSTS,
               'levels': levels_table(max_level),
               'chains': chains_table(max_chain),
               # 'first_hour': last_hour,
               # 'second_hour': first_hour}
               'first_hour': first_hour,
               'second_hour': last_hour}
    return sql


def get_select_for_cube_difficult_sql(first_hour, last_hour, max_level, max_chain):
    sql = """
        SELECT
            day, app_version, level_version, device_region,
            level_number, chain_number, payer, win_count, fail_count,
            count_user, win_without_boosts_count,
            decode(ab_group, '', '--', ab_group), value,
            round((count_user * variance), 2) as n_variance
        from
        (
            select
                to_date(from_unixtime(%(second_hour)s)) as day,
                w.app_version as app_version,
                w.level_version as level_version,
                w.ab_group,
                (CASE
                    WHEN
                        w.device_region IS NULL
                    THEN
                        "--"
                    WHEN
                        LENGTH(w.device_region) != 2
                    THEN
                        "--"
                    ELSE
                        w.device_region
                END) as device_region,
                w.level as level_number,
                w.chain as chain_number,
                decode(w.payer, 1, 'Payers', 0, 'Non payers') as payer,
                sum(w.win) as win_count,
                sum(w.fail) as fail_count,
                sum(w.count_user) as count_user,
                sum(w.win_without_boosts) as win_without_boosts_count,
                round(sum(w.value), 2) as value,
                round(sum(w.variance), 2) as variance
            from
                (%(levels)s) levels,
                (%(chains)s) chains,
                %(table_win_fail)s w
            where
                w.hour >= %(second_hour)s
                and w.hour < %(first_hour)s
                and w.level = cast(levels.id as int)
                and w.chain = cast(chains.id as int)
            group by
                day, app_version, w.level_version, w.ab_group, device_region,
                w.chain, w.level, payer
        ) s
        order by
            day, chain_number, level_number, app_version,
            level_version, device_region, payer, win_count
        """ % {'table_win_fail': TABLE_WIN_FAIL,
               'levels': levels_table(max_level),
               'chains': chains_table(max_chain),
               'first_hour': first_hour,
               'second_hour': last_hour}
    return sql


def get_select_for_cube_monetization_sql(first_hour, last_hour, max_level, max_chain):
    sql = """
        select
            day, app_version, level_version, device_region,
            level_number, chain_number, payer, boost, passed,
            decode(ab_group, '', '--', ab_group), total_monetization
        from
        (
            select
                to_date(from_unixtime(%(second_hour)s)) as day,
                b.app_version as app_version,
                b.level_version as level_version,
                b.ab_group,
                (CASE
                    WHEN
                        b.device_region IS NULL
                    THEN
                        "--"
                    WHEN
                        LENGTH(b.device_region) != 2
                    THEN
                        "--"
                    ELSE
                        b.device_region
                END) as device_region,
                b.level as level_number,
                b.chain as chain_number,
                decode(b.payer, 1, 'Payers', 0, 'Non payers') as payer,
                null as win_count,
                null as fail_count,
                null as count_user,
                boost as boost,
                w.prev_win as passed,
                0 as free_cash,
                0 as real_cash,
                null as win_without_boosts_count,
                case boost
                    when 'BoostsBombs' then sum(boost_count) * 1300
                    when 'BoostsDynamiteAndLightning' then sum(boost_count) * 1900
                    when 'BoostsShovel' then sum(boost_count) * 1900
                    when 'BoostsLightning' then sum(boost_count) * 900
                    when 'AdditionalTime_7' then sum(boost_count) * 3800
                    when 'AdditionalTime_6' then sum(boost_count) * 3800
                    when 'AdditionalTime_5' then sum(boost_count) * 3300
                    when 'AdditionalTime_4' then sum(boost_count) * 2800
                    when 'AdditionalTime_3' then sum(boost_count) * 2000
                    when 'AdditionalTime_2' then sum(boost_count) * 1200
                    when 'AdditionalTime_1' then sum(boost_count) * 900
                    when 'AdditionalMoves_7' then sum(boost_count) * 3800
                    when 'AdditionalMoves_6' then sum(boost_count) * 3800
                    when 'AdditionalMoves_5' then sum(boost_count) * 3300
                    when 'AdditionalMoves_4' then sum(boost_count) * 2800
                    when 'AdditionalMoves_3' then sum(boost_count) * 2000
                    when 'AdditionalMoves_2' then sum(boost_count) * 1200
                    when 'AdditionalMoves_1' then sum(boost_count) * 900
                end as total_monetization
            from
                %(table_boosts)s b,
                (%(levels)s) levels,
                (%(chains)s) chains,
                (
                    select
                        s.app_version,
                        s.level_version,
                        s.ab_group,
                        s.device_region,
                        s.payer,
                        s.chain,
                        s.level,
                        s.win,
                        lag(s.win, 1) over (
                            partition by s.app_version, s.level_version,
                                s.ab_group, s.device_region, s.payer, s.chain
                            order by s.year, s.month, s.day,
                                s.app_version, s.level_version, s.ab_group,
                                s.device_region, s.payer, s.chain, s.level
                        ) as prev_win
                    from
                    (
                        select
                            from_unixtime(%(second_hour)s, 'yyyy') as year,
                            from_unixtime(%(second_hour)s, 'MM') as month,
                            from_unixtime(%(second_hour)s, 'dd') as day,
                            w.app_version as app_version,
                            w.level_version,
                            w.ab_group,
                            w.device_region,
                            w.payer,
                            w.chain,
                            w.level,
                            sum(w.win) as win
                        from
                            %(table_win_fail)s w
                        where
                            w.hour >= %(second_hour)s
                            and w.hour < %(first_hour)s
                        group by
                            year, month, day, app_version, level_version, ab_group,
                            w.device_region, w.payer,
                            chain,
                            w.level
                     ) s
                ) w
            where
                b.hour >= %(second_hour)s
                and b.hour < %(first_hour)s
                and b.level = cast(levels.id as int)
                and b.chain = cast(chains.id as int)

                and b.level = w.level
                and b.chain = w.chain
                and b.level_version = w.level_version
                and b.app_version = w.app_version
                and b.ab_group = w.ab_group
                and b.payer = w.payer
                and (
                    b.device_region = w.device_region
                or (b.device_region is null and w.device_region is null))
            group by
                day, app_version, b.level_version, b.ab_group, device_region,
                b.chain, b.level, payer, boost,
                prev_win
            order by
                b.app_version, b.level, b.chain, boost
        ) s
        order by
            day, chain_number, level_number, app_version,
            level_version, device_region, payer
        """ % {'table_win_fail': TABLE_WIN_FAIL,
               'table_boosts': TABLE_BOOSTS,
               'levels': levels_table(max_level),
               'chains': chains_table(max_chain),
               'first_hour': first_hour,
               'second_hour': last_hour}
    return sql


def get_cash_sql(first_hour, last_hour, max_level, max_chain):
    sql = """
        select
            day, app_version, level_version, device_region,
            level_number, chain_number, payer,
            decode(ab_group, '', '--', ab_group), win_count,
            type_cash, cash_count
        from
        (
            select
                to_date(from_unixtime(%(second_hour)s)) as day,
                w.app_version as app_version,
                w.level_version as level_version,
                w.ab_group,
                (CASE
                    WHEN
                        w.device_region IS NULL
                    THEN
                        "--"
                    WHEN
                        LENGTH(w.device_region) != 2
                    THEN
                        "--"
                    ELSE
                        w.device_region
                END) as device_region,
                w.level as level_number,
                w.chain as chain_number,
                decode(w.payer, 1, 'Payers', 0, 'Non payers') as payer,
                sum(w.win) as win_count,
                'Free cash' as type_cash,
                sum(w.free_cash) as cash_count
            from
                (%(levels)s) levels,
                (%(chains)s) chains,
                %(table_win_fail)s w
            where
                w.hour >= %(second_hour)s
                and w.hour < %(first_hour)s
                and w.level = cast(levels.id as int)
                and w.chain = cast(chains.id as int)
            group by
                day, app_version, w.level_version, w.ab_group, device_region,
                w.chain, w.level, payer
            union all
            select
                to_date(from_unixtime(%(second_hour)s)) as day,
                w.app_version as app_version,
                w.level_version as level_version,
                w.ab_group,
                (CASE
                    WHEN
                        w.device_region IS NULL
                    THEN
                        "--"
                    WHEN
                        LENGTH(w.device_region) != 2
                    THEN
                        "--"
                    ELSE
                        w.device_region
                END) as device_region,
                w.level as level_number,
                w.chain as chain_number,
                decode(w.payer, 1, 'Payers', 0, 'Non payers') as payer,
                sum(w.win) as win_count,
                'Real cash' as type_cash,
                sum(w.real_cash) as cash_count
            from
                (%(levels)s) levels,
                (%(chains)s) chains,
                %(table_win_fail)s w
            where
                w.hour >= %(second_hour)s
                and w.hour < %(first_hour)s
                and w.level = cast(levels.id as int)
                and w.chain = cast(chains.id as int)
            group by
                day, app_version, w.level_version, w.ab_group, device_region,
                w.chain, w.level, payer
        ) s
        order by
            day, chain_number, level_number, app_version,
            level_version, device_region, payer, win_count
        """ % {'table_win_fail': TABLE_WIN_FAIL,
               'table_boosts': TABLE_BOOSTS,
               'levels': levels_table(max_level),
               'chains': chains_table(max_chain),
               # 'first_hour': last_hour,
               # 'second_hour': first_hour}
               'first_hour': first_hour,
               'second_hour': last_hour}
    return sql


def create_tables():
    sql = """
        create table if not exists %s (
            app_version VARCHAR(255),
            level_version INT,
            level BIGINT,
            ab_group VARCHAR(255),
            device_region VARCHAR(255),
            payer INT,
            chain BIGINT,
            boost VARCHAR(255),
            boost_count BIGINT
        )
        partitioned by (hour int)
        stored as parquet;
    """ % TABLE_BOOSTS
    execute(sql)

    sql = """
        create table if not exists %s (
            app_version VARCHAR(255),
            level_version INT,
            ab_group VARCHAR(255),
            device_region VARCHAR(255),
            level BIGINT,
            chain BIGINT,
            payer INT,
            count_user INT,
            win BIGINT,
            fail BIGINT,
            free_cash BIGINT,
            real_cash BIGINT,
            win_without_boosts BIGINT,
            value DOUBLE,
            variance DOUBLE
        )
        partitioned by (hour int)
        stored as parquet;
    """ % TABLE_WIN_FAIL
    execute(sql)


def update_table(hour_list):
    pool = multiprocessing.Pool(NUM_WORKERS)
    pool.map(update_boost, hour_list)
    pool.close()

    pool = multiprocessing.Pool(NUM_WORKERS)
    pool.map(ubdate_win_fail, hour_list)
    pool.close()


def update_boost(hour):
    attempt = 0
    complete = False
    while attempt < NUMBER_ATTEMPT and not complete:
        try:
            logging.info(('update_boost', hour))
            # sql = get_drop_partition_sql(TABLE_BOOSTS, hour)
            # try:
            #     logging.info(execute(sql))
            # except impala.error.RPCError:
            #     pass
            sql = get_insert_boosts_sql(hour, '3928')
            # logging.info(sql)
            execute(sql)
        except socket.timeout, impala.error.DisconnectedError:
            attempt += 1
            logging.info('SLEEP')
            time.sleep(TIME_SLEEP)
        else:
            complete = True


def ubdate_win_fail(hour):
    attempt = 0
    complete = False
    while attempt < NUMBER_ATTEMPT and not complete:
        try:
            logging.info(('ubdate_win_fail', hour))
            # sql = get_drop_partition_sql(TABLE_WIN_FAIL, hour)
            # try:
            #     logging.info(execute(sql))
            # except impala.error.RPCError:
            #     pass
            sql = get_insert_win_fail_sql(hour, '3928')
            # logging.info(sql)
            execute(sql)
        except socket.timeout, impala.error.DisconnectedError:
            attempt += 1
            logging.info('SLEEP')
            time.sleep(TIME_SLEEP)
        else:
            complete = True


def create_csv_files(first_hour, last_hour, headers, max_level, max_chain):
    logging.info('Create csv file for cube difficult')
    cube_name = 'cube_difficult_var_zm'
    last_hour_str = (
        datetime.fromtimestamp(last_hour) - timedelta(hours=2)
    ).strftime('%Y-%m-%d')
    sql = get_select_for_cube_difficult_sql(
        first_hour, last_hour, max_level, max_chain)
    filename = os.path.join(TEMP_PATH, cube_name, last_hour_str + '.csv')
    put_headers(filename, headers['TABLE_WIN_FAIL'])
    # logging.info(sql)
    execute_file(sql, filename)
    relocation_file(cube_name, last_hour_str)

    logging.info('Create csv file for cube monetization')
    cube_name = 'cube_two_zm'
    last_hour_str = (
        datetime.fromtimestamp(last_hour) - timedelta(hours=2)
    ).strftime('%Y-%m-%d')
    sql = get_select_for_cube_monetization_sql(
        first_hour, last_hour, max_level, max_chain)
    filename = os.path.join(TEMP_PATH, cube_name, last_hour_str + '.csv')
    put_headers(filename, headers['TABLE_BOOSTS'])
    execute_file(sql, filename)
    relocation_file(cube_name, last_hour_str)

    logging.info('Create csv file for cube cash')
    cube_name = 'cube_cash_zm'
    last_hour_str = (
        datetime.fromtimestamp(last_hour) - timedelta(hours=2)
    ).strftime('%Y-%m-%d')
    sql = get_cash_sql(first_hour, last_hour, max_level, max_chain)
    filename = os.path.join(TEMP_PATH, cube_name, last_hour_str + '.csv')
    put_headers(filename, headers['TABLE_CASH'])
    execute_file(sql, filename)
    relocation_file(cube_name, last_hour_str)


if __name__ == '__main__':
    if not try_lock(LOCK_FILE):
        logging.config.fileConfig(os.path.join(LOG_CONF_PATH, LOGGIN_FILE))
        logging = logging.getLogger()
        sl = StreamToLogger(logging)
        sys.stderr = sl
        logging.info('\n')
        logging.info('START')

        headers = {
            'TABLE_WIN_FAIL': [
                'day',
                'app_version',
                'level_version',
                'device_region',
                'level_number',
                'chain_number',
                'payer',
                'win_count',
                'fail_count',
                'count_user',
                'win_without_boosts_count',
                'ab_group',
                'value',
                'variance'],
            'TABLE_BOOSTS': [
                'day',
                'app_version',
                'level_version',
                'device_region',
                'level_number',
                'chain_number',
                'payer',
                'boost',
                # 'boost_count',
                'passed',
                'ab_group',
                'total_monetization',
            ],
            'TABLE_CASH': [
                'day',
                'app_version',
                'level_version',
                'device_region',
                'level_number',
                'chain_number',
                'payer',
                'ab_group',
                'win_count',
                'type_cash',
                'cash_cash'
            ]
        }

        app = '3928'
        max_level = get_max_level(app)
        max_chain = 50     # get_max_chain(cursor)

        create_tables()
        list_seg_day = get_last_days_seg_tables(app)
        if min(list_seg_day) != max(list_seg_day):
            hour_u = datetime.utcfromtimestamp(
                get_full_list_hour('seg_users_%s_pq' % app)[-1])
            hour_p = datetime.utcfromtimestamp(
                get_full_list_hour('seg_players_%s_pq' % app)[-1])
            logging.error(
                """Last hour seg_users_%(app)s_pq (%(hour_u)s)
                    not equal last hour seg_players_%(app)s_pq (%(hour_p)s)
                """ % {
                    'app': app,
                    'hour_u': hour_u.strftime('%Y-%m-%d'),
                    'hour_p': hour_p.strftime('%Y-%m-%d')})
        else:
            difference_days_w = get_difference_days_list(
                TABLE_WIN_FAIL,
                'seg_players_3928_pq',
            )[:STEP]
            # difference_days_w = get_difference_days_list(
            #     TABLE_WIN_FAIL,
            #     'seg_users_3928_pq',
            #     gte='2016-10-03'
            # )
            while difference_days_w:
                logging.info(difference_days_w)
                for hour1, hour2 in difference_days_w:
                    day1 = datetime.fromtimestamp(hour1) - timedelta(hours=3)
                    day2 = datetime.fromtimestamp(hour2) - timedelta(hours=3)
                    logging.info((day1.strftime('%Y-%m-%d'), day2.strftime('%Y-%m-%d')))
                    hour_list = get_list_hour(
                        'all_events_3928_pq',
                        hour1, hour2)
                    update_ab_table('3928', day2)
                    update_table(hour_list)
                    create_csv_files(hour2, hour1, headers, max_level, max_chain)
                difference_days_w = get_difference_days_list(
                    TABLE_WIN_FAIL,
                    'seg_players_3928_pq'
                )[:STEP]
                # difference_days_w = None
        delete_lock(LOCK_FILE)
        logging.info('End')
