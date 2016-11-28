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
import psycopg2
import thrift

from impala.dbapi import connect
from datetime import datetime, timedelta
from common_functions import (
    try_lock, delete_lock,
    StreamToLogger, get_full_list_hour,
    get_last_days_seg_tables, get_difference_days_list,
    execute, fetchall, relocation_file)

PARENT_PATH = '/home/yansen-a/analytics/local_cubes'

BACKUP_PATH = os.path.join(PARENT_PATH, 'backup')
IMPORT_CSV_PATH = os.path.join(PARENT_PATH, 'import_csv')
LOCK_PATH = os.path.join(PARENT_PATH, 'lock')
LOG_CONF_PATH = os.path.join(PARENT_PATH, 'log_conf')
LOGS_PATH = os.path.join(PARENT_PATH, 'logs')
TEMP_PATH = os.path.join(PARENT_PATH, 'temp')

LOCK_FILE = 'update_temp_table.lock'
LOGGIN_FILE = 'update_temp_table.conf'

TABLE_WIN_FAIL = 'win_fail_3426_pq_test_s'
TABLE_BOOSTS = 'boosts_3426_pq_test_s'

BEGIN_HOUR = 1452729600     # unix_timestamp('2016-01-14 00:00:00')
NUM_WORKERS = 3
NUMBER_ATTEMPT = 10
TIME_SLEEP = 10 * 60
STEP = 15

app_list = ['3426']


def execute_post(sql):
    conn = psycopg2.connect(host='172.16.5.28',
                            port=5432,
                            user='postgres',
                            password='postgres',
                            database='cubes')
    with conn.cursor() as cursor:
        cursor.execute(sql)
        conn.commit()


def fetchall_post(sql):
    conn = psycopg2.connect(host='172.16.5.28',
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


def get_values(ab_group_list):
    return '(%(id)s as id, "%(name)s" as name, "%(name)s" as description)%(other)s' % {
        'id': ab_group_list[0][1],
        'name': ab_group_list[0][0],
        'other': ', %s' % ','.join(
            map(
                lambda i: '(%s, "%s", "%s")' % (i[1], i[0], i[0]),
                ab_group_list[1:])) if ab_group_list[1:] else ''
    }


def get_element_sql(name):
    return """
        SELECT
            name,
            id
        from
            %s_fd_an
    """ % name


def list_element(name):
    return fetchall_post(get_element_sql(name))


def get_new_ab_groups_sql():
    ab_groups = list_element('ab_group')
    if ab_groups:
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
                        (values %(ab_groups)s) ab
                    right join
                    (
                        SELECT distinct
                            ab_names as name
                        from
                            ab_group_3426_pq
                    ) pq
                    on ab.name = pq.name
                ) s
            ) s2
            where old_id is null
        """ % {
            'ab_groups': get_values(ab_groups)
        }
    else:
        return """
             SELECT
                row_number() over (order by name) as new_id,
                name
            from
                (
                    SELECT
                        ab_names as name
                    from
                        ab_group_3426_pq
                    where
                        hour = %(seg_hour)s
                ) s
        """ % {'seg_hour': get_full_list_hour('seg_users_3426_pq')[-1]}


def get_new_app_version_sql(gte_hour, lt_hour):
    app_version = list_element('app_version')
    if app_version:
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
                        (values %(app_version)s) ab
                    right join
                    (
                        SELECT distinct
                            app_version as name
                        from
                            seg_players_3426_pq
                        where
                            level is not null
                    ) pq
                    on ab.name = pq.name
                ) s
            ) s2
            where old_id is null
        """ % {
            'app_version': get_values(app_version),
            'seg_hour': get_full_list_hour('seg_users_3426_pq')[-1]
        }
    else:
        return """
             SELECT
                row_number() over (order by name) as new_id,
                name
            from
                (
                    SELECT distinct
                        app_version as name
                    from
                        seg_players_3426_pq
                    where
                        hour = %(seg_hour)s
                        and level is not null
                ) s
        """ % {'seg_hour': get_full_list_hour('seg_users_3426_pq')[-1]}


def get_new_element(name, gte_hour=None, lt_hour=None):
    if name == 'ab_group':
        return fetchall(get_new_ab_groups_sql())
    elif name == 'app_version':
        return fetchall(get_new_app_version_sql(gte_hour, lt_hour))


def get_insert_table_sql(name, gte_hour=None, lt_hour=None):
    new_element = add_breakets(get_new_element(name, gte_hour, lt_hour))

    if new_element:
        return """
            INSERT INTO
                %(table_name)s_fd_an
            (id, name, description)
            VALUES
            %(new_element)s;
        """ % {
            'new_element': new_element,
            'table_name': name}
    return None


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
    """ % get_values(list_element('app_version'))


def get_create_table_sql(name):
    return """
        CREATE TABLE IF NOT EXISTS %s_fd_an
        (
          id smallint,
          name varchar(255),
          description varchar(255)
        )
    """ % name


def try_create_table(name):
    execute_post(get_create_table_sql(name))
    # try_update_table(name)


def try_update_table(name, gte_hour=None, lt_hour=None):
    new_element = get_insert_table_sql(name, gte_hour, lt_hour)
    if new_element:
        # logging.info(new_element)
        execute_post(new_element)


def get_dict(name):
    try_update_table(name)
    return dict(list_element(name))


def add_breaket(el):
    el = "(" + "'%s'" % el + ")"
    return el


def add_breaket_str(el):
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
    ids = map(add_breaket_str, ab_name)
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
    # logging.info(hour)
    table = 'ab_group_%(app)s_pq' % {'app': app}
    insert_sql = ids_table(get_ab_values(app, hour))
    # logging.info(insert_sql)
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
    # logging.info(hour)
    if hour not in exists_hours:
        execute(sql)
        # logging.info(sql)
    else:
        pass


def get_ab_values(app, hour):
    ab_names = []
    sql = """
    select distinct
        regexp_extract(payload, "ABGroups\\":\\"([^\\"]*)", 1) as ab_group
    from
        all_events_%(app)s_pq as p
    where
        hour >= unix_timestamp(to_date('%(hour)s'))
        and hour < unix_timestamp(days_add(to_date('%(hour)s'), 1))
        and p.event_type = 'event'
        and (p.parameters ilike '%%\\"Level.Complete\\"%%'
            or p.parameters ilike '%%\\"Level.Failed\\"%%')
        and payload like '%%ABGroups%%'
    """ % {'hour': hour,
           'app': app}
    res_sql = fetchall(sql)
    for elem in res_sql:
        for el in elem:
            ab = el.split(', ')
            ab_names += ab
    if [''] in res_sql:
        return map(lambda x: '--' if x == '' else x, list(set(ab_names)))
    else:
        return list(set(ab_names))


def get_insert_boosts_sql(hour):
    # app = '3426'
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
                all_events_3426_pq p,
                ab_group_3426_pq g
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
                nvl(cast(regexp_extract(payload, 'LevelChain\\":\\"(\\\\d+)', 1) as int), 0) as chain,
                cast(regexp_extract(payload, 'Level\\":\\"(\\\\d+)', 1) as int) as level,
                /*nvl(cast(regexp_extract(payload, 'LevelsVersion\\":\\"(\\\\d+)', 1) as int), -1) as level_version,*/
                nvl(ab.ab_names, '--') as ab_group,
                payload
            from
                all_events_3426_pq p left join ab
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
                seg_users_3426_pq
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
            payer, chain_number, cast(boost as VARCHAR(255)), boost_count
        from
            (
                select
                    from_unixtime(%(hour)s, 'yyyy') as year,
                    from_unixtime(%(hour)s, 'MM') as month,
                    from_unixtime(%(hour)s, 'dd') as day,
                    c.app_version as app_version,
                    c.levels_version as level_version,
                    p.chain as chain_number,
                    p.level as level_number,
                    p.ab_group,
                    s.device_region as device_region,
                    c.payer as payer,
                    boosts.name as boost,
                    sum(nvl(cast(regexp_extract(p.payload, concat(boosts.name, '\\":\\"(\\\\d+)'), 1) as int), 0)) as boost_count
                from
                    payload p,
                    seg_players_3426_pq c,
                    (values
                        ('BoostsBombs' as name),
                        ('BoostsDynamiteAndLightning'),
                        ('BoostsHammer'),
                        ('BoostsLightning'),
                        ('BoostsReshuffle'),
                        ('SquidSaves'),
                        ('TimebombSaves')
                    ) as boosts
                    left join
                    countries s
                    ON c.event_user = s.event_user
                where
                    c.hour = %(next_day)s
                    and c.cash_balance < 5000
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
                        c.levels_version as level_version,
                        p.chain as chain_number,
                        p.level as level_number,
                        p.ab_group,
                        s.device_region as device_region,
                        c.payer as payer,
                        concat('AdditionalMoves', '_', boost_number.num) as boost,
                        sum(if(nvl(cast(regexp_extract(p.payload, 'AdditionalMoves\\":\\"(\\\\d+)', 1) as int), 0) >= cast(boost_number.num as int),1,0)) as boost_count
                    from
                        payload p,
                        (%(boost_number)s) boost_number,
                        seg_players_3426_pq c
                        left join
                        countries s
                        ON c.event_user = s.event_user
                    where
                        c.hour = %(next_day)s
                        and c.cash_balance < 5000
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
                        c.levels_version as level_version,
                        p.chain as chain_number,
                        p.level as level_number,
                        p.ab_group,
                        s.device_region as device_region,
                        c.payer as payer,
                        concat('AdditionalTime', '_', boost_number.num) as boost,
                        sum(if(nvl(cast(regexp_extract(p.payload, 'AdditionalTime\\":\\"(\\\\d+)', 1) as int), 0) >= cast(boost_number.num as int),1,0)) as boost_count
                    from
                        payload p,
                        (%(boost_number)s) boost_number,
                        seg_players_3426_pq c
                        left join
                        countries s
                        ON c.event_user = s.event_user
                    where
                        c.hour = %(next_day)s
                        and c.cash_balance < 5000
                        and c.first_session is not Null
                        and c.level is not null
                        and c.event_user = p.event_user
                    group by
                        year, month, day, app_version, level_version,
                        chain_number, level_number, ab_group, device_region, payer, boost
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


def get_select_for_cube_difficult_sql(
        first_hour, last_hour, max_level, max_chain):
    sql = """
        WITH dr AS (
            select
                row_number() over (order by alpha2) as num_id,
                alpha2,
                alpha3
            from
                device_region
            order by
                alpha2
        )

        SELECT
            day,
            level_version,
            level_number,
            chain_number,
            win_count,
            fail_count,
            count_user,
            win_without_boosts_count,
            value,
            round((count_user * variance), 2) as n_variance,
            device_region,
            payer,
            ab_group,
            app_version
        from
        (
            select
                to_date(from_unixtime(%(second_hour)s)) as day,
                app.id as app_version,
                w.level_version as level_version,
                ab.id as ab_group,
                if(dr.num_id is null, 0, dr.num_id)  as device_region,
                w.level as level_number,
                w.chain as chain_number,
                w.payer as payer,
                sum(w.win) as win_count,
                sum(w.fail) as fail_count,
                sum(w.count_user) as count_user,
                sum(w.win_without_boosts) as win_without_boosts_count,
                round(sum(w.value), 2) as value,
                round(sum(w.variance), 2) as variance
            from
                (%(levels)s) levels,
                (%(chains)s) chains,
                %(table_win_fail)s w,
                (%(ab_group_table)s) ab,
                (%(app_version_table)s) app
                left join dr
                on
                    --w.device_region = dr.alpha2
                    w.device_region = (
                        case
                            when
                                LENGTH(w.device_region) = 2
                            then
                                dr.alpha2
                            when
                                LENGTH(w.device_region) = 3
                            then
                                dr.alpha3
                        end
                    )
            where
                w.hour >= %(second_hour)s
                and w.hour < %(first_hour)s
                and w.level = cast(levels.id as int)
                and w.chain = cast(chains.id as int)

                and decode(w.ab_group, '', '--', w.ab_group) = ab.name
                and w.app_version = app.name
            group by
                day, app.id, w.level_version, ab.id, device_region,
                w.chain, w.level, payer
        ) s
        order by
            day, chain_number, level_number, app_version,
            level_version, device_region, payer, win_count
        """ % {'table_win_fail': TABLE_WIN_FAIL,
               'levels': levels_table(max_level),
               'chains': chains_table(max_chain),
               'first_hour': first_hour,
               'second_hour': last_hour,
               'ab_group_table': ab_group_table(),
               'app_version_table': app_version_table()}
    return sql


def get_select_for_cube_monetization_sql(
        first_hour, last_hour, max_level, max_chain):
    sql = """
        WITH dr AS (
            select
                row_number() over (order by alpha2) as num_id,
                alpha2,
                alpha3
            from
                device_region
            order by
                alpha2
        )

        SELECT
            day,
            --app_version,
            level_version,
            level_number,
            chain_number,
            passed,
            --decode(ab_group, '', '--', ab_group),
            total_monetization,
            boost_id,
            device_region,
            payer,
            ab_group,
            app_version
        from
        (
            select
                to_date(from_unixtime(%(second_hour)s)) as day,
                --b.app_version as app_version,
                app.id as app_version,
                b.level_version as level_version,
                --b.ab_group,
                ab.id as ab_group,
                /*(CASE
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
                END) as device_region,*/
                if(dr.num_id is null, 0, dr.num_id)  as device_region,
                b.level as level_number,
                b.chain as chain_number,
                --decode(b.payer, 1, 'Payers', 0, 'Non payers') as payer,
                b.payer as payer,
                /*boost as boost,*/
                w.prev_win as passed,
                case boost
                    when 'SquidSaves' then sum(boost_count) * 18
                    when 'TimebombSaves' then sum(boost_count) * 18
                    when 'BoostsReshuffle' then sum(boost_count) * 19
                    when 'BoostsLightning' then sum(boost_count) * 9
                    when 'BoostsHammer' then sum(boost_count) * 19
                    when 'BoostsDynamiteAndLightning' then sum(boost_count) * 19
                    when 'BoostsBombs' then sum(boost_count) * 13
                    when 'AdditionalTime_7' then sum(boost_count) * 38
                    when 'AdditionalTime_6' then sum(boost_count) * 38
                    when 'AdditionalTime_5' then sum(boost_count) * 33
                    when 'AdditionalTime_4' then sum(boost_count) * 28
                    when 'AdditionalTime_3' then sum(boost_count) * 20
                    when 'AdditionalTime_2' then sum(boost_count) * 12
                    when 'AdditionalTime_1' then sum(boost_count) * 9
                    when 'AdditionalMoves_7' then sum(boost_count) * 38
                    when 'AdditionalMoves_6' then sum(boost_count) * 38
                    when 'AdditionalMoves_5' then sum(boost_count) * 33
                    when 'AdditionalMoves_4' then sum(boost_count) * 28
                    when 'AdditionalMoves_3' then sum(boost_count) * 20
                    when 'AdditionalMoves_2' then sum(boost_count) * 12
                    when 'AdditionalMoves_1' then sum(boost_count) * 9
                end as total_monetization,
                case boost
                    when 'TimebombSaves' then 15
                    when 'SquidSaves' then 12
                    when 'BoostsReshuffle' then 9
                    when 'BoostsLightning' then 6
                    when 'BoostsHammer' then 2
                    when 'BoostsDynamiteAndLightning' then 18
                    when 'BoostsBombs' then 20
                    when 'AdditionalTime_7' then 4
                    when 'AdditionalTime_6' then 17
                    when 'AdditionalTime_5' then 11
                    when 'AdditionalTime_4' then 13
                    when 'AdditionalTime_3' then 7
                    when 'AdditionalTime_2' then 19
                    when 'AdditionalTime_1' then 14
                    when 'AdditionalMoves_7' then 16
                    when 'AdditionalMoves_6' then 10
                    when 'AdditionalMoves_5' then 5
                    when 'AdditionalMoves_4' then 8
                    when 'AdditionalMoves_3' then 21
                    when 'AdditionalMoves_2' then 3
                    when 'AdditionalMoves_1' then 1
                end as boost_id
            from
                %(table_boosts)s b,
                (
                    select
                        row_number() over (
                            order by event_user
                        ) as id
                    from
                        seg_players_3444_pq
                    where
                        hour = unix_timestamp(days_add(to_date(from_unixtime(%(second_hour)s)), -5))
                    limit
                        %(max_level)s
                ) levels,
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
                ) w,
                (%(ab_group_table)s) ab,
                (%(app_version_table)s) app
                left join dr
                on
                    b.device_region = (
                        case
                            when
                                LENGTH(b.device_region) = 2
                            then
                                dr.alpha2
                            when
                                LENGTH(b.device_region) = 3
                            then
                                dr.alpha3
                        end
                    )
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
                and b.device_region = w.device_region
                and b.payer = w.payer

                and decode(b.ab_group, '', '--', b.ab_group) = ab.name
                and b.app_version = app.name
            group by
                day, app.id, b.level_version, ab.id, device_region,
                b.chain, b.level, payer, boost,
                prev_win
            order by
                app.id, b.level, b.chain, boost
        ) s
        order by
            day, chain_number, level_number, app_version,
            level_version, device_region, payer
        """ % {'table_win_fail': TABLE_WIN_FAIL,
               'table_boosts': TABLE_BOOSTS,
               'max_level': max_level,
               'chains': chains_table(max_chain),
               'first_hour': first_hour,
               'second_hour': last_hour,
               'ab_group_table': ab_group_table(),
               'app_version_table': app_version_table()}
    return sql


def create_tables():
    try_create_table('ab_group')
    try_create_table('app_version')
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


def get_drop_table_sql(table, hour):
    return """
        drop table %s;
    """ % table


def get_difference_days(first_table, second_table, first_hour=None):
    if not first_hour:
        partitions = fetchall('show partitions %s;' % first_table)
        if len(partitions) > 1:
            last_hour = int(partitions[-2][0])
        else:
            last_hour = BEGIN_HOUR
    else:
        last_hour = int(time.mktime((
            datetime.strptime(first_hour, '%Y-%m-%d') + timedelta(hours=3)
        ).timetuple()))

    partitions = fetchall('show partitions %s;' % second_table)
    difference_days = map(
        lambda hour: int(hour[0]),
        filter(
            lambda hour: hour[0].isdigit() and int(hour[0]) >= last_hour,
            partitions
        )
    )
    difference_days.sort()
    result = list()
    for i in range(len(difference_days) - 1):
        result.append([difference_days[i], difference_days[i + 1]])
    result.reverse()
    return result


def get_difference_days2(first_table, second_table):
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
    # for hour1, hour2 in result:
    #     day1 = datetime.fromtimestamp(hour1) - timedelta(hours=3)
    #     day2 = datetime.fromtimestamp(hour2) - timedelta(hours=3)
    #     logging.info((day1.strftime('%Y-%m-%d'), day2.strftime('%Y-%m-%d')))
    return result


def get_drop_partition_sql(table, hour):
    return """
        alter table %s drop partition(hour = %s)
    """ % (table, hour)


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
            sql = get_insert_boosts_sql(hour)
            # logging.info(sql)
            execute(sql)
        except (socket.timeout, impala.error.DisconnectedError,
                impala.error.OperationalError, impala.error.RPCError,
                thrift.transport.TTransport.TTransportException,
                socket.timeout) as msg:
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
            sql = get_insert_win_fail_sql(hour)
            # logging.info(sql)
            execute(sql)
        except (socket.timeout, impala.error.DisconnectedError,
                impala.error.OperationalError, impala.error.RPCError,
                thrift.transport.TTransport.TTransportException,
                socket.timeout) as msg:
            attempt += 1
            logging.info('SLEEP')
            time.sleep(TIME_SLEEP)
        else:
            complete = True


def get_insert_win_fail_sql(hour):
    # app = '3426'
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
                all_events_3426_pq p,
                ab_group_3426_pq g
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
                p.ab_group,
                p.event_name,
                p.reason,
                sum(nvl(p.free_cash, 0)) as free_cash,
                sum(nvl(p.real_cash, 0)) as real_cash,
                sum(nvl(p.boost_count, 0)) + sum(nvl(p.at_count, 0)) + sum(nvl(p.am_count, 0)) as all_boosts
            from
                (
                    select
                        p.event_id,
                        p.event_user,
                        decode(instr(parameters, 'Level.Failed'), 0, decode(instr(parameters, 'Chain.Failed'), 0, 1, 2), 2) as event_name,
                        nvl(cast(regexp_extract(payload, 'LevelChain\\":\\"(\\\\d+)', 1) as int), 0) as chain,
                        cast(regexp_extract(payload, 'Level\\":\\"(\\\\d+)', 1) as int) as level,
                        cast(regexp_extract(payload, 'FreeCashBalance\\":\\"(\\\\d+)', 1) as int) as free_cash,
                        cast(regexp_extract(payload, 'RealCashBalance\\":\\"(\\\\d+)', 1) as int) as real_cash,
                        cast(regexp_extract(payload, 'Boosts\\":\\"(\\\\d+)', 1) as int) as boost_count,
                        cast(regexp_extract(payload, 'AdditionalTimes\\":\\"(\\\\d+)', 1) as int) as at_count,
                        cast(regexp_extract(payload, 'AdditionalMoves\\":\\"(\\\\d+)', 1) as int) as am_count,
                        /*nvl(cast(regexp_extract(payload, 'LevelsVersion\\":\\"(\\\\d+)', 1) as int), -1) as level_version,*/
                        nvl(ab.ab_names, '--') as ab_group,
                        regexp_extract(payload, 'Reason\\":\\"(\\\\w+)', 1) as reason
                    from
                        all_events_3426_pq p left join ab
                    on
                        p.event_user = ab.event_user
                    where
                        p.hour = %(hour)s
                        and p.event_type = 'event'
                        and (p.parameters ilike '%%Level.Complete%%' or p.parameters ilike '%%Level.Failed%%')
                ) p
            group by p.event_id, p.event_user, p.chain, p.level, /*p.level_version, */p.ab_group, p.event_name, p.reason
        )

        select
            app_version, level_version, ab_group,
            device_region, level_number,
            chain_number, payer,
            cast(sum(count_user) as INT)  as count_user,
            sum(win_count) as win_count,
            sum(fail_count) as fail_count,
            sum(free_cash_value) as free_cash,
            sum(real_cash_value) as real_cash,
            sum(win_without_boosts) as win_without_boosts,
            round(sum(df), 2) as value,
            round(variance(df), 2) as variance
        from(
            select
                from_unixtime(%(hour)s, 'yyyy') as year,
                from_unixtime(%(hour)s, 'MM') as month,
                from_unixtime(%(hour)s, 'dd') as day,
                c.app_version as app_version,
                c.levels_version as level_version,
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
                sum(nvl(p.free_cash, 0)) as free_cash_value,
                sum(nvl(p.real_cash, 0)) as real_cash_value,
                sum(decode(p.event_name, 1, decode(p.all_boosts, 0, 1, 0), 0)) as win_without_boosts,
                zeroifnull(sum(decode(p.event_name, 2, decode(p.reason, 'Fail', 1, 0), 0))/(
                    nullif(sum(decode(p.event_name, 1, 1, 0)) + sum(decode(p.event_name, 2, decode(p.reason, 'Fail', 1, 0), 0)), 0)
                )) df
            from
                payload p,
                seg_players_3426_pq c
                left join
                (
                    SELECT
                        event_user,
                        MAX(device_region) AS device_region
                    FROM
                        seg_users_3426_pq
                    WHERE
                        hour = %(next_day)s
                    GROUP BY
                        event_user
                ) AS s
                ON c.event_user = s.event_user
            where
                c.hour = %(next_day)s
                and c.last_active is not Null
                and c.cash_balance < 5000
                and c.first_session is not Null
                and c.level is not null
                and c.event_user = p.event_user
                and p.reason not in ('ExitButton', 'BySystem')
            group by
                year, month, day, c.app_version, level_version,
                chain_number, level_number, ab_group, s.device_region, c.payer,
                p.event_user
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


def execute_file(sql, filename=None):
    conn = connect(host='172.16.5.22', protocol='beeswax', port=21000)
    with conn.cursor() as cursor:
        cursor.execute(sql)
        if not filename:
            return cursor.fetchall()
        else:
            block_size = 1000000
            block = cursor.fetchmany(size=block_size)
            with open(filename, 'a') as f:
                while block:
                    for row in block:
                        f.write('%s\n' % ';'.join(row))
                    block = cursor.fetchmany(size=block_size)


def get_day_list(cursor, table):
    partitions = fetchall('show partitions %s;' % table)
    first_hour = int(partitions[1][0])
    partitions = fetchall('show partitions seg_players_3426_pq;')
    day_list = map(
        lambda hour: int(hour[0]),
        filter(
            lambda hour: hour[0].isdigit() and int(hour[0]) >= first_hour,
            partitions
        )
    )
    day_list.sort()
    return day_list


def put_headers(filename=None, headers=[]):
    with open(filename, 'w') as f:
        f.write('%s\n' % ';'.join(headers))


def create_csv_files(first_hour, last_hour, headers, max_level, max_chain):
    cube_name = 'cube_difficult_var_fd_an'
    logging.info('Create csv file for %s' % cube_name)
    last_hour_str = (
        datetime.fromtimestamp(last_hour) - timedelta(hours=2)
    ).strftime('%Y-%m-%d')
    sql = get_select_for_cube_difficult_sql(
        first_hour, last_hour, max_level, max_chain)
    filename = os.path.join(TEMP_PATH, cube_name, last_hour_str + '.csv')
    put_headers(filename, headers[TABLE_WIN_FAIL])
    execute_file(sql, filename)
    relocation_file(cube_name, last_hour_str)

    cube_name = 'cube_two_fishdom_an'
    logging.info('Create csv file for %s' % cube_name)
    last_hour_str = (
        datetime.fromtimestamp(last_hour) - timedelta(hours=2)
    ).strftime('%Y-%m-%d')
    sql = get_select_for_cube_monetization_sql(
        first_hour, last_hour, max_level, max_chain)
    filename = os.path.join(TEMP_PATH, cube_name, last_hour_str + '.csv')
    put_headers(filename, headers[TABLE_BOOSTS])
    # logging.info(sql)
    # print 1/0
    execute_file(sql, filename)
    relocation_file(cube_name, last_hour_str)


def get_max_level():
    partitions = fetchall('show partitions seg_players_3426_pq;')
    last_hour = int(partitions[-2][0])
    sql = """
        select
            max(level)
        from
            seg_players_3426_pq
        where
            hour = %s
    """ % last_hour
    return int(fetchall(sql)[0][0]) + 1


if __name__ == '__main__':
    if not try_lock(LOCK_FILE):
        logging.config.fileConfig(os.path.join(LOG_CONF_PATH, LOGGIN_FILE))
        logging = logging.getLogger()
        sl = StreamToLogger(logging)
        sys.stderr = sl
        logging.info('\n')
        logging.info('START')

        headers = {
            TABLE_WIN_FAIL: [
                'day',
                'app_version',
                'level_version',
                'device_region',
                'level_number',
                'chain_number',
                'payer',
                'win_count',
                'fail_count',
                'win_without_boosts_count',
                'ab_group'
            ],
            TABLE_BOOSTS: [
                'day',
                'app_version',
                'level_version',
                'device_region',
                'level_number',
                'chain_number',
                'payer',
                'passed',
                'ab_group',
                'total_monetization',
                'boost'
            ]
        }

        max_level = get_max_level()
        max_chain = 50     # get_max_chain(cursor)

        create_tables()

        app = '3426'
        list_seg_day = get_last_days_seg_tables(app)
        if False:
        # if min(list_seg_day) != max(list_seg_day):
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
                'seg_users_3426_pq'
            )[:STEP]
            # difference_days_w = get_difference_days_list(
            #     TABLE_WIN_FAIL,
            #     'seg_users_3426_pq',
            #     gte='2016-07-01',
            #     lte='2016-07-24'
            # )
            # difference_days_w = [[1466640000, 1466726400]]
            while difference_days_w:
                logging.info(difference_days_w)
                # logging.info('Days: %s' len(difference_days_w))
                for hour1, hour2 in difference_days_w:

                    day1 = datetime.utcfromtimestamp(hour1)
                    day2 = datetime.utcfromtimestamp(hour2)
                    logging.info(
                        (day1.strftime('%Y-%m-%d'), day2.strftime('%Y-%m-%d')))

                    update_ab_table('3426', day2)
                    try_update_table('ab_group')
                    try_update_table('app_version', hour1, hour2)

                    hour_list = get_list_hour(
                        'all_events_3426_pq', hour1, hour2)
                    update_table(hour_list)

                    create_csv_files(
                        hour2, hour1, headers, max_level, max_chain)
                # difference_days_w = None
                difference_days_w = get_difference_days2(
                    TABLE_WIN_FAIL,
                    'seg_users_3426_pq'
                )[:STEP]
        delete_lock(LOCK_FILE)
        logging.info('END')
