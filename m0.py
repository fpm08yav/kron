from datetime import datetime, timedelta
from impala.dbapi import connect
import psycopg2
# import itertools
import os

from common_functions import (
    get_full_list_hour, create_sql_file, relocation_file, execute_pg,
    execute, fetchall_pg, fetchall)

# APPS = ['3426']
APPS = ['3606', '3789', '3928', '3858', '3790', '3791', '3426', '3444']
CUBES = ['cube_two', 'cube_distribution', 'cube_conv_drop',
         'retention_fishdom', 'retention_township']

MIN_DAY = {
    '3444': '2016-01-14',
    '3426': '2016-02-04',
    '758': '2015-12-23',
    '3789': '2016-06-04',
    '3928': '2016-06-27',
    '3858': '2016-07-01',
    '3606': '2016-06-12',
    '3790': '2016-07-07',
    '3791': '2016-08-24'
}


CUBE_APP = {
    '3444': 'cube_distribution',
    '3426': 'cube_distribution_fd_an',
    '3789': 'cube_distribution_gardenscapes',
    '3790': 'cube_distribution_gs_an',
    '3791': 'cube_distribution_gs_am',
    '3858': 'cube_distribution_gs_fb',
    '3928': 'cube_distribution_zm',
    '3606': 'cube_distribution_fd_am'
}

P_APP = {
    '3426': 'fd_an',
    '3606': 'fd_am',
    '3444': 'fd',
    '3789': 'gs',
    '3790': 'gs_an',
    '3791': 'gs_am',
    '3858': 'gs_fb',
    '3928': 'zm',
}

APP_TABLE_PG = {
    '3444': 'app_version_fd',
    '3426': 'app_version_fd_an',
    '3606': 'app_version_fd_am',
    '3789': 'app_version_gs',
    '3790': 'app_version_gs_an',
    '3791': 'app_version_gs_am',
    '3858': 'app_version_gs_fb',
    '3928': 'app_version_zm',
}


# def fetchall_post(sql):
#     conn = psycopg2.connect(host='172.16.5.28',
#                             port=5432,
#                             user='postgres',
#                             password='postgres',
#                             database='cubes')
#     with conn.cursor() as cursor:
#         cursor.execute(sql)
#         return cursor.fetchall()


def ab_group_table():
    return """
        select
            *
        from
            (values %s) as ab_group
    """ % get_values(list_element('ab_group'))


def app_version_table(app):
    return """
        select
            *
        from
            (values %s) as app_version
    """ % get_values(list_element('app_version', app))


def add_breakets(el):
    if el:
        s = ""
        for i in el:
            s += "(cast('%s' as varchar(255)), cast('%s' as varchar(255))), " % (
                i[0], i[0])
        return s[:-2]
    return None


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
    return fetchall_pg(get_element_sql(name, app))


# def get_last_seg(apps):
#     conn = connect(host='172.16.5.22', protocol='beeswax', port=21000)
#     cursor = conn.cursor()

#     for app in apps:
#         sql = """
#             select min(hour), max(hour)
#             from seg_players_%(app)s_pq
#             """ % {'app': app}
#         cursor.execute(sql)
#         row = cursor.fetchone()
#         print '=============seg_players', app
#         print 'start', datetime.fromtimestamp(int(row[0])).strftime("%Y-%m-%d")
#         print 'finish', datetime.fromtimestamp(int(row[1])).strftime("%Y-%m-%d")

#         sql = """
#             select min(hour), max(hour)
#             from seg_users_%(app)s_pq
#             """ % {'app': app}
#         cursor.execute(sql)
#         row = cursor.fetchone()
#         print '=============seg_users', app
#         print 'start', datetime.fromtimestamp(int(row[0])).strftime("%Y-%m-%d")
#         print 'finish', datetime.fromtimestamp(int(row[1])).strftime("%Y-%m-%d")


# def get_last_cubes(cubes):
#     conn = psycopg2.connect(host='172.16.5.2',
#                             port=5432,
#                             user='postgres',
#                             password='psql123456',
#                             database='cubes')
#     cursor = conn.cursor()
#     for cube in cubes:
#         sql = """
#             select min(cube_date), max(cube_date)
#             from %(cube)s
#             """ % {'cube': cube}

#         cursor.execute(sql)
#         row = cursor.fetchone()
#         print '=============', cube
#         print 'start', row[0]
#         print 'finish', row[1]
#         # print 'start', datetime.fromtimestamp(int(row[0])).strftime("%Y-%m-%d")
#         # print 'finish', datetime.fromtimestamp(int(row[1])).strftime("%Y-%m-%d")


def get_new_app_version_sql(app):
    sql = """
        SELECT
            name,
            id
        from
            %s
    """ % APP_TABLE_PG[app]
    app_version = fetchall_pg(sql)

    if app_version:
        if app == '3444':
            return """
                SELECT
                    pq.name
                from
                    (values %(app_version)s) ab
                    right join
                    (
                        SELECT distinct
                            app_version as name
                        from
                            seg_players_%(app)s_pq
                        where
                            level is not null
                            and app_version like '%\.%'
                    ) pq
                    on ab.name = pq.name
                where id is null
            """ % {
                'app_version': get_values(app_version),
                'app': app
            }
        else:
            return """
                SELECT
                    pq.name
                from
                    (values %(app_version)s) ab
                    right join
                    (
                        SELECT distinct
                            app_version as name
                        from
                            seg_players_%(app)s_pq
                        where
                            level is not null
                    ) pq
                    on ab.name = pq.name
                where id is null
            """ % {
                'app_version': get_values(app_version),
                'app': app
            }
    else:
        return """
            SELECT distinct
                app_version as name
            from
                seg_players_%(app)s_pq
            where
                level is not null
        """ % {'app': app}

    # if app_version:
    #     return """
    #         SELECT
    #             new_id,
    #             name
    #         from
    #             (
    #             SELECT
    #                 row_number() over (order by id, name) as new_id,
    #                 name,
    #                 id as old_id
    #             from
    #                 (
    #                 SELECT
    #                     id,
    #                     pq.name
    #                 from
    #                     (values %(app_version)s) ab
    #                 right join
    #                 (
    #                     SELECT distinct
    #                         app_version as name
    #                     from
    #                         seg_players_%(app)s_pq
    #                     where
    #                         level is not null
    #                 ) pq
    #                 on ab.name = pq.name
    #             ) s
    #         ) s2
    #         where old_id is null
    #     """ % {
    #         'app_version': get_values(app_version),
    #         'app': app
    #     }
    # else:
    #     return """
    #          SELECT
    #             row_number() over (order by name) as new_id,
    #             name
    #         from
    #             (
    #                 SELECT distinct
    #                     app_version as name
    #                 from
    #                     seg_players_%(app)s_pq
    #                 where
    #                     level is not null
    #             ) s
    #     """ % {'app': app}


def get_insert_table_sql(name):
    new_element = add_breakets(fetchall(get_new_app_version_sql(app)))

    if new_element:
        return """
            INSERT INTO
                %(table_name)s
            -- (id, name, description)
            (name, description)
            VALUES
            %(new_element)s;
        """ % {
            'new_element': new_element,
            'table_name': APP_TABLE_PG[app]}
    return None


def imp_test(app):
    # conn = connect(host='172.16.5.22', port=21050)
    conn = connect(host='172.16.5.22', protocol='beeswax', port=21000)
    cursor = conn.cursor()

    app_fs = MIN_DAY[app]   # '2016-01-14'
    if app in ['3789', '3790', '3791', '3858']:
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
                        level
                    from
                        seg_players_%(app)s_pq
                    where
                        hour = unix_timestamp('%(last_seg_date)s')
                        and level is not null
                """ % {'last_seg_date': last_seg_date, 'app': app}
    cursor.execute(levels_sql)
    levels = zip(*cursor.fetchall())[0]

    if app in ['3790', '3791', '3606']:
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

    params_sql = """
        WITH dr AS (
            select
                row_number() over (order by alpha2) as num_id,
                alpha2
            from
                device_region
            order by
                alpha2
        )

        SELECT
            to_date(cast(sp.first_session/1000 as timestamp)) as day,
            payer,
            app_version,
            levels_version,
            -- (case
            --     when
            --         device_region is null
            --     then
            --         "--"
            --     when
            --         LENGTH(device_region) != 2
            --     then
            --         "--"
            --     else
            --         device_region
            -- end) as device_region,
            nvl(dr.num_id, 0) as device_region,
            count(1) as cnt
        from
            (
            select
                event_user,
                first_session,
                level,
                -- decode(payer, 1, 'Payers', 0, 'Non payers') as payer,
                payer,
                app_version,
                %(levels_version)s as levels_version,
                hour
            from
                seg_players_%(app)s_pq
            where
                hour = unix_timestamp('%(last_seg_date)s')
                and first_session is not null
                and level is not null
                and first_session >= unix_timestamp('%(app_fs)s') * 1000
                and first_session < unix_timestamp('%(last_seg_date)s') * 1000
            ) as sp
        left join
            (
                %(device_region)s
            ) as su
        on
            sp.event_user = su.event_user
        left join dr
        on
            su.device_region = dr.alpha2
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
        """

    params_an_sql = """
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
            to_date(cast(sp.first_session/1000 as timestamp)) as day,
            payer,
            app.id as app_version,
            levels_version,
            -- (case
            --     when
            --         device_region is null
            --     then
            --         "--"
            --     when
            --         LENGTH(device_region) != 2
            --     then
            --         "--"
            --     else
            --         device_region
            -- end) as device_region,
            nvl(dr.num_id, 0) as device_region,
            count(1) as cnt
        from
            (
            select
                event_user,
                first_session,
                level,
                -- decode(payer, 1, 'Payers', 0, 'Non payers') as payer,
                payer,
                app_version,
                %(levels_version)s as levels_version,
                hour
            from
                seg_players_%(app)s_pq
            where
                hour = unix_timestamp('%(last_seg_date)s')
                and first_session is not null
                and level is not null
                and first_session >= unix_timestamp('%(app_fs)s') * 1000
                and first_session < unix_timestamp('%(last_seg_date)s') * 1000
            ) as sp,
            (%(app_version)s) as app
        left join
            (
                %(device_region)s
            ) as su
        on
            sp.event_user = su.event_user
        left join dr
        on
            su.device_region = dr.alpha2
        where
            app_version = cast(app.name as varchar(255))
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
        """

    # print params_sql
    if app in ['3426', '3606', '3444', '3789', '3790', '3791', '3858', '3928']:
        cursor.execute(params_an_sql % {'last_seg_date': last_seg_date,
                                        'app_fs': app_fs,
                                        'app': app,
                                        'levels_version': levels_version,
                                        'app_version': app_version_table(app),
                                        'device_region': device_region % {
                                           'app': app,
                                            'seg_date': last_seg_date,
                                        }})
        # print params_an_sql
    else:
        cursor.execute(params_sql % {'last_seg_date': last_seg_date,
               'app_fs': app_fs, 'app': app, 'levels_version': levels_version,
               'device_region': device_region % {
                    'app': app,
                    'seg_date': last_seg_date,
                    }})

    params = cursor.fetchall()

    params_dict = {}
    for row in params:
        params_dict[tuple(row[:-1])] = row[-1]

    print len(params_dict)

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
            to_date(cast(sp.first_session/1000 as timestamp)) as day,
            payer,
            app_version,
            levels_version,
            -- (case
            --     when
            --         device_region is null
            --     then
            --         "--"
            --     when
            --         LENGTH(device_region) != 2
            --     then
            --         "--"
            --     else
            --         device_region
            -- end) as device_region,
            nvl(dr.num_id, 0) as device_region,
            level,
            count(1) as cnt
        from
            (
            select
                event_user,
                first_session,
                level,
                -- decode(payer, 1, 'Payers', 0, 'Non payers') as payer,
                payer,
                app_version,
                %(levels_version)s as levels_version,
                hour
            from
                seg_players_%(app)s_pq
            where
                hour = unix_timestamp('%(last_seg_date)s')
                and first_session is not null
                and level is not null
                and first_session >= unix_timestamp('%(app_fs)s') * 1000
                and first_session < unix_timestamp('%(last_seg_date)s') * 1000
            ) as sp
         left join
            (
            %(device_region)s
            ) as su
        on
            sp.event_user = su.event_user
        left join dr
        on
            su.device_region = dr.alpha2
        group by
            day,
            payer,
            app_version,
            levels_version,
            device_region,
            level
        order by
            day,
            payer,
            app_version,
            levels_version,
            device_region,
            level
        """

    sql_an = """
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
            to_date(cast(sp.first_session/1000 as timestamp)) as day,
            payer,
            app.id as app_version,
            levels_version,
            -- (case
            --     when
            --         device_region is null
            --     then
            --         "--"
            --     when
            --         LENGTH(device_region) != 2
            --     then
            --         "--"
            --     else
            --         device_region
            -- end) as device_region,
            nvl(dr.num_id, 0) as device_region,
            level,
            count(1) as cnt
        from
            (
            select
                event_user,
                first_session,
                level,
                -- decode(payer, 1, 'Payers', 0, 'Non payers') as payer,
                payer,
                app_version,
                %(levels_version)s as levels_version,
                hour
            from
                seg_players_%(app)s_pq
            where
                hour = unix_timestamp('%(last_seg_date)s')
                and first_session is not null
                and level is not null
                and first_session >= unix_timestamp('%(app_fs)s') * 1000
                and first_session < unix_timestamp('%(last_seg_date)s') * 1000
            ) as sp,
            (%(app_version)s) as app
        left join
            (
            %(device_region)s
            ) as su
        on
            sp.event_user = su.event_user
        left join dr
        on
            su.device_region = dr.alpha2
        where
            app_version = cast(app.name as varchar(255))
        group by
            day,
            payer,
            app_version,
            levels_version,
            device_region,
            level
        order by
            day,
            payer,
            app_version,
            levels_version,
            device_region,
            level
        """
    if app in ['3426', '3606', '3444', '3789', '3790', '3791', '3858', '3928']:
        cursor.execute(sql_an % {
            'last_seg_date': last_seg_date,
            'app_fs': app_fs,
            'app': app,
            'levels_version': levels_version,
            'app_version': app_version_table(app),
            'device_region': device_region % {
                'app': app,
                'seg_date': last_seg_date}})
        # print sql_an
    else:
        cursor.execute(sql % {
            'last_seg_date': last_seg_date,
            'app_fs': app_fs, 'app': app, 'levels_version': levels_version,
            'device_region': device_region % {
                'app': app,
                'seg_date': last_seg_date}})
    res = cursor.fetchall()
    d = {}
    for row in res:
        d[tuple(row[:-1])] = row[-1]
        # print row

    print len(d)

    path_file = '/home/yansen-a/analytics/logs'
    file_name = 'dist_%s' % app
    with open(os.path.join(path_file, file_name + '.csv'), 'w') as f:
        f.write('first_session;payer;app_version;levels_version;device_region;level;count_user;sum_user\n')
        for level in levels:
            for param, cnt in params_dict.items():
                if param + tuple([level]) in d:
                    val = str(d[param + tuple([level])])
                else:
                    val = '0'
                f.write(';'.join(param + tuple([level, val, cnt])) + '\n')

    sql_name = 'dist_%s.sql' % app
    sql_file_name = os.path.join(path_file, sql_name)
    # if app in ['3858']:
    #     sql_file = create_sql_file('distribution_gs', file_name, CUBE_APP[app])
    if app in ['3789', '3790', '3791', '3858']:
        sql_file = create_sql_file(
            'distribution_gs_app_id',
            file_name, CUBE_APP[app], APP_TABLE_PG[app])
    # elif app == '3426':
    #     sql_file = create_sql_file(
    #         'distribution_fd_an', file_name, CUBE_APP[app])
    # elif app == '3606':
    #     sql_file = create_sql_file(
    #         'distribution_fd_am', file_name, CUBE_APP[app])
    elif app in ['3444', '3426', '3606', '3928']:
        sql_file = create_sql_file(
            'distribution_app_id', file_name, CUBE_APP[app], APP_TABLE_PG[app])
    else:
        sql_file = create_sql_file('distribution', file_name, CUBE_APP[app])
    with file(sql_file_name, 'w') as f:
        f.write(sql_file)
    relocation_file(CUBE_APP[app], file_name,
                    sql=sql_file_name, sql_name=sql_name,
                    from_path=os.path.join(path_file, '%s.csv' % file_name))


if __name__ == '__main__':
    # get_last_seg(APPS)
    # get_last_cubes(CUBES)
    for app in APPS:
        print app
        sql = get_insert_table_sql(app)
        if sql:
            execute_pg(sql)
        imp_test(app)
    print 'FINISH'
