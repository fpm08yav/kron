#! coding=utf-8
import os
import psutil
import logging
import psycopg2
import logging.config

from impala.dbapi import connect
from datetime import datetime, timedelta
import ftplib
# from hdfs_manager import HDFSManager

from config import *


def execute(sql):
    conn = connect(host='172.16.5.22', port=21050)
    with conn.cursor() as cursor:
        cursor.execute(sql)


def fetchall(sql):
    conn = connect(host='172.16.5.22', port=21050)
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def print_fetchmany(sql, size=100):
    conn = connect(host='172.16.5.22', port=21050)
    with conn.cursor() as cursor:
        cursor.execute(sql)
        res = cursor.fetchmany(size)
        # print zip(*cursor.description)[0]
        while res:
            for line in res:
                print line
            res = cursor.fetchmany(size)
        # return cursor.fetchall()


def get_last_day_seg_table(table):
    cursor.execute('show partitions %s;' % table)
    partitions = cursor.fetchall()
    partition_list = map(
        lambda partition: int(partition[0]),
        filter(
            lambda partition: partition[0].isdigit(), partitions
        )
    )
    day = datetime.fromtimestamp(max(partition_list)) - timedelta(hours=3)
    return day.strftime('%Y-%m-%d')


def get_full_list_hour(table):
    hours = fetchall('show partitions %s;' % table)
    list_hour = map(lambda hour: int(hour[0]) if hour[0].isdigit() else None, hours)[:-1]
    return sorted(set(list_hour))


def get_list_hour(max_hour, delta_days):
    date = max_hour
    hours = []
    for i in range(delta_days):
        for j in range(24):
            hours.append(date.replace(hour=date.hour+j).replace(day=date.day+i))
    return hours


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


def create_days(day_list):
    lds = map(add_breaket, day_list)
    for el in lds:
        if el == lds[0]:
            el = el[:-1] + " AS id)"
            lds[0] = el
    return ", ".join(lds)


def day_table(day_list):
    table = """
        select
            *
        from
            (values %s) as day
    """ % create_days(day_list)
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
    ids = map(add_breaket, ab_name)
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


def union_files(end_file, path_files, temp_path_files, headers=None, drop_file=False):
    list_file = os.listdir(temp_path_files)
    if list_file:
        list_file.sort()
        with open(os.path.join(path_files, end_file), 'w') as f_w:
            if headers:
                f_w.write(headers)
            for file_name in list_file:
                with open(os.path.join(temp_path_files, file_name), 'r') as f:
                    line_count = range(sum(1 for l in open(os.path.join(temp_path_files, file_name), 'r')))
                    for i in line_count:
                        line = f.readline()
                        f_w.write(line)
        if drop_file:
            for file_name in list_file:
                os.remove(file_name)


def get_difference_days2(first_table, second_table, first_hour=None):
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
    for i in range(len(second_table_p)-1):
        hours = list()
        for hour in first_table_p:
            if hour >= second_table_p[i] and hour < second_table_p[i+1]:
                hours.append(hour)
        if len(hours) < 24:
            result.append([second_table_p[i], second_table_p[i+1]])
    result.reverse()
    return result


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


if __name__ == '__main__':
    # hm = HDFSManager(is_local=False)
    # path = '/user/hive/warehouse/ab_test_20160714'
    # hm.copyFromLocal('/home/yansen-a/Загрузки/ab_test_20160714.csv', path)
    # union_files('test.txt', '', './test/', headers=None, drop_file=False)
    # conn = connect(host='172.16.5.22', port=21050)
    # with conn.cursor() as cursor:
        # hours = get_full_list_hour('all_events_3444_pq')
        # print len(hours)
        # for hour in hours:
        #     count = fetchall("""
        #         select
        #             count(1)
        #         from
        #             all_events_3444_pq
        #         where
        #             hour = %s
        #     """ % hour)
        #     # print count[0][0]

        #     distinct_count = fetchall("""
        #         select
        #             count(distinct event_id)
        #         from
        #             all_events_3444_pq
        #         where
        #             hour = %s
        #     """ % hour)
        #     # print distinct_count[0][0]
        #     if count != distinct_count:
        #         print hour, count[0][0], distinct_count[0][0]


    # #     hours = get_difference_days2('boosts_3444_pq_test_s', 'seg_players_3444_pq')
    # #     for hour1, hour2 in hours:
    # #         day1 = datetime.fromtimestamp(hour1) - timedelta(hours=3)
    # #         day2 = datetime.fromtimestamp(hour2) - timedelta(hours=3)
    # #         print day1.strftime('%Y-%m-%d'), day2.strftime('%Y-%m-%d')
    #     cursor.execute('show partitions all_events_3789_pq;')
    #     partitions = cursor.fetchall()
    #     difference_days = map(
    #         lambda hour: int(hour[0]),
    #         filter(
    #             lambda hour: hour[0].isdigit(), partitions
    #         )
    #     )
    #     difference_days.sort()
        # difference_days.reverse()
# #         # difference_days1 = map(
# #         #     lambda hour: int(hour[0]),
# #         #     filter(
# #         #         lambda hour: hour[0].isdigit(), partitions
# #         #     )
# #         # )
# #         # difference_days1.sort()

# #         # cursor.execute('show partitions new_all_events_3444_pq;')
# #         # partitions = cursor.fetchall()
# #         # difference_days2 = map(
# #         #     lambda hour: int(hour[0]),
# #         #     filter(
# #         #         lambda hour: hour[0].isdigit(), partitions
# #         #     )
# #         # )
# #         # difference_days2.sort()
# #         # coll = set(difference_days2)
# #         # result = [e for e in difference_days2 if e in coll and not coll.discard(e)]
# #         # print len(result), len(difference_days1)
# #         # print len(difference_days)
#         for days in difference_days:
#             day = datetime.fromtimestamp(days) - timedelta(hours=3)
#             print day.strftime('%Y-%m-%d %H:%M'), difference_days[-1]
# #        file = 'test.csv'
# #        path_files = './test'
# #        temp_path_files = './test/temp'
# #        headers = 'year;day;month;n;sum_active;count_active'
# #        drop_file = False
# #        union_files(file, path_files, temp_path_files)

#         # sql = """
#         #     insert into all_events_3444_test_s_pq
#         #     select (
#         #         event_id,
#         #         app_version,
#         #         payload,
#         #         device_id,
#         #         event_time,
#         #         parameters,
#         #         event_user,
#         #         seqnum,
#         #         hour,
#         #         event_type
#         #     ) from all_events_3444_pq
#         #     where hour between unix_timestamp('2016-04-01') and unix_timestamp('2016-04-05');
#         # """
#         # cursor.execute(sql)
    # h = get_full_list_hour('all_events_758_pq')[0]
    # # for h in hh:
    # print h, datetime.utcfromtimestamp(int(h))

    # import subprocess
    # p = subprocess.Popen(['./test.sh', '1'], stdout=subprocess.PIPE)

    # from subprocess import Popen, PIPE
    # proc = Popen(
    #     [
    #         "sshpass -p '123456' scp -r /home/yansen-a/analytics/test2.txt yansen-a@172.16.1.139:/home/yansen-a"
    #         # "sshpass -p '123456' scp -r /home/yansen-a/analytics/test2.txt ivanov-i@172.16.0.169:/home/ivanov-i"
    #     ],
    #     # [
    #     #     "sshpass",
    #     #     "-p",
    #     #     "'123456'",
    #     #     "scp",
    #     #     "-r",
    #     #     "/home/yansen-a/analytics/test1.txt",
    #     #     "playrix@172.16.1.139:/home/yansen-a/"
    #     # ],
    #     # ["ls", "-l"],
    #     shell=True,
    #     stdout=PIPE, stderr=PIPE
    # )
    # proc.wait()    # дождаться выполнения
    # res = proc.communicate()  # получить tuple('stdout', 'stderr')
    # if proc.returncode:
    #     print res[1]
    # print 'result:', res[0]

    # logging.basicConfig(format=u'%(filename)s [LINE:%(lineno)d] [%(asctime)s] > %(message)s', level=logging.DEBUG)
    # logging.info(1)

    # sql = """
    #     with payload as
    #     (
    #         select
    #             t.event_id,
    #             t.event_user,
    #             t.chain,
    #             t.level,
    #             t.level_version,
    #             t.ab_group,
    #             t.event_name,
    #             t.reason,
    #             t.free_coins,
    #             t.real_coins,
    #             t.all_boosts
    #         from
    #             (
    #                 select
    #                     p.event_id,
    #                     p.event_user,
    #                     p.chain,
    #                     p.level,
    #                     p.level_version,
    #                     p.ab_group,
    #                     p.event_name,
    #                     p.reason,
    #                     sum(nvl(p.free_coins, 0)) as free_coins,
    #                     sum(nvl(p.real_coins, 0)) as real_coins,
    #                     sum(nvl(p.boost_count, 0)) + sum(nvl(p.at_count, 0)) + sum(nvl(p.am_count, 0)) as all_boosts
    #                 from
    #                     (
    #                         select
    #                             p.event_id,
    #                             p.event_user,
    #                             decode(instr(parameters, 'Level.Failed'), 0, 1, 2) as event_name,
    #                             0 as chain,
    #                             cast(regexp_extract(payload, 'Level\\":\\"(\\\\d+)', 1) as int) as level,
    #                             cast(regexp_extract(payload, 'FreeCoinsBalance\\":\\"(\\\\d+)', 1) as int) as free_coins,
    #                             cast(regexp_extract(payload, 'RealCoinsBalance\\":\\"(\\\\d+)', 1) as int) as real_coins,
    #                             cast(regexp_extract(payload, 'Boosts\\":\\"(\\\\d+)', 1) as int) as boost_count,
    #                             cast(regexp_extract(payload, 'AdditionalTimes\\":\\"(\\\\d+)', 1) as int) as at_count,
    #                             cast(regexp_extract(payload, 'AdditionalMoves\\":\\"(\\\\d+)', 1) as int) as am_count,
    #                             cast(nvl(regexp_extract(payload, 'LevelsVersion\\":\\"([^\\"]+)', 1), "NOT_INSTALLED") as VARCHAR(255)) as level_version,
    #                             '--' as ab_group,
    #                             regexp_extract(payload, 'Reason\\":\\"(\\\\w+)', 1) as reason
    #                         from
    #                             all_events_3789_pq p
    #                         where
    #                             p.hour >= %(hour_gte)s
    #                             and p.hour < %(hour_lt)s
    #                             and p.event_type = 'event'
    #                             and (p.parameters ilike '%%Level.Complete%%' or p.parameters ilike '%%Level.Failed%%')
    #                     ) p
    #                 group by p.event_id, p.event_user, p.chain, p.level, p.level_version, p.ab_group, p.event_name, p.reason
    #                 union all
    #                 (
    #                     select
    #                         p.event_id,
    #                         p.event_user,
    #                         p.chain,
    #                         p.level,
    #                         p.level_version,
    #                         p.ab_group,
    #                         p.event_name,
    #                         p.reason,
    #                         sum(nvl(p.free_coins, 1)) as free_coins,
    #                         sum(nvl(p.real_coins, 1)) as real_coins,
    #                         sum(p.boost_count) + sum(p.at_count) + sum(p.am_count) as all_boosts
    #                     from
    #                         (
    #                             select
    #                                 p.event_id,
    #                                 p.event_user,
    #                                 decode(instr(parameters, 'Chains.Level.Failed'), 0, 1, 2) as event_name,
    #                                 cast(regexp_extract(payload, 'LevelChain\\":\\"(\\\\d+)', 1) as int) as chain,
    #                                 cast(regexp_extract(payload, 'Level\\":\\"(\\\\d+)', 1) as int) as level,
    #                                 cast(regexp_extract(payload, 'FreeCoinsBalance\\":\\"(\\\\d+)', 1) as int) as free_coins,
    #                                 cast(regexp_extract(payload, 'RealCoinsBalance\\":\\"(\\\\d+)', 1) as int) as real_coins,
    #                                 cast(regexp_extract(payload, 'Boosts\\":\\"(\\\\d+)', 1) as int) as boost_count,
    #                                 cast(regexp_extract(payload, 'AdditionalTimes\\":\\"(\\\\d+)', 1) as int) as at_count,
    #                                 cast(regexp_extract(payload, 'AdditionalMoves\\":\\"(\\\\d+)', 1) as int) as am_count,
    #                                 cast(nvl(regexp_extract(payload, 'LevelsVersion\\":\\"([^\\"]+)', 1), "NOT_INSTALLED") as VARCHAR(255)) as level_version,
    #                                 '--' as ab_group,
    #                                 regexp_extract(payload, 'Reason\\":\\"(\\\\w+)', 1) as reason
    #                             from
    #                                 all_events_3789_pq p
    #                             where
    #                                 p.hour >= %(hour_gte)s
    #                                 and p.hour < %(hour_lt)s
    #                                 and p.event_type = 'event'
    #                                 and (p.parameters ilike '%%Chains.Level.Complete%%' or p.parameters ilike '%%Chains.Level.Failed%%')
    #                         ) p
    #                     group by p.event_id, p.event_user, p.chain, p.level, p.level_version, p.ab_group, p.event_name, p.reason
    #                 )
    #             ) t
    #     )

    #     select
    #         day,
    #         app_version,
    #         level_version,
    #         ab_group,
    #         device_region,
    #         level_number,
    #         chain_number,
    #         payer,
    #         /*sum(win_count) as win_count,
    #         sum(fail_count) as fail_count,*/
    #         round(sum(df), 2) as value,
    #         sum(count_user) as count,
    #         round(variance(df), 2) as variance
    #     from(
    #         select
    #             to_date(from_unixtime(%(hour_gte)s)) as day,
    #             c.app_version as app_version,
    #             p.level_version,
    #             p.chain as chain_number,
    #             p.level as level_number,
    #             cast(p.ab_group as VARCHAR(255)) as ab_group,
    #             cast(s.device_region as VARCHAR(255)) as device_region,
    #             c.payer as payer,
    #             p.event_user as event_user,
    #             cast(count(distinct p.event_user) as INT) as count_user,
    #             sum(decode(p.event_name, 1, 1, 0)) as win_count,
    #             sum(decode(p.event_name, 2, decode(p.reason, 'Fail', 1, 0), 0)) as fail_count,
    #             zeroifnull(sum(decode(p.event_name, 2, decode(p.reason, 'Fail', 1, 0), 0))/(
    #                 nullif(sum(decode(p.event_name, 1, 1, 0)) + sum(decode(p.event_name, 2, decode(p.reason, 'Fail', 1, 0), 0)), 0)
    #             )) df
    #         from
    #             payload p,
    #             seg_players_3789_pq c
    #             left join
    #             (
    #                 SELECT
    #                     event_user,
    #                     MAX(device_region) AS device_region
    #                 FROM
    #                     seg_users_3789_pq
    #                 WHERE
    #                     hour = unix_timestamp(days_add(to_date(from_unixtime(%(hour_gte)s)), 1))
    #                 GROUP BY
    #                     event_user
    #             ) AS s
    #             ON c.event_user = s.event_user
    #         where
    #             c.hour = unix_timestamp(days_add(to_date(from_unixtime(%(hour_gte)s)), 1))
    #             and c.last_active is not Null
    #             and c.first_session is not Null
    #             and c.level is not null
    #             and c.event_user = p.event_user
    #         group by
    #             day, c.app_version, level_version,
    #             chain_number, level_number, ab_group, s.device_region,
    #             c.payer, p.event_user
    #         order by
    #             chain_number, level_number, ab_group, app_version, level_version, device_region, payer
    #     ) s
    #     group by
    #         day, app_version, level_version, chain_number, level_number,
    #         ab_group, device_region, payer
    #     order by
    #         chain_number, level_number, ab_group, app_version, level_version, device_region, payer

    # """ % {
    #     'hour_gte': 1466985600,
    #     'hour_lt': 1467072000
    # }
    # # print fetchall(sql)
    # size = 100
    # file_name = '/home/yansen-a/analytics/logs/variance.csv'
    # conn = connect(host='172.16.5.22', port=21050)
    # with conn.cursor() as cursor:
    #     with open(file_name, 'w') as f:
    #         f.write('day;app_version;level_version;ab_group;device_region;level_number;chain_number;payer;value;count;variance\n')
    #         # cursor.execute(sql)
    #         # res = cursor.fetchmany(size)
    #         res = fetchall(sql)
    #         for row in res:
    #             f.write('%s\n' % ';'.join(row))
    #             # res = cursor.fetchmany(size)
    # sql = """
    # with dr as (
    #     select
    #         row_number() over (order by alpha2) as num_id,
    #         alpha2
    #     from
    #         device_region
    #     order by
    #         alpha2
    # )
    # select
    #     --if(num_id is null, 0, num_id),
    #     num_id,
    #     device_region
    # from
    #     (
    #         select
    #             distinct device_region
    #         from
    #             boosts_3444_pq_test_s
    #         where
    #             hour = 1466467200
    #     ) as w
    #     right join dr
    #     on
    #         w.device_region = dr.alpha2
    # where w.device_region is null

    # """
    # print fetchall(sql)
    # size = 100
    # file_name = '/home/yansen-a/analytics/logs/device_region.csv'
    # conn = connect(host='172.16.5.22', port=21050)
    # with conn.cursor() as cursor:
    #     with open(file_name, 'w') as f:
    #         f.write('id;alpha2\n')
    #         res = fetchall(sql)
    #         for row in res:
    #             f.write('%s\n' % ';'.join(row))
    # обновляем

    # def get_ab_group_list_new_sql():
    #     sql = """
    #         SELECT
    #             ab_names
    #         FROM
    #             ab_group_%(app)s_pq
    #         WHERE
    #             hour = %(hour)s
    #         ORDER BY
    #             ab_names
    #     """ % {
    #         'app': APP,
    #         'hour': get_full_list_hour(
    #             'ab_group_%(app)s_pq' % {'app': APP}
    #         )[-1]}
    #     return sql

    # def get_ab_group_list_old_sql():
    #     sql = """
    #         SELECT
    #             ab_names
    #         FROM
    #             ab_group_%(app)s
    #         ORDER BY
    #             ab_names
    #     """ % {'app': APP}
    #     return sql

    # def get_create_emty_table_sql():
    #     sql = """
    #         CREATE TABLE IF NOT EXISTS
    #             ab_group_%(app)s
    #         (id SMALLINT, ab_names VARCHAR(255))
    #         LOCATION '/user/hive/warehouse/ab_group_3444';
    #     """ % {'app': APP}
    #     return sql

    # def get_ab_group_new_sql():
    #     sql = """
    #         SELECT
    #             new_id,
    #             ab_names
    #         from
    #             (
    #                 select
    #                     row_number() over (order by id, ab_names) as new_id,
    #                     ab_names,
    #                     id as old_id
    #                 from
    #                     (
    #                         select
    #                             ab.id,
    #                             pq.ab_names
    #                         from
    #                             ab_group_%(app)s ab
    #                         right join
    #                             (
    #                                 select
    #                                     ab_names
    #                                 from
    #                                     ab_group_%(app)s_pq
    #                                 where
    #                                     hour = 1467504000
    #                             ) pq
    #                         on ab.ab_names = pq.ab_names
    #                     ) s
    #             ) ab
    #         where old_id is null
    #     """ % {'app': APP}
    #     return sql

    # def get_ab_group_new():
    #     fetchall(get_create_emty_table_sql())
    #     ab_group_list_new = fetchall(get_ab_group_new_sql())
    #     return ab_group_list_new

    # def get_update_ab_group_table_sql():
    #     sql = """
    #         SELECT
    #             ROW_NUMBER() OVER (ORDER BY id, ab_names) AS id,
    #             ab_names
    #         FROM
    #             (SELECT
    #                 ab.id,
    #                 pq.ab_names
    #             FROM
    #                 ab_group_%(app)s ab
    #             RIGHT JOIN
    #                 (
    #                     SELECT
    #                         ab_names
    #                     FROM
    #                         ab_group_%(app)s_pq
    #                     WHERE
    #                         hour = %(hour)s
    #                 ) pq
    #             ON ab.ab_names = pq.ab_names) s
    #     """ % {
    #         'app': APP,
    #         'hour': get_full_list_hour(
    #             'ab_group_%(app)s_pq' % {'app': APP}
    #         )[-1]}
    #     return sql

    # def get_select_ab_group_sql():
    #     sql = """
    #         SELECT
    #             id,
    #             ab_names
    #         from
    #             ab_group_%(app)s
    #     """ % {'app': APP}
    #     return sql


    # def get_ab_groups_i_sql():
    #     return """
    #         SELECT
    #             ab_names
    #         from
    #             ab_group_3444_pq
    #         where
    #             hour = %(hour)s
    #     """ % {
    #         'hour': get_full_list_hour(
    #             'ab_group_%(app)s_pq' % {'app': APP}
    #         )[-1]}


    # conn = psycopg2.connect(host='127.0.0.1',
    #                         port=5432,
    #                         user='postgres',
    #                         password='postgres',
    #                         database='cubes')
    # with conn.cursor() as cursor:
    #     cursor.execute(get_ab_groups_p())
    #     ab_groups_p = cursor.fetchall()
    #     ab_groups_names = map(lambda names: names[1], ab_groups_p)
    # conn = connect(host='172.16.5.22', port=21050)
    # with conn.cursor() as cursor:
    #     cursor.execute(get_ab_groups_i_sql())
    #     ab_groups_i = cursor.fetchall()
    # if ab_groups_i != ab_groups_names:
    #     sql = get_new_ab_groups_sql(ab_groups_p)
    #     print sql
    #     conn = connect(host='172.16.5.22', port=21050)
    #     with conn.cursor() as cursor:
    #         cursor.execute(sql)
    #         new_ab_group = cursor.fetchall()
    #     sql = get_insert_ab_group_sql(new_ab_group)

    #     conn = psycopg2.connect(host='127.0.0.1',
    #                             port=5432,
    #                             user='postgres',
    #                             password='postgres',
    #                             database='cubes')
    #     with conn.cursor() as cursor:
    #         cursor.execute(sql)
    #     print sql

    # def execute_post(sql):
    #     conn = psycopg2.connect(host='localhost',
    #                             port=5432,
    #                             user='postgres',
    #                             password='postgres',
    #                             database='cubes')
    #     with conn.cursor() as cursor:
    #         cursor.execute(sql)
    #         conn.commit()

    # def fetchall_post(sql):
    #     conn = psycopg2.connect(host='localhost',
    #                             port=5432,
    #                             user='postgres',
    #                             password='postgres',
    #                             database='cubes')
    #     with conn.cursor() as cursor:
    #         cursor.execute(sql)
    #         return cursor.fetchall()

    # def add_breakets(el):
    #     s = ""
    #     for i in el:
    #         s += "(%s, cast('%s' as varchar(255))), " % (i[0], i[1])
    #     return s[:-2]

    # def get_values(ab_group_list):
    #     sql = '(%(id)s as id, "%(name)s" as name), %(other)s' % {
    #         'id': ab_group_list[0][1],
    #         'name': ab_group_list[0][0],
    #         'other': ','.join(
    #             map(
    #                 lambda i: '(%s, "%s")' % (i[1], i[0]),
    #                 ab_group_list[1:]))
    #     }
    #     return sql

    # def get_element_sql(name):
    #     return """
    #         SELECT
    #             name,
    #             id
    #         from
    #             %s_fd
    #     """ % name

    # def list_element(name):
    #     return fetchall_post(get_element_sql(name))

    # def get_new_ab_groups_sql():
    #     ab_groups = list_element('ab_group')
    #     if ab_groups:
    #         return """
    #             SELECT
    #                 new_id,
    #                 name
    #             from
    #                 (
    #                 SELECT
    #                     row_number() over (order by id, name) as new_id,
    #                     name,
    #                     id as old_id
    #                 from
    #                     (
    #                     SELECT
    #                         id,
    #                         pq.name
    #                     from
    #                         (values %(ab_groups)s) ab
    #                     right join
    #                     (
    #                         SELECT
    #                             ab_names as name
    #                         from
    #                             ab_group_3444_pq
    #                         where
    #                             hour = %(seg_hour)s
    #                     ) pq
    #                     on ab.name = pq.name
    #                 ) s
    #             ) s2
    #             where old_id is null
    #         """ % {
    #             'ab_groups': get_values(ab_groups),
    #             'seg_hour':  get_full_list_hour('seg_users_3444_pq')[-1]
    #         }
    #     else:
    #         return """
    #              SELECT
    #                 row_number() over (order by name) as new_id,
    #                 name
    #             from
    #                 (
    #                     SELECT
    #                         ab_names as name
    #                     from
    #                         ab_group_3444_pq
    #                     where
    #                         hour = %(seg_hour)s
    #                 ) s
    #         """ % {'seg_hour':  get_full_list_hour('seg_users_3444_pq')[-1]}

    # def get_new_app_version_sql():
    #     app_version = list_element('app_version')
    #     if app_version:
    #         return """
    #             SELECT
    #                 new_id,
    #                 name
    #             from
    #                 (
    #                 SELECT
    #                     row_number() over (order by id, name) as new_id,
    #                     name,
    #                     id as old_id
    #                 from
    #                     (
    #                     SELECT
    #                         id,
    #                         pq.name
    #                     from
    #                         (values %(app_version)s) ab
    #                     right join
    #                     (
    #                         SELECT distinct
    #                             app_version as name
    #                         from
    #                             seg_players_3444_pq
    #                         where
    #                             hour = %(seg_hour)s
    #                             and level is not null
    #                     ) pq
    #                     on ab.name = pq.name
    #                 ) s
    #             ) s2
    #             where old_id is null
    #         """ % {
    #             'app_version': get_values(app_version),
    #             'seg_hour':  get_full_list_hour('seg_users_3444_pq')[-1]
    #         }
    #     else:
    #         return """
    #              SELECT
    #                 row_number() over (order by name) as new_id,
    #                 name
    #             from
    #                 (
    #                     SELECT distinct
    #                         app_version as name
    #                     from
    #                         seg_players_3444_pq
    #                     where
    #                         hour = %(seg_hour)s
    #                         and level is not null
    #                 ) s
    #         """ % {'seg_hour':  get_full_list_hour('seg_users_3444_pq')[-1]}

    # def get_new_element(name):
    #     if name == 'ab_group':
    #         return fetchall(get_new_ab_groups_sql())
    #     elif name == 'app_version':
    #         return fetchall(get_new_app_version_sql())

    # def get_insert_table_sql(name):
    #     new_element = add_breakets(get_new_element(name))

    #     if new_element:
    #         return """
    #             INSERT INTO
    #                 %(table_name)s_fd
    #             (id, name)
    #             VALUES
    #             %(new_element)s
    #         """ % {
    #             'new_element': new_element,
    #             'table_name': name}
    #     return None

    # def get_create_table_sql(name):
    #     return """
    #         CREATE TABLE IF NOT EXISTS %s_fd
    #         (
    #           id smallint,
    #           name varchar(255)
    #         )
    #     """ % name

    # def try_create_table(name):
    #     execute_post(get_create_table_sql(name))

    # def try_update_table(name):
    #     try_create_table(name)
    #     new_element = get_insert_table_sql(name)
    #     if new_element:
    #         execute_post(new_element)

    # def get_dict(name):
    #     try_update_table(name)
    #     return dict(list_element(name))

    # ab_group_dict = get_dict('ab_group')
    # print ab_group_dict

    # app_version_dict = get_dict('app_version')
    # print app_version_dict
    # print get_full_list_hour('all_events_758_pq')[-1]
    # host = '172.16.1.139'
    # user = 'yansen-a'
    # password = 'Ph@enix0'

    # host = '172.16.5.28'
    # user = 'playrix'
    # password = '123456'

    # from paramiko import Transport, SFTPClient
    # trans = Transport((host, 22))
    # trans.connect(username=user, password=password)
    # sftp = SFTPClient.from_transport(trans)

    # localpath = '/home/yansen-a/analytics/local_cubes/temp/cube_two/2016-08-01.csv'
    # remotepath = '/tmp/test/2016-08-01.csv.temp'

    # sftp.
    # sftp.put(localpath, remotepath)
    # sftp.rename(remotepath, '/tmp/test/2016-08-01.csv')
    # print sftp.listdir('/tmp/test')
#------------------------------------------------------------------------------
    # a = get_full_list_hour('all_events_758_pq')
    # v = get_full_list_hour('valid_iap_758_pq')
    # s = sorted(set(a) - set(v))
    # sql = """
    #     select
    #         from_unixtime(h), h
    #     from
    #         (%s)s
    #         """ % 'values(%s as h), (%s)' % (s[0], '),('.join(
    #             map(lambda x: str(x), s[1:])
    #             ))
    # print sql
    # result = fetchall(sql)
    # for r in result:
    #     print r

#------------------------------------------------------------------------------
    # def create_sql_file(sql_name, file_name, cube_name):
    #     sql = ''
    #     with file(os.path.join(CSV_PATH, '%s.sql' % sql_name), 'r') as f:
    #         for line in f:
    #             sql += line
    #     return sql % {'file_name': file_name, 'cube_name': cube_name}

    # with file('1.sql', 'w') as f:
    #     f.write(create_sql_file('distribution_app', 'dist_3444', 'cube_distribution'))
    # print create_sql_file('distribution_app', 'dist_3444', 'cube_distribution')

    # PARENT_PATH = '/home/playrix/seg_tables'
    # PATH = {
    #     'LOCK_PATH': 'lock',
    #     'LOG_PATH': 'log'
    # }
    # print map(lambda path: os.path.join(PARENT_PATH, path), PATH.values())
    # for path in PATH.values():
    #     print os.path.join(PARENT_PATH, path)

    # logging.basicConfig(format=u'%(filename)s [LINE:%(lineno)d] [%(asctime)s] > %(message)s', level=logging.DEBUG)
    # logging.basicConfig(format=u'[%(asctime)s] - %(levelname)s - [%(filename)s:%(lineno)d] > %(message)s', datefmt=u'%d.%m.%Y %H:%M:%S', level=logging.DEBUG)
    # logger = logging.getLogger()
    # logger.setLevel(logging.DEBUG)
    # logger.info(1)
    # print psutil.Process(os.getpid()).status() == psutil.STATUS_RUNNING


    # def get_list_loaded_hours(path):
    #     h_list = []
    #     with open(path, 'r') as f:
    #         h_list = map(
    #             lambda hour: int(hour),
    #             filter(
    #                 lambda hour: line.rstrip('\n').isdigit(),
    #                 [line for line in f]))
    #     return h_list

    # def put_list_loaded_hours(path, h_list):
    #     if not isinstance(h_list, list) and isinstance(h_list, int):
    #         h_list = [h_list]
    #     if isinstance(h_list, list):
    #         with open(path, 'a') as f:
    #             for hour in h_list:
    #                 if isinstance(hour, int):
    #                     f.write('%s\n' % hour)
    #                 else:
    #                     return False
    #         return True
    #     return False

    # print get_list_loaded_hours('1.txt')
    # print put_list_loaded_hours('1.txt', [238, 5])


    # EVENT_TYPE_LIST = ['currency_given', 'event', 'iap', 'purchase',
    #                    'session_end', 'session_start', 'user']
    # def get_delete_parquet_sql2(app, table, log_hour, event_type):
    #         app_code = app.split('-')[1]
    #         sql = """
    #             alter table %s_%s_pq
    #             drop partition (hour=%s, event_type=%s)
    #         """ % (table,
    #                app_code,
    #                log_hour,
    #                event_type)
    #         return sql


    # for event_type in EVENT_TYPE_LIST:
    #     for hour in [1471917600, 1471680000, 1471687200]:
    #         event_type_c = "cast('%s' as VARCHAR(255))" % (
    #             event_type)
    #         sql = get_delete_parquet_sql2('app-758', 'all_events', hour, event_type_c)
    #         print sql



    # APP_LIST = ['3790', '3858', '3928', '3789', '3444', '3606', '3426', '758']

    # def execute_post(sql):
    #     conn = psycopg2.connect(
    #                             host='172.16.1.64',
    #                             port=5432,
    #                             user='postgres',
    #                             password='postgres',
    #                             database='cubes')
    #     with conn.cursor() as cursor:
    #         cursor.execute(sql)
    #         conn.commit()


    # def fetchall_post(sql):
    #     conn = psycopg2.connect(
    #                             host='172.16.1.64',
    #                             port=5432,
    #                             user='postgres',
    #                             password='postgres',
    #                             database='cubes')
    #     with conn.cursor() as cursor:
    #         cursor.execute(sql)
    #         return cursor.fetchall()


    # def add_breakets(el):
    #     s = ""
    #     for i in el:
    #         s += "(%s, cast('%s' as varchar(255)), cast('%s' as varchar(255))), " % (
    #             i[0], i[1], i[1])
    #     return s[:-2]


    # def get_values(ab_group_list):
    #     return '(%(id)s as id, "%(name)s" as name, "%(name)s" as description)%(other)s' % {
    #         'id': ab_group_list[0][1],
    #         'name': ab_group_list[0][0],
    #         'other': ', %s' % ','.join(
    #             map(
    #                 lambda i: '(%s, "%s", "%s")' % (i[1], i[0], i[0]),
    #                 ab_group_list[1:])) if ab_group_list[1:] else ''
    #     }


    # def get_element_sql(name):
    #     return """
    #         SELECT
    #             name,
    #             id
    #         from
    #             %s
    #     """ % name


    # def get_list_element(name):
    #     return fetchall_post(get_element_sql(name))


    # def get_new_publisher_sql():
    #     elements = get_list_element('publisher')
    #     if elements:
    #         return """
    #             SELECT
    #                 new_id,
    #                 name
    #             from
    #                 (
    #                 SELECT
    #                     row_number() over (order by id, name) as new_id,
    #                     name,
    #                     id as old_id
    #                 from
    #                     (
    #                     SELECT
    #                         id,
    #                         pq.name
    #                     from
    #                         (values %(elements)s) el
    #                     right join
    #                     (
    #                         SELECT
    #                             publisher as name
    #                         from
    #                             mat_device_id
    #                         group by
    #                             publisher
    #                     ) pq
    #                     on el.name = pq.name
    #                 ) s
    #             ) s2
    #             where old_id is null
    #         """ % {
    #             'elements': get_values(elements)
    #         }
    #     else:
    #         return """
    #              SELECT
    #                 row_number() over (order by name) as new_id,
    #                 name
    #             from
    #                 (
    #                     SELECT
    #                         publisher as name
    #                     from
    #                         mat_device_id
    #                     group by
    #                             publisher
    #                 ) s
    #         """


    # def get_new_element(name, gte_hour=None, lt_hour=None):
    #     if name == 'publisher':
    #         return fetchall(get_new_publisher_sql())


    # def get_insert_table_sql(name, gte_hour=None, lt_hour=None):
    #     new_element = add_breakets(get_new_element(name, gte_hour, lt_hour))

    #     if new_element:
    #         return """
    #             INSERT INTO
    #                 %(table_name)s
    #             (id, name, description)
    #             VALUES
    #             %(new_element)s;
    #         """ % {
    #             'new_element': new_element,
    #             'table_name': name}
    #     return None


    # def get_create_table_sql(name):
    #     return """
    #         CREATE TABLE IF NOT EXISTS %s
    #         (
    #           id smallint,
    #           name varchar(255),
    #           description varchar(255)
    #         )
    #     """ % name


    # def try_create_table(name):
    #     execute_post(get_create_table_sql(name))
    #     try_update_table(name)


    # def try_update_table(name, gte_hour=None, lt_hour=None):
    #     new_element = get_insert_table_sql(name, gte_hour, lt_hour)
    #     if new_element:
    #         execute_post(new_element)

    # try_create_table('publisher')

    # def get_publisher_list():
    #     publisher = []
    #     for app in APP_LIST:
    #         sql = """
    #             select
    #                 publisher
    #             from
    #                 mat_device_id_%s_pq
    #             group by publisher
    #         """ % app
    #         publishers = fetchall(sql)
    #         for publ in publishers:
    #             publisher.append(publ[0])
    #     return sorted(publisher)

    # print get_publisher_list()

    # print get_full_list_hour('all_events_3444_pq')[-1]
    # 1472403600, 1472403600, 1472306400

    # def get_hours(table):
    #     hours = []
    #     sql = "show partitions %s" % table
    #     inf_t = fetchall(sql)

    #     for h in inf_t:
    #         if not h[0] == "Total":
    #             hours += [int(h[0])]

    #     hours = sorted(hours)
    #     return hours

    # print get_hours('all_events_3444_pq')[-1]

    TABLE_WIN_FAIL = 'win_fail_3789_pq'

    sql = """
        SELECT
            chain
        from
            %(table)s
        group by
            chain
        order by
            chain
    """ % {'table': TABLE_WIN_FAIL}
    print fetchall(sql)
    chain_list = [chain[0] for chain in fetchall(sql)]
    print chain_list
    cds = map(add_breaket, chain_list)
    for el in cds:
        if el == cds[0]:
            el = el[:-1] + " AS id)"
            cds[0] = el
    print cds
