# -*- coding: utf-8 -*-

from impala.dbapi import connect
from datetime import datetime


def execute_sql(sql):
    con = connect(host='172.16.5.22', protocol='beeswax', port=21000)
    with con:
        cur = con.cursor()
        # cur.execute("set compression_codec=gzip;")
        cur.execute(sql)
        return cur.fetchall()


def get_hours(table):
    hours = []
    sql = "show partitions %s" % table
    inf_t = execute_sql(sql)

    for h in inf_t:
        if not h[0] == "Total":
            hours += [int(h[0])]

    hours = sorted(hours)
    return hours


def create_continuous_hours(table):
    table_hours = get_hours(table)

# min_hour = min(table_hours)
    min_TS = 1450828800
    min_FD = 1452729600
    min_hour = min_FD
    max_hour = max(table_hours)

    continuous_hours = []
    current_hour = min_hour
    while current_hour <= max_hour:
        continuous_hours += [current_hour]
        current_hour += 3600
    return continuous_hours


def find_data_hole(table):
    table_hours = set(get_hours(table))
    continuous_hours = set(create_continuous_hours(table))
    hole_hours = list(continuous_hours - table_hours)
    return sorted(hole_hours)

# datetime.utcfromtimestamp(int(current_hour)
for h in find_data_hole('all_events_758_pq'):
    print h, datetime.utcfromtimestamp(int(h))
    # print h
