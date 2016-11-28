#! coding=utf-8
from datetime import datetime
from common_functions import execute, get_full_list_hour


def get_insert_new_partitions_sql(app, f_hour):
    l_hour = f_hour + 24 * 3600
    return """
        INSERT INTO
            new_seg_players_%(app)s_pq
        (
            event_user,
            first_session,
            last_active,
            payer,
            valid_payer,
            jb,
            level,
            num_chain,
            chain_level,
            level_conv,
            app_version,
            Build_Version,
            Levels_Version,
            Cash_Balance,
            activ_5,
            activ_10,
            activ_20,
            activ_30,
            activ_60
        )
        partition (hour=%(l_hour)s)
        SELECT
            s.event_user,
            s.first_session,
            s.last_active,
            s.payer,
            s.valid_payer,
            s.jb,
            s.level,
            s.num_chain,
            s.chain_level,
            s.level_conv,
            s.app_version,
            cast(a.build_version as varchar(255)) as build_version,
            s.levels_version,
            s.cash_balance,
            s.activ_5,
            s.activ_10,
            s.activ_20,
            s.activ_30,
            s.activ_60
        from
            seg_players_%(app)s_pq s
            left join
            (select
                event_user,
                max(regexp_extract(
                    payload,
                    'BuildVersion\\":\\"([^\\"]*)',
                    1)) as build_version
            from
                all_events_%(app)s_pq
            where
                hour >= %(f_hour)s
                and hour < %(l_hour)s
                and event_type = 'event'
                and parameters ilike '%%Level.%%'
            group by
                event_user
            ) a
        on
            s.hour = %(l_hour)s
            and s.event_user = a.event_user
    """ % {'app': app, 'f_hour': f_hour, 'l_hour': l_hour}


def get_difference_days_list(app):
    new_table = 'new_seg_players_%s_pq' % app
    old_table = 'seg_players_%s_pq' % app
    new_partitons = get_full_list_hour(new_table)
    old_partitions = get_full_list_hour(old_table)
    return sorted(set(old_partitions) - set(new_partitons))


def main():
    app_list = ['3444']
    for app in app_list:
        difference_days_list = get_difference_days_list(app)
        for hour in difference_days_list:
            day = datetime.utcfromtimestamp(hour)
            print(day.strftime('%Y-%m-%d'))
            execute(get_insert_new_partitions_sql(app, hour))


if __name__ == '__main__':
    main()
