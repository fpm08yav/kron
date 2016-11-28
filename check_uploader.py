#! coding=utf-8
import os
import logging
import logging.config
import sys
import logging.handlers


from smtplib import SMTP_SSL
from email.utils import formatdate
from impala.dbapi import connect
from common_functions import get_full_list_hour, StreamToLogger

APP_LIST = [
    '758', '1170',
    '3444', '3426', '3606',
    '3789', '3790', '3858', '3791',
    '3928'
]
LOG_PATH = '/home/yansen-a/analytics/logs'


class YandexSMTPHandler(logging.handlers.SMTPHandler):
    """
    A handler class which sends an SMTP email for each logging event.
    """

    mailhost = 'smtp.yandex.ru'
    fromaddr = 'log.playrix@yandex.ru'
    username = fromaddr
    password = 'parol123456'

    def __init__(self, toaddrs, subject):
        logging.Handler.__init__(self)
        if isinstance(toaddrs, basestring):
            toaddrs = [toaddrs]
        self.toaddrs = toaddrs
        self.subject = subject
        self._timeout = 5.0

    def getSubject(self, record):
        """
        Determine the subject for the email.

        If you want to specify a subject line which is record-dependent,
        override this method.
        """
        return self.subject

    def emit(self, record):
        """
        Emit a record.

        Format the record and send it to the specified addressees.
        """
        try:
            smtp = SMTP_SSL()
            smtp.connect(self.mailhost)

            msg = self.format(record)
            msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\n\r\n%s" % (
                            self.fromaddr,
                            ",".join(self.toaddrs),
                            self.getSubject(record),
                            formatdate(), msg)
            smtp.login(self.username, self.password)
            smtp.sendmail(self.fromaddr, self.toaddrs, msg)
            smtp.quit()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


def put_count_partition(app, count):
    log_file = os.path.join(LOG_PATH, 'check_uploader_%s.txt' % app)
    with open(log_file, 'w') as f:
        f.write(str(count))


def get_count_partitions_file(app):
    log_file = os.path.join(LOG_PATH, 'check_uploader_%s.txt' % app)
    if not os.path.exists(log_file):
        return 0
    with open(log_file, 'r') as f:
        count = int(f.readline())
    return count


def get_count_partitions_impala(app):
    return len(get_full_list_hour('all_events_%s_pq' % app))


def check_all_events(log):
    error = True
    for app in APP_LIST:
        count_partitions_impala = get_count_partitions_impala(app)
        count_partitions_file = get_count_partitions_file(app)
        if count_partitions_impala != count_partitions_file:
            error = False
            put_count_partition(app, count_partitions_impala)
    if error:
        log.error('Была прекращена загрузка сырых данных')
        # log.error('Uploader')


def main():
    log_level = logging.ERROR
    log = logging.getLogger()
    h2 = YandexSMTPHandler(toaddrs=['yansen_av@playrix.com'], subject='The log')
    h2.setLevel(log_level)
    log.setLevel(log_level)
    log.addHandler(h2)
    h2.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s > %(message)s', '%d.%m.%Y %H:%M:%S'))
    # sl = StreamToLogger(log, log_level)
    # sys.stderr = sl

    check_all_events(log)

if __name__ == '__main__':
    main()
