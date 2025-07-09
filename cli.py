#!/usr/bin/env python3

import os
import logging
import sentry_sdk
import sys
import tomllib
import re
from zabbix_utils import Sender as ZabbixSender
from queue import Queue, SimpleQueue
from wb_backup2s3_core import BackupWB2S3, SENTRY_DENYLIST
from sentry_sdk.scrubber import EventScrubber
from sentry_sdk.integrations.logging import LoggingIntegration
from logging.handlers import RotatingFileHandler

SCRIPT_HOME = os.path.dirname(os.path.realpath(__file__))
SCRIPT_NAME = 'wb-backup2s3'
CONFIG_FILE = os.path.join(SCRIPT_HOME, 'config.toml')
LOGFILE_PATH = os.path.join(SCRIPT_HOME, SCRIPT_NAME + '.log')

MAX_WORKERS_DEFAULT = 6
ZAB_KEY_HEARTBEAT = 'wb-backup2s3.heartbeat'
ZAB_KEY_EXITCODE = 'wb-backup2s3.exitcode'
ZAB_KEY_UNBACKUPED = 'wb-backup2s3.unbackuped'
ZAB_KEY_FILESSIZE = 'wb-backup2s3.backup_files_size'
ZAB_KEY_BACKUPED = 'wb-backup2s3.backuped'

SENTRY_LOGGING = LoggingIntegration(
    level=logging.DEBUG,  # Capture logs at DEBUG level and above
    event_level=logging.ERROR  # Send events to Sentry for ERROR and above
)

class ZabSender(object):
    def __init__(
            self,
            config_file: str = '/etc/zabbix/zabbix_agentd.conf',
            stub: bool = False,
    ):
        self._stub = stub
        if not self._stub:
            self.logger = logging.getLogger('main.Zabbix_sender')
            zabbix_config = open(config_file).read()
            self._server = re.search(r'ServerActive=(.+)', zabbix_config).group(1)
            self.logger.debug(f"self.server: {self._server}")
            self._hostname = re.search(r'Hostname=(.+)', zabbix_config).group(1)
            self._sender = ZabbixSender(server=self._server)
            self.logger.debug(f"self.hostname: {self._hostname}")

    def send(self, key: str, value: str):
        if not self._stub:
            self.logger.debug(f"Send {key}={value}  to {self._server}")
            resp = self._sender.send_value(
                host=self._hostname,
                key=key,
                value=value,
            )
            return resp

def init_logger(
        debug: bool = False,
        log_name: str = 'main',
        path: str = None,
        max_bytes: int = 5242880,
        backup_count: int = 5,
):
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s: %(message)s'))
    if debug:
        sh.setLevel(logging.DEBUG)
    else:
        sh.setLevel(logging.INFO)
    logger.addHandler(sh)
    if path:
        fh = RotatingFileHandler(
            filename=path,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        if debug:
            fh.setLevel(logging.DEBUG)
        else:
            fh.setLevel(logging.INFO)
        logger.addHandler(fh)
    logger.debug('Set level DEBUG')


def main():
    debug_keys = ['-d', '--debug']
    args = sys.argv[1:]

    init_logger(
        debug=bool(set(args) & set(debug_keys)),
        path=LOGFILE_PATH
    )
    logger = logging.getLogger('main')

    args = [i for i in args if i not in debug_keys]

    zab_sender = ZabSender(stub=True if '--zs' in args else False)

    zab_sender.send(
        key=ZAB_KEY_HEARTBEAT,
        value='1'
    )

    config_file = CONFIG_FILE
    if '-c' in args:
        if len(args) > args.index('-c') + 1:
            config_file = os.path.join(SCRIPT_HOME, args[args.index('-c') + 1])
        else:
            logger.error('Specify the configuration file.')
            return

    with open(config_file, "rb") as f:
        config = tomllib.load(f)


    sentry_sdk.init(
        dsn=config['main'].get('senrty_dns'),
        event_scrubber=EventScrubber(denylist=SENTRY_DENYLIST),
        # integrations=[sentry_logging],
    )

    sentry_sdk.set_tags(
        {
            'script': SCRIPT_NAME,
            'jira': True,
        }
    )
    failed_q = SimpleQueue()
    successful_q = SimpleQueue()

    try:
        wb2s3 = BackupWB2S3(
            tableau_cred=(config['creds']['tableau']['user'], config['creds']['tableau']['pass'], config['creds']['tableau']['url']),
            s3_creds=(config['creds']['aws']['key_id'], config['creds']['aws']['access_key']),
            work_dir=config['main']['workdir'],
            failed_q=failed_q,
            successful_q=successful_q
        )
    except Exception as exp:
        logger.error('Error while init BackupWB2S3')
        logger.exception(exp)
        raise exp
    if config['backup'].get('sites'):
        wb2s3.full_backup(
            max_workers=config['main'].get('max_workers', MAX_WORKERS_DEFAULT),
            excluded_sites=config['backup']['sites'].get('excluded_sites', []),
            site_names= [os.environ['TS_SITE_NAME']] if 'TS_SITE_NAME' in  os.environ else None,
            s3_bucket_name = config['backup']['sites']['s3_bucket_name']
        )

    for projects  in config.get('backup', {}).get('projects'):
        wb2s3.backup_site(
            site_name=projects['site'],
            max_workers=config['main'].get('max_workers', MAX_WORKERS_DEFAULT),
            projects=projects['projects'],
            s3_bucket_name=projects['bucket'],
        )


    zab_sender.send(ZAB_KEY_UNBACKUPED, str(failed_q.qsize()))
    if not failed_q.empty():
        zab_sender.send(ZAB_KEY_EXITCODE, 1)
        print('There was the next errors:')
    else:
        zab_sender.send(ZAB_KEY_EXITCODE, 0)

    logger.info('##### REPORT #####')

    zab_sender.send(ZAB_KEY_BACKUPED, str(successful_q.qsize()))
    all_wb_size = 0

    backed_up_report = []
    while not successful_q.empty():
        wb = successful_q.get()
        backed_up_report.append(f'| {wb.site} | {wb.project} | {wb.name} |')
        # logger.info(f'| {wb.site} | {wb.project} | {wb.name} |')
        all_wb_size += wb.size
    logger.info(f'“Backed up workbooks:\n|| site || project || name ||\n{'\n'.join(backed_up_report)} ')

    failed_report = []
    while not failed_q.empty():
        e, wb = failed_q.get()
        if wb:
            failed_report.append(f'| {wb.site} | {wb.project} | {wb.name} | {wb.id} | {e} |')
            # logger.info(f'| {wb.site} | {wb.project} | {wb.name} | {e} |')
        else:
            logger.info(e)
    logger.info(f'“Failed workbooks:\n|| site || project || name || id || error ||\n{'\n'.join(failed_report)} ')

    zab_sender.send(ZAB_KEY_FILESSIZE, all_wb_size * 1048576)

if __name__ == '__main__':
    main()
