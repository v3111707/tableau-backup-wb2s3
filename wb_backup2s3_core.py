import boto3
import logging
import os
import botocore
import datetime
import json
import sentry_sdk
from dataclasses import dataclass
from urllib import parse
import tableauserverclient as TSC
from concurrent.futures import ThreadPoolExecutor
from queue import SimpleQueue
from tableauserverclient.models.workbook_item import WorkbookItem
from sentry_sdk import add_breadcrumb
from sentry_sdk.scrubber import DEFAULT_DENYLIST
from botocore.client import Config

SENTRY_DENYLIST = DEFAULT_DENYLIST + [
    'access_key',
    'key_id',
    's3_creds',
    'tab_pass',
    'tableau_cred',
]

@dataclass
class Workbook:
    name: str
    id: str
    project: str
    size: int


class BackupWB2S3:
    """Downloads workbooks from a Tableau Server and uploads them to AWS. "
    ...

    Attributes
    ----------
    tableau_cred : tuple
        tuple with tableau credentials
        Example: (username, password, url)

    s3_creds : tuple
        tuple with AWS credentials
        Example: (key_id, access_key, bucket name)

    work_dir: str
        Folder where script downloads files before uploading to AWS S3.


    """
    loger_name = 'main.BackupWB2S3'
    s3_upload_state_file = 'upload_state.json'
    _time_format = '%Y-%m-%d %H:%M:%S%z'
    _date_format = '%Y-%m-%d'

    def __init__(
            self,
            tableau_cred: tuple,
            s3_creds: tuple,
            work_dir: str,
            exeptions_queue: SimpleQueue = None,
            result_queue: SimpleQueue = None,
    ):

        self.logger = logging.getLogger(self.loger_name)
        self.exeptions_queue = exeptions_queue if exeptions_queue else SimpleQueue()
        self.result_queue = result_queue if result_queue else SimpleQueue()
        self.work_dir = work_dir
        self.current_site_name = None
        self.project_id_path = None
        self.user_id_username = None
        self.upload_state = {}
        self.wb_name_s3_object = {}

        tab_user, tab_pass, tab_url = tableau_cred
        key_id, access_key, self.bucket_name = s3_creds

        add_breadcrumb(
            category='__init__',
            message=f'TS: {tab_url=}, {tab_user=}\nS3: bucket_name "{self.bucket_name}"',
            level='info',
            type='debug',
        )

        self.ts = TSC.Server(
            server_address=tab_url,
            use_server_version=True
        )

        self.ts.auth.sign_in(
            TSC.TableauAuth(
                username=tab_user,
                password=tab_pass,
            )
        )

        self.s3 = boto3.client(
            service_name='s3',
            aws_access_key_id=key_id,
            aws_secret_access_key=access_key,
        )

        self._fill_user_id_username()

    def _get_wb_path(self, wb: WorkbookItem):
        return self.current_site_name + '/' + self.project_id_path[wb.project_id] + wb.name

    def _fill_user_id_username(self):
        ts_users = []
        for site in self._ts_get_all_sites():
            self.ts.auth.switch_site(site)
            ts_users += list(TSC.Pager(self.ts.users))
        self.user_id_username = {i.id: i.name for i in ts_users}

    def _fill_project_id_path(self):
        all_projects = list(TSC.Pager(self.ts.projects))
        all_projects = {i.id: i for i in all_projects}
        self.project_id_path = {}
        for _, project in all_projects.items():
            path = []
            parent_id = project.parent_id
            while True:
                if parent_id:
                    path.append(all_projects[parent_id].name)
                    parent_id = all_projects[parent_id].parent_id
                else:
                    break
            path.reverse()
            self.project_id_path[project.id] = '/'.join(path + [project.name]) + '/'

    def _ts_get_all_sites(self):
        return list(TSC.Pager(self.ts.sites.get))

    def _ts_switch_site(self, site_name: str):
        site = [i for i in self._ts_get_all_sites() if i.name == site_name]
        if site:
            self.logger.debug(f'Switch to:"{site[0].name}", url: "{site[0].content_url}"')
            self.ts.auth.switch_site(site[0])

            self.current_site_name = site[0].name
            self._s3_download_upload_state()
            self._fill_project_id_path()
        else:
            self.logger.warning(f'Site {site_name} not found')

    def _ts_get_all_workbooks(self):
        return list(TSC.Pager(self.ts.workbooks))

    def _ts_download_wb(self, wb: WorkbookItem):
        file_path = os.path.join(self.work_dir, wb.id)
        self.logger.debug(f'Download wb:{wb.project_name} / {wb.name} ({wb.id})')

        file_path = self.ts.workbooks.download(
            workbook_id=wb.id,
            include_extract=True,
            filepath=file_path,
        )
        return file_path

    def _backup_wb2s3(self, wb: WorkbookItem):
        wb_path = self._get_wb_path(wb)
        workbook = Workbook(
            name=wb.name,
            project=wb.project_name,
            id=wb.id,
            size=wb.size
        )
        self.logger.info(f'Backup "{wb_path}({wb.id})"')
        with sentry_sdk.new_scope() as scope:
            scope.add_breadcrumb(
                category='_backup_wb2s3',
                message=f'Backup wb:{wb.project_name} / {wb.name} ({wb.id})',
                level='info',
                type='debug',
            )
            try:
                file_path = self._ts_download_wb(wb)
            except Exception as e:
                self.logger.warning(f'Error while downloading  "{wb.name}({wb.id})"')
                self.logger.exception(e)
                self.exeptions_queue.put((e, workbook))
                sentry_sdk.capture_exception(e)
            else:
                obj_key = wb_path + '.' + file_path[-7:].split('.')[1]
                tags = {
                    'tab_owner': self.user_id_username.get(wb.owner_id),
                    'tab_id': wb.id,
                    'tab_created_at': wb.created_at.strftime(self._time_format),
                    'tab_updated_at': wb.updated_at.strftime(self._time_format),
                }
                try:
                    self._s3_upload(
                        file_path=file_path,
                        object_key=obj_key,
                        tags=tags
                    )
                except Exception as e:
                    self.logger.warning(f'Error while uploading {obj_key}')
                    self.logger.exception(e)
                    self.exeptions_queue.put((e, workbook))
                    sentry_sdk.capture_exception(e)
                else:
                    self.upload_state[wb_path] = {
                        'id': wb.id,
                        'name': wb.name,
                        'created_at': wb.created_at.strftime(self._time_format),
                        'updated_at': wb.updated_at.strftime(self._time_format),
                        'upload_date': datetime.date.today().strftime(self._date_format),
                        'object_key': obj_key,
                    }
                    self.result_queue.put(workbook)
                os.remove(file_path)

    def run_backup(
            self,
            site_names: list = None,
            last_modified_update_interval: int = 30,
            threads=False,
            max_workers: int = 10
    ):
        backup_wb_count = 0
        wb_sites = self._ts_get_all_sites()
        if site_names:
            wb_sites = [i for i in wb_sites if i.name in site_names]

        for site in wb_sites:
            backup_wb_count += self.backup_site(
                site_name=site.name,
                threads=threads,
                max_workers=max_workers
            )
            self.s3_update_outdated_last_modified_in_curr_site(last_modified_update_interval)
        return backup_wb_count

    def backup_site(
            self,
            site_name: str,
            threads=False,
            max_workers: int = 10
    ):
        self.logger.info(f'Backup site: "{site_name}"')
        queue_to_backup = []
        self._ts_switch_site(site_name)

        all_wbs = self._ts_get_all_workbooks()

        all_wbs_paths = [self._get_wb_path(w) for w in all_wbs]
        for wb_path, wb_data in [(k, v) for k, v in self.upload_state.items() if k not in all_wbs_paths]:
            self.logger.info(f'"{wb_data['object_key']}" no longer exists on the TS. Update Last modified field in S3')
            self._s3_update_last_modified(wb_data['object_key'])
            self.upload_state.pop(wb_path)

        for wb in all_wbs:
            wb_path = self._get_wb_path(wb)
            if self.upload_state.get(wb_path) and all([
                self.upload_state[wb_path]['id'] == wb.id,
                self.upload_state[wb_path]['updated_at'] == wb.updated_at.strftime(self._time_format),
                self.upload_state[wb_path]['created_at'] == wb.created_at.strftime(self._time_format),
            ]):
                self.logger.debug(f'"{wb_path}" already in S3 and has the same metadata. Ignore')
            else:
                queue_to_backup.append(wb)

        if threads:
            with ThreadPoolExecutor(max_workers=max_workers) as tpe:
                resp = [tpe.submit(self._backup_wb2s3, wb) for wb in queue_to_backup]
        else:
            for wb in queue_to_backup:
                self._backup_wb2s3(wb)
        self._s3_upload_upload_state()
        return len(queue_to_backup)

    def _s3_is_object_exists(self, object_key):
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=object_key)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise
        return True

    def _s3_upload(
            self,
            file_path: str,
            object_key: str,
            tags: dict = None
    ):
        params = {
            'Filename': file_path,
            'Bucket': self.bucket_name,
            'Key': object_key,
        }
        if tags:
            params['ExtraArgs'] = {
                "Tagging": parse.urlencode(tags)
            }
        self.logger.debug(f'Start upload: {object_key} to {self.bucket_name}')
        resp = self.s3.upload_file(**params)

    def _s3_list_all_objects_in_curr_ts_site(self):
        paginator = self.s3.get_paginator('list_objects_v2')
        response_iterator = paginator.paginate(Bucket=self.bucket_name)
        all_objects = []
        for page in response_iterator:
            if 'Contents' in page:
                all_objects += [i for i in page['Contents'] if i['Key'].startswith(self.current_site_name + '/')]
        return all_objects

    def s3_update_outdated_last_modified_in_curr_site(self, days: int = 30):
        all_object = self._s3_list_all_objects_in_curr_ts_site()
        #ToDo Add Threads
        for obj in all_object:
            if (datetime.datetime.now().astimezone() - obj['LastModified']).days >= days:
                self._s3_update_last_modified(obj['Key'])

    def _s3_update_last_modified(self, object_key: str):
        self.logger.debug(f'Update last_modified for {object_key}')
        resp = self.s3.copy_object(
            Bucket=self.bucket_name,
            CopySource={'Bucket': self.bucket_name, 'Key': object_key},
            Key=object_key
        )
        self.logger.debug(f'self.s3.copy_object resp: {resp}')

    def _s3_upload_upload_state(self):
        obj_key = self.current_site_name + '/' + self.s3_upload_state_file
        self.logger.info(f'Upload upload_state in {obj_key} ')
        with sentry_sdk.new_scope() as scope:
            upload_state = json.dumps(self.upload_state, indent=2)
            scope.add_breadcrumb(
                category='_s3_upload_upload_state',
                message=f's3.put_object "{obj_key}" to "{self.bucket_name}"',
                level='info',
                type='debug',
            )
            scope.add_attachment(
                bytes=upload_state.encode("utf-8"),
                filename=obj_key
            )
            try:
                self.s3.put_object(
                    Body=upload_state,
                    Bucket=self.bucket_name,
                    Key=obj_key
            )
            except Exception as e:
                sentry_sdk.capture_exception(e)
                raise

    def _s3_download_upload_state(self):
        obj_key = self.current_site_name + '/' + self.s3_upload_state_file
        self.logger.debug(f'Try to download {obj_key} ')
        with sentry_sdk.new_scope() as scope:
            add_breadcrumb(
                category='_s3_download_upload_state',
                message=f'get object "{obj_key}"',
                level='info',
                type='debug',
            )
            try:
                resp = self.s3.get_object(
                    Bucket=self.bucket_name,
                    Key=obj_key
                )
            except  botocore.exceptions.ClientError as e:
                if e.response.get('Error',{}).get('Code') == 'NoSuchKey':
                    self.upload_state = {}
                    return
                sentry_sdk.capture_exception(e)
                raise
            resp_body = resp["Body"].read()
            add_breadcrumb(
                category='_s3_download_upload_state',
                message='Parse JSON and convert it into dict',
                level='info',
                type='debug',
            )

            scope.add_attachment(bytes=resp_body, filename=self.s3_upload_state_file)
            try:
                text = resp_body.decode()
                self.upload_state = json.loads(text)
            except Exception as e:
                sentry_sdk.capture_exception(e)
                raise
