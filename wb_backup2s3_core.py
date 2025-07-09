import boto3
import logging
import os
import botocore
import datetime
import json
import sentry_sdk
import functools
import re
from dataclasses import dataclass
from urllib import parse
import tableauserverclient as TSC
from concurrent.futures import ThreadPoolExecutor
from queue import SimpleQueue
from tableauserverclient.models.workbook_item import WorkbookItem
from sentry_sdk import add_breadcrumb
from sentry_sdk.scrubber import DEFAULT_DENYLIST

logger = logging.getLogger('main.' + __name__)

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
    site: str


def retry(_func=None, *, times=6):
    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper_retry(*args, **kwargs):
            last_exception = None
            for attempt in range(times):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logger.debug(f"  Attempt {attempt + 1} failed: {e}. Retrying...")
            logger.debug("  Function failed after maximum retry attempts.")
            raise last_exception
        return wrapper_retry
    if _func is None:
        return decorator_retry
    else:
        return decorator_retry(_func)


def print_and_send_exceptions_sentry(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.exception(e)
            sentry_sdk.capture_exception(e)
            raise
    return wrapper


class BackupWB2S3:
    """Downloads workbooks from a Tableau Server and uploads them to AWS. "
    ...

    Attributes
    ----------
    tableau_cred : tuple
        tableau credentials
        Example: (username, password, url)

    s3_creds : tuple
        AWS credentials
        Example: (key_id, access_key, bucket name)

    work_dir: str
        Folder where script downloads files before uploading to AWS S3.

    """
    loger_name = 'main.BackupWB2S3'
    s3_upload_state_file = 'upload_state.json'
    _time_format = '%Y-%m-%d %H:%M:%S%z'
    _date_format = '%Y-%m-%d'
    _download_error_ignore_tag = 'WBBackupIgnoreErrors'

    def __init__(
            self,
            tableau_cred: tuple,
            s3_creds: tuple,
            work_dir: str,
            failed_q: SimpleQueue = None,
            successful_q: SimpleQueue = None,
            ts_http_timeout: int = 1200,
    ):

        self.logger = logging.getLogger(self.loger_name)
        self.failed_q = failed_q if failed_q else SimpleQueue()
        self.successful_q = successful_q if successful_q else SimpleQueue()
        self.work_dir = work_dir
        self.current_site_name = None
        self.project_id_path: dict = {}
        self.projects_hierarchy: dict = {}
        self.user_id_username = None
        self.bucket_name: dict = {}
        self.upload_state = {}
        self.wb_name_s3_object = {}

        tab_user, tab_pass, tab_url = tableau_cred
        key_id, access_key = s3_creds

        add_breadcrumb(
            category='__init__',
            message=f'TS: {tab_url=}, {tab_user=}',
            level='info',
            type='debug',
        )

        self.ts = TSC.Server(
            server_address=tab_url,
            use_server_version=True
        )
        self.ts.http_options['timeout'] = ts_http_timeout

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
        # self._fill_user_id_username()

    def _get_wb_path(self, wb: WorkbookItem):
        return self.current_site_name + '/' + self.project_id_path[wb.project_id] + wb.name

    def _fill_user_id_username(self):
        ts_users = []
        for site in self._ts_get_all_sites():
            self.ts.auth.switch_site(site)
            ts_users += list(TSC.Pager(self.ts.users))
        self.user_id_username = {i.id: i.name for i in ts_users}

    def _build_project_structure(self):
        all_projects = list(TSC.Pager(self.ts.projects))
        all_projects = {i.id: i for i in all_projects}
        for project in all_projects.values():
            parent_id = project.parent_id
            self.projects_hierarchy.setdefault(parent_id,[]).append(project.id)
            path = []
            while True:
                if parent_id:
                    path.append(all_projects[parent_id].name)
                    parent_id = all_projects[parent_id].parent_id
                else:
                    break
            path.reverse()
            self.project_id_path[project.id] = '/'.join(path + [project.name]) + '/'

    def _get_sub_projects(self, project_id: str):
        resp = []
        if self.projects_hierarchy.get(project_id):
            resp.extend(self.projects_hierarchy.get(project_id))
        for sub_project_id in self.projects_hierarchy.get(project_id, []):
            resp.extend(self._get_sub_projects(sub_project_id))
        return resp


    def _ts_get_all_sites(self):
        return list(TSC.Pager(self.ts.sites.get))

    def _ts_switch_site(self, site_name: str):
        site = [i for i in self._ts_get_all_sites() if i.name == site_name]
        if site:
            self.logger.debug(f'Switch to:"{site[0].name}", url: "{site[0].content_url}"')
            self.ts.auth.switch_site(site[0])

            self.current_site_name = site[0].name
            self._s3_download_upload_state()
            self._build_project_structure()
        else:
            self.logger.warning(f'Site {site_name} not found')

    def _ts_get_all_workbooks(self):
        return list(TSC.Pager(self.ts.workbooks))

    @retry
    def _ts_download_wb(self, wb: WorkbookItem, include_extract: bool = True):
        file_path = os.path.join(self.work_dir, wb.id)
        self.logger.info(f' download:{wb.project_name} / {wb.name} ({wb.id})')
        return self.ts.workbooks.download(
            workbook_id=wb.id,
            include_extract=include_extract,
            filepath=file_path,
        )

    @print_and_send_exceptions_sentry
    def _do_backup(self, wb: WorkbookItem, include_extract: bool = True):
        wb_path = self._get_wb_path(wb)

        try:
            file_path = self._ts_download_wb(
                wb=wb,
                include_extract=include_extract
            )
        except Exception as e:
            self.logger.warning(f'{wb.name} download failed. Try with include_extract=False')
            include_extract = False
            file_path = self._ts_download_wb(
                wb=wb,
                include_extract=include_extract
            )

        obj_key = wb_path + '.' + file_path[-7:].split('.')[1]
        tags = {
            'tab_owner': self.user_id_username.get(wb.owner_id),
            'tab_id': wb.id,
            'tab_created_at': wb.created_at.strftime(self._time_format),
            'tab_updated_at': wb.updated_at.strftime(self._time_format),
            'tab_description': self.convert_to_s3_compliant_tag(wb.description)[:256] if wb.description else '',
        }
        self._s3_upload(
            file_path=file_path,
            object_key=obj_key,
            tags=tags
        )
        if include_extract or self._download_error_ignore_tag in wb.tags:
            self.upload_state[wb_path] = {
                'id': wb.id,
                'name': wb.name,
                'created_at': wb.created_at.strftime(self._time_format),
                'updated_at': wb.updated_at.strftime(self._time_format),
                'upload_date': datetime.date.today().strftime(self._date_format),
                'object_key': obj_key,
            }
        os.remove(file_path)

    def _backup_wb(self, wb: WorkbookItem):
        workbook = Workbook(
            name=wb.name,
            project=wb.project_name,
            id=wb.id,
            size=wb.size,
            site=self.current_site_name,
        )
        self.logger.info(f'Backup "{wb.project_name}"/"{wb.name}" ({wb.id}), {wb.size} MB"')
        with sentry_sdk.new_scope() as scope:
            scope.add_breadcrumb(
                category='_backup',
                message=f'Backup(project / wb (id)): {wb.project_name} / {wb.name} ({wb.id})',
                level='info',
                type='debug',
            )
            try:
                self._do_backup(wb)
            except Exception as e:
                self.failed_q.put((e, workbook))
            else:
                self.successful_q.put(workbook)

    def full_backup(
            self,
            s3_bucket_name: str,
            site_names: list = None,
            last_modified_update_interval: int = 60,
            max_workers: int = 10,
            excluded_sites: list = []
    ):
        self.logger.debug(f'Run run_sites_backup: {s3_bucket_name=}, {site_names=}, {excluded_sites=}, {last_modified_update_interval=}, {max_workers=}')
        ts_sites = [s for s in self._ts_get_all_sites() if s.name not in excluded_sites]

        if site_names:
            ts_sites = [i for i in ts_sites if i.name in site_names]

        for site in ts_sites:
            self.backup_site(
                site_name=site.name,
                max_workers=max_workers,
                s3_bucket_name = s3_bucket_name,
                last_modified_update_interval=last_modified_update_interval,
            )
        return

    def backup_site(
            self,
            site_name: str,
            max_workers: int,
            s3_bucket_name: str,
            projects: list = [],
            last_modified_update_interval: int = 60,
    ):
        self.logger.info(f'#Backup site: "{site_name}" in to "{s3_bucket_name}", {projects=}')
        if not self.user_id_username:
            self._fill_user_id_username()
        self.bucket_name = s3_bucket_name
        self._ts_switch_site(site_name)

        project_ids_to_backup = []
        if projects:
            self.logger.info(f'Backup only next projects: "{projects}"')
        for project_path in projects:
            if not project_path.endswith('/'):
                project_path = project_path + '/'

            if not project_path in self.project_id_path.values():
                self.logger.warning(f'Project "{project_path}" not found')
            else:
                project_id = [k for k,v in self.project_id_path.items() if v == project_path][0]
                project_ids_to_backup.append(project_id)
                sub_projects = self._get_sub_projects(project_id)
                if sub_projects:
                    project_ids_to_backup.extend(sub_projects)

        queue_to_backup = []

        all_wbs = self._ts_get_all_workbooks()
        if projects:
            all_wbs = [w for w in all_wbs if w.project_id in project_ids_to_backup]
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
                if self.upload_state.get(wb_path):
                    msg_parts = []
                    if self.upload_state[wb_path]['id'] != wb.id:
                        msg_parts.append(f'wb id was changed: {self.upload_state[wb_path]['id']} -> {wb.id}')
                    elif self.upload_state[wb_path]['updated_at'] != wb.updated_at.strftime(self._time_format):
                        msg_parts.append(
                            f'wb updated_at was changed: {self.upload_state[wb_path]['updated_at']} -> {wb.updated_at.strftime(self._time_format)}'
                        )
                    elif self.upload_state[wb_path]['created_at'] == wb.created_at.strftime(self._time_format):
                        msg_parts.append(
                            f'wb created_at was changed: {self.upload_state[wb_path]['created_at']} -> {wb.created_at.strftime(self._time_format)}'
                        )
                    self.logger.info(f'"{wb_path}": ' + ' ,'.join(msg_parts))
                queue_to_backup.append(wb)

        with ThreadPoolExecutor(max_workers=max_workers) as tpe:
            resp = [tpe.submit(self._backup_wb, wb) for wb in queue_to_backup]
        for r in resp:
            if r.exception():
                self.logger.exception(r.exception())
                self.failed_q.put((r.exception(), None))
                sentry_sdk.capture_exception(r.exception())

        self._s3_update_outdated_last_modified(last_modified_update_interval)
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

    @retry
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
        self.logger.info(f' upload: {object_key} to {self.bucket_name}')
        self.s3.upload_file(**params)

    def _s3_list_all_objects_in_curr_ts_site(self):
        paginator = self.s3.get_paginator('list_objects_v2')
        response_iterator = paginator.paginate(Bucket=self.bucket_name)
        all_objects = []
        for page in response_iterator:
            if 'Contents' in page:
                all_objects += [i for i in page['Contents'] if i['Key'].startswith(self.current_site_name + '/')]
        return all_objects

    @staticmethod
    def convert_to_s3_compliant_tag(data: str, replacement_char='_'):
        allowed_pattern = r'[а-яА-Яa-zA-Z0-9 +\-=\.:/@]'

        output = ''.join(
            char if re.match(allowed_pattern, char) else replacement_char
            for char in data
        )
        return output

    def _s3_update_outdated_last_modified(self, days: int = 30, threads: bool = True, max_workers: int = 10):
        curr_date = datetime.datetime.now().astimezone()
        all_object = self._s3_list_all_objects_in_curr_ts_site()

        if threads:
            with ThreadPoolExecutor(max_workers=max_workers) as tpe:
                resp = [tpe.submit(self._s3_update_last_modified, obj['Key']) for obj in
                        [i for i in all_object if (curr_date - i['LastModified']).days >= days]]
            for r in resp:
                if r.exception():
                    self.logger.exception(r.exception())
                    self.failed_q.put((r.exception(), None))
                    sentry_sdk.capture_exception(r.exception())
        else:
            for obj in [i for i in all_object if (curr_date - i['LastModified']).days >= days]:
                self._s3_update_last_modified(obj['Key'])

    def _s3_update_last_modified(self, object_key: str):
        self.logger.debug(f'Update last_modified for {object_key}')
        self.s3.copy_object(
            Bucket=self.bucket_name,
            CopySource={'Bucket': self.bucket_name, 'Key': object_key},
            Key=object_key
        )
        # self.logger.debug(f's3.copy_object resp: {resp}')

    @retry(times=3)
    def _s3_upload_upload_state(self):
        obj_key = self.current_site_name + '/' + self.s3_upload_state_file
        self.logger.info(f'Upload {obj_key} ')
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
            scope.add_breadcrumb(
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
                if e.response.get('Error', {}).get('Code') == 'NoSuchKey':
                    self.logger.warning(obj_key + ' not found. Set upload_state = {}')
                    self.upload_state = {}
                    return
                sentry_sdk.capture_exception(e)
                raise
            resp_body = resp["Body"].read()
            scope.add_breadcrumb(
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
