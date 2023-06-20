import logging
from datetime import datetime, timedelta

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.connector.spaceone_connector import SpaceONEConnector
from spaceone.cost_analysis.model.job_model import Tasks

_LOGGER = logging.getLogger(__name__)
_DEFAULT_DATABASE = 'MZC'


class JobManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.space_connector: SpaceONEConnector = self.locator.get_connector('SpaceONEConnector')

    def get_tasks(self, options, secret_data, schema, start, last_synchronized_at, domain_id):
        tasks = []
        changed = []

        start_time = self._get_start_time(start, last_synchronized_at)
        start_date = start_time.strftime('%Y-%m-%d')
        changed_time = start_time
        self.space_connector.init_client(options, secret_data, schema)
        response = self.space_connector.list_projects(domain_id)
        total_count = response.get('total_count') or 0

        if total_count > 0:
            project_info = response['results'][0]
            _LOGGER.debug(f'[get_tasks] project info: {project_info}')

            project_id = project_info['project_id']
            database = project_info.get('tags', {}).get('database', _DEFAULT_DATABASE)

            response = self.space_connector.list_service_accounts(project_id)
            for service_account_info in response.get('results', []):
                service_account_id = service_account_info['service_account_id']
                service_account_name = service_account_info['name']
                account_id = service_account_info['data']['account_id']
                is_sync = service_account_info['tags'].get('is_sync', 'false')
                database = service_account_info['tags'].get('database', database)

                if is_sync != 'true':
                    is_sync = 'false'

                _LOGGER.debug(f'[get_tasks] service_account({service_account_id}): name={service_account_name}, account_id={account_id}')
                task_options = {
                    'is_sync': is_sync,
                    'service_account_id': service_account_id,
                    'service_account_name': service_account_name,
                    'account_id': account_id,
                    'database': database
                }

                if is_sync == 'false':
                    first_sync_time = self._get_start_time(start)
                    task_options['start'] = first_sync_time.strftime('%Y-%m-%d')

                    changed.append({
                        'start': first_sync_time,
                        'filter': {
                            'account': account_id
                        }
                    })
                else:
                    task_options['start'] = start_date

                tasks.append({'task_options': task_options})

            changed.append({
                'start': changed_time
            })

            _LOGGER.debug(f'[get_tasks] tasks: {tasks}')
            _LOGGER.debug(f'[get_tasks] changed: {changed}')

            tasks = Tasks({'tasks': tasks, 'changed': changed})

            tasks.validate()
            return tasks.to_primitive()

        else:
            _LOGGER.debug(f'[get_tasks] no project: tags.domain_id = {domain_id}')

        return {
            'tasks': tasks,
            'changed': changed
        }

    @staticmethod
    def _get_start_time(start, last_synchronized_at=None):

        if start:
            start_time: datetime = start
        elif last_synchronized_at:
            start_time: datetime = last_synchronized_at - timedelta(days=7)
            start_time = start_time.replace(day=1)
        else:
            start_time: datetime = datetime.utcnow() - timedelta(days=365)
            start_time = start_time.replace(day=1)

        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

        return start_time
