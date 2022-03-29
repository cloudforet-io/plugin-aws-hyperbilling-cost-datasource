import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.connector.aws_hyperbilling_connector import AWSHyperBillingConnector
from spaceone.cost_analysis.model.job_model import Tasks

_LOGGER = logging.getLogger(__name__)


class JobManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_hb_collector: AWSHyperBillingConnector = self.locator.get_connector('AWSHyperBillingConnector')

    def get_tasks(self, options, secret_data, schema, start, last_synchronized_at):
        self.aws_hb_collector.create_session(options, secret_data, schema)
        results = self.aws_hb_collector.get_linked_accounts()

        if start:
            start_time: datetime = start
        elif last_synchronized_at:
            start_time: datetime = last_synchronized_at - timedelta(days=7)
        else:
            start_time: datetime = datetime.utcnow() - timedelta(days=365)
            start_time = start_time.replace(day=1)

        _LOGGER.debug(f'[get_tasks] linked accounts: {results}')
        tasks = []
        changed = []
        for account in results:
            tasks.append({
                'task_options': {
                    'start': start_time.strftime('%Y-%m-%d'),
                    'account': account
                }
            })

            changed.append({
                'start': start_time
            })

        _LOGGER.debug(f'[get_tasks] tasks: {tasks}')
        _LOGGER.debug(f'[get_tasks] changed: {changed}')

        tasks = Tasks({'tasks': tasks, 'changed': changed})

        tasks.validate()
        return tasks.to_primitive()
