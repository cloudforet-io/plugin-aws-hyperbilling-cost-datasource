import logging
from datetime import datetime, timedelta

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.connector.spaceone_connector import SpaceONEConnector
from spaceone.cost_analysis.model.cost_model import Cost

_LOGGER = logging.getLogger(__name__)

_REGION_MAP = {
    'APE1': 'ap-east-1',
    'APN1': 'ap-northeast-1',
    'APN2': 'ap-northeast-2',
    'APN3': 'ap-northeast-3',
    'APS1': 'ap-southeast-1',
    'APS2': 'ap-southeast-2',
    'APS3': 'ap-south-1',
    'CAN1': 'ca-central-1',
    'CPT': 'af-south-1',
    'EUN1': 'eu-north-1',
    'EUC1': 'eu-central-1',
    'EU': 'eu-west-1',
    'EUW2': 'eu-west-2',
    'EUW3': 'eu-west-3',
    'MES1': 'me-south-1',
    'SAE1': 'sa-east-1',
    'UGW1': 'AWS GovCloud (US-West)',
    'UGE1': 'AWS GovCloud (US-East)',
    'USE1': 'us-east-1',
    'USE2': 'us-east-2',
    'USW1': 'us-west-1',
    'USW2': 'us-west-2',
    'AP': 'Asia Pacific',
    'AU': 'Australia',
    'CA': 'Canada',
    # 'EU': 'Europe and Israel',
    'IN': 'India',
    'JP': 'Japan',
    'ME': 'Middle East',
    'SA': 'South America',
    'US': 'United States',
    'ZA': 'South Africa',
}


class CostManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.space_connector: SpaceONEConnector = self.locator.get_connector('SpaceONEConnector')

    def get_data(self, options, secret_data, schema, task_options):
        self.space_connector.init_client(options, secret_data, schema)
        self._check_task_options(task_options)

        start = task_options['start']
        account = task_options['account']
        date_ranges = self._get_date_range(start)

        for date in date_ranges:
            next_token = None

            while True:
                response = self.aws_hb_connector.get_cost_data(account, date['start'], date['end'], next_token)
                next_token = response.get('NextDataToken')
                results = response.get('Results', [])
                yield self._make_cost_data(results, account)

                if not next_token:
                    break

    def _make_cost_data(self, results, account):
        costs_data = []

        for result in results:
            try:
                region = result['GroupBy']['REGION'] or 'USE1'
                data = {
                    'cost': result['Value']['USAGE_COST'],
                    'currency': 'USD',
                    'usage_quantity': result['Value']['USAGE_QUANTITY'],
                    'provider': 'aws',
                    'region_code': _REGION_MAP.get(region, region),
                    'product': result['GroupBy']['SERVICE_CODE'],
                    'account': account,
                    'usage_type': self._parse_usage_type(result),
                    'billed_at': datetime.strptime(result['GroupBy']['USAGE_DATE'], '%Y-%m-%d'),
                    'additional_info': {
                        'raw_usage_type': result['GroupBy']['USAGE_TYPE']
                    }
                }
            except Exception as e:
                _LOGGER.error(f'[_make_cost_data] make data error: {e}', exc_info=True)
                raise e

            costs_data.append(data)

            # Excluded because schema validation is too slow
            # cost_data = Cost(data)
            # cost_data.validate()
            #
            # costs_data.append(cost_data.to_primitive())

        return costs_data

    @staticmethod
    def _parse_usage_type(cost_info):
        service_code = cost_info['GroupBy']['SERVICE_CODE']
        usage_type = cost_info['GroupBy']['USAGE_TYPE']

        if service_code == 'AWSDataTransfer':
            if usage_type.find('-In-Bytes') > 0:
                return 'data-transfer.in'
            elif usage_type.find('-Out-Bytes') > 0:
                return 'data-transfer.out'
            else:
                return 'data-transfer.etc'
        elif service_code == 'AmazonCloudFront':
            if usage_type.find('-HTTPS') > 0:
                return 'requests.https'
            elif usage_type.find('-Out-Bytes') > 0:
                return 'data-transfer.out'
            else:
                return 'requests.http'
        else:
            return cost_info['GroupBy']['INSTANCE_TYPE']

    @staticmethod
    def _check_task_options(task_options):
        if 'start' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.start')

        if 'account' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.account')

    @staticmethod
    def _get_date_range(start):
        start_time = datetime.strptime(start, '%Y-%m-%d')
        now = datetime.utcnow()
        end_time = start_time + timedelta(days=30)

        date_ranges = []

        while True:
            if end_time > now:
                date_ranges.append({
                    'start': start_time.strftime('%Y-%m-%d'),
                    'end': now.strftime('%Y-%m-%d')
                })
                break

            date_ranges.append({
                'start': start_time.strftime('%Y-%m-%d'),
                'end': end_time.strftime('%Y-%m-%d')
            })

            start_time = start_time + timedelta(days=31)
            end_time = start_time + timedelta(days=30)

        return date_ranges
