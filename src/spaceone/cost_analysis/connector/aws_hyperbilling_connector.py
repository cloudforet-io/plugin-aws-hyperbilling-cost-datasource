import logging
import requests
import copy

from spaceone.core.transaction import Transaction
from spaceone.core.connector import BaseConnector
from typing import List

from spaceone.cost_analysis.error import *

__all__ = ['AWSHyperBillingConnector']

_LOGGER = logging.getLogger(__name__)

_DEFAULT_HEADERS = {
    'Content-Type': 'application/json',
    'accept': 'application/json'
}


class AWSHyperBillingConnector(BaseConnector):

    def __init__(self, transaction: Transaction, config: dict):
        super().__init__(transaction, config)
        self.endpoint = None
        self.headers = copy.deepcopy(_DEFAULT_HEADERS)

    def create_session(self, options: dict, secret_data: dict, schema: str = None) -> None:
        self._check_secret_data(secret_data)

        self.headers['X-Client-Id'] = secret_data['client_id']
        self.headers['X-Client-Secret'] = secret_data['client_secret']
        self.endpoint = secret_data['endpoint']

    @staticmethod
    def _check_secret_data(secret_data: dict) -> None:
        if 'client_id' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.client_id')

        if 'client_secret' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.client_secret')

        if 'endpoint' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.endpoint')

    def get_linked_accounts(self) -> List[dict]:
        url = f'{self.endpoint}/v1/search/linkedaccount'
        data = {}

        _LOGGER.debug(f'[list_linked_accounts] ({self.headers["X-Client-Id"]}) {url} => {data}')

        response = requests.get(url, json=data, headers=self.headers)

        if response.status_code == 200:
            return response.json().get('Results', [])
        else:
            _LOGGER.error(f'[get_linked_accounts] error code: {response.status_code}')
            try:
                error_message = response.json()
            except Exception as e:
                error_message = str(response)

            _LOGGER.error(f'[get_linked_accounts] error message: {error_message}')
            raise ERROR_CONNECTOR_CALL_API(reason=error_message)

    def get_cost_data(self, account, start: str, end: str, next_token: str = None) -> dict:
        url = f'{self.endpoint}/v1/search/billing'

        data = {
            'Filter': {
                'LinkedAccount': [account],
                'Granularity': 'DAILY',
                'TimePeriod': {
                    'Start': start,
                    'End': end
                }
            },
            'GroupBy': [
                'USAGE_DATE',
                'REGION',
                'SERVICE_CODE',
                'USAGE_TYPE',
                'INSTANCE_TYPE'
            ],
            'Result': [
                'USAGE_COST',
                'USAGE_QUANTITY'
            ],
        }

        if next_token:
            data['Filter']['NextDataToken'] = next_token

        _LOGGER.debug(f'[get_cost_data] ({self.headers["X-Client-Id"]}) {url} => {data}')

        response = requests.post(url, json=data, headers=self.headers)

        if response.status_code == 200:
            return response.json()

        elif response.status_code == 204:
            return {}
        else:
            _LOGGER.error(f'[get_cost_data] error code: {response.status_code}')
            try:
                error_message = response.json()
            except Exception as e:
                error_message = str(response)

            _LOGGER.error(f'[get_cost_data] error message: {error_message}')
            raise ERROR_CONNECTOR_CALL_API(reason=error_message)
