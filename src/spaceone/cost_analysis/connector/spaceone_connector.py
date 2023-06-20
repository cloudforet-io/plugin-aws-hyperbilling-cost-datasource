import logging
import re
import requests
from requests import HTTPError
from google.protobuf.json_format import MessageToDict

from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.core.connector import BaseConnector
from spaceone.cost_analysis.error import *

__all__ = ['SpaceONEConnector']

_LOGGER = logging.getLogger(__name__)


class SpaceONEConnector(BaseConnector):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.grpc_client = None
        self.token = None
        self.billing_domain_id = None
        self.protocol = None
        self.endpoint = None

    def init_client(self, options: dict, secret_data: dict, schema: str = None) -> None:
        self._check_secret_data(secret_data)
        spaceone_endpoint = secret_data['spaceone_endpoint']
        self.billing_domain_id = secret_data['spaceone_domain_id']
        self.token = secret_data['spaceone_api_key']

        if spaceone_endpoint.startswith('http') or spaceone_endpoint.startswith('https'):
            self.protocol = 'http'
            self.endpoint = spaceone_endpoint
        elif spaceone_endpoint.startswith('grpc') or spaceone_endpoint.startswith('grpc+ssl'):
            self.protocol = 'grpc'
            self.grpc_client: SpaceConnector = SpaceConnector(endpoint=spaceone_endpoint, token=self.token)

    def verify_plugin(self):
        method = 'Domain.get'
        self.dispatch(method, {'domain_id': self.billing_domain_id})

    def list_projects(self, domain_id: str):
        params = {
            'query': {
                'filter': [
                    {'k': 'tags.domain_id', 'v': domain_id, 'o': 'eq'}
                ]
            },
            'domain_id': self.billing_domain_id
        }

        return self.dispatch('Project.list', params)

    def get_service_account(self, service_account_id):
        params = {
            'service_account_id': service_account_id,
            'domain_id': self.billing_domain_id
        }

        return self.dispatch('ServiceAccount.update', params)

    def update_service_account(self, service_account_id, tags):
        params = {
            'service_account_id': service_account_id,
            'tags': tags,
            'domain_id': self.billing_domain_id
        }

        return self.dispatch('ServiceAccount.update', params)

    def list_service_accounts(self, project_id: str):
        params = {
            'provider': 'aws',
            'service_account_type': 'GENERAL',
            'project_id': project_id,
            'domain_id': self.billing_domain_id
        }

        return self.dispatch('ServiceAccount.list', params)

    def _get_metadata(self):
        return ('token', self.token),

    def dispatch(self, method: str = None, params: dict = None, **kwargs):
        if self.protocol == 'grpc':
            return self.grpc_client.dispatch(method, params, **kwargs)
        else:
            return self.request(method, params, **kwargs)

    def request(self, method, params, **kwargs):
        method = self._convert_method_to_snake_case(method)
        url = f'{self.endpoint}/{method}'

        headers = self._make_request_header(self.token, **kwargs)

        _LOGGER.debug(f'[request] url: {url}')
        response = requests.post(url, json=params, headers=headers)

        if response.status_code >= 400:
            raise HTTPError(f'HTTP {response.status_code} Error: {response.json()["detail"]}')

        response = response.json()
        _LOGGER.debug(f'[response] {response}')
        return response

    @staticmethod
    def _convert_method_to_snake_case(method):
        method = re.sub(r'(?<!^)(?=[A-Z])', '_', method)
        method = method.replace('.', '/').replace('_', '-').lower()
        return method

    @staticmethod
    def _make_request_header(token, **kwargs):
        access_token = token
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
        }
        return headers

    @staticmethod
    def _change_message(message):
        return MessageToDict(message, preserving_proto_field_name=True)

    @staticmethod
    def _check_secret_data(secret_data: dict):
        if 'spaceone_endpoint' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.spaceone_endpoint')

        if 'spaceone_api_key' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.spaceone_api_key')

        if 'spaceone_domain_id' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.spaceone_domain_id')
