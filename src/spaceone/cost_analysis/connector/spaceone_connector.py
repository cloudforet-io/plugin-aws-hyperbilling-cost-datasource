import logging
from google.protobuf.json_format import MessageToDict

from spaceone.core.connector import BaseConnector
from spaceone.core.utils import parse_grpc_endpoint
from spaceone.core import pygrpc
from spaceone.cost_analysis.error import *

__all__ = ['SpaceONEConnector']

_LOGGER = logging.getLogger(__name__)


class SpaceONEConnector(BaseConnector):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = None
        self.token = None
        self.billing_domain_id = None

    def init_client(self, options: dict, secret_data: dict, schema: str = None) -> None:
        self._check_secret_data(secret_data)
        e = parse_grpc_endpoint(secret_data['spaceone_endpoint'])
        self.client = pygrpc.client(endpoint=e['endpoint'], ssl_enabled=e['ssl_enabled'])
        self.token = secret_data['spaceone_api_key']
        self.billing_domain_id = secret_data['spaceone_domain_id']

    def verify_plugin(self):
        self.client.Domain.get({'domain_id': self.billing_domain_id}, metadata=self._get_metadata())

    def list_projects(self, domain_id: str):
        params = {
            'query': {
                'filter': [
                    {'k': 'tags.domain_id', 'v': domain_id, 'o': 'eq'}
                ]
            },
            'domain_id': self.billing_domain_id
        }

        message = self.client.Project.list(params, metadata=self._get_metadata())
        return self._change_message(message)

    def list_service_accounts(self, project_id: str):
        params = {
            'provider': 'aws',
            'service_account_type': 'GENERAL',
            'project_id': project_id,
            'domain_id': self.billing_domain_id
        }

        message = self.client.ServiceAccount.list(params, metadata=self._get_metadata())
        return self._change_message(message)

    def _get_metadata(self):
        return ('token', self.token),

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
