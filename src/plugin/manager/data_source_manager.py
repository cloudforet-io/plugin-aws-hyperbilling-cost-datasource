import logging

from spaceone.core.manager import BaseManager
from ..connector.aws_s3_connector import AWSS3Connector
from ..connector.spaceone_connector import SpaceONEConnector

_LOGGER = logging.getLogger(__name__)


class DataSourceManager(BaseManager):

    @staticmethod
    def init_response(options: dict) -> dict:
        return {
            'metadata': {
                'currency': 'USD',
                'supported_secret_types': ['MANUAL'],
                'data_source_rules': [
                    {
                        'name': 'match_service_account',
                        'conditions_policy': 'ALWAYS',
                        'actions': {
                            'match_service_account': {
                                'source': 'additional_info.Account ID',
                                'target': 'data.account_id'
                            }
                        },
                        'options': {
                            'stop_processing': True
                        }
                    }
                ]
            }
        }

    @staticmethod
    def verify_plugin(options: dict, secret_data: dict, domain_id: str, schema: str = None) -> None:
        space_connector = SpaceONEConnector()
        space_connector.init_client(options, secret_data, schema)
        space_connector.verify_plugin(domain_id)

        aws_s3_connector = AWSS3Connector()
        aws_s3_connector.create_session(options, secret_data, schema)
