import logging

from spaceone.core.error import ERROR_INVALID_PARAMETER
from spaceone.core.manager import BaseManager
from ..connector.aws_s3_connector import AWSS3Connector
from ..connector.spaceone_connector import SpaceONEConnector

_LOGGER = logging.getLogger(__name__)


class DataSourceManager(BaseManager):

    def init_response(self, options: dict) -> dict:
        self._check_options(options)
        currency: str = options.get("currency", "USD")

        return {
            "metadata": {
                "currency": currency,
                "supported_secret_types": ["MANUAL"],
                "data_source_rules": [
                    {
                        "name": "match_service_account",
                        "conditions_policy": "ALWAYS",
                        "actions": {
                            "match_service_account": {
                                "source": "additional_info.Account ID",
                                "target": "data.account_id",
                            }
                        },
                        "options": {"stop_processing": True},
                    }
                ],
            }
        }

    @staticmethod
    def verify_plugin(
        options: dict, secret_data: dict, domain_id: str, schema: str = None
    ) -> None:
        space_connector = SpaceONEConnector()
        space_connector.init_client(options, secret_data, schema)
        space_connector.verify_plugin(domain_id)

        aws_s3_connector = AWSS3Connector()
        aws_s3_connector.create_session(options, secret_data, schema)

    @staticmethod
    def _check_options(options: dict):
        task_type = options.get("task_type", "identity")
        resync_days = options.get("resync_days_from_last_synced_at", 7)

        if task_type not in ["identity", "directory"]:
            raise ERROR_INVALID_PARAMETER(
                task_type=task_type,
                reason="task_type should be 'identity' or 'directory'",
            )

        if isinstance(resync_days, float):
            resync_days = int(resync_days)
        elif not isinstance(resync_days, int):
            raise ERROR_INVALID_PARAMETER(
                key="resync_days_from_last_synced_at",
                reason="resync_days_from_last_synced_at should be integer",
            )

        if resync_days < 3 or resync_days > 27:
            raise ERROR_INVALID_PARAMETER(
                resync_days_from_last_synced_at=resync_days,
                reason="resync_days_from_last_synced_at should be 4 ~ 26",
            )
