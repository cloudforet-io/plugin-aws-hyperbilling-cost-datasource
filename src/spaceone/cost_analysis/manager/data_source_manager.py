import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.data_source_model import PluginMetadata
from spaceone.cost_analysis.connector.spaceone_connector import SpaceONEConnector
from spaceone.cost_analysis.connector.aws_s3_connector import AWSS3Connector

_LOGGER = logging.getLogger(__name__)


class DataSourceManager(BaseManager):

    @staticmethod
    def init_response(options):
        plugin_metadata = PluginMetadata()
        plugin_metadata.validate()

        return {"metadata": plugin_metadata.to_primitive()}

    def verify_plugin(self, options, secret_data, schema):
        task_type = options.get("task_type", "identity")
        if task_type == "identity":
            space_connector: SpaceONEConnector = self.locator.get_connector(
                "SpaceONEConnector"
            )
            space_connector.init_client(options, secret_data, schema)
            space_connector.verify_plugin()

        aws_s3_connector: AWSS3Connector = self.locator.get_connector("AWSS3Connector")
        aws_s3_connector.create_session(options, secret_data, schema)
