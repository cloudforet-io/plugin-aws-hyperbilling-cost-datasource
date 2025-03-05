import logging
from datetime import datetime, timedelta
from dateutil import rrule

from spaceone.core import utils
from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.connector.aws_s3_connector import AWSS3Connector
from spaceone.cost_analysis.connector.spaceone_connector import SpaceONEConnector
from spaceone.cost_analysis.model.cost_model import Cost

_LOGGER = logging.getLogger(__name__)

_REGION_MAP = {
    "APE1": "ap-east-1",
    "APN1": "ap-northeast-1",
    "APN2": "ap-northeast-2",
    "APN3": "ap-northeast-3",
    "APS1": "ap-southeast-1",
    "APS2": "ap-southeast-2",
    "APS3": "ap-south-1",
    "CAN1": "ca-central-1",
    "CPT": "af-south-1",
    "EUN1": "eu-north-1",
    "EUC1": "eu-central-1",
    "EU": "eu-west-1",
    "EUW2": "eu-west-2",
    "EUW3": "eu-west-3",
    "MES1": "me-south-1",
    "SAE1": "sa-east-1",
    "UGW1": "AWS GovCloud (US-West)",
    "UGE1": "AWS GovCloud (US-East)",
    "USE1": "us-east-1",
    "USE2": "us-east-2",
    "USW1": "us-west-1",
    "USW2": "us-west-2",
    "AP": "Asia Pacific",
    "AU": "Australia",
    "CA": "Canada",
    # 'EU': 'Europe and Israel',
    "IN": "India",
    "JP": "Japan",
    "ME": "Middle East",
    "SA": "South America",
    "US": "United States",
    "ZA": "South Africa",
}


class CostManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_s3_connector: AWSS3Connector = self.locator.get_connector(
            "AWSS3Connector"
        )
        self.space_connector: SpaceONEConnector = self.locator.get_connector(
            "SpaceONEConnector"
        )

    def get_data(self, options, secret_data, schema, task_options):
        task_type = options.get("task_type", "identity")

        self.aws_s3_connector.create_session(options, secret_data, schema)
        self._check_task_options(task_options, task_type)

        start = task_options["start"]
        database = task_options["database"]
        account_id = task_options["account_id"]

        if task_type == "identity":
            service_account_id = task_options["service_account_id"]
            is_sync = task_options["is_sync"]
            if is_sync == "false":
                self._update_sync_state(
                    options, secret_data, schema, service_account_id
                )

        date_ranges = self._get_date_range(start)

        for date in date_ranges:
            year, month = date.split("-")

            path = f"SPACE_ONE/billing/database={database}/account_id={account_id}/year={year}/month={month}"
            response = self.aws_s3_connector.list_objects(path)
            contents = response.get("Contents", [])
            for content in contents:
                if content.get("Size", 0) == 0:
                    _LOGGER.debug(f"[get_data] empty file: {content}")
                    continue

                response_stream = self.aws_s3_connector.get_cost_data(content["Key"])
                for results in response_stream:
                    yield self._make_cost_data(results, account_id)

        yield []

    def _update_sync_state(self, options, secret_data, schema, service_account_id):
        self.space_connector.init_client(options, secret_data, schema)
        service_account_info = self.space_connector.get_service_account(
            service_account_id
        )
        tags = service_account_info.get("tags", {})
        tags["is_sync"] = "true"
        self.space_connector.update_service_account(service_account_id, tags)

    def _make_cost_data(self, results, account_id):
        costs_data = []

        """ Source Data Model
        class CostSummaryItem(BaseModel):
            usage_date: str
            region: str
            service_code: str
            usage_type: str
            usage_unit: str
            instance_type: str
            tag_application: str
            tag_environment: str
            tag_name: str
            tag_role: str
            tag_service: str
            usage_quantity: float
            usage_cost: float
        """

        for result in results:
            try:
                region = result["region"] or "USE1"
                data = {
                    "cost": result["usage_cost"],
                    "currency": "USD",
                    "usage_quantity": result["usage_quantity"],
                    "provider": "aws",
                    "region_code": _REGION_MAP.get(region, region),
                    "product": result["service_code"],
                    "account": account_id,
                    "usage_type": result["usage_type"],
                    "usage_unit": None,
                    "billed_at": datetime.strptime(result["usage_date"], "%Y-%m-%d"),
                    "additional_info": {
                        "Instance Type": self._parse_usage_type(result)
                    },
                    "tags": self._get_tags_from_cost_data(result),
                }

                tag_application = result.get("tag_application")
                tag_environment = result.get("tag_environment")
                tag_name = result.get("tag_name")
                tag_role = result.get("tag_role")
                tag_service = result.get("tag_service")

                if tag_application:
                    data["tags"]["Application"] = tag_application

                if tag_environment:
                    data["tags"]["Environment"] = tag_environment

                if tag_name:
                    data["tags"]["Name"] = tag_name

                if tag_role:
                    data["tags"]["Role"] = tag_role

                if tag_service:
                    data["tags"]["Service"] = tag_service

            except Exception as e:
                _LOGGER.error(f"[_make_cost_data] make data error: {e}", exc_info=True)
                raise e

            costs_data.append(data)

            # Excluded because schema validation is too slow
            # cost_data = Cost(data)
            # cost_data.validate()
            #
            # costs_data.append(cost_data.to_primitive())

        return costs_data

    @staticmethod
    def _get_tags_from_cost_data(cost_data: dict) -> dict:
        tags = {}

        if tags_str := cost_data.get("tags"):
            try:
                tags_dict: dict = utils.load_json(tags_str)
                for key, value in tags_dict.items():
                    key = key.replace("user:", "")
                    tags[key] = value
            except Exception as e:
                _LOGGER.debug(e)

        if tag_application := cost_data.get("tag_application"):
            tags["Application"] = tag_application

        if tag_environment := cost_data.get("tag_environment"):
            tags["Environment"] = tag_environment

        if tag_name := cost_data.get("tag_name"):
            tags["Name"] = tag_name

        if tag_role := cost_data.get("tag_role"):
            tags["Role"] = tag_role

        if tag_service := cost_data.get("tag_service"):
            tags["Service"] = tag_service

        return tags

    @staticmethod
    def _parse_usage_type(cost_info):
        service_code = cost_info["service_code"]
        usage_type = cost_info["usage_type"]

        if service_code == "AWSDataTransfer":
            if usage_type.find("-In-Bytes") > 0:
                return "data-transfer.in"
            elif usage_type.find("-Out-Bytes") > 0:
                return "data-transfer.out"
            else:
                return "data-transfer.etc"
        elif service_code == "AmazonCloudFront":
            if usage_type.find("-HTTPS") > 0:
                return "requests.https"
            elif usage_type.find("-Out-Bytes") > 0:
                return "data-transfer.out"
            else:
                return "requests.http"
        else:
            return cost_info["instance_type"]

    @staticmethod
    def _check_task_options(task_options, task_type):

        if "start" not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key="task_options.start")

        if "database" not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key="task_options.database")

        if task_type == "identity":
            if "account_id" not in task_options:
                raise ERROR_REQUIRED_PARAMETER(key="task_options.account_id")

            if "service_account_id" not in task_options:
                raise ERROR_REQUIRED_PARAMETER(key="task_options.service_account_id")

        if "is_sync" not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key="task_options.is_sync")

    @staticmethod
    def _get_date_range(start):
        date_ranges = []
        start_time = datetime.strptime(start, "%Y-%m-%d")
        now = datetime.utcnow()
        for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_time, until=now):
            billed_month = dt.strftime("%Y-%m")
            date_ranges.append(billed_month)

        return date_ranges
