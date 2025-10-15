import logging
from typing import Generator
from datetime import datetime
from dateutil import rrule

from spaceone.core import utils
from spaceone.core.manager import BaseManager
from spaceone.core.error import *
from ..connector.aws_s3_connector import AWSS3Connector
from ..connector.spaceone_connector import SpaceONEConnector

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
        self.aws_s3_connector = AWSS3Connector()
        self.space_connector = SpaceONEConnector()

    def get_data(
        self, options: dict, secret_data: dict, task_options: dict, schema: str = None
    ) -> Generator[dict, None, None]:
        task_type = task_options.get("task_type", "identity")

        self.aws_s3_connector.create_session(options, secret_data, schema)
        self._check_task_options(task_options)

        start = task_options["start"]
        account_id = task_options["account_id"]
        database = task_options["database"]

        # update SpaceONE service account tags info
        if task_type == "identity":
            service_account_id = task_options["service_account_id"]
            is_sync = task_options["is_sync"]
            if is_sync == "false":
                self._update_sync_state(
                    options, secret_data, schema, service_account_id
                )

        date_ranges = self._get_date_range(start)

        include_credit = options.get("include_credit", True)

        for date in date_ranges:
            year, month = date.split("-")
            path = f"SPACE_ONE/billing/database={database}/account_id={account_id}/year={year}/month={month}"
            response = self.aws_s3_connector.list_objects(path)
            contents = response.get("Contents", [])
            for content in contents:
                response_stream = self.aws_s3_connector.get_cost_data(content["Key"])
                for results in response_stream:
                    yield self._make_cost_data(results, account_id, include_credit)

        yield {"results": []}

    def _update_sync_state(self, options, secret_data, schema, service_account_id):
        self.space_connector.init_client(options, secret_data, schema)
        service_account_info = self.space_connector.get_service_account(
            service_account_id
        )
        tags = service_account_info.get("tags", {})
        tags["is_sync"] = "true"
        self.space_connector.update_service_account(service_account_id, tags)

    def _make_cost_data(self, results, account_id, include_credit):
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
                service_code = result["service_code"]
                usage_type = result["usage_type"]

                if not include_credit and service_code == "Credit":
                    continue

                data = {
                    "cost": result.get("usage_cost", 0.0) or 0.0,
                    "usage_quantity": result["usage_quantity"],
                    "usage_unit": None,
                    "provider": "aws",
                    "region_code": _REGION_MAP.get(region, region),
                    "product": service_code,
                    "usage_type": usage_type,
                    "billed_date": result["usage_date"],
                    "additional_info": {
                        "Instance Type": result["instance_type"],
                        "Account ID": account_id,
                    },
                    "tags": self._get_tags_from_cost_data(result),
                }

                if service_code == "AWSDataTransfer":
                    data["usage_unit"] = "Bytes"
                    if usage_type.find("-In-Bytes") > 0:
                        data["additional_info"]["Usage Type Details"] = "Transfer In"
                    elif usage_type.find("-Out-Bytes") > 0:
                        data["additional_info"]["Usage Type Details"] = "Transfer Out"
                    else:
                        data["additional_info"]["Usage Type Details"] = "Transfer Etc"
                elif service_code == "AmazonCloudFront":
                    if usage_type.find("-HTTPS") > 0:
                        data["usage_unit"] = "Count"
                        data["additional_info"]["Usage Type Details"] = "HTTPS Requests"
                    elif usage_type.find("-Out-Bytes") > 0:
                        data["usage_unit"] = "GB"
                        data["additional_info"]["Usage Type Details"] = "Transfer Out"
                    else:
                        data["usage_unit"] = "Count"
                        data["additional_info"]["Usage Type Details"] = "HTTP Requests"
                else:
                    data["additional_info"]["Usage Type Details"] = None

            except Exception as e:
                _LOGGER.error(f"[_make_cost_data] make data error: {e}", exc_info=True)
                raise e

            costs_data.append(data)

        return {"results": costs_data}

    @staticmethod
    def _get_tags_from_cost_data(cost_data: dict) -> dict:
        tags = {}

        if tags_str := cost_data.get("tags"):
            try:
                tags_dict: dict = utils.load_json(tags_str)
                for key, value in tags_dict.items():
                    # todo: remove this condition after fixing the issue
                    if "." in key:
                        continue
                    key = key.replace("user:", "")
                    tags[key] = value
            except Exception as e:
                _LOGGER.debug(e)

        return tags

    @staticmethod
    def _check_task_options(task_options: dict):
        task_type = task_options.get("task_type", "identity")

        if "start" not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key="task_options.start")

        if "database" not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key="task_options.database")

        if "is_sync" not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key="task_options.is_sync")

        if task_type == "identity":

            if "account_id" not in task_options:
                raise ERROR_REQUIRED_PARAMETER(key="task_options.account_id")

            if "service_account_id" not in task_options:
                raise ERROR_REQUIRED_PARAMETER(key="task_options.service_account_id")

    @staticmethod
    def _get_date_range(start):
        date_ranges = []
        start_time = datetime.strptime(start, "%Y-%m")
        now = datetime.utcnow()
        for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_time, until=now):
            billed_month = dt.strftime("%Y-%m")
            date_ranges.append(billed_month)

        return date_ranges
