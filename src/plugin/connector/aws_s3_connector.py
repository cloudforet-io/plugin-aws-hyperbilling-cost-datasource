import logging
import boto3
import io
import pandas as pd
import numpy as np

from spaceone.core import utils
from spaceone.core.connector import BaseConnector
from spaceone.core.error import *

__all__ = ['AWSS3Connector']

_LOGGER = logging.getLogger(__name__)

_PAGE_SIZE = 2000


class AWSS3Connector(BaseConnector):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = None
        self.s3_client = None
        self.s3_bucket = None

    def create_session(self, options: dict, secret_data: dict, schema: str):
        self._check_secret_data(secret_data)

        self.s3_bucket = secret_data["aws_s3_bucket"]
        aws_access_key_id = secret_data["aws_access_key_id"]
        aws_secret_access_key = secret_data["aws_secret_access_key"]
        region_name = secret_data.get("region_name")
        role_arn = secret_data.get("role_arn")
        external_id = secret_data.get("external_id")

        if role_arn:
            self._create_session_aws_assume_role(
                aws_access_key_id,
                aws_secret_access_key,
                region_name,
                role_arn,
                external_id,
            )
        else:
            self._create_session_aws_access_key(
                aws_access_key_id, aws_secret_access_key, region_name
            )

        self.s3_client = self.session.client("s3")

    def list_objects(self, path, delimiter=None):
        params = {"Bucket": self.s3_bucket, "Prefix": path}
        if delimiter is not None:
            params["Delimiter"] = delimiter

        return self.s3_client.list_objects(**params)

    def get_cost_data(self, key):
        obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        df = df.replace({np.nan: None})

        costs_data = df.to_dict("records")

        _LOGGER.debug(f"[get_cost_data] costs count({key}): {len(costs_data)}")

        # Paginate
        page_count = int(len(costs_data) / _PAGE_SIZE) + 1

        for page_num in range(page_count):
            offset = _PAGE_SIZE * page_num
            yield costs_data[offset : offset + _PAGE_SIZE]

    @staticmethod
    def _check_secret_data(secret_data):
        if "aws_access_key_id" not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key="secret_data.aws_access_key_id")

        if "aws_secret_access_key" not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key="secret_data.aws_secret_access_key")

        if "aws_s3_bucket" not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key="secret_data.aws_s3_bucket")

    def _create_session_aws_access_key(
        self, aws_access_key_id, aws_secret_access_key, region_name
    ):
        self.session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

        sts = self.session.client("sts")
        sts.get_caller_identity()

    def _create_session_aws_assume_role(
        self,
        aws_access_key_id,
        aws_secret_access_key,
        region_name,
        role_arn,
        external_id,
    ):
        self._create_session_aws_access_key(
            aws_access_key_id, aws_secret_access_key, region_name
        )

        sts = self.session.client("sts")

        _assume_role_request = {
            "RoleArn": role_arn,
            "RoleSessionName": utils.generate_id("AssumeRoleSession"),
        }

        if external_id:
            _assume_role_request.update({"ExternalId": external_id})

        assume_role_object = sts.assume_role(**_assume_role_request)
        credentials = assume_role_object["Credentials"]

        self.session = boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            region_name=region_name,
            aws_session_token=credentials["SessionToken"],
        )
