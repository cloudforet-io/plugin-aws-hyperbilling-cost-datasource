import logging
from datetime import datetime, timedelta

from spaceone.core.error import *
from spaceone.core.manager import BaseManager

from ..connector.aws_s3_connector import AWSS3Connector
from ..connector.spaceone_connector import SpaceONEConnector

_LOGGER = logging.getLogger("spaceone")
_DEFAULT_DATABASE = "MZC"
_DEFAULT_RESYNC_DAYS = 10


class JobManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.space_connector = SpaceONEConnector()

    def get_tasks(
        self,
        domain_id: str,
        options: dict,
        secret_data: dict,
        schema: str = None,
        start: str = None,
        last_synchronized_at: datetime = None,
    ) -> dict:
        tasks = []
        changed = []

        _LOGGER.debug(f"[get_tasks] options: {options}")

        start_month = self._get_start_month(options, start, last_synchronized_at)
        self.space_connector.init_client(options, secret_data, schema)
        response = self.space_connector.list_projects(domain_id)
        total_count = response.get("total_count") or 0

        if total_count > 0:
            project_info = response["results"][0]
            _LOGGER.debug(f"[get_tasks] project info: {project_info}")

            project_id = project_info["project_id"]
            database = project_info.get("tags", {}).get("database", _DEFAULT_DATABASE)

            response = self.space_connector.list_service_accounts(project_id)
            for service_account_info in response.get("results", []):
                service_account_tags = service_account_info.get("tags", {})
                service_account_id = service_account_info["service_account_id"]
                service_account_name = service_account_info["name"]
                account_id = service_account_info["data"]["account_id"]
                is_sync = service_account_tags.get("is_sync", "false")
                database = service_account_tags.get("database", database)

                if is_sync != "true":
                    is_sync = "false"

                _LOGGER.debug(
                    f"[get_tasks] service_account({service_account_id}): "
                    f"name={service_account_name}, account_id={account_id}"
                )
                task_options = {
                    "is_sync": is_sync,
                    "service_account_id": service_account_id,
                    "service_account_name": service_account_name,
                    "account_id": account_id,
                    "database": database,
                    "task_type": "identity",
                }

                if is_sync == "false":
                    first_sync_month = self._get_start_month(options, start)
                    task_options["start"] = first_sync_month

                    task_changed = {
                        "start": first_sync_month,
                        "filter": {"additional_info.Account ID": account_id},
                    }

                else:
                    task_options["start"] = start_month
                    task_changed = {"start": start_month}

                tasks.append(
                    {"task_options": task_options, "task_changed": task_changed}
                )

            changed.append({"start": start_month})

            _LOGGER.debug(f"[get_tasks] tasks: {tasks}")
            _LOGGER.debug(f"[get_tasks] changed: {changed}")

        else:
            _LOGGER.debug(f"[get_tasks] no project: tags.domain_id = {domain_id}")

        return {"tasks": tasks, "changed": changed}

    def get_tasks_directory_type(
        self,
        domain_id: str,
        options: dict,
        secret_data: dict,
        schema: str = None,
        start: str = None,
        last_synchronized_at: datetime = None,
    ) -> dict:
        tasks = []
        changed = []

        aws_s3_connector = AWSS3Connector()
        aws_s3_connector.create_session(options, secret_data, schema)

        start_month: str = self._get_start_month(options, start, last_synchronized_at)

        database = options.get("database", _DEFAULT_DATABASE)
        accounts = secret_data.get("accounts", [])

        path = f"SPACE_ONE/billing/database={database}/"

        if not accounts:
            response = aws_s3_connector.list_objects(path, delimiter="/")
            path_length = len(path)

            for prefix in response.get("CommonPrefixes", []):
                folder = prefix["Prefix"]
                relative_path = folder[path_length:].lstrip("/")

                if relative_path:
                    first_folder = relative_path.split("/")[0]
                    if first_folder.startswith("account_id="):
                        account_id = first_folder.split("=", 1)[-1]
                        if account_id and account_id.strip():
                            accounts.append(account_id)

        for account_id in accounts:
            task_options = {
                "account_id": account_id,
                "database": database,
                "start": start_month,
                "is_sync": "true",
                "task_type": "directory",
            }
            task_changed = {
                "start": start_month,
                "filter": {"additional_info.Account ID": account_id},
            }
            tasks.append({"task_options": task_options, "task_changed": task_changed})

        changed.append({"start": start_month})

        _LOGGER.debug(f"[get_tasks] tasks: {tasks}")
        _LOGGER.debug(f"[get_tasks] changed: {changed}")

        return {"tasks": tasks, "changed": changed}

    def _get_start_month(
        self,
        options: dict,
        start: str = None,
        last_synchronized_at: datetime = None,
    ) -> str:
        resync_days = options.get(
            "resync_days_from_last_synced_at", _DEFAULT_RESYNC_DAYS
        )

        if start:
            start_time: datetime = self._parse_start_time(start)
        elif last_synchronized_at:
            start_time: datetime = last_synchronized_at - timedelta(days=resync_days)
            start_time = start_time.replace(day=1)
        else:
            start_time: datetime = datetime.utcnow() - timedelta(days=365)
            start_time = start_time.replace(day=1)

        start_time = start_time.replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )

        return start_time.strftime("%Y-%m")

    @staticmethod
    def _parse_start_time(start_str):
        date_format = "%Y-%m"

        try:
            return datetime.strptime(start_str, date_format)
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key="start", type=date_format)
