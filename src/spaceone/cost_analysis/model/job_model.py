from schematics.models import Model
from schematics.types import ListType, IntType, DateTimeType, StringType, DictType
from schematics.types.compound import ModelType

__all__ = ['Tasks']


class TaskOptions(Model):
    start = StringType(required=True)
    service_account_id = StringType(required=True)
    service_account_name = StringType(required=True)
    account_id = StringType(required=True)
    database = StringType(required=True)
    is_sync = StringType(required=True, choices=('true', 'false'))


class Task(Model):
    task_options = ModelType(TaskOptions, required=True)


class Changed(Model):
    start = DateTimeType(required=True)
    end = DateTimeType(default=None)
    filter = DictType(StringType, default={})


class Tasks(Model):
    tasks = ListType(ModelType(Task), required=True)
    changed = ListType(ModelType(Changed), default=[])
