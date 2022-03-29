from schematics.models import Model
from schematics.types import DictType, StringType, FloatType, DateTimeType

__all__ = ['Cost']


class Cost(Model):
    cost = FloatType(required=True)
    currency = StringType(default='USD')
    usage_quantity = FloatType(required=True)
    provider = StringType(required=True)
    region_code = StringType()
    product = StringType()
    account = StringType(required=True)
    usage_type = StringType()
    billed_at = DateTimeType(required=True)
