from typing import Generator
from spaceone.cost_analysis.plugin.data_source.lib.server import DataSourcePluginServer
from .manager.data_source_manager import DataSourceManager
from .manager.job_manager import JobManager
from .manager.cost_manager import CostManager

app = DataSourcePluginServer()


@app.route('DataSource.init')
def data_source_init(params: dict) -> dict:
    """ init plugin by options

    Args:
        params (DataSourceInitRequest): {
            'options': 'dict',    # Required
            'domain_id': 'str'    # Required
        }

    Returns:
        PluginResponse: {
            'metadata': 'dict'
        }
    """
    options = params['options']

    data_source_mgr = DataSourceManager()
    return data_source_mgr.init_response(options)


@app.route('DataSource.verify')
def data_source_verify(params: dict) -> None:
    """ Verifying data source plugin

    Args:
        params (CollectorVerifyRequest): {
            'options': 'dict',      # Required
            'secret_data': 'dict',  # Required
            'schema': 'str',
            'domain_id': 'str'
        }

    Returns:
        None
    """

    options = params['options']
    secret_data = params['secret_data']
    schema = params.get('schema')

    data_source_mgr = DataSourceManager()
    data_source_mgr.verify_plugin(options, secret_data, schema)


@app.route('Job.get_tasks')
def job_get_tasks(params: dict) -> dict:
    """ Get job tasks

    Args:
        params (JobGetTaskRequest): {
            'options': 'dict',      # Required
            'secret_data': 'dict',  # Required
            'schema': 'str',
            'start': 'str',
            'last_synchronized_at': 'datetime',
            'domain_id': 'str'
        }

    Returns:
        TasksResponse: {
            'tasks': 'list',
            'changed': 'list'
        }

    """

    domain_id = params['domain_id']
    options = params['options']
    secret_data = params['secret_data']
    schema = params.get('schema')
    start = params.get('start')
    last_synchronized_at = params.get('last_synchronized_at')

    job_mgr = JobManager()
    return job_mgr.get_tasks(domain_id, options, secret_data, schema, start, last_synchronized_at)


@app.route('Cost.get_data')
def cost_get_data(params: dict) -> Generator[dict, None, None]:
    """ Get external cost data

    Args:
        params (CostGetDataRequest): {
            'options': 'dict',      # Required
            'secret_data': 'dict',  # Required
            'schema': 'str',
            'task_options': 'dict',
            'domain_id': 'str'
        }

    Returns:
        Generator[ResourceResponse, None, None]
        {
            'cost': 'float',
            'usage_quantity': 'float',
            'usage_unit': 'str',
            'provider': 'str',
            'region_code': 'str',
            'product': 'str',
            'usage_type': 'str',
            'resource': 'str',
            'tags': 'dict'
            'additional_info': 'dict'
            'data': 'dict'
            'billed_date': 'str'
        }
    """

    options = params['options']
    secret_data = params['secret_data']

    task_options = params.get('task_options', {})
    schema = params.get('schema')

    cost_mgr = CostManager()
    return cost_mgr.get_data(options, secret_data, task_options, schema)
