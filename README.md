# bigrays
`bigrays` is a framework for writing ETL jobs.

# Installation
`bigrays` can be installed with `pip` as follows

```shell
pip install -e git+https://github.com/CadentTech/bigrays#egg=bigrays
```

Note that without the `-e` flag and `#egg=bigrays` on the end of the url `pip freeze` will output `bigrays==<version>`
rather than `-e git+https://...` as typically desired.

If you want to install `bigrays` from a specific commit or tag, e.g. tag `1.0.0` simply and 
`@<commit-or-tag>` immediately before `#egg=bigrays`.

```shell
pip install -e git+https://github.com/CadentTech/bigrays@1.0.0#egg=bigrays
```

For more details on this topic see [here](https://codeinthehole.com/tips/using-pip-and-requirementstxt-to-install-from-the-head-of-a-github-branch/)

# Usage
With `bigrays` users can define (and run) an ETL job as a series of tasks. A task is simply a subclass
of `bigrays.tasks.Task` such as `bigrays.tasks.SQLExecute` or `bigrays.tasks.S3Upload`.

Once defined a job is executed by calling `bigrays.bigrays_run()`. Users can pass the sequence of tasks
to to `bigrays_run` explicitly

```python
bigrays_run(MyQuery, ProcessResultSet, Upload)
```

Or for convenience users can simply call `bigrays_run()` without any arguments and the tasks will be run
in the order they were defined.

## Example
The example below demonstrates the following
- How to query a database and retrieve the results as a `pandas.DataFrame` (`PullTrainingData`, `PullProductionData`).
- How to define a custom task and work with output from previous tasks (`MakePredictions`).
- How to upload the result of a previous task to S3 (`UploadPredictions`) as a CSV file.
- How to publish to an SNS topic.
- How to customize a `bigrays` script with parameters not known until runtime (`update_format_kws()`,
  `PullTrainingData`, `PullProductionData`, `UploadPredictions`).


```python
from datetime import datetime, timedelta
from bigrays import tasks, bigrays_run

class PullTrainingData(tasks.SQLQuery):
    query = '''
        select *
        from my_training_data
        where date between {train_period_begin} and {train_period_end}
    '''
    
class PullProductionData(tasks.SQLQuery):
    query = '''
       select *
       from my_production_data
    '''

class MakePredictions(tasks.Task):
    def run(self):
        # get training data and fit model
        X = TrainingData.output.drop('target', axis=1)
        y = TrainingData.output[['target']]
        model.fit(X, y)

        # predict on unseen data
        production_data = PullProductionData.output
        production_data['prediction'] = model.predict(production_data)
        return production_data
        
class UploadPredictions(tasks.ToS3):
    input = MakePredictions.output
    bucket = 'my-bucket/{train_period_begin}-{train_period_end}.csv'
    key = 'predictions.csv'
    
class EmailTeam(tasks.SNSPublishEmail):
    input = 'Predictions are upload to s3'
    topic = 's3-email-topic'
    subject = 'Predictions are finished'
    
if __name__ == '__main__':
    tasks.update_format_kws(
        'train_period_begin': (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d'),
        'train_period_end': datetime.now().strftime('%Y-%m-%d')
    )
    bigrays_run()
```

For additional examples see [the examples directory](./examples).

# Functional Interface

The subclass-based interface is convenient for defining pipelines of jobs since `bigrays` handles
the boilerplate of executing the tasks in order and it is declarative. However for one-off tasks
or tasks you want to run interactively. For this purpose bigrays comes with a functional interface.
For each `Task` class, `bigrays` exposes a functional interface to that class. The signature of the
function takes keyword arguments only and these arguments are the same that you would define in the
class body of the `Task`. For example with the functional interface

```python
from bigrays import tasks, bigrays_run

class MyQuery(tasks.SQLQuery):
    query = 'select top 1 * my_table'
    
bigrays_run(MyQuery)
results = MyQuery.output
```

becomes

```python
from bigrays import sql_query
results = sql_query(query='select top 1 * from my_table')
```

# Additional usage details

## Task execution order
Note that tasks are executed sequentially in the same order as they are defined unless passed explicitly
to `bigrays_run`.

## The Task protocol
Tasks are the central feature in `bigrays`. Tasks are any class that inherits from `bigrays.tasks.BaseTask`
and implements a `run()` method.
Additionally if a task requires a particular resource it should set the attribute `.required_resource`
to the resource it needs (e.g. `bigrays.resources.SQLSession`).

Users should rarely need to define their own tasks however as `bigrays` provides tasks to accomplish
common ETL tasks. Before defining your own task you should be sure that `bigrays` has not already
implemented one to get the job you need done for you.

# Resources
`bigrays` is designed to manage external resources (such as database or S3 connections) as
needed so that the user need not be concerned with this task. However there are two things
about how `bigrays` handles resources to keep in mind when defining a job.

1. Only one resource is ever open at a time and for is kept open as long as possible. This
   means once a resource is opened it will remain open until a task is executed which
   requires a different context.
2. Each resource may require its own set of credentials needed to open the resource. On how to
   configure the credentials for your job see [Configuration](Configuration).

The first point implies that consecutive tasks which require the same resource will all access
the same resource. This is why several `SQLQuery` tasks can access the same temporary tables
if they are executed consecutively. However this also means that if another `SQLTask` were to
follow `ToS3` it **would not** have access to any of these temporary tables.

# Configuration
In order to access certain resources, the following attributes of `bigrays.config.BigraysConfig`
may need to be set.

- `AWS_REQUIRE_SECRETS`: Boolean indicating if AWS credentials required. Set to
  False if using AWS roles or ~/.aws/credentials.
- `AWS_ACCESS_KEY_ID`: Required by tasks which interact with AWS services if `AWS_REQUIRE_SECRETS=TRUE`.
- `AWS_SECRET_ACCESS_KEY`: Required by tasks which interact with AWS services if `AWS_REQUIRE_SECRETS=TRUE`.
- `AWS_REGION`: Required by tasks which interact with AWS services if `AWS_REQUIRE_SECRETS=TRUE`.
- `ODBC_UID`: UID value for ODBC connections
- `ODBC_PWD`: PWD value for ODBC connections
- `ODBC_DSN`: DSN value for ODBC connections
- `ODBC_FLAVOR`: The SQL flavor, or dialect as compatible with `pyodbc`. E.g. `mssql`
- `ODBC_CONNECT_PARAMS`: List of query parameters to include

These can be assigned directly within a script (e.g. `BigraysConfig.AWS_REGION = 'us-east'`)
or by setting the environment variable `BIGRAYS_<PARAMETER_NAME>` (e.g. `export BIGRAYS_AWS_REGION='us-east'`).

# Logging
By default `bigrays` logs silently to a null handler. However, the `bigrays` logger can be
retrieved with `logging.getLogger('bigrays')` and configured as usual.
