import os
import time

os.environ['TZ'] = 'UTC'
time.tzset()


# Import the standard integration fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]
