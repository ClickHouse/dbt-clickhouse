"""
This adds all subdirectories of directories on `sys.path` to this package’s `__path__` .
It effectively combines all adapters into a single namespace (dbt.adapter).
"""

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)
