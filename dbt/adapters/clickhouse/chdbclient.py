import json
import uuid
from pathlib import Path
from typing import List

import pkg_resources
from chdb import session, ChdbError
from chdb.dbapi import converters
from dbt.adapters.__about__ import version as dbt_adapters_version
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.clickhouse import ClickHouseColumn, ClickHouseCredentials
from dbt.adapters.clickhouse.__version__ import version as dbt_clickhouse_version
from dbt.adapters.clickhouse.dbclient import ChClientWrapper, ChRetryableException
from dbt.adapters.clickhouse.logger import logger
from dbt.adapters.clickhouse.query import quote_identifier

try:
    driver_version = pkg_resources.get_distribution("chdb").version
except pkg_resources.ResolutionError:
    driver_version = "unknown"


class ChDBClient(ChClientWrapper):
    def query(self, sql, **kwargs):
        # TODO: we might need to preprocess `sql`
        try:
            result = self._client.query(sql, "JSON", **kwargs)
            result = CHDBResult(result=result)
            result.read()
            return result
        except CHDBResultError as ex:
            raise DbtDatabaseError(f"reading result from chdb query using json failed: {str(ex).strip()}") from ex
        except ChdbError as ex:
            raise DbtDatabaseError(f"chdb query failed with exception: {str(ex).strip()}") from ex
        except Exception as ex:
            raise DbtDatabaseError(str(ex).strip()) from ex

    def command(self, sql, **kwargs):
        try:
            result = self._client.query(sql, **kwargs)
            if result.has_error():
                raise DbtDatabaseError(str(result.error_message.strip()))
            elif result.size() == 0:
                return True
            else:
                result = int(result.data())
            return result
        except Exception as ex:
            raise DbtDatabaseError(f"chdb command failed with exception: {str(ex).strip()}") from ex


    def columns_in_query(self, sql: str, **kwargs) -> List[ClickHouseColumn]:
        try:
            query_result = self._client.query(
                f"SELECT * FROM ( \n" f"{sql} \n" f") LIMIT 0",
                **kwargs,
            )
            return [
                ClickHouseColumn.create(name, ch_type.name)
                for name, ch_type in zip(query_result.column_names, query_result.column_types)
            ]
        except ChdbError as ex:
            raise DbtDatabaseError(f"chdb columns_in_query failed with exception: {str(ex).strip()}") from ex
        except Exception as ex:
            raise DbtDatabaseError(str(ex).strip()) from ex

    def get_ch_setting(self, setting_name):
        try:
            result = self._client.query(
                f"SELECT value, readonly FROM system.settings WHERE name = '{setting_name}'",
                "JSON",
            )
            if result.has_error():
                raise DbtDatabaseError(str(result.error_message.strip()))
            else:
                result = json.loads(result.data())
                result = result["data"][0]
                return (result["value"], int(result["readonly"])) if result else (None, 0)
        except Exception as ex:
            logger.warning("Unexpected error retrieving ClickHouse server setting", ex)
            return None

    def close(self):
        pass
        # self._client.cleanup()

    def _create_client(self, credentials: ClickHouseCredentials):
        chdb_state_dir = Path(credentials.chdb_state_dir)

        if not chdb_state_dir.exists():
            logger.debug(f"Provided chdb_state_dir doesn't exist: {chdb_state_dir}")
            chdb_state_dir.mkdir(parents=True, exist_ok=True)

        session_dir = chdb_state_dir / f"{self._conn_settings['session_id']}"
        logger.info(f"Provided session_dir: {session_dir}")
        client = session.Session(path=session_dir.as_posix())

        chdb_dump_dir = Path(credentials.chdb_dump_dir)
        chdb_dump_files = list(chdb_dump_dir.glob("*.sql"))
        if len(chdb_dump_files) == 0:
            logger.warning(f"Provided chdb_dump_files is empty: {chdb_dump_files}")
            return

        for chdb_dump_file in chdb_dump_files:
            sql_content = chdb_dump_file.read_text()
            try:
                client.query(sql_content)
            except ChdbError as ex:
                raise DbtDatabaseError(f"client creation failed with exception: {str(ex).strip()}") from ex
        return client

    def _set_client_database(self):
        pass

    def _server_version(self):
        return self._client.query("select version()").data().strip().replace('"', "")


class CHDBResultError(Exception):
    pass


# TODO: This is from https://github.com/chdb-io/chdb/blob/e326128df44248b187b4f421bf6a5c796791b2dc/chdb/dbapi/connections.py#L175C1-L217C70
#       We might want to use the dbApi instead
class CHDBResult:
    def __init__(self, result):
        """
        :type connection: Connection
        """
        self.result = result
        self.affected_rows = 0
        self.insert_id = None
        self.warning_count = 0
        self.message = None
        self.field_count = 0
        self.description = None
        self.rows = None
        self.has_next = None
        self.result_set = None
        self.column_names = None

    def read(self):
        # Handle empty responses (for instance from CREATE TABLE)
        if self.result is None:
            return

        if self.result.has_error():
            raise CHDBResultError(str(self.result.error_message.strip()))

        try:
            data = json.loads(self.result.data())
        except Exception as error:
            raise CHDBResultError("Unexpected error when loading query result in JSON") from error

        try:
            self.field_count = len(data["meta"])
            description = []
            column_names = []
            for meta in data["meta"]:
                fields = [meta["name"], meta["type"]]
                column_names.append(meta["name"])
                description.append(tuple(fields))
            self.description = tuple(description)
            self.column_names = column_names
            rows = []
            for line in data["data"]:
                row = []
                for i in range(self.field_count):
                    column_data = converters.convert_column_data(
                        self.description[i][1], line[self.description[i][0]]
                    )
                    row.append(column_data)
                rows.append(tuple(row))
            self.rows = tuple(rows)
            self.result_set = tuple(rows)
        except Exception as error:
            raise CHDBResultError("Read return data err") from error
