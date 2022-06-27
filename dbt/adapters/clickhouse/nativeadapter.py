import clickhouse_driver


class ChNativeAdapter:
    def __init__(self, client: clickhouse_driver.Client):
        self.client = client

    def query(self, sql, **kwargs):
        return NativeAdapterResult(self.client.execute(sql, with_column_types=True, **kwargs))

    def command(self, sql, **kwargs):
        result = self.client.execute(sql, **kwargs)
        if len(result) and len(result[0]):
            return result[0][0]

    def close(self):
        self.client.disconnect()

    @property
    def database(self):
        return self.client.connection.database

    @database.setter
    def database(self, database):
        self.client.connection.database = database


class NativeAdapterResult:
    def __init__(self, native_result):
        self.result_set = native_result[0]
        self.column_names = [col[0] for col in native_result[1]]
