import clickhouse_driver


class ChNativeAdapter:
    def __init__(self, client: clickhouse_driver.Client):
        self.client = client

    def query(self, sql):
        return NativeAdapterResult(self.client.execute(sql, with_column_types=True))

    def command(self, sql):
        self.client.execute(sql)

    def close(self):
        self.client.disconnect()


class NativeAdapterResult:
    def __init__(self, native_result):
        self.result_set = native_result[0]
        self.column_names = [col[0] for col in native_result[1]]
