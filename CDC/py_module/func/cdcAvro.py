from py_module.metadata.connection.ConnectionRegistry import BaseConnection
from py_module.metadata.render_jinja.RenderRegistry import (
    ChangeDataCaptureExtraction as cdc,
    RetrieveSchema as rs,
    SchemaSource as ss
)
from py_module.metadata.connection.StorageBaseConnection import StorageConn as storage
from py_module.metadata.datatype_conversion.avro import DataTypeConverter as dtc
from py_module.exec.SourceToAvro import ExecuteGCS
from sqlalchemy import text
from py_module.metadata.logging.logger import BaseLogger
from typing import Union


class table(BaseLogger):
    def __init__(self, params: dict):
        BaseLogger.__init__(self, logger_name="table")
        self.params = params
        self.database = params["database"].lower()

        if self.database not in BaseConnection._registry:
            self.get_logger().error(
                f"Unknown database type '{self.database}'. "
                f"Available: {list(BaseConnection._registry.keys())}"
            )
            raise ValueError(
                f"Unknown database type '{self.database}'. "
                f"Available: {list(BaseConnection._registry.keys())}"
            )

        self.engine = BaseConnection._registry[self.database].get_engine(**params)
        self.get_logger().info(f"Using engine for database '{self.database}'")

        self.cdc_query = cdc.render_jinja(database=self.database)
        self.retrieve_schema = rs.render_jinja(database=self.database)
        self.schema_source = ss.render_jinja()
        self.dtype_conversion = dtc()

    def _connect(self):
        try:
            conn = self.engine.connect()
            self.get_logger().debug("Database connection established")
            return conn
        except Exception as e:
            self.get_logger().exception(f"Connection refused to db {self.params['db']}")
            raise ConnectionError(f"Connection refused to db {self.params['db']}") from e

    def _get_table_columns(self, table_schema: str, table_name: str):
        render_schema = self.retrieve_schema.render(
            table_schema=table_schema,
            table_name=table_name
        )
        conn = self._connect()
        schema = conn.execute(text(render_schema))

        self.dbt_columns = []
        self.avro_columns = []
        self.cdc_columns = []

        for i in schema:
            col_name = i[0]
            db_type = i[1]
            precision = i[2]
            scale = i[3]

            avro_type = self.dtype_conversion.source_to_avro(
                db_system=self.database,
                db_type=db_type,
                numeric_precision=precision,
                numeric_scale=scale
            )
            bq_type = self.dtype_conversion.source_to_bigquery(
                db_system=self.database,
                db_type=db_type
            )

            self.cdc_columns.append(col_name)

            avro_field = self.dtype_conversion.generate_avro_field(
                name=col_name,
                avro_type=avro_type
            )
            self.avro_columns.append(avro_field)

            self.dbt_columns.append({col_name: bq_type})

        conn.close()
        self.get_logger().info(
            f"Retrieved {len(self.avro_columns)} columns for table {table_schema}.{table_name}"
        )

    def exec_cdc(self,
                 table_schema: str,
                 table_name: str,
                 bucket_name: str,
                 delta: bool | None = None,
                 delta_column: str | None = None,
                 delta_timestamp: str | None = None,
                 where_conditions: Union[list, str] | None = None,
                 **storage_kwargs
                 ):
        self.get_logger().info(f"Starting CDC for {table_schema}.{table_name}")
        self._get_table_columns(table_schema=table_schema, table_name=table_name)

        query = self.cdc_query.render(
            columns=self.cdc_columns,
            schema_name=table_schema,
            table_name=table_name,
            delta=delta,
            delta_column=delta_column,
            delta_timestamp=delta_timestamp,
            where_conditions=where_conditions
        )

        with self._connect() as conn:
            cdc_exec = conn.execution_options(stream_results=True).execute(text(query))

            executor = ExecuteGCS(
                table_schema=table_schema,
                table_name=table_name,
                bucket_name=bucket_name,
                db_name=self.database,
                **storage_kwargs
            )
            executor.get_logger().info(f"Writing CDC data to bucket {bucket_name}")
            executor.yield_chunks(
                avro_columns=self.avro_columns,
                cdc_executor=cdc_exec
            )
        self.get_logger().info(f"CDC completed for {table_schema}.{table_name}")
