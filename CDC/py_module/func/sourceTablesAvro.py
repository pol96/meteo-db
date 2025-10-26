from py_module.metadata.connection.ConnectionRegistry import PostgresConnection as pgconn
from py_module.metadata.render_jinja.RenderRegistry import ChangeDataCaptureExtraction as cdc, RetrieveSchema as rs, SchemaSource as ss 
from py_module.metadata.datatype_conversion.avro import DataTypeConverter as dtc
from py_module.metadata.logging.logger import BaseLogger
from sqlalchemy import text
import json
from fastavro import parse_schema
import os
from py_module.metadata.connection.ConnectionRegistry import BaseConnection

class sourceTable(BaseLogger):
    def __init__(
            self, 
            params:dict,
            bucket_name:str,
            table_name:str | None = None,
            table_schema:str | None = None,
            base_dir:str | None = None,
            staging_dataset:str = 'staging'
    ):
        BaseLogger.__init__(self, logger_name="table")


        self.database = params['database']
        self.db = params['db']
        self.table_name = table_name
        self.table_schema = table_schema
        self.bucket_name = bucket_name
        self.staging_dataset = staging_dataset

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

        self.schema_source = ss.render_jinja()

        retrieve_schema = rs.render_jinja(database=params['database'])
        self.rendered_schema = retrieve_schema.render(
                                        table_schema=self.table_schema,
                                        table_name=self.table_name
                                    )
        
        self.converter = dtc()
        self.base_dir = base_dir if base_dir else os.path.join(os.getcwd(),'source_db_schema')

    def _connect(self):
        try:
            conn = self.engine.connect()
            self.get_logger().debug("Database connection established")
            return conn
        except Exception as e:
            self.get_logger().exception(f"Connection refused to db {self.params['db']}")
            raise ConnectionError(f"Connection refused to db {self.params['db']}") from e

    def _get_tables(self):
        conn = self._connect()
        iterator = conn.execute(text(self.rendered_schema))
        self.tables = iterator.fetchall()
        self.dist_datasets = list(set(i[0] for i in self.tables))
        self.dist_tables = list(set(i[1] for i in self.tables))
    
    def extract_metadata(self):
        self._get_tables()

        for d in self.dist_datasets:
            for t in self.dist_tables:

                tmp_records = [col for col in self.tables if col[0] == str(d) and col[1] == str(t)]
                target_dir = os.path.join(self.base_dir,self.database,self.db,d)

                cdc_columns = []
                avro_columns = []
                dbt_columns = []

                for c in tmp_records:
                    col_name = c[2]
                    db_type = c[3]
                    precision = c[4]
                    scale = c[5]

                    avro_type = self.converter.source_to_avro(self.database, db_type, numeric_precision=precision, numeric_scale=scale)
                    bq_type = self.converter.source_to_bigquery(self.database, db_type)

                    # setup for cdc model injection
                    cdc_columns.append(col_name)

                    # setup the columns for avro injection with default
                    avro_field = self.converter.generate_avro_field(col_name, avro_type)
                    avro_columns.append(avro_field)

                    # setup the columns for dbt source
                    dbt_columns.append({col_name: bq_type})

                dbt_source_model = self.schema_source.render(
                    schema_name = d, 
                    table_name = t, 
                    staging_dataset = self.staging_dataset, 
                    database = self.database, 
                    istance_name = self.db,
                    bucket_name = self.bucket_name,
                    version = 'v1', 
                    cols = dbt_columns
                )

                avro_schema = {
                    "name": f"{d}_{t}__record",
                    "type": "record",
                    "fields": avro_columns
                }

                parsed_schema = parse_schema(avro_schema)
                
                os.makedirs(target_dir,exist_ok=True)

                with open(os.path.join(target_dir,f'{d}__{t}.yml'),'w') as f:
                    f.write(dbt_source_model)
                        
                with open(os.path.join(target_dir,f'{d}__{t}_avro.json'), "w", encoding="utf-8") as f:
                    json.dump(parsed_schema, f, indent=2)

                
                    