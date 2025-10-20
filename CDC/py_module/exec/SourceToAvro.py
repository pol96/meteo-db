from fastavro import writer, parse_schema
from decimal import Decimal, ROUND_DOWN
import time 
from datetime import datetime as dt
from py_module.metadata.connection.StorageBaseConnection import StorageConn

class ExecuteGCS(StorageConn):
    def __init__(self,
                 table_schema: str,
                 table_name: str,
                 bucket_name: str,
                 db_name: str,
                 max_file_size_mb: int = 400,
                 credential_path: str | None = None,
                 blob_name: str | None = None,
                 hive_partition_dir: str | None = None
                 ):
        
        super().__init__(credential_path=credential_path)
        today = dt.today().strftime("%Y-%m-%d")
        now = dt.now().strftime("%d%m%Y%H%M%S")
        base_hive_partition = f'{db_name}/{table_schema}/{table_name}/ingestion_date={today}/'
        base_blob_name = f'chunk_{now}'

        if blob_name:
            base_blob_name = blob_name
        
        if hive_partition_dir:
            base_hive_partition = hive_partition_dir
        
        self.bucket_name = bucket_name
        self.blob_name = ''.join([base_hive_partition,base_blob_name])
        self.max_file_size = max_file_size_mb * 1024 * 1024
        self.table_schema = table_schema
        self.table_name = table_name

    def fix_decimal(self, value, scale):
        quantize_map = {0: Decimal('1')}
        quantize_map.update({i: Decimal('0.' + '0' * (i-1) + '1') for i in range (1,21)})
        if value is None:
            return None 
        return Decimal(value).quantize(quantize_map[scale], rounding=ROUND_DOWN)

    def avro_record_generator(self, cdc_exec, avro_schema):
        cols = cdc_exec.keys()

        for row in cdc_exec:
            record = dict(zip(cols, row))
            for field in avro_schema['fields']:
                f_name = field['name']
                f_type = field['type']

                types = f_type if isinstance(f_type, list) else [f_type]
                for t in types:
                    if isinstance(t, dict) and t.get('logicalType') == 'decimal':
                        if f_name in record and record[f_name] is not None:
                            record[f_name] = self.fix_decimal(value = record[f_name], 
                                                              scale = t['scale'])
            yield record
    
    def define_avro_schema(self, avro_columns):
        avro_schema = {
            "name": f"{self.table_schema}_{self.table_name}__record",
            "type": "record",
            "fields": avro_columns
        }

        return parse_schema(avro_schema)

    def yield_chunks(self,
                     avro_columns: list | dict,
                     cdc_executor
                     ):

        avro_schema = self.define_avro_schema(avro_columns=avro_columns)
        file_counter = 1 
        record_iter = self.avro_record_generator(avro_schema=avro_schema,
                                                 cdc_exec=cdc_executor)
        total_records_written = 0
        file_records = []

        while True:
            chunk_records = []
            chunk_size = 0
            start_time = time.time()

            try:
                while True:
                    record = next(record_iter)
                    chunk_records.append(record)

                    chunk_size += len(str(record).encode('utf-8'))
                    if chunk_size >= self.max_file_size:
                        break
            except StopIteration: 
                if not chunk_records:
                    break
            
            blob_name = ''.join([self.blob_name,f'__part{file_counter:04d}.avro'])
            blob = self.define_blob(
                bucket_name = self.bucket_name,
                blob_name = blob_name
            )

            with blob.open('wb', ignore_flush = True) as file:
                writer(file, avro_schema, chunk_records)
            
            records_written = len(chunk_records)
            total_records_written += records_written
            file_records.append(records_written)

            elapsed_time = time.time() - start_time
            throughput = (chunk_size / elapsed_time / (1024 * 1024)) if elapsed_time > 0 else 0

            file_counter += 1
