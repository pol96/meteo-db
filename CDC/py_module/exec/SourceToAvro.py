from fastavro import writer, parse_schema
from decimal import Decimal, ROUND_DOWN
import time
from datetime import datetime as dt
from py_module.metadata.connection.StorageBaseConnection import StorageConn
from py_module.metadata.logging.logger import BaseLogger
import psutil
import os
class ExecuteGCS(StorageConn, BaseLogger):
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
        StorageConn.__init__(self, credential_path=credential_path)
        BaseLogger.__init__(self, logger_name=self.__class__.__name__)

        try:
            today = dt.today().strftime("%Y-%m-%d")
            now = dt.now().strftime("%d%m%Y%H%M%S")
            base_hive_partition = f'{db_name}/{table_schema}/{table_name}/ingestion_date={today}/'
            base_blob_name = f'chunk_{now}'

            if blob_name:
                base_blob_name = blob_name
            if hive_partition_dir:
                base_hive_partition = hive_partition_dir

            self.bucket_name = bucket_name
            self.blob_name = ''.join([base_hive_partition, base_blob_name])
            self.max_file_size = max_file_size_mb * 1024 * 1024
            self.table_schema = table_schema
            self.table_name = table_name

            self.get_logger().info(f"Initialized ExecuteGCS")

        except Exception as e:
            self.get_logger().exception(f"Failed to initialize ExecuteGCS: {e}")
            raise

    def fix_decimal(self, value, scale):
        if value is None:
            return None
        quantize_map = {0: Decimal('1')}
        quantize_map.update({i: Decimal('0.' + '0' * (i - 1) + '1') for i in range(1, 21)})
        return Decimal(value).quantize(quantize_map[scale], rounding=ROUND_DOWN)

    def avro_record_generator(self, cdc_exec, avro_schema):
        logger = self.get_logger()
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
                            record[f_name] = self.fix_decimal(value=record[f_name], scale=t['scale'])
            yield record
        logger.debug("Completed generating Avro records")

    def define_avro_schema(self, avro_columns):
        avro_schema = {
            "name": f"{self.table_schema}_{self.table_name}__record",
            "type": "record",
            "fields": avro_columns
        }
        parsed_schema = parse_schema(avro_schema)
        self.get_logger().debug(f"Defined Avro schema with {len(avro_columns)} fields")
        return parsed_schema

    def yield_chunks(self, avro_columns: list | dict, cdc_executor): 
        logger = self.get_logger()
        avro_schema = self.define_avro_schema(avro_columns=avro_columns)
        file_counter = 1
        record_iter = self.avro_record_generator(avro_schema=avro_schema, cdc_exec=cdc_executor)
        total_records_written = 0

        # Track memory
        process = psutil.Process(os.getpid())
        memory_samples = []

        while True:
            chunk_records = []
            chunk_size = 0
            start_time = time.time()

            try:
                while True:
                    record = next(record_iter)
                    chunk_records.append(record)
                    chunk_size += len(str(record).encode('utf-8'))
                    memory_samples.append(process.memory_info().rss)  # record memory usage
                    if chunk_size >= self.max_file_size:
                        break
            except StopIteration:
                if not chunk_records:
                    break

            blob_name = f'{self.blob_name}__part{file_counter:04d}.avro'
            blob = self.define_blob(bucket_name=self.bucket_name, blob_name=blob_name)

            try:
                with blob.open('wb', ignore_flush=True) as file:
                    writer(file, avro_schema, chunk_records)

                records_written = len(chunk_records)
                total_records_written += records_written
                elapsed_time = time.time() - start_time
                throughput = (chunk_size / elapsed_time / (1024 * 1024)) if elapsed_time > 0 else 0

                avg_mem_mb = sum(memory_samples) / len(memory_samples) / (1024 * 1024) if memory_samples else 0

                logger.info(f"Wrote {records_written} records to {blob_name}")
                logger.info(f"({chunk_size / (1024*1024):.2f} MB, {throughput:.2f} MB/s, avg memory {avg_mem_mb:.2f} MB)")

            except Exception as e:
                logger.exception(f"Failed to write chunk {blob_name}: {e}")
                raise

            file_counter += 1

        total_avg_mem_mb = sum(memory_samples) / len(memory_samples) / (1024 * 1024) if memory_samples else 0
        logger.info(f"Completed writing {total_records_written} total records in {file_counter-1} files")
        logger.info(f"Average memory usage during process: {total_avg_mem_mb:.2f} MB")