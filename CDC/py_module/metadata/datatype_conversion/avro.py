from datetime import date, datetime, time
from py_module.metadata.logging.logger import BaseLogger

class DataTypeConverter(BaseLogger):
    def __init__(self, defaults: dict | None = None):
        """
        Initialize the DataTypeConverter with type mappings and default values.

        Args:
            defaults (dict, optional): Override default values for types. 
                                       Keys are type names or logical types, 
                                       values are the desired default.
        """
        super().__init__()
        self.logger = self.get_logger()

        self.avro_to_bq_map = {
                                    "boolean": "BOOL",
                                    "int": "INT64",
                                    "long": "INT64",
                                    "float": "FLOAT64",
                                    "double": "FLOAT64",
                                    "string": "STRING",
                                    "bytes": "BYTES",

                                    "logicalTypes": {
                                        "decimal": "NUMERIC",
                                        "date": "DATE",
                                        "time-millis": "TIME",
                                        "time-micros": "TIME",
                                        "timestamp-millis": "TIMESTAMP",
                                        "timestamp-micros": "TIMESTAMP"
                                    },

                                    "complexTypes": {
                                        "enum": "STRING",
                                        "array": "ARRAY",
                                        "map": "STRUCT",
                                        "record": "STRUCT"
                                    }
                                }

        self.source_to_avro_map = {
                                    "postgres": {
                                        "boolean": ["null", "boolean"],
                                        "smallint": ["null", "int"],
                                        "integer": ["null", "int"],
                                        "bigint": ["null", "long"],
                                        "real": ["null", "float"],
                                        "double precision": ["null", "double"],
                                        "numeric": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 6}],
                                        "decimal": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 6}],
                                        "money": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 6}],
                                        "char": ["null", "string"],
                                        "character": ["null","string"],
                                        "character varying": ["null","string"],
                                        "varchar": ["null", "string"],
                                        "text": ["null", "string"],
                                        "uuid": ["null", "string"],
                                        "json": ["null", "string"],
                                        "jsonb": ["null", "string"],
                                        "bytea": ["null", "bytes"],
                                        "date": ["null", {"type": "int", "logicalType": "date"}],
                                        "time": ["null", {"type": "int", "logicalType": "time-millis"}],
                                        "time without time zone": ["null", {"type": "int", "logicalType": "time-millis"}],
                                        "timestamp": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "timestamp without time zone": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "timestamptz": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "timestamp with time zone": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "interval": ["null", "string"],
                                        "enum": ["null", "enum"],
                                        "array": ["null", {"type": "array", "items": "string"}],
                                        "hstore": ["null", {"type": "map", "values": "string"}],
                                        "inet": ["null", "string"],
                                        "cidr": ["null", "string"],
                                        "macaddr": ["null", "string"],
                                        "point": ["null", "string"],
                                        "polygon": ["null", "string"]
                                    },

                                    "mysql": {
                                        "bit": ["null", "boolean"],
                                        "bool": ["null", "boolean"],
                                        "boolean": ["null", "boolean"],
                                        "tinyint": ["null", "int"],
                                        "smallint": ["null", "int"],
                                        "mediumint": ["null", "int"],
                                        "int": ["null", "int"],
                                        "integer": ["null", "int"],
                                        "bigint": ["null", "long"],
                                        "float": ["null", "float"],
                                        "double": ["null", "double"],
                                        "numeric": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 6}],
                                        "decimal": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 6}],
                                        "char": ["null", "string"],
                                        "varchar": ["null", "string"],
                                        "text": ["null", "string"],
                                        "tinytext": ["null", "string"],
                                        "mediumtext": ["null", "string"],
                                        "longtext": ["null", "string"],
                                        "json": ["null", "string"],
                                        "blob": ["null", "bytes"],
                                        "tinyblob": ["null", "bytes"],
                                        "mediumblob": ["null", "bytes"],
                                        "longblob": ["null", "bytes"],
                                        "binary": ["null", "bytes"],
                                        "varbinary": ["null", "bytes"],
                                        "date": ["null", {"type": "int", "logicalType": "date"}],
                                        "datetime": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "timestamp": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "time": ["null", {"type": "int", "logicalType": "time-millis"}],
                                        "year": ["null", "int"],
                                        "enum": ["null", "enum"],
                                        "set": ["null", {"type": "array", "items": "string"}]
                                    },

                                    "mariadb": {
                                        "boolean": ["null", "boolean"],
                                        "tinyint": ["null", "int"],
                                        "smallint": ["null", "int"],
                                        "mediumint": ["null", "int"],
                                        "int": ["null", "int"],
                                        "bigint": ["null", "long"],
                                        "numeric": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 6}],
                                        "float": ["null", "float"],
                                        "double": ["null", "double"],
                                        "char": ["null", "string"],
                                        "varchar": ["null", "string"],
                                        "text": ["null", "string"],
                                        "json": ["null", "string"],
                                        "blob": ["null", "bytes"],
                                        "binary": ["null", "bytes"],
                                        "varbinary": ["null", "bytes"],
                                        "date": ["null", {"type": "int", "logicalType": "date"}],
                                        "datetime": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "timestamp": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "time": ["null", {"type": "int", "logicalType": "time-millis"}],
                                        "enum": ["null", "enum"],
                                        "set": ["null", {"type": "array", "items": "string"}]
                                    },

                                    "oracle": {
                                        "char": ["null", "string"],
                                        "nchar": ["null", "string"],
                                        "varchar": ["null", "string"],
                                        "varchar2": ["null", "string"],
                                        "nvarchar2": ["null", "string"],
                                        "clob": ["null", "string"],
                                        "nclob": ["null", "string"],
                                        "numeric": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 6}],
                                        "float": ["null", "double"],
                                        "binary_float": ["null", "float"],
                                        "binary_double": ["null", "double"],
                                        "date": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "timestamp": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "timestamp with time zone": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "timestamp with local time zone": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "raw": ["null", "bytes"],
                                        "blob": ["null", "bytes"],
                                        "bfile": ["null", "string"],
                                        "long": ["null", "string"],
                                        "rowid": ["null", "string"]
                                    },

                                    "mssql": {
                                        "bit": ["null", "boolean"],
                                        "tinyint": ["null", "int"],
                                        "smallint": ["null", "int"],
                                        "int": ["null", "int"],
                                        "bigint": ["null", "long"],
                                        "numeric": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 6}],
                                        "decimal": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 6}],
                                        "money": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 6}],
                                        "smallmoney": ["null", {"type": "bytes", "logicalType": "decimal"}],
                                        "float": ["null", "double"],
                                        "real": ["null", "float"],
                                        "char": ["null", "string"],
                                        "varchar": ["null", "string"],
                                        "text": ["null", "string"],
                                        "nchar": ["null", "string"],
                                        "nvarchar": ["null", "string"],
                                        "ntext": ["null", "string"],
                                        "xml": ["null", "string"],
                                        "binary": ["null", "bytes"],
                                        "varbinary": ["null", "bytes"],
                                        "image": ["null", "bytes"],
                                        "date": ["null", {"type": "int", "logicalType": "date"}],
                                        "datetime": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "datetime2": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "smalldatetime": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "datetimeoffset": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
                                        "time": ["null", {"type": "int", "logicalType": "time-millis"}],
                                        "uniqueidentifier": ["null", "string"],
                                        "sql_variant": ["null", "string"]
                                    }
                                }
        # Epoch and default for logical types
        EPOCH = date(1970, 1, 1)
        DEFAULT_DATE = date(1900, 1, 1)
        DEFAULT_TIME = time(0, 0, 0)

        base_defaults = {
            "boolean": None,
            "int": None,
            "long": None,
            "float": None,
            "double": None,
            "string": None,
            "bytes": None,
            "array": None,
            "map": None,
            "enum": None,
            "record": None,
            "decimal": None,
            "date": None,
            "time-millis": None,
            "time-micros": None,
            "timestamp-millis": None,
            "timestamp-micros": None
        }

        # If user passes a defaults dict, update the base defaults
        if defaults:
            self.DEFAULTS = defaults
        else:
            self.DEFAULTS = base_defaults

    # -------------------------------
    # Type conversion methods
    # -------------------------------
    def source_to_avro(
        self, db_system: str, db_type: str, numeric_precision: int | None = None, numeric_scale: int | None = None
    ) -> dict | str:
        """
        Return the Avro type for a source DB column. For decimal/numeric types, inject
        precision and scale if provided.
        """
        db_type_norm = db_type.lower().strip()
        avro_type = self.source_to_avro_map.get(db_system, {}).get(db_type_norm)

        if not avro_type:
            self.logger.warning(f"No mapping for {db_system}.{db_type}, defaulting to ['null','string']")

            return ["null","string"]

        # Handle numeric/decimal with precision/scale
        if isinstance(avro_type, list) and len(avro_type) == 2 and isinstance(avro_type[1], dict):
            type_dict = avro_type[1]
            if type_dict.get("logicalType") == "decimal":
                # Override precision/scale if provided
                if numeric_precision is not None:
                    type_dict["precision"] = numeric_precision
                if numeric_scale is not None:
                    type_dict["scale"] = numeric_scale
                avro_type[1] = type_dict
                self.logger.debug(f"Decimal type precision/scale set for {db_system}.{db_type}: {type_dict}")

        return avro_type


    def avro_to_bigquery(self, avro_type) -> str:
        # Handle union types (list), e.g. ["null", {...}]
        if isinstance(avro_type, list):
            non_null_types = [t for t in avro_type if t != "null"]
            if len(non_null_types) == 1:
                return self.avro_to_bigquery(non_null_types[0])
            else:
                self.logger.warning(f"Complex union type {avro_type} encountered, defaulting to STRING")
                return "STRING"

        # If simple string type
        if isinstance(avro_type, str):
            return self.avro_to_bq_map.get(avro_type, "STRING")

        # If dict type
        if isinstance(avro_type, dict):
            logical_type = avro_type.get("logicalType")
            base_type = avro_type.get("type")

            # Handle logical types first
            if logical_type:
                logical_map = self.avro_to_bq_map.get("logicalTypes", {})
                if logical_type in logical_map:
                    return logical_map[logical_type]

            # Handle complex types: array, map, record
            complex_map = self.avro_to_bq_map.get("complexTypes", {})
            if base_type == "array":
                # Recursively resolve item type
                item_type = avro_type.get("items")
                bq_item_type = self.avro_to_bigquery(item_type)
                return f"ARRAY<{bq_item_type}>"

            if base_type == "map":
                # Maps in BigQuery are usually STRUCT<key STRING, value ...>
                # Here keys are always strings in Avro maps
                value_type = avro_type.get("values")
                bq_value_type = self.avro_to_bigquery(value_type)
                # Return STRUCT type with key and value fields
                return f"STRUCT<key STRING, value {bq_value_type}>"

            if base_type == "record":
                self.logger.warning(f"Unknown Avro type {avro_type}, defaulting to STRING")
                return "STRUCT"

            # For simple base types (int, long, string, etc.)
            if base_type:
                return self.avro_to_bigquery(base_type)

        # Fallback
        return "STRING"


    def source_to_bigquery(self, db_system: str, db_type: str) -> str:
        avro_type = self.source_to_avro(db_system, db_type)
        bq_type = self.avro_to_bigquery(avro_type)
        self.logger.debug(f"Converted {db_system}.{db_type} -> {bq_type}")
        return bq_type
    # -------------------------------
    # Avro field generation with defaults
    # -------------------------------
    def generate_avro_field(self, name: str, avro_type: dict | str) -> dict:
        field = {"name": name, "type": avro_type}
        default_value = self._default_for_type(avro_type)
        if default_value is not None:
            field["default"] = default_value
        self.logger.debug(f"Generated Avro field {name}: {field}")
        return field

    def _default_for_type(self, avro_type: dict | str):
        if isinstance(avro_type, str):
            return self.DEFAULTS.get(avro_type, None)
        elif isinstance(avro_type, dict):
            base_type = avro_type.get("type")
            logical_type = avro_type.get("logicalType")
            if logical_type and logical_type in self.DEFAULTS:
                return self.DEFAULTS[logical_type]
            if base_type in self.DEFAULTS:
                return self.DEFAULTS[base_type]
        return None
