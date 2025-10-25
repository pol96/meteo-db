from py_module.metadata.logging.logger import BaseLogger
from sqlalchemy import create_engine, Engine
from urllib.parse import quote_plus
from py_module.metadata.connection.SourceBaseConnection import BaseConnection


@BaseConnection.register("postgres")
class PostgresConnection(BaseConnection, BaseLogger):
    @classmethod
    def get_engine(cls, **kwargs) -> Engine:
        logger = cls.get_logger()
        try:
            import psycopg2  # noqa: F401
        except ImportError as e:
            logger.exception("psycopg2 is required for PostgreSQL connections")
            raise ImportError(
                "psycopg2 is required for PostgreSQL connections. "
                "Install it with: pip install psycopg2-binary"
            ) from e

        try:
            engine = create_engine(
                f"postgresql+psycopg2://{kwargs['user']}:{kwargs['password']}"
                f"@{kwargs['host']}:{kwargs['port']}/{kwargs['db']}"
            )
            logger.info("PostgreSQL engine created successfully")
            return engine
        except Exception as e:
            logger.exception("Failed to create PostgreSQL engine")
            raise


@BaseConnection.register("mysql")
class MySQLConnection(BaseConnection, BaseLogger):
    @classmethod
    def get_engine(cls, **kwargs) -> Engine:
        logger = cls.get_logger()
        try:
            import pymysql  # noqa: F401
        except ImportError as e:
            logger.exception("pymysql is required for MySQL connections")
            raise ImportError(
                "pymysql is required for MySQL connections. "
                "Install it with: pip install pymysql"
            ) from e

        try:
            engine = create_engine(
                f"mysql+pymysql://{kwargs['user']}:{kwargs['password']}"
                f"@{kwargs['host']}:{kwargs['port']}/{kwargs['db']}"
            )
            logger.info("MySQL engine created successfully")
            return engine
        except Exception as e:
            logger.exception("Failed to create MySQL engine")
            raise


@BaseConnection.register("mariadb")
class MariaDBConnection(BaseConnection, BaseLogger):
    @classmethod
    def get_engine(cls, **kwargs) -> Engine:
        logger = cls.get_logger()
        try:
            import pymysql  # noqa: F401
        except ImportError as e:
            logger.exception("pymysql is required for MariaDB connections")
            raise ImportError(
                "pymysql is required for MariaDB connections. "
                "Install it with: pip install pymysql"
            ) from e

        try:
            engine = create_engine(
                f"mariadb+pymysql://{kwargs['user']}:{kwargs['password']}"
                f"@{kwargs['host']}:{kwargs['port']}/{kwargs['db']}"
            )
            logger.info("MariaDB engine created successfully")
            return engine
        except Exception as e:
            logger.exception("Failed to create MariaDB engine")
            raise


@BaseConnection.register("oracle")
class OracleConnection(BaseConnection, BaseLogger):
    @classmethod
    def get_engine(cls, **kwargs) -> Engine:
        logger = cls.get_logger()
        driver = "oracledb" if not kwargs.get("cx_oracle", False) else "cx_Oracle"

        try:
            __import__(driver)
        except ImportError as e:
            logger.exception(f"{driver} is required for Oracle connections")
            raise ImportError(
                f"{driver} is required for Oracle connections. "
                f"Install it with: pip install {driver}"
            ) from e

        try:
            engine = create_engine(
                f"oracle+{driver}://{kwargs['user']}:{kwargs['password']}"
                f"@{kwargs['host']}:{kwargs['port']}/{kwargs['db']}"
            )
            logger.info("Oracle engine created successfully")
            return engine
        except Exception as e:
            logger.exception("Failed to create Oracle engine")
            raise


@BaseConnection.register("mssql")
class MSSQLConnection(BaseConnection, BaseLogger):
    @classmethod
    def get_engine(cls, **kwargs) -> Engine:
        logger = cls.get_logger()
        try:
            import pyodbc  # noqa: F401
        except ImportError as e:
            logger.exception("pyodbc is required for SQL Server connections")
            raise ImportError(
                "pyodbc is required for SQL Server connections. "
                "Install it with: pip install pyodbc"
            ) from e

        try:
            mssql_driver = kwargs.get("mssql_driver", "ODBC Driver 17 for SQL Server")
            conn_str = (
                f"DRIVER={{{mssql_driver}}};"
                f"SERVER={kwargs['host']},{kwargs['port']};"
                f"DATABASE={kwargs['db']};UID={kwargs['user']};PWD={kwargs['password']}"
            )
            params = quote_plus(conn_str)
            engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
            logger.info("MSSQL engine created successfully")
            return engine
        except Exception as e:
            logger.exception("Failed to create MSSQL engine")
            raise
