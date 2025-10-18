from sqlalchemy import create_engine, Engine
from urllib.parse import quote_plus
from py_module.metadata.connection.SourceBaseConnection import BaseConnection


@BaseConnection.register("postgres")
class PostgresConnection(BaseConnection):
    @classmethod
    def get_engine(cls, **kwargs) -> Engine:
        try:
            import psycopg2  # noqa: F401
        except ImportError:
            raise ImportError(
                "psycopg2 is required for PostgreSQL connections. "
                "Install it with: pip install psycopg2-binary"
            )
        return create_engine(
            f"postgresql+psycopg2://{kwargs['user']}:{kwargs['password']}"
            f"@{kwargs['host']}:{kwargs['port']}/{kwargs['db']}"
        )


@BaseConnection.register("mysql")
class MySQLConnection(BaseConnection):
    @classmethod
    def get_engine(cls, **kwargs) -> Engine:
        try:
            import pymysql  # noqa: F401
        except ImportError:
            raise ImportError(
                "pymysql is required for MySQL connections. "
                "Install it with: pip install pymysql"
            )
        return create_engine(
            f"mysql+pymysql://{kwargs['user']}:{kwargs['password']}"
            f"@{kwargs['host']}:{kwargs['port']}/{kwargs['db']}"
        )


@BaseConnection.register("mariadb")
class MariaDBConnection(BaseConnection):
    @classmethod
    def get_engine(cls, **kwargs) -> Engine:
        try:
            import pymysql  # noqa: F401
        except ImportError:
            raise ImportError(
                "pymysql is required for MariaDB connections. "
                "Install it with: pip install pymysql"
            )
        return create_engine(
            f"mariadb+pymysql://{kwargs['user']}:{kwargs['password']}"
            f"@{kwargs['host']}:{kwargs['port']}/{kwargs['db']}"
        )


@BaseConnection.register("oracle")
class OracleConnection(BaseConnection):
    @classmethod
    def get_engine(cls, **kwargs) -> Engine:
        driver = "oracledb" if not kwargs.get("cx_oracle", False) else "cx_Oracle"

        try:
            __import__(driver)  # dynamic import
        except ImportError:
            raise ImportError(
                f"{driver} is required for Oracle connections. "
                f"Install it with: pip install {driver}"
            )

        return create_engine(
            f"oracle+{driver}://{kwargs['user']}:{kwargs['password']}"
            f"@{kwargs['host']}:{kwargs['port']}/{kwargs['db']}"
        )


@BaseConnection.register("mssql")
class MSSQLConnection(BaseConnection):
    @classmethod
    def get_engine(cls, **kwargs) -> Engine:
        try:
            import pyodbc  # noqa: F401
        except ImportError:
            raise ImportError(
                "pyodbc is required for SQL Server connections. "
                "Install it with: pip install pyodbc"
            )

        mssql_driver = kwargs.get("mssql_driver", "ODBC Driver 17 for SQL Server")
        conn_str = (
            f"DRIVER={{{mssql_driver}}};"
            f"SERVER={kwargs['host']},{kwargs['port']};"
            f"DATABASE={kwargs['db']};UID={kwargs['user']};PWD={kwargs['password']}"
        )
        params = quote_plus(conn_str)
        return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
