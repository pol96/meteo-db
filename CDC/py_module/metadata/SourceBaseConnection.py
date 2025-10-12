from sqlalchemy import Engine

class BaseConnection:
    """
    Base class and dynamic registry for database connection engines.
    Each subclass registers itself with a name (e.g. "postgres", "mysql").
    """

    _registry: dict[str, type["BaseConnection"]] = {}

    # ---------- Registration mechanism ----------
    @classmethod
    def register(cls, name: str):
        """
        Decorator used by subclasses to register themselves under a given name.
        Example:
            @BaseConnection.register("postgres")
            class PostgresConnection(BaseConnection):
                ...
        """
        def decorator(subclass):
            cls._registry[name.lower()] = subclass
            return subclass
        return decorator

    # ---------- Interface ----------
    @classmethod
    def get_engine(cls, **kwargs) -> Engine:
        """
        Subclasses must override this to return a SQLAlchemy Engine.
        """
        raise NotImplementedError(
            f"{cls.__name__} must implement the 'get_engine' classmethod."
        )
