from sqlalchemy import Engine
from py_module.metadata.logging.logger import BaseLogger
import logging

class BaseConnection(BaseLogger):
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
        """
        def decorator(subclass):
            cls._registry[name.lower()] = subclass
            logging.getLogger(cls.__name__).info(f"Registered connection: {name.lower()}")
            return subclass
        return decorator

    # ---------- Interface ----------
    @classmethod
    def get_engine(cls, **kwargs) -> Engine:
        """
        Subclasses must override this to return a SQLAlchemy Engine.
        """
        logger = logging.getLogger(cls.__name__)
        try:
            raise NotImplementedError(
                f"{cls.__name__} must implement the 'get_engine' classmethod."
            )
        except NotImplementedError as e:
            logger.error(e)
            raise
