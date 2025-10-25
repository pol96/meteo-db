from jinja2 import Environment, FileSystemLoader
from py_module.metadata.logging.logger import BaseLogger

class BaseRender(BaseLogger):
    """
    Base class and dynamic registry for Jinja template import.
    Each subclass registers itself with a name which represents the model (e.g. "cdc_extraction").
    """

    _registry: dict[str, type["BaseRender"]] = {}
    env = Environment(loader=FileSystemLoader('jinja_template/'))

    # ---------- Registration mechanism ----------
    @classmethod
    def register(cls, name: str):
        """
        Decorator used by subclasses to register themselves under a given name.
        """
        def decorator(subclass):
            cls._registry[name.lower()] = subclass
            cls.get_logger().debug(f"Registered render class '{subclass.__name__}' as '{name.lower()}'")
            return subclass
        return decorator

    # ---------- Interface ----------
    @classmethod
    def render_jinja(cls, **kwargs) -> str:
        """
        Subclasses must override this to return the rendered template.
        """
        logger = cls.get_logger()
        try:
            raise NotImplementedError(
                f"{cls.__name__} must implement the 'render_jinja' classmethod."
            )
        except NotImplementedError as e:
            logger.error(e)
            raise
