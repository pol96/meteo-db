from jinja2 import Environment, FileSystemLoader

class BaseRender():
    """
    Base class and dynamic registry for jinja template import.
    Each subclass registers itself with a name which represents the model (e.g. "cdc_extraction").
    """

    _registry: dict[str, type["BaseRender"]] = {}

    env = Environment(loader=FileSystemLoader('jinja_template/'))


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
    def render_jinja(cls, **kwargs) -> str:
        """
        Subclasses must override this to return the rendered template.
        """
        raise NotImplementedError(
            f"{cls.__name__} must implement the 'render_jinja' classmethod."
        )