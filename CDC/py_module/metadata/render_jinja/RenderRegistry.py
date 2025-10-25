from py_module.metadata.render_jinja.JinjaBaseRender import BaseRender

@BaseRender.register("cdc_extraction")
class ChangeDataCaptureExtraction(BaseRender):
    @classmethod
    def render_jinja(cls, database: str):
        logger = cls.get_logger()
        template_path = f'/{database}/cdc_extraction.sql.j2'
        try:
            template = cls.env.get_template(template_path)
            logger.debug(f"Loaded template for CDC extraction: {template_path}")
            return template
        except Exception as e:
            logger.error(f"Failed to load CDC template '{template_path}': {e}")
            raise


@BaseRender.register("retrieve_schema")
class RetrieveSchema(BaseRender):
    @classmethod
    def render_jinja(cls, database: str):
        logger = cls.get_logger()
        template_path = f'/{database}/retrieve_schema.sql.j2'
        try:
            template = cls.env.get_template(template_path)
            logger.debug(f"Loaded template for schema retrieval: {template_path}")
            return template
        except Exception as e:
            logger.error(f"Failed to load schema template '{template_path}': {e}")
            raise


@BaseRender.register("schema_sources")
class SchemaSource(BaseRender):
    @classmethod
    def render_jinja(cls):
        logger = cls.get_logger()
        template_path = 'schema_sources.yml.j2'
        try:
            template = cls.env.get_template(template_path)
            logger.debug(f"Loaded schema sources template: {template_path}")
            return template
        except Exception as e:
            logger.error(f"Failed to load schema sources template '{template_path}': {e}")
            raise
