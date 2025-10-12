from py_module.metadata.JinjaBaseRender import BaseRender

@BaseRender.register("cdc_extraction")
class ChangeDataCaptureExtraction(BaseRender):
    @classmethod
    def render_jinja(cls, 
                    database:str
                     ) -> str:
        template = f'/{database}/cdc_extraction.sql.j2'        
        return cls.env.get_template(template)

@BaseRender.register("retrieve_schema")
class RetrieveSchema(BaseRender):
    @classmethod
    def render_jinja(cls, 
                    database:str
                     ) -> str:
        template = f'/{database}/retrieve_schema.sql.j2'        
        return cls.env.get_template(template)
    
@BaseRender.register("schema_sources")
class SchemaSource(BaseRender):
    @classmethod
    def render_jinja(cls) -> str:
        template = f'schema_sources.yml.j2'        
        return cls.env.get_template(template)