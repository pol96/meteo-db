from sqlalchemy import create_engine, text, Engine
import sqlalchemy
from urllib.parse import quote_plus
import polars
from fastavro import writer, parse_schema
from jinja2 import Environment, FileSystemLoader
from typing import Union
import json

class db_conn(sqlalchemy):
    def __init__():
        pass