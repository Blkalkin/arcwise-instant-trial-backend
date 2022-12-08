from pydantic import BaseModel


class SqlQuery(BaseModel):
    file_name: str
    sql_string: str | None = None