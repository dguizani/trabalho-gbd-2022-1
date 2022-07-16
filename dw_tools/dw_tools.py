import typing as tp

import sqlalchemy as sa
import unidecode as ud
import re as re


def connect_mysql(
    username: str,
    password: str,
    database: str,
    server: str,
    port: int
) -> sa.engine.Engine:
    url = f"mysql+pymysql://{username}:{password}@{server}:{port}/{database}"

    return sa.create_engine(url=url)


def has_record_in_table(
    conn: sa.engine.Engine,
    table_name: str,
):
    query = f"""
        SELECT 1
        FROM {table_name}
        LIMIT 1
    """

    return conn.execute(query).fetchone() is not None


def get_columns_db(
    conn: sa.engine.Engine,
    table_name: str
):
    query = f"""
        SELECT *
        FROM {table_name}
        LIMIT 1
    """

    cursor = conn.execute(query)

    return cursor.keys()


def remove_multiples_spaces(
    string: str
):
    pattern = r"[ ]{2,}"

    comp = re.compile(pattern)

    return comp.sub(str(string), " ")


def treat_column_name(
    column_name: tp.Any
):
    treat_name = ud.unidecode(
        remove_multiples_spaces(
            str(column_name)
                .strip(" ")
                .replace(" ", "_")
        )
    )

    return treat_name
