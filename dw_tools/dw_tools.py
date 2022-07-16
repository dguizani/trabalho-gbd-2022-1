import sqlalchemy as sa


def connect_mysql(
    username: str,
    password: str,
    database: str,
    server: str,
    port: int
) -> sa.engine.Engine:
    url = f"mysql+pymysql://{username}:{password}@{server}:{port}/{database}"

    return sa.create_engine(url=url)


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
