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
