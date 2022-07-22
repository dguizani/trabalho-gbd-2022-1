import pandas as pd

from dw_tools import dw_tools as dwt


def extract_d_local(
    conn_input: dwt.sa.engine.Engine
):
    query = """
        SELECT DISTINCT
            uf AS cd_uf
            , UPPER(TRIM(nome_uf)) AS ds_uf
            , municipio AS cd_municipio
            , UPPER(TRIM(nome_municipio)) AS ds_municipio
        FROM stg_rdb_municipio
    """

    tbl = pd.read_sql_query(
        sql=query,
        con=conn_input
    )

    return tbl


def treat_d_local(
    tbl: pd.DataFrame
):
    tbl_ = tbl.assign(
        sk_local=lambda df: range(1, df.shape[0] + 1)
    ).astype({
        "sk_local": "int64",
        "cd_uf": "int64",
        "ds_uf": "string",
        "cd_municipio": "int64",
        "ds_municipio": "string"
    })

    default_values = {
        "sk_local": [-1],
        "cd_uf": [-1],
        "ds_uf": ["Não Informado"],
        "cd_municipio": [-1],
        "ds_municipio": ["Não Informado"]
    }

    tbl_ = pd.concat([
        pd.DataFrame(default_values),
        tbl_
    ])

    return tbl_


def load_d_local(
    conn_output: dwt.sa.engine.Engine,
    tbl: pd.DataFrame,
    table_name: str
):
    conn_output.execute(f"TRUNCATE TABLE {table_name}")

    tbl.to_sql(
        name=table_name,
        con=conn_output,
        if_exists="append",
        index=False
    )


def run_d_local(
    conn_input: dwt.sa.engine.Engine,
    conn_output: dwt.sa.engine.Engine
):
    table_name = "d_local"

    tbl_extract = extract_d_local(conn_input)

    tbl_treat = treat_d_local(tbl_extract)

    load_d_local(conn_output, tbl_treat, table_name)


if __name__ == "__main__":
    conn_dw = dwt.connect_mysql(
        username="root",
        password="1234",
        database="dw",
        server="localhost",
        port=33033
    )

    conn_stg = dwt.connect_mysql(
        username="root",
        password="1234",
        database="stg",
        server="localhost",
        port=33033
    )

    run_d_local(conn_stg, conn_dw)
