import pandas as pd

from dw_tools import dw_tools as dwt


def extract_d_data(
    conn_input: dwt.sa.engine.Engine
):
    query = """
        WITH stg_influd AS (
            SELECT * FROM stg_influd_13 UNION
            SELECT * FROM stg_influd_14 UNION
            SELECT * FROM stg_influd_15 UNION
            SELECT * FROM stg_influd_16 UNION
            SELECT * FROM stg_influd_17 UNION
            SELECT * FROM stg_influd_18
        )
        SELECT MAX(dt.dt_completa)
            , MIN(dt.dt_completa)
        FROM (
            SELECT dt_notific AS dt_completa FROM stg_influd UNION
            SELECT dt_nasc AS dt_completa FROM stg_influd UNION
            SELECT dt_sin_pri AS dt_completa FROM stg_influd UNION
            SELECT dt_interna AS dt_completa FROM stg_influd UNION
            SELECT dt_raiox AS dt_completa FROM stg_influd
        ) AS dt
    """

    reff = conn_input.execute(query)

    date_max, date_min = reff.fetchone()

    tbl = pd.DataFrame({
        "dt_completa": pd.date_range(date_min, date_max)
    })

    return tbl


def treat_d_data(
    tbl: pd.DataFrame
):
    tbl_ = tbl.assign(
        sk_data=lambda df: range(1, df.shape[0] + 1),
        nu_ano=lambda df: df.dt_completa.dt.year,
        nu_mes=lambda df: df.dt_completa.dt.month,
        nu_dia=lambda df: df.dt_completa.dt.day,
    )

    return tbl_


def load_d_data(
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


def run_d_data(
    conn_input: dwt.sa.engine.Engine,
    conn_output: dwt.sa.engine.Engine
):
    table_name = "d_data"

    tbl_extract = extract_d_data(conn_input)

    tbl_treat = treat_d_data(tbl_extract)

    load_d_data(conn_output, tbl_treat, table_name)


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

    run_d_data(conn_stg, conn_dw)
