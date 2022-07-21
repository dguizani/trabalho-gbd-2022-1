import pandas as pd

from dw_tools import dw_tools as dwt


def extract_d_tipo_sintoma():
    sintomas = [
        "FEBRE",
        "TOSSE",
        "CALAFRIO",
        "DISPNÉIA",
        "DOR DE GARGANTA",
        "ARTRALGIA",
        "MIALGIA",
        "CORIZA",
        "DIARREIA"
    ]

    df = pd.DataFrame({
        "ds_tipo_sintoma": sintomas
    })

    return df


def treat_d_tipo_sintoma(
    tbl: pd.DataFrame
):
    tbl_ = tbl.assign(
        sk_tipo_sintoma=lambda df: range(1, df.shape[0] + 1)
    ).astype({
        "sk_tipo_sintoma": "int64",
        "ds_tipo_sintoma": "string"
    })

    return tbl_


def load_d_tipo_sintoma(
    tbl: pd.DataFrame,
    conn_output: dwt.sa.engine.Engine,
    table_name: str
):
    conn_output.execute(f"TRUNCATE TABLE {table_name}")

    tbl.to_sql(
        name=table_name,
        con=conn_output,
        if_exists="append",
        index=False
    )


def run_d_tipo_sintoma(
    conn_output: dwt.sa.engine.Engine
):
    table_name = "d_tipo_sintoma"

    tbl_extract = extract_d_tipo_sintoma()

    tbl_treat = treat_d_tipo_sintoma(tbl_extract)

    load_d_tipo_sintoma(tbl_treat, conn_output, table_name)


if __name__ == "__main__":
    conn_dw = dwt.connect_mysql(
        username="root",
        password="1234",
        database="dw",
        server="localhost",
        port=33033
    )

    run_d_tipo_sintoma(conn_dw)
