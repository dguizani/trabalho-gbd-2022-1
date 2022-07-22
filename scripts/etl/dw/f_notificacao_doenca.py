import pandas as pd

from dw_tools import dw_tools as dwt


def extract_f_notificacao_doenca(
    conn_input: dwt.sa.engine.Engine,
    conn_output: dwt.sa.engine.Engine
):
    query = """
        WITH stg_influd AS (
            SELECT * FROM stg_influd_13 UNION ALL
            SELECT * FROM stg_influd_14 UNION ALL
            SELECT * FROM stg_influd_15 UNION ALL
            SELECT * FROM stg_influd_16 UNION ALL
            SELECT * FROM stg_influd_17 UNION ALL
            SELECT * FROM stg_influd_18
        )
        SELECT sg_uf_not * 1 AS sg_uf_not
            , co_uf_inte * 1 AS co_uf_inte
            , sg_uf * 1 AS sg_uf
            , id_municip * 1 AS id_municip
            , co_mu_inte * 1 AS co_mu_inte
            , id_mn_resi * 1 AS id_mn_resi
            , nu_idade_n * 1 AS nu_idade_n
            , cs_sexo
            , cs_raca * 1 AS cs_raca
            , vacina * 1 AS vacina
            , evolucao * 1 AS evolucao
            , dt_notific
            , dt_nasc
            , dt_sin_pri
            , dt_interna
            , dt_raiox
        FROM stg_influd
    """

    tbl_stg_influd = pd.read_sql_query(
        sql=query,
        con=conn_input
    ).astype({
        "dt_notific": "datetime64[ns]",
        "dt_nasc": "datetime64[ns]",
        "dt_sin_pri": "datetime64[ns]",
        "dt_interna": "datetime64[ns]",
        "dt_raiox": "datetime64[ns]"
    })

    tbl_d_local = pd.read_sql_table(
        table_name="d_local",
        con=conn_output,
        schema="dw"
    )

    tbl_d_paciente = pd.read_sql_table(
        table_name="d_paciente",
        con=conn_output,
        schema="dw"
    ).astype({
        "NU_IDADE_DEDUZIDA": "int64"
    })

    tbl_d_data = pd.read_sql_table(
        table_name="d_data",
        con=conn_output,
        schema="dw"
    )

    tbl = pd.merge(
        left=tbl_stg_influd,
        right=tbl_d_local,
        how="left",
        left_on=["sg_uf_not", "id_municip"],
        right_on=["CD_UF", "CD_MUNICIPIO"]
    ).merge(
        right=tbl_d_local,
        how="left",
        left_on=["co_uf_inte", "co_mu_inte"],
        right_on=["CD_UF", "CD_MUNICIPIO"],
        suffixes=("", "_internacao")
    ).merge(
        right=tbl_d_local,
        how="left",
        left_on=["sg_uf", "id_mn_resi"],
        right_on=["CD_UF", "CD_MUNICIPIO"],
        suffixes=("", "_paciente")
    ).merge(
        right=tbl_d_paciente,
        how="left",
        left_on=["nu_idade_n", "cs_sexo", "cs_raca", "vacina", "evolucao"],
        right_on=["NU_IDADE_DEDUZIDA", "CD_SEXO", "CD_RACA", "CD_VACINADO",
            "CD_EVOLUCAO"]
    ).merge(
        right=tbl_d_data,
        how="left",
        left_on="dt_notific",
        right_on="DT_COMPLETA"
    ).merge(
        right=tbl_d_data,
        how="left",
        left_on="dt_nasc",
        right_on="DT_COMPLETA",
        suffixes=("", "_nascimento")
    ).merge(
        right=tbl_d_data,
        how="left",
        left_on="dt_sin_pri",
        right_on="DT_COMPLETA",
        suffixes=("", "_primeiros_sintomas")
    ).merge(
        right=tbl_d_data,
        how="left",
        left_on="dt_interna",
        right_on="DT_COMPLETA",
        suffixes=("", "_internacao")
    ).merge(
        right=tbl_d_data,
        how="left",
        left_on="dt_raiox",
        right_on="DT_COMPLETA",
        suffixes=("", "_raiox")
    )

    return tbl


def treat_f_notificacao_doenca(
    tbl: pd.DataFrame
):
    select_columns = [
        "sk_local_notificacao", "sk_local_internacao", "sk_local_paciente",
        "sk_paciente", "sk_data_notificacao", "sk_data_nascimento",
        "sk_data_primeiros_sintomas", "sk_data_internacao", "sk_data_raiox"
    ]

    rename_columns = {
        "SK_LOCAL": "sk_local_notificacao",
        "SK_LOCAL_internacao": "sk_local_internacao",
        "SK_LOCAL_paciente": "sk_local_paciente",
        "SK_PACIENTE": "sk_paciente",
        "SK_DATA": "sk_data_notificacao",
        "SK_DATA_nascimento": "sk_data_nascimento",
        "SK_DATA_primeiros_sintomas": "sk_data_primeiros_sintomas",
        "SK_DATA_internacao": "sk_data_internacao",
        "SK_DATA_raiox": "sk_data_raiox"
    }

    tbl_ = tbl.rename(
        columns=rename_columns
    ).fillna({
        "sk_local_notificacao": -1,
        "sk_local_internacao": -1,
        "sk_local_paciente": -1,
        "sk_paciente": -1,
        "sk_data_notificacao": -1,
        "sk_data_nascimento": -1,
        "sk_data_primeiros_sintomas": -1,
        "sk_data_internacao": -1,
        "sk_data_raiox": -1
    })[select_columns]

    return tbl_


def load_f_notificacao_doenca(
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


def run_f_notificacao_doenca(
    conn_input: dwt.sa.engine.Engine,
    conn_output: dwt.sa.engine.Engine
):
    table_name = "f_notificacao_doenca"

    tbl_extract = extract_f_notificacao_doenca(conn_input, conn_input)

    tbl_treat = treat_f_notificacao_doenca(tbl_extract)

    load_f_notificacao_doenca(conn_output, tbl_treat, table_name)


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

    run_f_notificacao_doenca(conn_stg, conn_dw)
