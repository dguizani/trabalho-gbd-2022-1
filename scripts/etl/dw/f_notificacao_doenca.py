import pandas as pd

from dw_tools import dw_tools as dwt

import datetime as dt


def sum_value_columns(
    row: pd.Series,
    columns: list
):
    return sum(row[columns])


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
            , IFNULL(cs_sexo, 'I') AS cs_sexo
            , IFNULL(cs_raca, '9') * 1 AS cs_raca
            , IFNULL(vacina, '9') * 1 AS vacina
            , IFNULL(evolucao, '9') * 1 AS evolucao
            , dt_notific
            , dt_nasc
            , dt_sin_pri
            , dt_interna
            , dt_raiox
            -- Sintomas
            , IF(IFNULL(febre, '9') = '9', '0', febre) * 1 AS febre
            , IF(IFNULL(tosse, '9') = '9', '0', tosse) * 1 AS tosse
            , IF(IFNULL(calafrio, '9') = '9', '0', calafrio) * 1 AS calafrio
            , IF(IFNULL(dispneia, '9') = '9', '0', dispneia) * 1 AS dispneia
            , IF(IFNULL(garganta, '9') = '9', '0', garganta) * 1 AS garganta
            , IF(IFNULL(artralgia, '9') = '9', '0', artralgia) * 1 AS artralgia
            , IF(IFNULL(mialgia, '9') = '9', '0', mialgia) * 1 AS mialgia
            , IF(IFNULL(conjuntiv, '9') = '9', '0', conjuntiv) * 1 AS conjuntiv
            , IF(IFNULL(coriza, '9') = '9', '0', coriza) * 1 AS coriza
            , IF(IFNULL(diarreia, '9') = '9', '0', diarreia) * 1 AS diarreia
            , IF(IFNULL(outro_sin, '9') = '9', '0', outro_sin) * 1 AS outro_sin
            -- Morbidade
            , IF(IFNULL(pneumopati, '9') = '9', '0', pneumopati) * 1 AS pneumopati
            , IF(IFNULL(cardiopati, '9') = '9', '0', cardiopati) * 1 AS cardiopati
            , IF(IFNULL(imunodepre, '9') = '9', '0', imunodepre) * 1 AS imunodepre
            , IF(IFNULL(hepatica, '9') = '9', '0', hepatica) * 1 AS hepatica
            , IF(IFNULL(neurologic, '9') = '9', '0', neurologic) * 1 AS neurologic
            , IF(IFNULL(renal, '9') = '9', '0', renal) * 1 AS renal
            , IF(IFNULL(sind_down, '9') = '9', '0', sind_down) * 1 AS sind_down
            , IF(IFNULL(metabolica, '9') = '9', '0', metabolica) * 1 AS metabolica
            , IF(IFNULL(puerpera, '9') = '9', '0', puerpera) * 1 AS puerpera
            , IF(IFNULL(obesidade, '9') = '9', '0', obesidade) * 1 AS obesidade
            , IF(IFNULL(out_morbi, '9') = '9', '0', out_morbi) * 1 AS out_morbi
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
        "sk_data_primeiros_sintomas", "sk_data_internacao", "sk_data_raiox",
        "qtt_sintomas", "qtt_morbidades"
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

    list_columns_sintomas = [
        "febre", "tosse", "calafrio", "dispneia", "garganta", "artralgia",
        "mialgia", "conjuntiv", "coriza", "diarreia", "outro_sin"
    ]

    list_columns_morbidade = [
        "pneumopati", "cardiopati", "imunodepre", "hepatica", "neurologic",
        "renal", "sind_down", "metabolica", "puerpera", "obesidade", "out_morbi"
    ]

    replace_zeros = {
        k: {2: 0}
        for k in (
            list_columns_sintomas
            + list_columns_morbidade
        )
    }

    tbl_ = tbl.replace(replace_zeros).assign(
        qtt_sintomas=lambda df: df.apply(
            sum_value_columns,
            columns=list_columns_sintomas,
            axis=1
        ),
        qtt_morbidades=lambda df: df.apply(
            sum_value_columns,
            columns=list_columns_morbidade,
            axis=1
        )
    ).rename(
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
    })

    return tbl_[select_columns]


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
