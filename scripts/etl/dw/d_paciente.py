import pandas as pd

from dw_tools import dw_tools as dwt


def extract_d_paciente(
    conn_input: dwt.sa.engine.Engine
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
        SELECT DISTINCT
            nu_idade_n AS nu_idade_deduzida
            , IFNULL(cs_sexo, 'I') AS cd_sexo
            , IFNULL(cs_raca, '9') AS cd_raca
            , IFNULL(vacina, '9') AS cd_vacinado
            , IFNULL(evolucao, '9') AS cd_evolucao
        FROM stg_influd
    """

    tbl = pd.read_sql_query(
        sql=query,
        con=conn_input
    )

    return tbl


def treat_d_paciente(
    tbl: pd.DataFrame
):
    tbl_ = tbl.assign(
        sk_paciente=lambda df: range(1, df.shape[0] + 1),
        ds_sexo=lambda df: df.cd_sexo.replace({
            "M": "MASCULINO",
            "F": "FEMININO",
            "I": "Não Informado"
        }),
        ds_raca=lambda df: df.cd_raca.replace({
            "1": "BRANCA",
            "2": "PRETA",
            "3": "AMARELA",
            "4": "PARDA",
            "5": "INDÍGENA",
            "9": "Não Informado"
        }),
        fl_vacinado=lambda df: df.cd_vacinado.replace({
            "1": "SIM",
            "2": "NÃO",
            "9": "Não Informado"
        }),
        ds_evolucao=lambda df: df.cd_evolucao.replace({
            "1": "RECEBEU ALTA POR CURA",
            "2": "EVOLUIU PARA ÓBITO",
            "9": "Não Informado"
        })
    ).astype({
        "sk_paciente": "int64",
        "cd_raca": "int64",
        "cd_evolucao": "int64",
        "cd_sexo": "string",
        "fl_vacinado": "string",
        "ds_sexo": "string",
        "ds_raca": "string",
        "fl_vacinado": "string",
        "ds_evolucao": "string"
    })

    default_values = {
        "sk_paciente": [-1],
        "nu_idade_deduzida": [-1],
        "cd_raca": [-1],
        "cd_evolucao": [-1],
        "cd_vacinado": [-1],
        "cd_sexo": ["I"],
        "ds_sexo": ["Não Informado"],
        "ds_raca": ["Não Informado"],
        "fl_vacinado": ["Não Informado"],
        "ds_evolucao": ["Não Informado"]
    }

    tbl_ = pd.concat([
        pd.DataFrame(default_values),
        tbl_
    ])

    return tbl_


def load_d_paciente(
    conn_output: dwt.sa.engine.Engine,
    tbl: pd.DataFrame,
    table_name: str
):
    conn_output.execute(f"TRUNCATE TABLE {table_name}")

    tbl.to_sql(
        name=table_name,
        con=conn_output,
        if_exists="append",
        index=False,
        chunksize=1000
    )


def run_d_paciente(
    conn_input: dwt.sa.engine.Engine,
    conn_output: dwt.sa.engine.Engine
):
    table_name = "d_paciente"

    tbl_extract = extract_d_paciente(conn_input)

    tbl_treat = treat_d_paciente(tbl_extract)

    load_d_paciente(conn_output, tbl_treat, table_name)


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

    run_d_paciente(conn_stg, conn_dw)
