import pandas as pd

from dw_tools import dw_tools as dwt


def extract_stg_rdb(
    file_input: str,
    table_name: str,
    conn_output: dwt.sa.engine.Engine
):
    columns = dwt.get_columns_db(
        conn=conn_output,
        table_name=table_name
    )

    tbl = pd.read_excel(file_input)

    return tbl, columns


def treat_stg_rdb(
    tbl: pd.DataFrame,
    columns: list[str]
):
    tbl_columns = tbl.columns.to_list()
    tbl_treat = tbl[tbl_columns]

    rename_columns = [
        dwt.treat_column_name(c).upper()
        for c in tbl_columns
    ]

    tbl_treat.columns = rename_columns

    return tbl_treat[columns]


def load_stg_rdb(
    tbl: pd.DataFrame,
    table_name: str,
    conn_output: dwt.sa.engine.Engine
):
    if not dwt.has_record_in_table(
        conn=conn_output,
        table_name=table_name
    ):
        tbl.to_sql(
            name=table_name,
            con=conn_output,
            if_exists="append",
            chunksize=1000,
            index=False
        )


def run_stg_rdb(
    conn_output: dwt.sa.engine.Engine,
    table_name: str,
    file_input: str
):
    tbl_extract, columns = extract_stg_rdb(
        file_input=file_input,
        table_name=table_name,
        conn_output=conn_output
    )

    tbl_treat = treat_stg_rdb(
        tbl=tbl_extract,
        columns=columns
    )

    load_stg_rdb(
        tbl=tbl_treat,
        table_name=table_name,
        conn_output=conn_output
    )


if __name__ == "__main__":
    conn_stg = dwt.connect_mysql(
        username="root",
        password="1234",
        database="stg",
        server="localhost",
        port=33033
    )

    run_stg_rdb(
        file_input="../../../origin/RELATORIO_DTB_BRASIL_DISTRITO.xls",
        table_name="stg_rdb_distrito",
        conn_output=conn_stg
    )

    run_stg_rdb(
        file_input="../../../origin/RELATORIO_DTB_BRASIL_MUNICIPIO.xls",
        table_name="stg_rdb_municipio",
        conn_output=conn_stg
    )

    run_stg_rdb(
        file_input="../../../origin/RELATORIO_DTB_BRASIL_SUBDISTRITO.xls",
        table_name="stg_rdb_subdistrito",
        conn_output=conn_stg
    )
