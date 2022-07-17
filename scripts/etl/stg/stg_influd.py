import pandas as pd

from dw_tools import dw_tools as dwt


def extract_stg_influd(
    files_input: list[tuple[str]],
    conn_output: dwt.sa.engine.Engine,
    table_name: str
):
    list_tbl = [
        pd.read_csv(
            f,
            sep=";",
            encoding="ISO-8859-1",
            low_memory=False,
            usecols=dwt.get_columns_db(conn_output, f"{table_name}_{y}")
        ) for f, y in files_input
    ]

    return list_tbl


def treat_stg_influd(
    list_tbl: list[pd.DataFrame]
):
    l_tbl = [
        tbl.assign(
            DT_NOTIFIC=lambda df: pd.to_datetime(df.DT_NOTIFIC, format="%d/%m/%Y"),
            DT_SIN_PRI=lambda df: pd.to_datetime(df.DT_SIN_PRI, format="%d/%m/%Y"),
            DT_NASC=lambda df: pd.to_datetime(df.DT_NASC, format="%d/%m/%Y"),
            DT_INTERNA=lambda df: pd.to_datetime(df.DT_INTERNA, format="%d/%m/%Y"),
            DT_PCR=lambda df: pd.to_datetime(df.DT_PCR, format="%d/%m/%Y"),
            DT_CULTURA=lambda df: pd.to_datetime(df.DT_CULTURA, format="%d/%m/%Y"),
            DT_HEMAGLU=lambda df: pd.to_datetime(df.DT_HEMAGLU, format="%d/%m/%Y"),
            DT_RAIOX=lambda df: pd.to_datetime(df.DT_RAIOX, format="%d/%m/%Y"),
            DT_OBITO=lambda df: pd.to_datetime(df.DT_OBITO, format="%d/%m/%Y"),
            DT_ENCERRA=lambda df: pd.to_datetime(df.DT_ENCERRA, format="%d/%m/%Y",
                errors="coerce"),
            DT_DIGITA=lambda df: pd.to_datetime(df.DT_DIGITA, format="%d/%m/%Y"),
            DT_COLETA=lambda df: pd.to_datetime(df.DT_COLETA, format="%d/%m/%Y"),
            DT_ENTUTI=lambda df: pd.to_datetime(df.DT_ENTUTI, format="%d/%m/%Y"),
            DT_ANTIVIR=lambda df: pd.to_datetime(df.DT_ANTIVIR, format="%d/%m/%Y"),
            DT_IFI=lambda df: pd.to_datetime(df.DT_IFI, format="%d/%m/%Y"),
            DT_OUTMET=lambda df: pd.to_datetime(df.DT_OUTMET, format="%d/%m/%Y"),
            DT_PCR_1=lambda df: pd.to_datetime(df.DT_PCR_1, format="%d/%m/%Y"),
            DT_SAIDUTI=lambda df: pd.to_datetime(df.DT_SAIDUTI, format="%d/%m/%Y"),
        ) for tbl in list_tbl
    ]

    return l_tbl


def load_stg_influd(
    list_tbl: list[pd.DataFrame],
    files_input: list[tuple[str]],
    table_name: str,
    conn_output: dwt.sa.engine.Engine
):
    [
        tbl.to_sql(
            name=f"{table_name}_{year}",
            con=conn_output,
            if_exists="append",
            chunksize=1000,
            index=False
        ) for tbl, year in list(zip(list_tbl, [y for _, y in files_input]))
        if not dwt.has_record_in_table(
            conn=conn_output,
            table_name=f"{table_name}_{year}"
        )
    ]


def run_stg_influd(
    files_input: list[tuple[str]],
    conn_output: dwt.sa.engine.Engine
):
    table_name = "stg_influd"

    list_tbl_extract = extract_stg_influd(
        files_input=files_input,
        conn_output=conn_output,
        table_name=table_name
    )

    list_tbl_treat = treat_stg_influd(
        list_tbl=list_tbl_extract
    )

    load_stg_influd(
        list_tbl=list_tbl_treat,
        table_name=table_name,
        files_input=files_input,
        conn_output=conn_output
    )


if __name__ == "__main__":
    first_y = 13
    last_y = 18

    files_input = [
        (f"./origin/INFLUD{y:02}.csv", f"{y:02}")
        for y in range(first_y, last_y + 1)
    ]

    conn_output = dwt.connect_mysql(
        username="root",
        password="1234",
        database="stg",
        server="localhost",
        port=33033
    )

    run_stg_influd(
        files_input=files_input,
        conn_output=conn_output
    )
