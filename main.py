from dw_tools import dw_tools as dwt

from scripts.etl.stg.stg_influd import run_stg_influd
from scripts.etl.stg.stg_rdb import run_stg_rdb


def run_stg(
    conn_stg: dwt.sa.engine.Engine
):
    run_stg_influd(
        files_input=[
            (f"./origin/INFLUD{y:02}.csv", f"{y:02}")
            for y in range(13, 18 + 1)
        ],
        conn_output=conn_stg
    )

    for f_path, t_name in [
        ("./origin/RELATORIO_DTB_BRASIL_DISTRITO.xls", "stg_rdb_distrito"),
        ("./origin/RELATORIO_DTB_BRASIL_MUNICIPIO.xls", "stg_rdb_municipio"),
        ("./origin/RELATORIO_DTB_BRASIL_SUBDISTRITO.xls", "stg_rdb_subdistrito")
    ]:
        run_stg_rdb(
            file_input=f_path,
            table_name=t_name,
            conn_output=conn_stg
        )


def main(
    conn_dw: dwt.sa.engine.Engine,
    conn_stg: dwt.sa.engine.Engine
):
    run_stg(conn_stg)


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

    main(conn_dw, conn_stg)
