CREATE TABLE IF NOT EXISTS dw.d_paciente (
    SK_PACIENTE         INTEGER
    , NU_IDADE_DEDUZIDA INTEGER
    , CD_RACA           INTEGER
    , CD_EVOLUCAO       INTEGER
    , CD_VACINADO       INTEGER
    , CD_SEXO           CHAR(1)
    , DS_SEXO           VARCHAR(15)
    , DS_RACA           VARCHAR(15)
    , FL_VACINADO       VARCHAR(15)
    , DS_EVOLUCAO       VARCHAR(25)
)
