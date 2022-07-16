-- stage do arquivo RELATORIO_DTB_BRASIL_MUNICIPIO.xls
CREATE TABLE IF NOT EXISTS stg_rdb_municipio (
    UF                                      
    , NOME_UF                               
    , REGIAO_GEOGRAFICA_INTERMEDIARIA       
    , NOME_REGIAO_GEOGRAFICA_INTERMEDIARIA  
    , REGIAO_GEOGRAFICA_IMEDIATA            
    , NOME_REGIAO_GEOGRAFICA_IMEDIATA       
    , MESORREGIAO_GEOGRAFICA                
    , NOME_MESORREGIAO                      
    , MICRORREGIAO_GEOGRAFICA               
    , NOME_MICRORREGIAO                     
    , MUNICIPIO                             
    , CODIGO_MUNICIPIO_COMPLETO             
    , NOME_MUNICÍPIO                        
);
