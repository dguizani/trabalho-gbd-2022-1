-- stage do arquivo RELATORIO_DTB_BRASIL_DISTRITO.xls
CREATE TABLE IF NOT EXISTS stg_rdb_distrito (
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
    , NOME_MUNICIPIO                        
    , DISTRITO                              
    , CODIGO_DE_DISTRITO_COMPLETO           
    , NOME_DISTRITO                         
);
