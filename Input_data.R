#
# Internalizando dados de um Excel (fonte Sidra-IBGE)
#

to_load(readxl)
va_ind <- readxl::read_excel("tabela5938.xlsx",
                    sheet="Tabela 4",
                    #encoding="UTF-8",
                    range = "A4:C5574",
                    guess_max=10000)


#
# Internalizando um arquivo disponibilizado diretamente na internet - Portal Gov.br
#

# Fazendo o download do arquivo para um diretório definido nas variáveis "diretorio_base" e "diretorio_arq_recebidos" com a inclusão da data de download
download.file("https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip",
              paste(diretorio_base,
                    diretorio_arq_recebidos,
                    "/acessos_banda_larga_fixa_",
                    Sys.Date(),
                    ".zip",
                    sep=""))

arq_recente <- function(prefixo_arquivo, tipo="zip"){    # Retorna o nome do arquivo mais recente com base na codificação do nome
  arquivos <- dir()
  arquivos <- arquivos[str_detect(arquivos,prefixo_arquivo)]
  arquivos <- sort(arquivos[str_detect(arquivos,paste(".",tipo,"$",sep=""))],decreasing=T)[1]
}

# Descompacta o arquivo no diretório definido nas variáveis "diretorio_base", "diretorio_arq_recebido","diretorio_banda_larga"
arquivo <- arq_recente("acessos_banda_larga_fixa_") 
unzip(arquivo,           # Extrai o novo histórico na pasta
      exdir=paste(diretorio_base,      
                  diretorio_arq_recebidos,
                  diretorio_banda_larga,
                  sep="")
        ) 

###################################################
#
# Internalizando arquivo csv com mais de 1Gb
#

# Variável que aponta para o local que que os arquivos que serão lidos estão
arq_recebidos<-paste(local_analise,"/Arq_Recebidos/acessos_banda_larga_fixa",sep="") 
setwd(arq_recebidos)

bd_21<-readr::read_delim("Acessos_Banda_Larga_Fixa_2021.csv",col_names=F,delim=";",skip=1,
                  col_types=cols(
                    X1='c',  # Ano (tratado como character)
                    X2='c',  # Mes
                    X3='c',  # Grupo economico
                    X4='c',  # Empresa
                    X5='c',  # CNPJ
                    X6='c',  # Porte prestadora
                    X7='c',  # UF ( se quiser mudar para fator 'f')
                    X8='c',  # Municipio
                    X9='i',  # Cod_IBGE (tratado como integer, 'n' para tratar como number, 'd' para double)
                    X10='c', # Faixa de velocidade
                    X11='c', # Velocidade
                    X12='c', # Tecnologia
                    X13='c', # Meio de acesso
                    X14='c', # Tipo de pesoa
                    X15='c', # Tipo de produto
                    X16='i') # Acessos
                  # Se quiser pular a coluna usa o tipo '-'
)

colnames(bd_21)<-c("ANO",    #1
                   "MES",    #2
                   "GRP_ECON", #3
                   "EMP",         #4
                   "CNPJ", #5
                   "PORTE_OPE",   #6
                   "UF",     #7
                   "MUNC",   #8
                   "ID_MUN",      #9
                   "FX_VELOC",    #10
                   "VELOC",  #11
                   "TECNOLOGIA",  #12
                   "MEIO_ACES",   #13
                   "PESSOA",      #14
                   "PRODUTO",     #15
                   "ACESSOS")     #16


colnames(bd_21)
dim(bd_21)

#############################################################################################################################
#
# Internalizando no Spaklyr (Huge data) 1.5Gb para cada semestre com os dados das operadoras de telefonia móvel
#

# Configuração do sparklyr
to_load(sparklyr)
spark_available_versions()
spark_installed_versions()

# Initialize configuration with defaults
config <- spark_config()
# Memory
config["sparklyr.shell.driver-memory"] <- "32g"
# Cores
config["sparklyr.connect.cores.local"] <- 4
# Connect to local cluster with custom configuration
sc <- spark_connect(master = "local", config = config)


# Definição dos diretórios base
local_analise<-getwd()
arq_recebidos<-paste(local_analise,"/Arq_Recebidos/acessos_telefonia_movel",sep="")
setwd(arq_recebidos)


# Internalizando os arquivos CSV para o Spark
# Internalizando segundo 2021 1S
bd_21_1S <- spark_read_csv(
  sc,
  name = "bd_21_1S",
  path = "Acessos_Telefonia_Movel_2021_1S.csv",
  header = TRUE,
  #columns = NULL,
  #infer_schema = is.null(columns),
  delimiter = ";",
  memory = FALSE,
  overwrite = TRUE)

# Internalizando segundo 2021 2S
bd_21_2S <- spark_read_csv(
  sc,
  name = "bd_21_2S",
  path = "Acessos_Telefonia_Movel_2021_2S.csv",
  header = TRUE,
  #columns = NULL,
  #infer_schema = is.null(columns),
  delimiter = ";",
  memory = FALSE,
  overwrite = TRUE)

# Internalizando segundo 2022 1S
bd_22_1S <- spark_read_csv(
  sc,
  name = "bd_22_1S",
  path = "Acessos_Telefonia_Movel_2022_1S.csv",
  header = TRUE,
  #columns = NULL,
  #infer_schema = is.null(columns),
  delimiter = ";",
  memory = FALSE,
  overwrite = TRUE)

# Internalizando segundo 2022 2S
bd_22_2S <- spark_read_csv(
  sc,
  name = "bd_22_2S",
  path = "Acessos_Telefonia_Movel_2022_2S.csv",
  header = TRUE,
  #columns = NULL,
  #infer_schema = is.null(columns),
  delimiter = ";",
  memory = FALSE,
  overwrite = TRUE)

# Internalizando segundo 2023 1S
bd_23_1S <- spark_read_csv(
  sc,
  name = "bd_23_1S",
  path = "Acessos_Telefonia_Movel_2023_1S.csv",
  header = TRUE,
  #columns = NULL,
  #infer_schema = is.null(columns),
  delimiter = ";",
  memory = FALSE,
  overwrite = TRUE)

# Internalizando segundo 2023 2S
bd_23_2S <- spark_read_csv(
  sc,
  name = "bd_23_2S",
  path = "Acessos_Telefonia_Movel_2023_2S.csv",
  header = TRUE,
  #columns = NULL,
  #infer_schema = is.null(columns),
  delimiter = ";",
  memory = FALSE,
  overwrite = TRUE)

# Internalizando segundo 2024 1S
bd_24_1S <- spark_read_csv(
  sc,
  name = "bd_24_1S",
  path = "Acessos_Telefonia_Movel_2024_1S.csv",
  header = TRUE,
  #columns = NULL,
  #infer_schema = is.null(columns),
  delimiter = ";",
  memory = FALSE,
  overwrite = TRUE)



bd_movel <- sdf_bind_rows(bd_21_1S,
                          bd_21_2S,
                          bd_22_1S,
                          bd_22_2S,
                          bd_23_1S,
                          bd_23_2S,
                          bd_24_1S) 

# glimpse(bd_movel)
# src_tbls(sc)

# Criação e correção dos dados

bd_movel <- bd_movel %>%
  mutate(Periodo = Ano*100+Mes) %>%
  mutate(Grupo_Economico=ifelse(Grupo_Economico=="TELEFONICA","TELEFÔNICA",Grupo_Economico)) %>%
  mutate(CHAVE=ifelse(!is.na(Grupo_Economico),Grupo_Economico,substr(CNPJ,1,8)))


