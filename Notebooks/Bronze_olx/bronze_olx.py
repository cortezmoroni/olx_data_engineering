
#Projeto de Engenharia de dados - Pipeline  Medallion (OLX)
##Este projeto demonstra a construção  de um pipeline de engenharia de dados utilizando 
# a arquitetura medallion(BRONZE, SILVER, GOLD).Os dados foram  extraídos 
# de anuncios de imóveis da OLX  e processamento  em databricks utilizando Pyspark.


from pyspark.sql import functions as F



#Extração dos dados  da Olx
#O dataset em formaato CSV é carregado para on databricks.O arquivo contém 
# anúncios  de imóveis  da OLX com informações  como titulo, preço, localização, área , 
# qtd de quartos e demais caracteristicas.

df =  (

spark.read
.option("header", "true")
.option("inferSchema", "true")
.csv("/Volumes/workspace/default/dados/dataset_olx_raw.csv")
)


#BRONZE

#A camada bronze representa a ingestao dos dados brutos.
#Nesta etapa:
# Os Dados sao diretamente do arquivo csv
#Nenhuma transformação de negócio é aplicada 
#Os dados sao no formato delta lake.
# O objetivo é preservar a informação original para auditoria e processamento.


df.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("workspace.default.bronze_olx")
