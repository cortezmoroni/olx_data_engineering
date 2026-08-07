from pyspark.sql import functions as F


#A camada Silver é responsável pelo tratamento e padronização dos dados.
#Transformações realizadas:
#Remoção de espaços em branco.
#Padronização de colunas textuais.
#Conversão dos tipos de dados.
#Tratamento de valores nulos.
#Remoção de registros sem informações essenciais.
#Eliminação de duplicidades.
#Preparação dos dados para análise.





#leitura_da_bronze
df_bronze = spark.table("workspace.default.bronze_olx")

#tratamento_camada_bronze

df_silver =(

    df_bronze

    #remover espaços extras
    .withColumn("titulo", F.trim(F.col("titulo")))
     .withColumn("url", F.trim(F.col("url")))
     .withColumn("tipo", F.trim(F.col("tipo")))
    .withColumn("bairro", F.trim(F.col("bairro")))
    .withColumn("cidade", F.trim(F.col("cidade")))
    .withColumn("estado", F.trim(F.col("estado")))
    .withColumn("cep", F.trim(F.col("cep")))
    .withColumn("descricao", F.trim(F.col("descricao")))

    #TRANSFORMAR TEXTOS VAZIOS EM NULL
    .replace("", None)


    #converter as colunas numericas
    .withColumn("preco", F.col("preco").cast("double"))
    .withColumn("quartos", F.col("quartos").cast("double"))
    .withColumn("banheiros", F.col("banheiros").cast("double"))
    .withColumn("garagens", F.col("garagens").cast("double"))
    .withColumn("area_m2", F.col("area_m2").cast("double"))

    #remover somente registros sem informação
    .na.drop(subset = ["titulo", "url"])

    #remove somente registros sem informação
    .dropDuplicates(["url"])

     )
                                

(

   df_silver.write\
    .mode("overwrite")\
    .saveAsTable("workspace.default.silver_real_estate_listings")
   
)
