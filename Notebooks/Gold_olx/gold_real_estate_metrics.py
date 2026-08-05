
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, coalesce, lit, round, current_timestamp

#A camada Gold disponibiliza dados prontos para consumo analítico.
#Nesta etapa são criados indicadores de negócio como:
#Preço por metro quadrado.
#Classificação do imóvel por faixa de preço.
#Quantidade total de cômodos.
#Classificação da área do imóvel.
#Data de processamento



df_silver = spark.table("workspace.default.silver_real_estate_listings")
df_gold = (df_silver
.withColumn("preco_m2",
            when((col("area_m2").isNotNull()) & (col("area_m2") > 0),
                 round(col("preco") / col("area_m2"), 2))
)
.withColumn("categoria_preco",
            when(col("preco") < 300000, "baixo")
            .when(col("preco") < 700000, "medio")
            .otherwise("alto")
)
.withColumn("total_comodos",
            coalesce(col("quartos"), lit(0))
            + coalesce(col("banheiros"), lit(0))
)
.withColumn("Possui_garagem",
            when(col("garagens") > 0, "sim").otherwise("nao")
)
.withColumn("dt_carga",
            current_timestamp()
)
)

(

df_gold.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("workspace.default.gold_real_estate_metrics")
    )