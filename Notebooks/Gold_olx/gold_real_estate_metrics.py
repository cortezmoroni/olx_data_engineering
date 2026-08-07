
from pyspark.sql import functions as F


#A camada Gold disponibiliza dados prontos para consumo analítico.
#Nesta etapa são criados indicadores de negócio como:
#Preço por metro quadrado.
#Classificação do imóvel por faixa de preço.
#Quantidade total de cômodos.
#Classificação da área do imóvel.
#Data de processamento



df_silver = spark.table(
    "workspace.default.silver_real_estate_listings"
)

df_gold = (
    df_silver

    .withColumn(
        "preco_m2",
        F.when(
            (F.col("area_m2").isNotNull()) & (F.col("area_m2") > 0),
            F.round(F.col("preco") / F.col("area_m2"), 2)
        )
    )

    .withColumn(
        "categoria_preco",
        F.when(F.col("preco") < 300000, "baixo")
         .when(F.col("preco") < 700000, "medio")
         .otherwise("alto")
    )

    .withColumn(
        "total_comodos",
        F.coalesce(F.col("quartos"), F.lit(0))
        + F.coalesce(F.col("banheiros"), F.lit(0))
    )

    .withColumn(
        "possui_garagem",
        F.when(F.col("garagens") > 0, "sim")
         .otherwise("nao")
    )

    .withColumn(
        "dt_carga",
        F.current_timestamp()
    )
)

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_real_estate_metrics")
