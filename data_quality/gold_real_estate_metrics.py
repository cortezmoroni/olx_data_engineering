#Data quality tabela Gold

from pyspark.sql import  functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
  )

  #leitura da tabela gold
df = spark.table("workspace.default.gold_real_estate_metrics")

#execução das regras de dataquality
#regra que procura registros onde o titulo é nulo.
dq_titulo_nulo = (
    df
    .filter(F.col("titulo").isNull())
    .count()
)
#preço maior que zero
#verifica duas situações: preço anulo , preço menor ou igual zero.
dq_preco_invalido = (
    df
    .filter(
        F.col("preco").isNull() |
        (F.col("preco") <= 0)
    )
    .count()
)
#se a área foi informada, ela precisa ser maior que zero
dq_area_invalida = (
    df
    .filter(
        F.col("area_m2").isNotNull() &
        (F.col("area_m2") <= 0)
    )
    .count()
)
# regra remove as URLs nulas , agrupa todas as URLs iguais.
dq_url_duplicada = (
    df
    .filter(F.col("url").isNotNull())
    .groupBy("url")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
#verifica se a categoria pertence aos valores permitidos
dq_categoria_invalida = (
    df
    .filter(
        F.col("categoria_preco").isNull() |
        ~F.col("categoria_preco").isin(
            "baixo",
            "medio",
            "alto"
        )
    )
    .count()
)
# qualquer outro valor gera zero, será connsiderado invalido, pois a regra espera exatamente os valores definidos.
dq_garagem_invalida = (
    df
    .filter(
        F.col("possui_garagem").isNull() |
        ~F.col("possui_garagem").isin(
            "sim",
            "nao"
        )
    )
    .count()
)
# regra verifica se possui data de processamento
dq_data_carga_nula = (
    df
    .filter(F.col("dt_carga").isNull())
    .count()
)


#resultado data quality 

resultados_dq = [
    (
        "DQ01",
        "Título obrigatório",
        "titulo",
        dq_titulo_nulo,
        "APROVADO" if dq_titulo_nulo == 0 else "REPROVADO"
    ),
    (
        "DQ02",
        "Preço maior que zero",
        "preco",
        dq_preco_invalido,
        "APROVADO" if dq_preco_invalido == 0 else "REPROVADO"
    ),
    (
        "DQ03",
        "Área maior que zero quando informada",
        "area_m2",
        dq_area_invalida,
        "APROVADO" if dq_area_invalida == 0 else "REPROVADO"
    ),
    (
        "DQ04",
        "URL sem duplicidade",
        "url",
        dq_url_duplicada,
        "APROVADO" if dq_url_duplicada == 0 else "REPROVADO"
    ),
    (
        "DQ05",
        "Categoria de preço válida",
        "categoria_preco",
        dq_categoria_invalida,
        "APROVADO" if dq_categoria_invalida == 0 else "REPROVADO"
    ),
    (
        "DQ06",
        "Indicador de garagem válido",
        "possui_garagem",
        dq_garagem_invalida,
        "APROVADO" if dq_garagem_invalida == 0 else "REPROVADO"
    ),
    (
        "DQ07",
        "Data de carga obrigatória",
        "dt_carga",
        dq_data_carga_nula,
        "APROVADO" if dq_data_carga_nula == 0 else "REPROVADO"
    )
]




#Relatório data quality 


schema_dq = StructType([
    StructField("id_regra", StringType(), False),
    StructField("regra", StringType(), False),
    StructField("coluna", StringType(), False),
    StructField("quantidade_erros", IntegerType(), False),
    StructField("status", StringType(), False)
])

df_relatorio_dq = spark.createDataFrame(
    resultados_dq,
    schema=schema_dq
)






df_relatorio_dq = (
    df_relatorio_dq
    .withColumn(
        "dt_validacao",
        F.current_timestamp()
    )
)

display(df_relatorio_dq)
