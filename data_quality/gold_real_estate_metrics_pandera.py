
#Instalação da biblioteca Pandera

%pip install "pandera[pyspark]"

import json
import pandera.pyspark as pa
import pyspark.sql.types as T
from pandera.pyspark import DataFrameModel
from pyspark.sql import functions as F


# #Leitura da tabela gold

df_gold = spark.table(
    "workspace.default.gold_real_estate_metrics"
)


#Definição das regras de data quality com Panderas

class GoldRealEstateSchema(DataFrameModel):

    # DQ01 - título obrigatório
    titulo: T.StringType() = pa.Field(
        nullable=False
    )

    # DQ02 - preço obrigatório e maior que zero
    preco: T.DoubleType() = pa.Field(
        nullable=False,
        gt=0
    )

    # DQ03 - área pode ser nula,
    # mas quando existir precisa ser maior que zero
    area_m2: T.DoubleType() = pa.Field(
        nullable=True,
        gt=0
    )

    # DQ04 - URL
    url: T.StringType() = pa.Field(
        nullable=True
    )

    # DQ05 - categoria válida
    categoria_preco: T.StringType() = pa.Field(
        nullable=False,
        isin=[
            "baixo",
            "medio",
            "alto"
        ]
    )

    # DQ06 - indicador de garagem válido
    Possui_garagem: T.StringType() = pa.Field(
        nullable=False,
        isin=[
            "sim",
            "nao"
        ]
    )

    # DQ07 - data de carga obrigatória
    dt_carga: T.TimestampType() = pa.Field(
        nullable=False
    )

    class Config:
        # DQ04 - URL deve ser única
        unique = ["url"]
        
        
        #Validação
        
        df_validado = GoldRealEstateSchema.validate(
    check_obj=df_gold
)
        
        
        
# CONTAGEM DAS REGRAS DE DATA QUALITY




dq_titulo_nulo = (
    df_gold
    .filter(F.col("titulo").isNull())
    .count()
)

dq_preco_invalido = (
    df_gold
    .filter(
        F.col("preco").isNull() |
        (F.col("preco") <= 0)
    )
    .count()
)

dq_area_invalida = (
    df_gold
    .filter(
        F.col("area_m2").isNotNull() &
        (F.col("area_m2") <= 0)
    )
    .count()
)

dq_url_duplicada = (
    df_gold
    .filter(F.col("url").isNotNull())
    .groupBy("url")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

dq_categoria_invalida = (
    df_gold
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

dq_garagem_invalida = (
    df_gold
    .filter(
        F.col("Possui_garagem").isNull() |
        ~F.col("Possui_garagem").isin(
            "sim",
            "nao"
        )
    )
    .count()
)

dq_data_carga_nula = (
    df_gold
    .filter(F.col("dt_carga").isNull())
    .count()
)



#Consolidação dos resultados de Data Quality


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
        "Possui_garagem",
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




#Criação de relatório Data quality


from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

schema_dq = StructType([
    StructField("id_regra", StringType(), False),
    StructField("regra", StringType(), False),
    StructField("coluna", StringType(), False),
    StructField("quantidade_erros", IntegerType(), False),
    StructField("status", StringType(), False)
])

df_relatorio_pandera = spark.createDataFrame(
    resultados_dq,
    schema=schema_dq
)



#Adicionar data e hora


df_relatorio_pandera = (
    df_relatorio_pandera
    .withColumn(
        "dt_validacao",
        F.current_timestamp()
    )
)



# Salva histórico em Delta

df_relatorio_pandera.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        "workspace.default.data_quality_gold_pandera"
    )
    
    
    
    #Visualização do resultado do data quality
    
    
    display(df_relatorio_pandera)
        