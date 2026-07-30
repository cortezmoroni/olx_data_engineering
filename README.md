# olx_data_engineering Pipeline
Pipeline engenharia de dados desenvolvido com databricks, pyspark e delta lake  seguindo a arquitetura Medallion.

##Arquitetura Medallion
Arquivo CSV da OLX
        │
        ▼
     Bronze
(Dados Brutos)
        │
        ▼
     Silver
(Limpeza e Tratamento)
        │
        ▼
      Gold
(Métricas de Negócio)
        │
        ▼
 Análises e Dashboards

##Bronze
-Ingestão dos dados da OLX.
-Armazenamento  inicial em tabela Delta

##Silver
-limpeza
-Padronização
-Conversão de tipos
-Remoção de duplicidades

##Gold
-Preço por metro quadrado
-categoria de preço
-total de cômodos
-Indicador de garagem
-Data processamento

##Tecnologias
-Python
-PySpark
-Databricks
-Delta Lake
-Github

