# olx_data_engineering Pipeline
Pipeline engenharia de dados desenvolvido com databricks, pyspark e delta lake  seguindo a arquitetura Medallion.

#Arquitetura Medallion
```mermaid
flowchart LR
    A[Arquivo CSV da OLX] --> B[Bronze<br/>Dados brutos]
    B --> C[Silver<br/>Limpeza, tipagem e deduplicação]
    C --> D[Gold<br/>Métricas e regras de negócio]
    D --> E[Análises e dashboards]
    ```

#Bronze
-Ingestão dos dados da OLX.
-Armazenamento  inicial em tabela Delta

#Silver
-limpeza
-Padronização
-Conversão de tipos
-Remoção de duplicidades

#Gold
-Preço por metro quadrado
-categoria de preço
-total de cômodos
-Indicador de garagem
-Data processamento

#Tecnologias
-Python
-PySpark
-Databricks
-Delta Lake
-Github

