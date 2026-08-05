# olx_data_engineering Pipeline
Pipeline engenharia de dados desenvolvido com databricks, pyspark e delta lake  seguindo a arquitetura Medallion(Bronze, Silver, Gold).

# Objetivo
Este projeto tem como objetivo construir um pipeline de dados utilizando anúncios  de imóveis da  OLX, aplicando boas práticas da Engenharia de dados para transformar dados brutos em informações confiáveis para análise e deshboards.
Ao longo do pipeline são realizadas etapas  de ingestão, limpeza, padronização, enriquecimento  dos dados de  métricas de negócios.

# Justificativa
Em projetos reais de dados, as informações  normalmente chegam incompletas, com registros  duplicados, valores nulos e diferentes  formatos.
Este projeto demonstra como organizar essas informações  em camadas,garantido maior  qualidade dos dados, melhor governança  e facilidade para consumo por ferramentas analíticas.
Além disso, foi desenvolvido para práticar  conceitos utilizados no mercado, como Delta Lake, Databricks, Pyspark e a  Arquitetura Medallion

# Arquitetura Medallion
A arquitetura Medallion organiza os dados em três camadas:

# Bronze
-Responsável pela ingestão dos dados exatamente como foram  recebidos da origem.
-atividades
-Leitura do arquivo CSV da OLX.
-Armazenamento  dos dados brutos.
-Escrita em formato Delta.

# Silver
Camada responsável pela qualidade dos dados.
-limpeza
-Padronização
-Conversão de tipos
-Remoção de duplicidades

# Gold
-Camada destinada ás regras de negócios.
-Preço por metro quadrado
-categoria de preço
-total de cômodos
-Indicador de garagem
-Data processamento

# Dicionário de Dados - Camada Gold

Objetivo
A camada Gold contém os dados refinados e enriquecidos da OLX, preparados para consumo analítico, dashboards e geração de indicadores. Nesta camada são criadas métricas derivadas para facilitar a análise dos imóveis.

# Estrutura da Tabela 
## Estrutura da Tabela

| Coluna | Tipo | Origem | Descrição |
|---------|------|---------|-----------|
| titulo | STRING | Silver | Título do anúncio do imóvel. |
| url | STRING | Silver | Link original do anúncio da OLX. |
| preco | DOUBLE | Silver | Valor anunciado do imóvel. |
| tipo | STRING | Silver | Tipo do imóvel (Casa, Apartamento, Terreno etc.). |
| bairro | STRING | Silver | Bairro do imóvel. |
| cidade | STRING | Silver | Cidade onde o imóvel está localizado. |
| estado | STRING | Silver | Unidade Federativa (UF). |
| cep | STRING | Silver | CEP informado no anúncio. |
| quartos | INT | Silver | Quantidade de quartos. |
| banheiros | INT | Silver | Quantidade de banheiros. |
| garagens | INT | Silver | Quantidade de vagas de garagem. |
| area_m2 | DOUBLE | Silver | Área do imóvel em metros quadrados. |
| descricao | STRING | Silver | Descrição completa do anúncio. |
| imagens | STRING | Silver | URL da imagem principal do imóvel. |
| preco_m2 | DOUBLE | Gold | Preço do metro quadrado (preço ÷ área). |
| categoria_preco | STRING | Gold | Classificação do imóvel em baixo, médio ou alto. |
| total_comodos | INT | Gold | Soma da quantidade de quartos e banheiros. |
| possui_garagem | STRING | Gold | Indica se o imóvel possui garagem. |
| dt_carga | TIMESTAMP | Gold | Data e hora de processamento da tabela. |

# Fluxo do PIPELINE
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



# Tecnologias
-Python
-PySpark
-Databricks
-Delta Lake
-Github

# Resultado Final 
Ao final do pipeline é gerada uma tabela  Gold pronta para consumo analítico, contendo  dados tratados , padronizados e enriquecidos  para apoiar
tomada de decisão.
