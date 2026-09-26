# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # PUC_2_Análise
# MAGIC Respostas às 10 perguntas do MVP, construídas em cima das tabelas Gold do catálogo `PUC_Sprint_2.anp`.
# MAGIC Cada pergunta tem sua própria seção: contexto/lógica em markdown, seguido da consulta.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "PUC_Sprint_2"
SCHEMA = "anp"

fact_producao = spark.table(f"{CATALOGO}.{SCHEMA}.fact_producao_mensal")
fact_reservas = spark.table(f"{CATALOGO}.{SCHEMA}.fact_reservas_anual")
fact_economico = spark.table(f"{CATALOGO}.{SCHEMA}.fact_economico")
dim_campo = spark.table(f"{CATALOGO}.{SCHEMA}.dim_campo")
dim_poco = spark.table(f"{CATALOGO}.{SCHEMA}.dim_poco")
dim_tempo = spark.table(f"{CATALOGO}.{SCHEMA}.dim_tempo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1
# MAGIC **Qual foi a produção total de óleo e gás natural por ano?**
# MAGIC
# MAGIC Somei as colunas de produção da tabela fact_producao_mensal agrupando pelo ano, vindo de dim_tempo. Gás precisou juntar duas colunas, associado e não associado, já que o BMP não tem uma coluna única para isso, e mantive óleo e gás em eixos separados no gráfico por causa da diferença de unidade, m³ contra milhões de m³. Usei “display” para melhor visualização.
# MAGIC

# COMMAND ----------

resposta_1 = (fact_producao
    .join(dim_tempo, "tempo_id")
    .groupBy("ano")
    .agg(
        F.sum("producao_oleo_m3").alias("producao_oleo_m3"),
        F.sum(F.col("producao_gas_associado_mm3") + F.col("producao_gas_nao_associado_mm3")).alias("producao_gas_mm3"),
    )
    .orderBy("ano"))

display(resposta_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2
# MAGIC **Quais os 10 campos com maior produção acumulada?**
# MAGIC Inner join entre a fact_produção com dim_campo, agrupado por campo e bacia, já que nome de campo sozinho não é único, e somei a produção de óleo de cada um, ordenando do maior pro menor.
# MAGIC

# COMMAND ----------

resposta_2 = (fact_producao
    .join(dim_campo, "campo_id")
    .groupBy("campo", "bacia")
    .agg(
        F.sum("producao_oleo_m3").alias("producao_oleo_m3_acumulada"),
        F.sum(F.col("producao_gas_associado_mm3") + F.col("producao_gas_nao_associado_mm3")).alias("producao_gas_mm3_acumulada"),
    )
    .orderBy(F.col("producao_oleo_m3_acumulada").desc()))
display(resposta_2)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3 
# MAGIC **Qual a distribuição de poços por bacia?**
# MAGIC
# MAGIC Como a bacia de cada poço não está diretamente na tabela do BDEP de um jeito confiável, segui o caminho poço até campo até bacia, usando o join já validado entre a fato e dim_campo, e contei poços distintos por bacia.

# COMMAND ----------

resposta_3 = (fact_producao
    .join(dim_campo, "campo_id")
    .groupBy("bacia")
    .agg(F.countDistinct("poco_id").alias("pocos_distintos"))
    .orderBy(F.col("pocos_distintos").desc()))

display(resposta_3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 4
# MAGIC **Existe correlação entre o preço do Brent e o volume de produção nos meses seguintes (defasagem)?**
# MAGIC
# MAGIC Agreguei a produção de óleo por mês e dei inner join com o Brent médio mensal de fact_economico. 
# MAGIC
# MAGIC A "defasagem" é simulada para cada mês, com o valor do Brent de N meses atrás. Comparamos a correlação sem defasagem com a de 3 meses de defasagem. 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Pergunta 4
from pyspark.sql import Window

producao_mensal_total = (fact_producao
    .groupBy("tempo_id")
    .agg(F.sum("producao_oleo_m3").alias("producao_oleo_total")))

janela_tempo = Window.orderBy("tempo_id")
LAG_MESES = 3

base_correlacao = (producao_mensal_total
    .join(fact_economico.select("tempo_id", "preco_brent_usd_medio"), "tempo_id")
    .orderBy("tempo_id")
    .withColumn("brent_defasado_3m", F.lag("preco_brent_usd_medio", LAG_MESES).over(janela_tempo)))

resposta_4 = base_correlacao.select(
    F.corr("producao_oleo_total", "preco_brent_usd_medio").alias("correlacao_sem_defasagem"),
    F.corr("producao_oleo_total", "brent_defasado_3m").alias("correlacao_defasagem_3_meses"),
)

resposta_4.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 5 
# MAGIC **Qual seria a receita bruta estimada (produção × Brent × câmbio) por campo/ano, e como ela se compara
# MAGIC entre bacias?**
# MAGIC
# MAGIC Converti a produção de óleo de m³ para barris usando o fator padrão da indústria e multipliquei pelo Brent médio mensal e pelo câmbio USD/BRL, chegando a uma receita estimada em reais.

# COMMAND ----------

BARRIS_POR_M3 = 6.2898  # fator de conversão padrão da indústria: 1 m³ de óleo = 6,2898 barris

producao_mensal_valorada = (fact_producao
    .join(dim_tempo, "tempo_id")
    .join(dim_campo, "campo_id")
    .join(fact_economico.select("tempo_id", "preco_brent_usd_medio", "cambio_usd_brl_medio"), "tempo_id")
    .withColumn("producao_oleo_bbl", F.col("producao_oleo_m3") * F.lit(BARRIS_POR_M3))
    .withColumn("receita_estimada_brl", F.col("producao_oleo_bbl") * F.col("preco_brent_usd_medio") * F.col("cambio_usd_brl_medio")))

resposta_5_por_campo_ano = (producao_mensal_valorada
    .groupBy("campo", "bacia", "ano")
    .agg(F.sum("receita_estimada_brl").alias("receita_estimada_brl"))
    .orderBy(F.col("receita_estimada_brl").desc()))

resposta_5_por_bacia_ano = (producao_mensal_valorada
    .groupBy("bacia", "ano")
    .agg(F.sum("receita_estimada_brl").alias("receita_estimada_brl"))
    .orderBy("ano", F.col("receita_estimada_brl").desc()))

resposta_5_por_campo_ano.show(20, truncate=False)
resposta_5_por_bacia_ano.show(20, truncate=False)