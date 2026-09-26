# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # PUC_Sprint2_Gold
# MAGIC Modelo estrela (fact constellation) a partir das tabelas Silver: `dim_campo`, `dim_poco`, `dim_tempo`,
# MAGIC `fact_producao_mensal`, `fact_reservas_anual` e `fact_economico`.
# MAGIC
# MAGIC Papel da Gold: transformar o dado limpo da Silver em dimensões e fatos prontos para responder as
# MAGIC perguntas do MVP. Princípio seguido em toda dimensão construída aqui: **nunca assumir que uma coluna
# MAGIC é chave única só porque parece que deveria ser** - cada dimensão só é montada depois de um diagnóstico
# MAGIC confirmando isso, e cada join é validado por cobertura (não só "rodou sem erro").

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

CATALOGO = "PUC_Sprint_2"
SCHEMA = "anp"

df_bmp = spark.table(f"{CATALOGO}.{SCHEMA}.silver_bmp")
df_bar = spark.table(f"{CATALOGO}.{SCHEMA}.silver_bar")
df_bdep = spark.table(f"{CATALOGO}.{SCHEMA}.silver_bdep")
df_cambio = spark.table(f"{CATALOGO}.{SCHEMA}.silver_cambio")
df_brent = spark.table(f"{CATALOGO}.{SCHEMA}.silver_brent")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. dim_campo
# MAGIC
# MAGIC **O que foi encontrado:** testamos se `campo` sozinho identifica uma entidade única (bacia/estado
# MAGIC sempre iguais para o mesmo campo) e não é o caso - 80 campos têm mais de uma bacia ou estado
# MAGIC associado. Isso tem duas causas diferentes que pedem tratamento oposto:
# MAGIC - **Bacia diverge (12 casos)**: nomes de campo repetidos em bacias diferentes (homônimos reais, ex.
# MAGIC   "Candeias") - não é sujeira, são campos genuinamente distintos. Resolvido usando a chave composta
# MAGIC   `(campo, bacia)` em vez de só `campo`.
# MAGIC - **Só estado diverge, mesma bacia (68 casos)**: mais provável ser o mesmo campo com cadastro
# MAGIC   inconsistente entre boletins, ou perto de fronteira estadual. Resolvido escolhendo o estado com
# MAGIC   maior produção histórica associada (critério objetivo, via `Window` + `row_number`).
# MAGIC
# MAGIC Validado ao final: `dim_campo.count()` bate exatamente com o total de combinações `(campo, bacia)`
# MAGIC distintas no BMP (960 = 960) - confirma que a transformação não perdeu nem duplicou nenhuma linha.

# COMMAND ----------

# 1a. Diagnóstico: quantos campos têm bacia/estado inconsistente
conflitos_campo = (df_bmp
    .groupBy("campo")
    .agg(F.countDistinct("bacia").alias("qtd_bacias"), F.countDistinct("estado").alias("qtd_estados"))
    .filter((F.col("qtd_bacias") > 1) | (F.col("qtd_estados") > 1)))

print(f"Campos com bacia/estado inconsistente: {conflitos_campo.count()}")

# COMMAND ----------

# 1b. Resolve o estado "vencedor" por (campo, bacia) usando o critério de maior produção histórica
janela_campo_bacia = Window.partitionBy("campo", "bacia").orderBy(F.desc("producao_total"))

estado_dominante = (df_bmp
    .groupBy("campo", "bacia", "estado")
    .agg(F.sum("producao_oleo_m3").alias("producao_total"))
    .withColumn("rank", F.row_number().over(janela_campo_bacia))
    .filter(F.col("rank") == 1)
    .select("campo", "bacia", "estado"))

dim_campo = estado_dominante.withColumn("campo_id", F.monotonically_increasing_id())

# 1c. Validação: deve bater exatamente com o total de combinações (campo, bacia) distintas
print(f"dim_campo: {dim_campo.count()}")
print(f"combinações distintas (campo, bacia) no BMP: {df_bmp.select('campo', 'bacia').distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. dim_poco
# MAGIC
# MAGIC **O que foi encontrado:** ao contrário do BAR, o BDEP não tem nenhuma duplicidade de poço (31.304
# MAGIC linhas = 31.304 poços distintos) - mesmo sendo uma lista "poços que se tornaram públicos por ano",
# MAGIC não há repetição do mesmo poço em anos diferentes. `poco` já é chave única direta, sem precisar de
# MAGIC nenhuma lógica de desempate.

# COMMAND ----------

# 2a. Diagnóstico: confirma ausência de duplicidade
total_bdep = df_bdep.count()
pocos_distintos = df_bdep.select("poco").distinct().count()
print(f"Total de linhas: {total_bdep} | poços distintos: {pocos_distintos} | duplicados: {total_bdep - pocos_distintos}")

# COMMAND ----------

# 2b. Monta a dimensão diretamente, sem tratamento de duplicidade
dim_poco = df_bdep.select(
    "poco", "cadastro", "operador", "estado", "bacia", "campo", "sig_campo",
    "terra_mar", "tipo", "categoria", "situacao",
    "profundidade_vertical_m", "profundidade_sondador_m", "profundidade_medida_m",
    "lamina_d_agua_m", "latitude_base_dd", "longitude_base_dd",
    "unidade_estratigrafica", "geologia_grupo_final", "geologia_formacao_final",
).withColumnRenamed("poco", "poco_id")

print(f"dim_poco: {dim_poco.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. dim_tempo
# MAGIC
# MAGIC **O que foi encontrado:** a coluna `mes_ano` do BMP não é uniformemente numérica - parte dos
# MAGIC arquivos usa abreviação em português por extenso para os últimos meses do ano (`out`, `nov`, `dez`
# MAGIC em vez de `10`, `11`, `12`). A extração do mês foi isolada numa função (`extrair_mes_numerico`) para
# MAGIC ser reaproveitada também em `fact_producao_mensal` - calcular o mês de dois jeitos diferentes em
# MAGIC lugares diferentes gerou `tempo_id` inconsistente entre dimensão e fato numa versão anterior deste
# MAGIC notebook, quebrando o join sem erro aparente (a fato simplesmente não achava correspondência para os
# MAGIC meses out/nov/dez). Usar uma única função garante que as duas calculam o mesmo valor sempre.
# MAGIC
# MAGIC Validado: todos os `mes_str` distintos no BMP são ou numéricos (01-12) ou uma dessas 3 abreviações -
# MAGIC nenhuma outra abreviação de mês aparece na base, e zero linhas ficam com `mes` nulo após a conversão.

# COMMAND ----------

def extrair_mes_numerico(df, coluna_mes_ano="mes_ano"):
    """Extrai o mês como inteiro de 'mes_ano' (formato 'MM/AAAA'), tratando os
    casos em que o mês vem abreviado por extenso em português (out/nov/dez)
    em vez de numérico. Usada tanto em dim_tempo quanto em fact_producao_mensal
    para garantir que as duas calculam o tempo_id da mesma forma."""
    mes_str = F.split(F.col(coluna_mes_ano), "/").getItem(0)
    mes_num = (F.when(mes_str == "out", F.lit(10))
                .when(mes_str == "nov", F.lit(11))
                .when(mes_str == "dez", F.lit(12))
                .otherwise(mes_str.try_cast("int")))
    return df.withColumn("mes", mes_num)

# COMMAND ----------

# 3a. Diagnóstico: confirma que não sobra nenhuma abreviação de mês não tratada
(df_bmp
    .withColumn("mes_str", F.split(F.col("mes_ano"), "/").getItem(0))
    .select("mes_str").distinct().orderBy("mes_str").show(30))

# COMMAND ----------

# 3b. Monta a dimensão e valida ausência de nulos após a conversão
dim_tempo = (extrair_mes_numerico(df_bmp.select("ano", "mes_ano").distinct())
    .withColumn("trimestre", F.ceil(F.col("mes") / 3).cast("int"))
    .withColumn("tempo_id", (F.col("ano") * 100 + F.col("mes")).cast("int"))
    .select("tempo_id", "ano", "mes", "trimestre")
    .orderBy("tempo_id"))

print(f"dim_tempo: {dim_tempo.count()} linhas")
print(f"linhas com mês não mapeado: {dim_tempo.filter(F.col('mes').isNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. fact_producao_mensal
# MAGIC
# MAGIC **O que foi encontrado:** o join com `dim_campo` bate 100% (esperado, já que a dimensão foi
# MAGIC construída a partir do próprio BMP). O join com `dim_poco` deixa 987 linhas sem correspondência
# MAGIC (7 poços específicos, ~4,1 milhões de m³ de um total de ~4,2 **bilhões** - 0,1% da produção). São
# MAGIC poços recentes ainda não constantes no BDEP (que só lista poços já tornados públicos) - aceito como
# MAGIC limitação documentada.

# COMMAND ----------

fact_producao_mensal = (extrair_mes_numerico(df_bmp)
    .withColumn("tempo_id", (F.col("ano") * 100 + F.col("mes")).cast("int"))
    .join(dim_campo.select("campo", "bacia", "campo_id"), on=["campo", "bacia"], how="left")
    .join(dim_poco.select("poco_id"), df_bmp["poco"] == dim_poco["poco_id"], how="left")
    .select(
        "tempo_id", "campo_id", "poco_id",
        "producao_oleo_m3", "producao_condensado_m3", "producao_gas_associado_mm3",
        "producao_gas_nao_associado_mm3", "producao_agua_m3", "injecao_gas_mm3",
        "injecao_agua_recuperacao_secundaria_m3", "injecao_agua_descarte_m3",
        "injecao_gas_carbonico_mm3", "injecao_nitrogenio_mm3", "injecao_vapor_agua_t",
        "injecao_polimeros_m3", "injecao_outros_fluidos_m3",
    ))

# 4a. Validação: cobertura dos joins e ausência de tempo_id nulo
print(f"BMP original: {df_bmp.count()} | fact_producao_mensal: {fact_producao_mensal.count()}")
print(f"Sem campo_id (join campo/bacia falhou): {fact_producao_mensal.filter(F.col('campo_id').isNull()).count()}")
print(f"Sem poco_id (join poço falhou): {fact_producao_mensal.filter(F.col('poco_id').isNull()).count()}")
print(f"Sem tempo_id (mês não mapeado): {fact_producao_mensal.filter(F.col('tempo_id').isNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. fact_reservas_anual
# MAGIC
# MAGIC **O que foi encontrado (duas descobertas separadas):**
# MAGIC
# MAGIC 1. **Grão real do BAR não é `(campo, bacia, ano)`, é `(campo, bacia, ano, situacao)`.** 13 combinações
# MAGIC    apareciam duplicadas - investigando o conteúdo, não eram linhas repetidas por erro: um campo pode
# MAGIC    ter uma acumulação de óleo já `MADURO` (com VOIP preenchido) e uma acumulação de gás ainda
# MAGIC    `NAO MADURO` (com VOIP nulo, só VGIP preenchido) na mesma linha de ano - são reservatórios
# MAGIC    fisicamente diferentes do mesmo campo, declarados em linhas separadas pela ANP. A correção foi
# MAGIC    incluir `situacao` na chave, não colapsar as linhas.
# MAGIC
# MAGIC 2. **Cobertura do join campo+bacia com o BMP evoluiu em 3 etapas**, cada uma corrigindo um problema
# MAGIC    real diferente: começou em 82,3% do VOIP (17% sem match) por causa de um bug em que a correção de
# MAGIC    mojibake do BAR era aplicada incondicionalmente e corrompia texto que já estava em UTF-8 correto
# MAGIC    (ex. `BÚZIOS` virava `B�ZIOS`, então não batia com o BMP). Uma primeira correção (condicionar a
# MAGIC    aplicação à presença de `Ã`/`Â` na entrada) subiu para 95,6% (4,4% sem match) mas ainda tinha um
# MAGIC    problema mais sutil: `Ã`/`Â` também são letras portuguesas legítimas (`GAVIÃO`, `SÃO`), então esse
# MAGIC    filtro ainda corrompia palavras corretas. A correção definitiva testa o **resultado** da
# MAGIC    reconversão (só aplica se não gerar caractere de erro `�`), chegando a 96,6% (3,4% sem match) - o
# MAGIC    que sobra são campos satélite/extensão pequenos (mesmo padrão visto antes com `SOCORRO EXTENSAO`),
# MAGIC    aceito como limitação documentada.

# COMMAND ----------

# 5a. Diagnóstico: grão (campo, bacia, ano) tem duplicidade?
duplicados_bar = (df_bar.groupBy("campo", "bacia", "ano").count().filter(F.col("count") > 1))
print(f"Combinações (campo, bacia, ano) duplicadas: {duplicados_bar.count()}")

# 5b. Confirma que incluir 'situacao' resolve o grão (deve dar 0)
duplicados_com_situacao = (df_bar.groupBy("campo", "bacia", "ano", "situacao").count().filter(F.col("count") > 1))
print(f"Combinações (campo, bacia, ano, situacao) ainda duplicadas: {duplicados_com_situacao.count()}")

# COMMAND ----------

fact_reservas_anual = (df_bar
    .join(dim_campo.select("campo", "bacia", "campo_id"), on=["campo", "bacia"], how="left")
    .select(
        "ano", "campo_id", "situacao",
        "voip_bbl", "vgip_m3", "petroleo_acumulado_bbl",
        "gas_natural_acumulado_m3", "fracao_recuperada_petroleo",
    ))

# 5c. Validação: contagem preservada (não deve colapsar nenhuma linha) e cobertura do join por VOIP
print(f"BAR original: {df_bar.count()} | fact_reservas_anual: {fact_reservas_anual.count()}")

campos_bar_sem_match = (df_bar.select("campo", "bacia").distinct()
    .join(dim_campo.select("campo", "bacia"), on=["campo", "bacia"], how="left_anti"))
voip_sem_match = (df_bar.join(campos_bar_sem_match, on=["campo", "bacia"], how="inner")
    .agg(F.sum("voip_bbl")).collect()[0][0])
voip_total = df_bar.agg(F.sum("voip_bbl")).collect()[0][0]
print(f"Combinações campo+bacia sem match: {campos_bar_sem_match.count()}")
print(f"VOIP sem match: {voip_sem_match:,.0f} bbl ({voip_sem_match/voip_total:.1%} do total)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. fact_economico
# MAGIC
# MAGIC **O que foi encontrado:** câmbio (BCB) vem em formato de data `dd/MM/yyyy`, Brent (EIA) vem em
# MAGIC `yyyy-MM-dd` - formatos diferentes entre as duas fontes, cada uma precisando da máscara certa no
# MAGIC `to_date`. Os valores numéricos já vêm limpos nas duas (ponto como decimal, sem separador de milhar),
# MAGIC diferente do BMP/BAR - não precisam da lógica de conversão de vírgula/ponto. Como o grão do BMP é
# MAGIC mensal, as duas séries diárias são agregadas por média mensal antes do join. Usamos `outer join`
# MAGIC entre câmbio e Brent (não `inner`): o câmbio só começa em 2010, mas o Brent tem histórico desde 1987
# MAGIC - um `inner` esconderia essa lacuna real; o `outer` preserva os 281 meses onde só uma das duas séries
# MAGIC está disponível, para que fique explícito que perguntas de receita em BRL só são calculáveis a partir
# MAGIC de 2010.

# COMMAND ----------

# 6a. Diagnóstico de formato de data (rodado antes de escrever o parse, para não assumir máscara errada)
df_cambio.select("data", "cambio_usd_brl").show(5, truncate=False)
df_brent.select("data", "preco_brent_usd").show(5, truncate=False)

# COMMAND ----------

df_cambio_tipado = (df_cambio
    .withColumn("data", F.to_date(F.col("data"), "dd/MM/yyyy"))
    .withColumn("cambio_usd_brl", F.col("cambio_usd_brl").try_cast("double")))

df_brent_tipado = (df_brent
    .withColumn("data", F.to_date(F.col("data"), "yyyy-MM-dd"))
    .withColumn("preco_brent_usd", F.col("preco_brent_usd").try_cast("double")))

# 6b. Validação: o parse de data não deve gerar nenhum NULL
print(f"Câmbio - datas nulas após parse: {df_cambio_tipado.filter(F.col('data').isNull()).count()} de {df_cambio_tipado.count()}")
print(f"Brent - datas nulas após parse: {df_brent_tipado.filter(F.col('data').isNull()).count()} de {df_brent_tipado.count()}")

# COMMAND ----------

cambio_mensal = (df_cambio_tipado
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .groupBy("ano", "mes")
    .agg(F.avg("cambio_usd_brl").alias("cambio_usd_brl_medio")))

brent_mensal = (df_brent_tipado
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .groupBy("ano", "mes")
    .agg(F.avg("preco_brent_usd").alias("preco_brent_usd_medio")))

fact_economico = (cambio_mensal
    .join(brent_mensal, on=["ano", "mes"], how="outer")
    .withColumn("tempo_id", (F.col("ano") * 100 + F.col("mes")).cast("int"))
    .select("tempo_id", "cambio_usd_brl_medio", "preco_brent_usd_medio")
    .orderBy("tempo_id"))

# 6c. Validação: quantifica a lacuna esperada (meses só com uma das duas séries)
print(f"fact_economico: {fact_economico.count()} linhas")
qtd_lacuna = fact_economico.filter(F.col("cambio_usd_brl_medio").isNull() | F.col("preco_brent_usd_medio").isNull()).count()
print(f"Meses com alguma série faltando: {qtd_lacuna}")

# COMMAND ----------

# MAGIC %md ## 7. Gravação de todas as tabelas Gold

# COMMAND ----------

tabelas_gold = ["dim_campo", "dim_poco", "dim_tempo", "fact_producao_mensal", "fact_reservas_anual", "fact_economico"]

for tabela in tabelas_gold:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOGO}.{SCHEMA}.{tabela}")

dim_campo.write.mode("overwrite").saveAsTable(f"{CATALOGO}.{SCHEMA}.dim_campo")
dim_poco.write.mode("overwrite").saveAsTable(f"{CATALOGO}.{SCHEMA}.dim_poco")
dim_tempo.write.mode("overwrite").saveAsTable(f"{CATALOGO}.{SCHEMA}.dim_tempo")
fact_producao_mensal.write.mode("overwrite").saveAsTable(f"{CATALOGO}.{SCHEMA}.fact_producao_mensal")
fact_reservas_anual.write.mode("overwrite").saveAsTable(f"{CATALOGO}.{SCHEMA}.fact_reservas_anual")
fact_economico.write.mode("overwrite").saveAsTable(f"{CATALOGO}.{SCHEMA}.fact_economico")

for tabela in tabelas_gold:
    n = spark.table(f"{CATALOGO}.{SCHEMA}.{tabela}").count()
    print(f"{tabela}: {n} linhas")