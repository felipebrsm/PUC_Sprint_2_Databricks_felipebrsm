# Databricks notebook source
# MAGIC %md
# MAGIC # PUC_Sprint2_Silver
# MAGIC Consolidação da camada Silver a partir das tabelas Bronze de BMP, BAR, BDEP e ECO (câmbio + Brent).
# MAGIC
# MAGIC Aqui entram as correções de conteúdo: mojibake, formato numérico (BR vs. US), tipagem e padronização
# MAGIC de texto.

# COMMAND ----------

# MAGIC %md ## 0. Funções auxiliares

# COMMAND ----------

import os
import re
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

ACCENTS_FROM = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
ACCENTS_TO   = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"


def remover_acentos(texto: str) -> str:
    return texto.translate(str.maketrans(ACCENTS_FROM, ACCENTS_TO))


def limpar_nome_generico(nome: str) -> str:
    """Deixa o nome de coluna sem acento, minúsculo e sem espaço/parêntese/barra."""
    nome = remover_acentos(nome.strip().lower())
    nome = re.sub(r"\(.*?\)", "", nome)
    nome = re.sub(r"[ ,;{}()\n\t=/-]+", "_", nome)
    return nome.strip("_")


def padronizar_texto(df, colunas=None):
    """Upper + trim + remove acento nas colunas de texto indicadas (ou todas as string)."""
    alvo = colunas or [c for c, t in df.dtypes if t == "string"]
    for c in alvo:
        df = df.withColumn(c, F.upper(F.trim(F.translate(F.col(c), ACCENTS_FROM, ACCENTS_TO))))
    return df


def corrigir_mojibake_colunas(df, colunas):
    """Corrige mojibake testando o RESULTADO da correção, não a entrada. Um 'Ã'
    seguido de letra pode ser mojibake real ('PetrÃ³leo') ou uma letra portuguesa
    legítima ('GaviÃO'), e olhar só a entrada não diferencia os dois casos - foi
    o que uma versão anterior fazia, e corrompia palavras corretas. Por isso só
    aplicamos a correção quando reconverter o texto NÃO gera um caractere de
    erro '�'."""
    for c in colunas:
        candidato = F.decode(F.encode(F.col(c), "ISO-8859-1"), "UTF-8")
        usar_candidato = F.col(c).rlike("[ÃÂ]") & ~candidato.contains("\uFFFD")
        df = df.withColumn(c, F.when(usar_candidato, candidato).otherwise(F.col(c)))
    return df


def numero_br_para_double(df, colunas):
    """Converte número em formato brasileiro (ponto = milhar, vírgula = decimal),
    usado no BMP. Remove também aspas soltas e quebras de linha residuais."""
    for c in colunas:
        valor = F.col(c).cast("string")
        valor = F.regexp_replace(valor, r'["\n\r]', "")
        valor = F.trim(valor)
        valor = F.regexp_replace(valor, r"\.", "")
        valor = F.regexp_replace(valor, ",", ".")
        df = df.withColumn(c, valor.try_cast(DoubleType()))
    return df


def numero_us_para_double(df, colunas):
    """Formato americano: vírgula = milhar, ponto = decimal. Usado no BAR."""
    for c in colunas:
        valor = F.col(c).cast("string")
        valor = F.regexp_replace(valor, r'["\n\r]', "")
        valor = F.trim(valor)
        valor = F.regexp_replace(valor, ",", "")
        df = df.withColumn(c, valor.try_cast(DoubleType()))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. BMP (Boletim Mensal de Produção)
# MAGIC
# MAGIC **Ajuste de arquitetura:** esta seção lia o `raw/bmp` direto e reimplementava (de novo) a correção de
# MAGIC encapsulamento duplo/encoding que já tinha sido resolvida na Bronze - duas cópias da mesma lógica.
# MAGIC Na prática isso mordeu a gente duas vezes: (1) quando o BMP de 2024 foi incorporado só na Bronze, a
# MAGIC Silver continuou cega pra ele, e (2) um arquivo de 2025 (`producao_por_poco_terra_2025_4_trim.csv`)
# MAGIC tinha a ORDEM das colunas diferente do padrão - a Bronze foi reescrita pra renomear por nome (não mais
# MAGIC por posição, ver `padronizar_colunas_bmp`/`MAPA_COLUNAS_BMP` no notebook Bronze) exatamente por causa
# MAGIC disso. Corrigido lendo `bronze_bmp` direto em vez de reprocessar o raw - a tabela já sai com nomes de
# MAGIC coluna padronizados (`ano`, `mes_ano`, `estado`, ...), então nem precisamos mais renomear por posição
# MAGIC aqui. Daqui pra frente, qualquer fonte nova só precisa ser incorporada na Bronze - a Silver herda
# MAGIC automaticamente, sem risco de ficar com uma cópia desatualizada da lógica de ingestão.

# COMMAND ----------

df_bmp_silver = spark.table("PUC_Sprint_2.anp.bronze_bmp")

# Confere que a Bronze não deixou passar nenhuma linha malformada (coluna "ano" fora do padrão AAAA)
malformadas_final = df_bmp_silver.filter(~F.col("ano").rlike("^(19|20)[0-9]{2}$")).count()
print(f"Linhas na bronze_bmp: {df_bmp_silver.count()} | malformadas: {malformadas_final}")

# remove qualquer linha residual malformada (deve ser ~0) e tipa o ano
df_bmp_silver = df_bmp_silver.filter(F.col("ano").rlike("^(19|20)[0-9]{2}$"))
df_bmp_silver = df_bmp_silver.withColumn("ano", F.col("ano").try_cast(IntegerType()))

df_bmp_silver = padronizar_texto(df_bmp_silver, ["estado", "bacia", "campo", "poco", "ambiente", "instalacao"])

colunas_numericas_bmp = [c for c in df_bmp_silver.columns if c.startswith("producao_") or c.startswith("injecao_")]
df_bmp_silver = numero_br_para_double(df_bmp_silver, colunas_numericas_bmp)

df_bmp_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. BAR (Boletim Anual de Recursos e Reservas)
# MAGIC
# MAGIC **O que foi encontrado:** o arquivo tem uma linha de rodapé ("Filtros aplicados: ...") que precisa ser
# MAGIC descartada antes de qualquer cast. O texto (campo/bacia/estado/situação) veio com mojibake - precisa
# MAGIC ser decodificado como se fosse ISO-8859-1 lido incorretamente. Os números vêm em **formato americano**
# MAGIC (vírgula = milhar, ponto = decimal) - o oposto do BMP. A fração recuperada vem como percentual com
# MAGIC símbolo `%` (ex.: "3.79%"), tratada separadamente e armazenada como fração decimal (0.0379).

# COMMAND ----------

df_bar = spark.table("PUC_Sprint_2.anp.bronze_bar")

nomes_bar = [
    "ano", "campo", "bacia", "estado", "voip_bbl", "vgip_m3",
    "petroleo_acumulado_bbl", "gas_natural_acumulado_m3",
    "fracao_recuperada_petroleo", "situacao",
]
df_bar_silver = df_bar.toDF(*nomes_bar)

# remove linha(s) de rodapé/metadado que não são dado de verdade
df_bar_silver = df_bar_silver.filter(F.col("ano").rlike("^[0-9]{4}$"))

colunas_texto_bar = ["campo", "bacia", "estado", "situacao"]
df_bar_silver = corrigir_mojibake_colunas(df_bar_silver, colunas_texto_bar)
df_bar_silver = padronizar_texto(df_bar_silver, colunas_texto_bar)

colunas_numericas_bar = ["voip_bbl", "vgip_m3", "petroleo_acumulado_bbl", "gas_natural_acumulado_m3"]
df_bar_silver = numero_us_para_double(df_bar_silver, colunas_numericas_bar)

valor_fracao = F.regexp_replace(F.col("fracao_recuperada_petroleo").cast("string"), "%", "")
df_bar_silver = df_bar_silver.withColumn(
    "fracao_recuperada_petroleo", (valor_fracao.try_cast(DoubleType()) / 100)
)

df_bar_silver = df_bar_silver.withColumn("ano", F.col("ano").try_cast(IntegerType()))
df_bar_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. BDEP (Poços Perfurados Públicos)
# MAGIC
# MAGIC **O que foi encontrado:** o arquivo usa `;` como separador e encoding `ISO-8859-1` (diferente do BAR,
# MAGIC que é `,`/UTF-8) - já corrigido na leitura da Bronze. Aqui só falta limpar os nomes de coluna
# MAGIC (maiúsculas, sem acento) para ficar no mesmo padrão das demais tabelas Silver; o conteúdo já é
# MAGIC majoritariamente texto/código, sem conversão numérica necessária nesta etapa.

# COMMAND ----------

df_bdep = spark.table("PUC_Sprint_2.anp.bronze_bdep")
df_bdep_silver = df_bdep.toDF(*[limpar_nome_generico(c) for c in df_bdep.columns])
df_bdep_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ECO (Câmbio + Brent)
# MAGIC
# MAGIC **O que foi encontrado:** as duas fontes vêm de API (BCB e EIA), não de exportação da ANP, e por isso
# MAGIC não apresentaram nenhum dos problemas de encoding/formatação numérica vistos acima - só foi necessário
# MAGIC renomear colunas para nomes mais descritivos.

# COMMAND ----------

df_cambio = spark.table("PUC_Sprint_2.anp.bronze_cambio")
df_brent = spark.table("PUC_Sprint_2.anp.bronze_brent")

df_cambio_silver = df_cambio.withColumnRenamed("valor", "cambio_usd_brl")

df_brent_silver = (df_brent
    .toDF(*[limpar_nome_generico(c) for c in df_brent.columns])
    .withColumnRenamed("period", "data")
    .withColumnRenamed("value", "preco_brent_usd"))

df_cambio_silver.printSchema()
df_brent_silver.printSchema()

# COMMAND ----------

# MAGIC %md ## 5. Validação de qualidade (nulos gerados pela conversão numérica)

# COMMAND ----------

print("--- BAR ---")
for c in colunas_numericas_bar + ["fracao_recuperada_petroleo", "ano"]:
    print(f"{c}: {df_bar_silver.filter(F.col(c).isNull()).count()} nulos")

print("\n--- BMP ---")
for c in colunas_numericas_bmp + ["ano"]:
    print(f"{c}: {df_bmp_silver.filter(F.col(c).isNull()).count()} nulos")

# COMMAND ----------

# MAGIC %md ## 6. Gravação das tabelas Silver

# COMMAND ----------

for tabela in ["silver_bmp", "silver_bar", "silver_bdep", "silver_cambio", "silver_brent"]:
    spark.sql(f"DROP TABLE IF EXISTS PUC_Sprint_2.anp.{tabela}")

df_bmp_silver.write.mode("overwrite").saveAsTable("PUC_Sprint_2.anp.silver_bmp")
df_bar_silver.write.mode("overwrite").saveAsTable("PUC_Sprint_2.anp.silver_bar")
df_bdep_silver.write.mode("overwrite").saveAsTable("PUC_Sprint_2.anp.silver_bdep")
df_cambio_silver.write.mode("overwrite").saveAsTable("PUC_Sprint_2.anp.silver_cambio")
df_brent_silver.write.mode("overwrite").saveAsTable("PUC_Sprint_2.anp.silver_brent")

for tabela in ["silver_bmp", "silver_bar", "silver_bdep", "silver_cambio", "silver_brent"]:
    existe = spark.catalog.tableExists(f"PUC_Sprint_2.anp.{tabela}")
    print(f"{tabela}: {'existe' if existe else 'AINDA NÃO EXISTE'}")