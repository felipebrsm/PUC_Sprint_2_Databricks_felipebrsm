# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # PUC_Sprint2_Silver
# MAGIC Consolidação da camada Silver a partir das tabelas Bronze / arquivos raw de BMP, BAR, BDEP e ECO (câmbio + Brent).
# MAGIC
# MAGIC Este notebook incorpora todas as correções descobertas durante a fase de diagnóstico (mojibake, formato
# MAGIC numérico BR vs. US, encapsulamento duplo de linhas, encoding misto por arquivo).

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
    """Nome de coluna sem acento, minúsculo, sem espaço/parêntese/barra."""
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
    for c in colunas:
        candidato = F.decode(F.encode(F.col(c), "ISO-8859-1"), "UTF-8")
        usar_candidato = F.col(c).rlike("[ÃÂ]") & ~candidato.contains("\uFFFD")
        df = df.withColumn(c, F.when(usar_candidato, candidato).otherwise(F.col(c)))
    return df

def numero_br_para_double(df, colunas):
    """Formato brasileiro: ponto = milhar, vírgula = decimal. Usado no BMP.
    Remove também aspas soltas/quebras de linha residuais (artefato de exportação)."""
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
# MAGIC **O que foi encontrado na etapa de diagnóstico:** ~345 mil linhas (7% do total) vinham malformadas -
# MAGIC concentradas em 15 arquivos específicos (terra trimestral 2018-2021 e mar 2019-2021). A causa raiz era
# MAGIC dupla: (1) cada linha desses arquivos estava reencapsulada inteira entre aspas, com toda aspa interna
# MAGIC duplicada (`"122,119"` virou `""122,119""`) - um bug clássico de exportação que trata a linha já-CSV como
# MAGIC se fosse um único campo de texto a escapar; e (2) esses 15 arquivos não têm um encoding único - alguns
# MAGIC são Latin-1, outros UTF-8 com BOM - então o encoding precisa ser detectado por arquivo, não fixado.
# MAGIC O restante do histórico (1941-2018 e 2022+) sempre leu corretamente, sem esse problema.

# COMMAND ----------

RAW_BMP = "/Volumes/PUC_Sprint_2/anp/raw/bmp"
PASTA_BMP_CORRIGIDO = "/Volumes/PUC_Sprint_2/anp/raw/bmp_corrigido"

# 1a. Identifica quais arquivos do raw têm alta taxa de malformação (coluna "ano" fora do padrão AAAA)
df_bmp_diagnostico = (spark.read
    .option("header", True)
    .csv(f"{RAW_BMP}/*.csv")
    .withColumn("arquivo_origem", F.col("_metadata.file_path")))

primeira_coluna = df_bmp_diagnostico.columns[0]

resumo_por_arquivo = (df_bmp_diagnostico
    .withColumn("malformado", ~F.col(primeira_coluna).rlike("^(19|20)[0-9]{2}$"))
    .groupBy("arquivo_origem")
    .agg(F.count("*").alias("total"), F.sum(F.col("malformado").cast("int")).alias("malformadas"))
    .withColumn("pct", F.round(F.col("malformadas") / F.col("total") * 100, 1)))

arquivos_problematicos = [r["arquivo_origem"] for r in resumo_por_arquivo.filter(F.col("pct") > 5).collect()]
arquivos_ok = [r["arquivo_origem"] for r in resumo_por_arquivo.filter(F.col("pct") <= 5).collect()]
print(f"{len(arquivos_problematicos)} arquivo(s) problemático(s), {len(arquivos_ok)} arquivo(s) ok")

# COMMAND ----------

# 1b. Corrige os arquivos problemáticos: detecta encoding por arquivo (UTF-8 com BOM ou Latin-1) e
# desfaz o encapsulamento duplo de aspas, linha a linha. Grava versão corrigida em subpasta separada,
# sem alterar os arquivos originais do raw.
os.makedirs(PASTA_BMP_CORRIGIDO, exist_ok=True)


def ler_texto_com_encoding_automatico(caminho: str) -> str:
    with open(caminho, "rb") as f:
        dados_brutos = f.read()
    try:
        return dados_brutos.decode("utf-8-sig")
    except UnicodeDecodeError:
        return dados_brutos.decode("ISO-8859-1")


def corrigir_linha(linha: str) -> str:
    linha = linha.rstrip("\r\n")
    if linha.startswith('"') and linha.endswith('"'):
        linha = linha[1:-1].replace('""', '"')
    return linha + "\n"


for caminho_original in arquivos_problematicos:
    caminho_local = caminho_original.replace("dbfs:", "")
    nome_arquivo = os.path.basename(caminho_local)
    destino = os.path.join(PASTA_BMP_CORRIGIDO, nome_arquivo)

    texto = ler_texto_com_encoding_automatico(caminho_local)
    with open(destino, "w", encoding="UTF-8") as f_out:
        for linha in texto.splitlines():
            f_out.write(corrigir_linha(linha))

print(f"{len(arquivos_problematicos)} arquivo(s) corrigido(s) em {PASTA_BMP_CORRIGIDO}")

# COMMAND ----------

# 1c. Lê os dois conjuntos (arquivos que já liam certo + arquivos corrigidos) e junta em um só DataFrame
df_bmp_ok = (spark.read
    .option("header", True)
    .csv(arquivos_ok))

df_bmp_corrigido = (spark.read
    .option("header", True)
    .csv(f"{PASTA_BMP_CORRIGIDO}/*.csv"))

df_bmp = df_bmp_ok.unionByName(df_bmp_corrigido, allowMissingColumns=True)

malformadas_final = df_bmp.filter(~F.col(df_bmp.columns[0]).rlike("^(19|20)[0-9]{2}$")).count()
print(f"Total após união: {df_bmp.count()} | ainda malformadas: {malformadas_final}")

# COMMAND ----------

# 1d. Padronização final: nomes de coluna, texto (upper/sem acento) e conversão numérica (formato BR)
nomes_bmp = [
    "ano", "mes_ano", "estado", "bacia", "campo", "poco", "ambiente", "instalacao",
    "producao_oleo_m3", "producao_condensado_m3", "producao_gas_associado_mm3",
    "producao_gas_nao_associado_mm3", "producao_agua_m3", "injecao_gas_mm3",
    "injecao_agua_recuperacao_secundaria_m3", "injecao_agua_descarte_m3",
    "injecao_gas_carbonico_mm3", "injecao_nitrogenio_mm3", "injecao_vapor_agua_t",
    "injecao_polimeros_m3", "injecao_outros_fluidos_m3",
]
df_bmp_silver = df_bmp.toDF(*nomes_bmp)

# remove qualquer linha residual malformada (deve ser ~0 depois da correção acima)
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