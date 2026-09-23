# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # PUC_Sprint2_Bronze
# MAGIC Ingestão das 4 fontes (BDEP, BAR, BMP, ECO) do volume `raw` para tabelas Delta Bronze.
# MAGIC
# MAGIC Papel da Bronze: trazer o dado para dentro do Unity Catalog preservando-o o mais próximo possível do
# MAGIC original - a única transformação aceita aqui é o mínimo necessário para o Delta aceitar a tabela
# MAGIC (nome de coluna sem caractere proibido) e, no caso do BMP, corrigir uma falha de parsing que impedia
# MAGIC a leitura correta de ~7% das linhas. Acento, caixa alta/baixa, tipo numérico e limpeza "bonita" ficam
# MAGIC para a Silver.

# COMMAND ----------

# MAGIC %md ## 0. Funções auxiliares

# COMMAND ----------

import os
import re
from pyspark.sql import functions as F

CATALOGO = "PUC_Sprint_2"
SCHEMA = "anp"


def sanitizar_para_delta(df):
    """O mínimo necessário para o Delta aceitar a tabela: troca só os caracteres
    proibidos (espaço, vírgula, ponto-e-vírgula, chaves, parênteses, colchetes, tab,
    quebra de linha, igual) por '_'. Mantém acento, maiúscula, tudo o resto como veio
    do arquivo original - deixar "bonito" é trabalho da Silver, não da Bronze.
    Colchetes '[]' foram adicionados depois de descobrir que o arquivo de BMP de 2024
    (producao_por_poco_2024.csv e os 4 trimestrais de terra) vem com o nome de cada
    coluna literalmente entre colchetes, ex.: '[Mês/Ano]' - não é um jeito de exibição,
    é a string real da coluna, então sem esse ajuste ela sobrevivia à sanitização."""
    novos_nomes = [re.sub(r"[ ,;{}()\[\]\n\t=/]+", "_", c).strip("_") for c in df.columns]
    return df.toDF(*novos_nomes)


def ler_texto_com_encoding_automatico(caminho: str) -> str:
    """Tenta UTF-8 (removendo BOM se existir); se falhar, assume Latin-1."""
    with open(caminho, "rb") as f:
        dados_brutos = f.read()
    try:
        return dados_brutos.decode("utf-8-sig")
    except UnicodeDecodeError:
        return dados_brutos.decode("ISO-8859-1")


def desfazer_encapsulamento_duplo(linha: str) -> str:
    """Desfaz o bug de exportação em que uma linha de CSV já válida foi
    reencapsulada inteira entre aspas, com toda aspa interna duplicada
    (ex.: '"122,119"' virou '""122,119""'). Ver seção 3 para o contexto completo."""
    linha = linha.rstrip("\r\n")
    if linha.startswith('"') and linha.endswith('"'):
        linha = linha[1:-1].replace('""', '"')
    return linha + "\n"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. BDEP (Poços Perfurados Públicos)
# MAGIC
# MAGIC **O que foi encontrado:** a primeira leitura (com `sep=","` padrão) concatenou todas as colunas em
# MAGIC uma só - o arquivo usa `;` como separador, não vírgula. Depois de corrigir o separador, os valores de
# MAGIC texto vieram com acentuação corrompida (`Ã‡`, `Ã©`) - sinal de arquivo em `ISO-8859-1` lido como UTF-8.

# COMMAND ----------

RAW_BDEP = f"/Volumes/{CATALOGO}/{SCHEMA}/raw/bdep"

df_bdep = (spark.read
    .option("header", True)
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .csv(f"{RAW_BDEP}/*.csv"))

df_bdep_bronze = sanitizar_para_delta(df_bdep)
df_bdep_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. BAR (Boletim Anual de Recursos e Reservas)
# MAGIC
# MAGIC **O que foi encontrado:** ao reaproveitar por engano as options do BDEP (`sep=";"`,
# MAGIC `encoding="ISO-8859-1"`) o arquivo do BAR virou uma bagunça - ele é, na verdade, separado por vírgula
# MAGIC e já está em UTF-8 (foi exportado localmente via pandas a partir do xlsx original). Corrigido, os nomes
# MAGIC de coluna originais (`VOIP (bbl)`, `Campo/Área de desenvolvimento`, etc.) ainda têm caracteres que o
# MAGIC Delta rejeita - por isso passam pelo mesmo `sanitizar_para_delta` do BDEP.

# COMMAND ----------

RAW_BAR = f"/Volumes/{CATALOGO}/{SCHEMA}/raw/bar"

df_bar = (spark.read
    .option("header", True)
    .option("sep", ",")
    .option("encoding", "UTF-8")
    .csv(f"{RAW_BAR}/*.csv"))

df_bar_bronze = sanitizar_para_delta(df_bar)
df_bar_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. BMP (Boletim Mensal de Produção)
# MAGIC
# MAGIC **O que foi encontrado:** nomes de coluna com espaço/parênteses (`Produção de Óleo (m³)`) geravam o
# MAGIC erro `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES` - resolvido com `sanitizar_para_delta`, igual às
# MAGIC outras fontes. Só que, ao investigar por que a contagem de linhas parecia baixa, foi descoberto algo
# MAGIC mais sério: **~345 mil linhas (7% do total)**, concentradas em 15 arquivos específicos (terra
# MAGIC trimestral 2018-2021 e mar 2019-2021), vinham malformadas. A causa: cada linha desses arquivos foi
# MAGIC reencapsulada inteira entre aspas, com toda aspa interna duplicada - um bug de exportação que trata
# MAGIC uma linha já-CSV como se fosse um único campo de texto a escapar. Além disso, esses 15 arquivos não
# MAGIC têm um encoding único entre si (alguns Latin-1, alguns UTF-8 com BOM), então a correção detecta o
# MAGIC encoding arquivo a arquivo antes de desfazer o encapsulamento.

# COMMAND ----------

RAW_BMP = f"/Volumes/{CATALOGO}/{SCHEMA}/raw/bmp"
PASTA_BMP_CORRIGIDO = f"/Volumes/{CATALOGO}/{SCHEMA}/raw/bmp_corrigido"

# 3a. Diagnóstico: identifica quais arquivos têm alta taxa de malformação (coluna "ano" fora do padrão AAAA)
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
print(f"{len(arquivos_problematicos)} arquivo(s) problemático(s) de {len(arquivos_problematicos) + len(arquivos_ok)} total")

# COMMAND ----------

# 3b. Corrige os arquivos problemáticos: detecta encoding por arquivo e desfaz o encapsulamento duplo,
# linha a linha. Grava a versão corrigida em subpasta separada, sem alterar os arquivos originais do raw.
os.makedirs(PASTA_BMP_CORRIGIDO, exist_ok=True)

for caminho_original in arquivos_problematicos:
    caminho_local = caminho_original.replace("dbfs:", "")
    nome_arquivo = os.path.basename(caminho_local)
    destino = os.path.join(PASTA_BMP_CORRIGIDO, nome_arquivo)

    texto = ler_texto_com_encoding_automatico(caminho_local)
    with open(destino, "w", encoding="UTF-8") as f_out:
        for linha in texto.splitlines():
            f_out.write(desfazer_encapsulamento_duplo(linha))

print(f"{len(arquivos_problematicos)} arquivo(s) corrigido(s) em {PASTA_BMP_CORRIGIDO}")

# COMMAND ----------

# 3c. Lê os dois conjuntos (arquivos que já liam certo + arquivos corrigidos) e junta em um só DataFrame
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

# MAGIC %md
# MAGIC ### 3d. BMP 2024 - fonte separada, nomenclatura diferente
# MAGIC
# MAGIC **O que foi encontrado:** a página de dados abertos não tinha, no scrape original, nenhum arquivo de
# MAGIC 2024 no padrão `producao-terra-*`/`producao-mar-*`. O ano existe, só que publicado com outro nome de
# MAGIC arquivo: `producao_por_poco_2024.csv` (ambiente MAR, ano inteiro, 15229 linhas) e 4 arquivos
# MAGIC trimestrais `producao-por-poco-terra-trim-{1..4}.csv` (ambiente TERRA, um trimestre cada). Validado que
# MAGIC os 5 juntos cobrem jan-dez/2024 sem sobrepor nenhum mês (cada trimestral tem exatamente 3 meses
# MAGIC distintos, o anual tem os 12). Além disso, o cabeçalho desses 5 arquivos vem com cada nome de coluna
# MAGIC literalmente entre colchetes (`[Mês/Ano]`, `[Ano]`, ...) - por isso o `sanitizar_para_delta` acima
# MAGIC precisou aprender a remover `[` e `]`. Sanitizamos cada fonte (2024 e o restante do BMP) separadamente
# MAGIC antes de unir, porque só depois da sanitização os nomes ficam iguais o suficiente pra bater
# MAGIC (`[Mês/Ano]` -> `Mês_Ano` == `Mês/Ano` -> `Mês_Ano`). Esse arquivo também trouxe duas colunas que os
# MAGIC anos anteriores não tinham (`Ambiente`, `Instalação`) - ficam como novo campo, nulo nas linhas antigas,
# MAGIC via `allowMissingColumns=True`.

# COMMAND ----------

RAW_BMP_2024 = f"/Volumes/{CATALOGO}/{SCHEMA}/raw/bmp-2024"

arquivos_2024 = [
    "producao_por_poco_2024.csv",
    "producao-por-poco-terra-trim-1.csv",
    "producao-por-poco-terra-trim-2.csv",
    "producao-por-poco-terra-trim-3.csv",
    "producao_por_poco_terra_trim_4.csv",
]

df_bmp_2024 = (spark.read
    .option("header", True)
    .csv([f"{RAW_BMP_2024}/{nome}" for nome in arquivos_2024]))

# Validação: os 12 meses presentes, sem duplicidade entre os 5 arquivos
meses_2024 = [r[0] for r in df_bmp_2024.select("[Mês/Ano]").distinct().orderBy("[Mês/Ano]").collect()]
print(f"BMP 2024: {df_bmp_2024.count()} linhas | {len(meses_2024)} meses distintos: {meses_2024}")

# Sanitiza cada fonte separadamente e só então une - ver explicação acima
df_bmp_bronze_historico = sanitizar_para_delta(df_bmp)
df_bmp_bronze_2024 = sanitizar_para_delta(df_bmp_2024)

df_bmp_bronze = df_bmp_bronze_historico.unionByName(df_bmp_bronze_2024, allowMissingColumns=True)
print(f"BMP total (histórico + 2024): {df_bmp_bronze.count()} linhas")
df_bmp_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ECO (Câmbio + Brent)
# MAGIC
# MAGIC **O que foi encontrado:** as duas fontes vêm de API (BCB e EIA), baixadas localmente e já em UTF-8
# MAGIC padrão, sem os problemas de encoding/separador da ANP - só passam pelo `sanitizar_para_delta` por
# MAGIC consistência com as demais tabelas (nenhum dos nomes de coluna dessas fontes tem caractere proibido,
# MAGIC mas mantemos o passo para garantir robustez caso a API mude o formato no futuro).

# COMMAND ----------

RAW_ECO = f"/Volumes/{CATALOGO}/{SCHEMA}/raw/economico"

df_cambio = (spark.read
    .option("header", True)
    .csv(f"{RAW_ECO}/cambio_usd_brl.csv"))

df_brent = (spark.read
    .option("header", True)
    .csv(f"{RAW_ECO}/brent_spot.csv"))

df_cambio_bronze = sanitizar_para_delta(df_cambio)
df_brent_bronze = sanitizar_para_delta(df_brent)

df_cambio_bronze.printSchema()
df_brent_bronze.printSchema()

# COMMAND ----------

# MAGIC %md ## 5. Gravação das tabelas Bronze
# MAGIC
# MAGIC Usamos `DROP TABLE` antes de recriar, em vez de confiar só em `overwrite`/`overwriteSchema` -
# MAGIC durante o desenvolvimento iterativo deste pipeline, uma tabela chegou a reter metadado de schema
# MAGIC de versões anteriores (nomes de coluna antigos), causando `DELTA_COLUMN_NOT_FOUND_IN_SCHEMA` mesmo
# MAGIC com o DataFrame novo correto. Apagar e recriar elimina esse tipo de inconsistência.

# COMMAND ----------

for tabela in ["bronze_bmp", "bronze_bar", "bronze_bdep", "bronze_cambio", "bronze_brent"]:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOGO}.{SCHEMA}.{tabela}")

df_bmp_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOGO}.{SCHEMA}.bronze_bmp")
df_bar_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOGO}.{SCHEMA}.bronze_bar")
df_bdep_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOGO}.{SCHEMA}.bronze_bdep")
df_cambio_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOGO}.{SCHEMA}.bronze_cambio")
df_brent_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOGO}.{SCHEMA}.bronze_brent")

for tabela in ["bronze_bmp", "bronze_bar", "bronze_bdep", "bronze_cambio", "bronze_brent"]:
    existe = spark.catalog.tableExists(f"{CATALOGO}.{SCHEMA}.{tabela}")
    print(f"{tabela}: {'existe' if existe else 'AINDA NÃO EXISTE'}")