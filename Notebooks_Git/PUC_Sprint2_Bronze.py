# Databricks notebook source
# MAGIC %md
# MAGIC # PUC_Sprint2_Bronze
# MAGIC Ingestão das 4 fontes (BDEP, BAR, BMP, ECO) do volume `raw` para tabelas Delta Bronze.
# MAGIC
# MAGIC Aqui o dado entra no Unity Catalog o mais próximo possível do original. Só ajustamos o mínimo pro
# MAGIC Delta aceitar a tabela (nomes de coluna sem caractere proibido) e, no BMP, corrigimos um problema de
# MAGIC parsing que estava derrubando ~7% das linhas. Acento, caixa alta/baixa, tipo numérico e limpeza
# MAGIC "bonita" ficam pra Silver.

# COMMAND ----------

# MAGIC %md ## 0. Funções auxiliares

# COMMAND ----------

import os
import re
import unicodedata
from functools import reduce
from pyspark.sql import functions as F

CATALOGO = "PUC_Sprint_2"
SCHEMA = "anp"


def sanitizar_para_delta(df):
    """Troca só os caracteres que o Delta proíbe em nome de coluna (espaço, vírgula,
    ponto-e-vírgula, chaves, parênteses, colchetes, tab, quebra de linha, igual) por
    '_'. Mantém acento e maiúscula como veio do arquivo - deixar "bonito" é trabalho
    da Silver. Os colchetes entraram na lista depois que os arquivos de BMP de 2024
    apareceram com o nome de cada coluna literalmente entre colchetes (ex.:
    '[Mês/Ano]'), e não como um efeito visual do Databricks."""
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
    """Desfaz um bug de exportação: uma linha de CSV já válida foi reencapsulada
    inteira entre aspas, com toda aspa interna duplicada (ex.: '"122,119"' virou
    '""122,119""'). Detalhes na seção 3."""
    linha = linha.rstrip("\r\n")
    if linha.startswith('"') and linha.endswith('"'):
        linha = linha[1:-1].replace('""', '"')
    return linha + "\n"


def detectar_encoding_arquivo(caminho: str) -> str:
    """Escolhe o encoding a passar pro spark.read, testando só a 1ª linha
    (cabeçalho) como UTF-8; se falhar, assume ISO-8859-1. Precisa ser por arquivo
    porque nem todo BMP está no mesmo encoding (ex.: producao-mar-2016-2018.csv
    é Latin-1, a maioria é UTF-8).

    Testamos só a 1ª linha, não uma amostra maior, porque uma versão anterior
    testava os primeiros 64KB e se enganava com producao-terra-2005-1sem.csv:
    o cabeçalho é UTF-8 válido, mas um byte problemático mais adiante no arquivo
    quebrava a amostra inteira, levando a função a concluir (errado) Latin-1 e
    corromper um cabeçalho que já estava certo. Como essa função só decide o
    nome das colunas, basta olhar a linha do cabeçalho."""
    caminho_local = caminho.replace("dbfs:", "")
    with open(caminho_local, "rb") as f:
        primeira_linha = f.readline()
    try:
        primeira_linha.decode("utf-8-sig")
        return "UTF-8"
    except UnicodeDecodeError:
        return "ISO-8859-1"


def chave_normalizada(nome: str) -> str:
    """Normaliza um nome de coluna para comparação: remove colchetes, tira os
    acentos (via NFKD, funciona independente de como o acento foi codificado),
    troca ³/² por dígito normal e baixa a caixa. É a base pra renomear as colunas
    do BMP por nome em vez de por posição (seção 3 explica o porquê)."""
    nome = nome.strip("[] \t")
    nome = unicodedata.normalize("NFKD", nome)
    nome = "".join(c for c in nome if not unicodedata.combining(c))
    nome = nome.replace("³", "3").replace("²", "2")
    return nome.lower().strip()


# Mapa fixo com toda variação conhecida de nome de coluna do BMP -> nome padronizado,
# independente da ordem em que a coluna aparece no arquivo.
MAPA_COLUNAS_BMP = {
    "ano": "ano",
    "mes/ano": "mes_ano",
    "estado": "estado",
    "bacia": "bacia",
    "campo": "campo",
    "poco": "poco",
    "ambiente": "ambiente",
    "instalacao": "instalacao",
    "producao de oleo (m3)": "producao_oleo_m3",
    "producao de condensado (m3)": "producao_condensado_m3",
    "producao de gas associado (mm3)": "producao_gas_associado_mm3",
    "producao de gas nao associado (mm3)": "producao_gas_nao_associado_mm3",
    "producao de agua (m3)": "producao_agua_m3",
    "injecao de gas (mm3)": "injecao_gas_mm3",
    "injecao de agua para recuperacao secundaria (m3)": "injecao_agua_recuperacao_secundaria_m3",
    "injecao de agua para descarte (m3)": "injecao_agua_descarte_m3",
    "injecao de gas carbonico (mm3)": "injecao_gas_carbonico_mm3",
    "injecao de nitrogenio (mm3)": "injecao_nitrogenio_mm3",
    "injecao de vapor de agua (t)": "injecao_vapor_agua_t",
    "injecao de polimeros (m3)": "injecao_polimeros_m3",
    "injecao de outros fluidos (m3)": "injecao_outros_fluidos_m3",
}

ORDEM_CANONICA_BMP = list(dict.fromkeys(MAPA_COLUNAS_BMP.values()))


def padronizar_colunas_bmp(df, identificador_arquivo=""):
    """Renomeia as colunas do BMP por nome (usando MAPA_COLUNAS_BMP), nunca por
    posição, sempre devolvendo na mesma ordem (ORDEM_CANONICA_BMP).

    Isso existe porque um dos arquivos de 2025 tem a ordem física das colunas
    diferente do padrão histórico (Campo e Poço trocados de lugar, por exemplo).
    Se os arquivos fossem lidos juntos e renomeados por posição, como era antes,
    esses valores ficariam silenciosamente trocados - sem erro, só dado errado.
    Renomear por nome, arquivo a arquivo, elimina esse risco de vez."""
    mapa_renome = {}
    for coluna_original in df.columns:
        chave = chave_normalizada(coluna_original)
        if chave not in MAPA_COLUNAS_BMP:
            raise ValueError(
                f"Coluna do BMP não reconhecida em {identificador_arquivo!r}: "
                f"{coluna_original!r} (chave normalizada: {chave!r}). "
                f"Adicione essa variação ao MAPA_COLUNAS_BMP antes de prosseguir."
            )
        mapa_renome[coluna_original] = MAPA_COLUNAS_BMP[chave]

    for original, novo in mapa_renome.items():
        df = df.withColumnRenamed(original, novo)
    return df.select(*ORDEM_CANONICA_BMP)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. BDEP (Poços Perfurados Públicos)
# MAGIC
# MAGIC A leitura com `sep=","` padrão concatenou todas as colunas numa só - o arquivo usa `;`. Depois de
# MAGIC corrigir o separador, o texto veio com acentuação corrompida (`Ã‡`, `Ã©`), sinal de arquivo em
# MAGIC `ISO-8859-1` lido como UTF-8. Ajustamos os dois pontos direto na leitura.

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
# MAGIC Reaproveitamos por engano as options do BDEP (`;`/ISO-8859-1) e o arquivo virou bagunça. O BAR é, na
# MAGIC verdade, separado por vírgula e já em UTF-8 (exportado localmente via pandas a partir do xlsx
# MAGIC original). Corrigido isso, os nomes de coluna (`VOIP (bbl)`, `Campo/Área de desenvolvimento`, etc.)
# MAGIC ainda têm caracteres que o Delta rejeita, então passam pelo mesmo `sanitizar_para_delta` do BDEP.

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
# MAGIC Essa foi a fonte com mais problemas, encontrados em ordem: nomes de coluna com espaço/parênteses
# MAGIC quebravam a gravação em Delta; cerca de 345 mil linhas (7% do total, em 15 arquivos) vinham malformadas
# MAGIC porque a linha inteira tinha sido reencapsulada entre aspas por um bug de exportação; o ano de 2024
# MAGIC nunca foi baixado pelo scraper original, porque o nome dos arquivos seguia um padrão diferente; e o
# MAGIC mais sério, um arquivo de 2025 veio com a ORDEM das colunas trocada (Campo e Poço, por exemplo,
# MAGIC invertidos de posição). Como o Spark lê vários CSVs de uma vez alinhando por posição e não por nome do
# MAGIC cabeçalho, esse arquivo embaralhava valores silenciosamente ao ser lido junto com os outros - sem erro,
# MAGIC só dado errado.
# MAGIC
# MAGIC A correção definitiva foi mudar a estratégia: em vez de ler vários arquivos de uma vez confiando na
# MAGIC posição, agora lemos cada arquivo individualmente, renomeamos as colunas por nome
# MAGIC (`padronizar_colunas_bmp`, com o mapa `MAPA_COLUNAS_BMP` da seção 0) e só então unimos tudo com
# MAGIC `unionByName`. Isso resolve não só o arquivo de 2025 já identificado, mas qualquer outro com ordem
# MAGIC diferente que ainda não tenha aparecido.

# COMMAND ----------

RAW_BMP = f"/Volumes/{CATALOGO}/{SCHEMA}/raw/bmp"
RAW_BMP_2024 = f"/Volumes/{CATALOGO}/{SCHEMA}/raw/bmp-2024"
PASTA_BMP_CORRIGIDO = f"/Volumes/{CATALOGO}/{SCHEMA}/raw/bmp_corrigido"

arquivos_2024 = [
    f"{RAW_BMP_2024}/producao_por_poco_2024.csv",
    f"{RAW_BMP_2024}/producao-por-poco-terra-trim-1.csv",
    f"{RAW_BMP_2024}/producao-por-poco-terra-trim-2.csv",
    f"{RAW_BMP_2024}/producao-por-poco-terra-trim-3.csv",
    f"{RAW_BMP_2024}/producao_por_poco_terra_trim_4.csv",
]

# 3a. Diagnóstico: identifica quais arquivos têm alta taxa de malformação (coluna "ano" fora do padrão AAAA).
# Continua válido mesmo com a ordem de coluna variável, porque "Ano" está sempre na 1a posição em todo
# arquivo já visto - só a partir da 3a coluna em diante que a ordem pode variar.
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

# 3c. Lê CADA arquivo individualmente (histórico ok + histórico corrigido + os 5 de 2024), detectando o
# encoding arquivo a arquivo (achado: nem todo BMP é UTF-8, ex. producao-mar-2016-2018.csv é Latin-1) e
# padronizando colunas por NOME antes de unir - nunca em lote, pra não sofrer o embaralhamento por posição
# do achado 4. Os arquivos já corrigidos na 3b foram regravados em UTF-8 explicitamente, então a detecção
# vai confirmar "UTF-8" pra eles sem custo extra - não precisa de tratamento especial aqui.
caminhos_corrigidos = [
    f"{PASTA_BMP_CORRIGIDO}/{os.path.basename(c.replace('dbfs:', ''))}"
    for c in arquivos_problematicos
]
todos_os_caminhos_bmp = list(arquivos_ok) + caminhos_corrigidos + arquivos_2024

dfs_bmp_padronizados = []
for caminho in todos_os_caminhos_bmp:
    encoding = detectar_encoding_arquivo(caminho)
    df_arquivo = spark.read.option("header", True).option("encoding", encoding).csv(caminho)
    dfs_bmp_padronizados.append(padronizar_colunas_bmp(df_arquivo, caminho))

df_bmp_bronze = reduce(lambda a, b: a.unionByName(b), dfs_bmp_padronizados)

malformadas_final = df_bmp_bronze.filter(~F.col("ano").rlike("^(19|20)[0-9]{2}$")).count()
print(f"BMP total ({len(todos_os_caminhos_bmp)} arquivos lidos): {df_bmp_bronze.count()} linhas | "
      f"ainda malformadas: {malformadas_final}")
df_bmp_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ECO (Câmbio + Brent)
# MAGIC
# MAGIC Essas duas fontes vêm de API (BCB e EIA), já em UTF-8 padrão, sem os problemas de encoding/separador
# MAGIC da ANP. Passam pelo `sanitizar_para_delta` só por consistência com as demais tabelas.

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
# MAGIC Foi usada `DROP TABLE` antes de recriar, em vez de confiar só em overwrite: durante o desenvolvimento,
# MAGIC uma tabela chegou a reter metadado de schema de uma versão anterior e dava erro de coluna não
# MAGIC encontrada mesmo com o DataFrame novo correto. Apagar e recriar evita esse tipo de inconsistência.

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