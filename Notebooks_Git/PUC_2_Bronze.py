# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
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
import unicodedata
from functools import reduce
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


def detectar_encoding_arquivo(caminho: str) -> str:
    """Decide o encoding a passar pro spark.read (não reescreve o arquivo, só
    escolhe a option certa): tenta decodificar o CABEÇALHO (só a 1a linha) como
    UTF-8 (removendo BOM se houver); se falhar, assume ISO-8859-1. Necessário
    porque nem todo arquivo do BMP está no mesmo encoding - ex.:
    producao-mar-2016-2018.csv é Latin-1, mas a maioria é UTF-8.

    Por que só a 1a linha, e não uma amostra maior do arquivo: a primeira versão
    testava os primeiros 64KB, mas producao-terra-2005-1sem.csv tem cabeçalho em
    UTF-8 válido e um byte problemático mais adiante no arquivo (fora do
    cabeçalho) que quebrava a decodificação da amostra inteira - a função
    concluía (errado) que era Latin-1 e corrompia um cabeçalho que já estava
    certo (virava mojibake: 'MÃªs/Ano' em vez de 'Mês/Ano'). Testar só a 1a
    linha resolve porque é só o nome das colunas que essa função decide - o
    conteúdo do arquivo, linha a linha, o Spark já lê com o encoding escolhido."""
    caminho_local = caminho.replace("dbfs:", "")
    with open(caminho_local, "rb") as f:
        primeira_linha = f.readline()
    try:
        primeira_linha.decode("utf-8-sig")
        return "UTF-8"
    except UnicodeDecodeError:
        return "ISO-8859-1"


def chave_normalizada(nome: str) -> str:
    """Normaliza um nome de coluna para comparação: remove colchetes, decompõe
    acentos via NFKD e descarta os caracteres combinantes (funciona tanto para
    'ê' como um único code point quanto para 'e' + acento separado - a causa do
    bug em que "Mês/Ano" digitado à mão não batia com a coluna real), troca
    superescritos (³, ²) por dígito normal, e baixa a caixa. Usado para renomear
    BMP por NOME em vez de por POSIÇÃO - ver seção 3 para o porquê disso ser
    necessário (arquivos da ANP não têm ordem de coluna garantida)."""
    nome = nome.strip("[] \t")
    nome = unicodedata.normalize("NFKD", nome)
    nome = "".join(c for c in nome if not unicodedata.combining(c))
    nome = nome.replace("³", "3").replace("²", "2")
    return nome.lower().strip()


# Mapa fixo: toda variação conhecida de rótulo de coluna do BMP (com/sem colchete,
# com/sem acento problemático) -> nome final padronizado. Independente da ordem em
# que a coluna aparece no arquivo.
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
    """Renomeia as colunas do BMP por NOME (usando MAPA_COLUNAS_BMP), nunca por
    posição, e devolve sempre na mesma ordem (ORDEM_CANONICA_BMP).

    Por quê: descobrimos que 'producao_por_poco_terra_2025_4_trim.csv' tem a
    ordem física das colunas diferente do padrão histórico (Estado/Bacia/Campo/
    Poço/Ambiente/Instalação viram Campo/Bacia/Instalação/Poço/Estado/Ambiente
    nesse arquivo específico) - Estado e Ambiente coincidem por acaso na posição,
    mas Campo, Poço e Instalação não. Se esse arquivo fosse lido junto com outros
    num único spark.read.csv([...]) e só depois renomeado por posição (como era
    antes), os valores dessas colunas ficariam silenciosamente trocados - sem
    erro, sem aviso, só dado errado. Renomear por nome, arquivo a arquivo, antes
    de qualquer união, elimina essa classe inteira de bug."""
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
# MAGIC **Histórico de achados nesta fonte (do mais antigo ao mais recente):**
# MAGIC 1. Nomes de coluna com espaço/parênteses (`Produção de Óleo (m³)`) geravam
# MAGIC    `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES`.
# MAGIC 2. **~345 mil linhas (7% do total)**, concentradas em 15 arquivos (terra trimestral 2018-2021 e mar
# MAGIC    2019-2021), vinham malformadas: cada linha estava reencapsulada inteira entre aspas, com toda aspa
# MAGIC    interna duplicada - bug de exportação que trata uma linha já-CSV como um único campo de texto a
# MAGIC    escapar. Esses 15 arquivos também não têm encoding único entre si (Latin-1 e UTF-8+BOM misturados).
# MAGIC 3. O ano de 2024 nunca foi baixado pelo scraper original (nome de arquivo em padrão diferente:
# MAGIC    `producao_por_poco_2024.csv` + 4 trimestrais `producao-por-poco-terra-trim-*.csv`), e o cabeçalho
# MAGIC    desses vem com cada nome de coluna literalmente entre colchetes (`[Mês/Ano]`).
# MAGIC 4. **O mais grave:** o arquivo `producao_por_poco_terra_2025_4_trim.csv` tem a ORDEM FÍSICA das colunas
# MAGIC    diferente do padrão (`Estado,Bacia,Campo,Poço,Ambiente,Instalação` virou
# MAGIC    `Campo,Bacia,Instalação,Poço,Estado,Ambiente`). Como o Spark, ao ler vários CSVs de uma vez só
# MAGIC    (`spark.read.csv([lista])`), não alinha colunas pelo nome do cabeçalho entre arquivos diferentes -
# MAGIC    só por posição -, esse arquivo vinha silenciosamente embaralhando `Campo`/`Estado`/`Instalação` com
# MAGIC    os outros arquivos lidos junto. Sem erro, sem aviso, só dado errado (ex.: nome de estado aparecendo
# MAGIC    na coluna `Ambiente`).
# MAGIC
# MAGIC **Correção definitiva (achado 4 exige repensar a estratégia toda):** em vez de ler vários arquivos de
# MAGIC uma vez e confiar na posição das colunas, agora lemos **cada arquivo individualmente**, renomeamos suas
# MAGIC colunas **por nome** (função `padronizar_colunas_bmp`, usando o mapa fixo `MAPA_COLUNAS_BMP` definido na
# MAGIC seção 0) e só então unimos tudo com `unionByName`. Isso elimina de vez essa classe de bug, não só para o
# MAGIC arquivo de 2025 que já detectamos, mas para qualquer outro arquivo com ordem diferente que ainda não
# MAGIC tenha sido notado.

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