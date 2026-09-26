# MVP - Análise de Produção de Petróleo e Gás (ANP)

Projeto de conclusão da disciplina de Engenharia de Dados (PUC - Pós-Graduação em Ciência de Dados), construído em Databricks com arquitetura medallion (Bronze / Silver / Gold) sobre dados públicos da ANP.

## Objetivo

Consolidar dados de produção de poços, reservas e indicadores econômicos (câmbio e Brent) num modelo de dados único, capaz de responder a 10 perguntas de negócio sobre a produção de óleo e gás no Brasil - da evolução histórica simples até análises cruzadas de correlação, declínio de campos e receita estimada.

## Fontes de dados

| Fonte | Conteúdo |
|---|---|
| BMP (Boletim Mensal de Produção) | Produção e injeção mensal por poço |
| BDEP (Poços Perfurados Públicos) | Cadastro e localização dos poços |
| BAR (Boletim Anual de Recursos e Reservas) | Reservas (VOIP/VGIP), volume acumulado e fração recuperada por campo |
| Câmbio (BCB) e Brent (EIA) | Séries diárias de câmbio USD/BRL e preço do petróleo |

Todos os dados foram baixados via scripts próprios (scraping/API) e armazenados em Volumes do Unity Catalog antes de entrar no pipeline.

## Arquitetura

- **Bronze**: ingestão dos arquivos raw com o mínimo de transformação, só o necessário para o Delta aceitar a tabela.
- **Silver**: limpeza de conteúdo - correção de encoding e mojibake, padronização de texto e conversão de formato numérico (BR vs. US).
- **Gold**: modelo estrela (fact constellation), com as dimensões `dim_campo`, `dim_poco` e `dim_tempo` compartilhadas entre três fatos - `fact_producao_mensal`, `fact_reservas_anual` e `fact_economico`.
- **Análise**: notebook com as 10 perguntas do projeto, cada uma com consulta em PySpark e visualização.

## Principais dificuldades encontradas

- **Dados de 2024 ausentes**: o scraper original não reconhecia o padrão de nome de arquivo usado pela ANP para esse ano.
- **Arquivos malformados**: cerca de 345 mil linhas do BMP vinham com a linha inteira reencapsulada entre aspas, um bug de exportação da própria fonte.
- **Ordem de colunas inconsistente**: um arquivo de 2025 tinha a ordem física das colunas diferente do padrão, o que causava troca silenciosa de valores quando os arquivos eram lidos em lote. Corrigido lendo e renomeando cada arquivo individualmente por nome, nunca por posição.
- **Encoding misto**: os arquivos alternam entre UTF-8 e Latin-1 sem padrão fixo, exigindo detecção arquivo a arquivo.
- **Chave duplicada em `dim_tempo`**: duas representações de texto diferentes para o mesmo mês (`"12/2025"` e `"dez/2025"`) geravam duas linhas para o mesmo período, inflando a produção de dezembro/2025 por contagem dupla no join.
- **Formato numérico**: BMP usa vírgula decimal (padrão brasileiro) e BAR usa ponto decimal (padrão americano), exigindo tratamento separado.

## Perguntas respondidas

1. Produção total de óleo e gás por ano
2. Top 10 campos por produção acumulada
3. Distribuição de poços por bacia
4. Correlação entre o preço do Brent e a produção, com defasagem de meses
5. Receita bruta estimada por campo e bacia (produção × Brent × câmbio)

## Principais resultados

- A produção de óleo e gás apresenta crescimento contínuo desde o início da série.
- Os campos de terra tem muitos poços com pouca produção, enquantos os campos de mar possuem menos poços com mais produçao.
- A correlação entre o preço do Brent e a produção de óleo é moderada a forte (~0,69), mas a defasagem de 3 meses não altera esse resultado de forma relevante - a relação provavelmente reflete mais uma tendência estrutural de crescimento conjunto do que uma resposta direta de curto prazo ao preço.
- A receita estimada é fortemente concentrada em poucos campos e bacias, coerente com a concentração de reservas do pré-sal.

## Estrutura dos notebooks

```
PUC_Sprint2_Bronze.py    → ingestão dos dados raw
PUC_Sprint2_Silver.py    → limpeza e padronização
PUC_Sprint2_Gold.py      → modelo estrela (dimensões e fatos)
PUC_2_Analise.py         → as 10 perguntas do projeto
```

## Tecnologias

Databricks (Unity Catalog, Delta Lake, PySpark)
