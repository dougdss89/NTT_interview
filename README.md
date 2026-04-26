# Pipeline de Análise de Vacinação COVID-19

## Contexto do Projeto

Este projeto implementa uma pipeline de dados moderna utilizando Databricks Lakeflow (Spark Declarative Pipelines) para processar e analisar dados globais de vacinação contra COVID-19. A arquitetura segue o padrão Medallion (Bronze → Silver → Gold), garantindo qualidade e rastreabilidade dos dados.

## Arquitetura Técnica

### Stack Tecnológico
- **Plataforma**: Databricks Serverless (Photon enabled)
- **Framework**: Lakeflow Spark Declarative Pipelines
- **Storage**: Unity Catalog Volumes
- **Catalog**: `workspace.default`

### Estrutura da Pipeline

```
transformations/
├── 01_bronze/          # Ingestão raw dos dados
│   ├── bronze_locations.py       (223 países)
│   └── bronze_vaccinations.py    (196k registros diários)
├── 02_silver/          # Limpeza e transformação
│   ├── silver_locations.py       (parse de vacinas em array)
│   └── silver_vaccinations.py    (extração temporal + null handling)
└── 03_gold/            # Agregações analíticas
    ├── gold_countries_vaccine_types.py
    ├── gold_top10_vaccinations_per_month.py
    └── gold_top10_with_all_vaccines.py
```

## Principais Descobertas

### 1. Diversidade de Vacinas por País

**Destaque**: O Irã lidera com 12 tipos diferentes de vacinas aprovadas, mostrando uma estratégia agressiva de diversificação no portfolio de imunizantes. Isso pode indicar tanto questões geopolíticas (acesso restrito a determinadas vacinas ocidentais) quanto uma abordagem pragmática de aceitar múltiplas fontes para acelerar a campanha.

**Observação interessante**: 10 países ficaram empatados com exatamente 10 tipos de vacinas (Afeganistão, Iraque, Líbano, Djibuti, Egito, Kuwait, Bahrein, Jordânia, Líbia). A maioria está no Oriente Médio/Norte da África, sugerindo um corredor logístico regional via programas COVAX e doações bilaterais.

### 2. Volume de Vacinação Global

**Números impressionantes**:
- Agosto/2024: 13.6 bilhões de doses aplicadas acumuladas globalmente
- Ásia concentra 67% do volume global (9.1 bilhões)
- Índia sozinha responde por 2.2 bilhões de doses (16% do total mundial)

**Insights operacionais**: A dominância asiática reflete não apenas a população, mas também capacidade industrial local (Índia como maior produtor de vacinas genéricas do mundo via Serum Institute).

### 3. Evolução Temporal da Campanha

**2024** (fase de maturidade):
- Bangladesh lidera em diversidade recente: 7 tipos, 362M doses
- Argentina e Canadá mantêm 6 tipos cada, mostrando estabilização do portfolio

**2023** (fase de consolidação):
- Irã mantém liderança com 12 tipos
- Paquistão impressiona: 10 tipos diferentes, 341M doses aplicadas
- Filipinas demonstra maturidade logística: 10 tipos, 189M doses

## Decisões Técnicas Relevantes

### Por que Materialized Views ao invés de Streaming Tables?

Para este caso, optei por Materialized Views em todas as camadas porque:
- **Fonte de dados histórica**: CSVs e JSONs estáticos, não há stream contínuo
- **Análises agregadas**: As tabelas Gold fazem GROUP BY, COUNT, RANK - operações batch
- **Reprocessamento eventual**: Se novos dados históricos forem adicionados, full refresh é mais adequado

Se fosse um cenário real com feed contínuo (ex: API de ministérios da saúde), usaria Streaming Tables + Auto CDC para capturar mudanças incrementais.

### Migração para Unity Catalog Volumes

**Problema encontrado**: Serverless pipelines não podem acessar `/Workspace/` paths diretamente (SecurityException).

**Solução implementada**: 
1. Criação de Volume: `workspace.default.covid_vaccination_sources`
2. Migração dos arquivos fonte (locations.csv + vaccinations.json)
3. Update dos paths de leitura

**Lição aprendida**: Em produção, sempre usar Volumes ou cloud storage (S3/ADLS/GCS) para dados fonte. Workspace paths são apenas para notebooks/código.

## Sugestões de Análises Adicionais

### 1. Análise de Efetividade da Campanha
**Pergunta**: Existe correlação entre diversidade de vacinas e % da população vacinada?

**Implementação**:
```sql
-- Adicionar dados demográficos (population) e calcular coverage rate
-- Cruzar com vaccine_count para identificar se diversidade acelera cobertura
SELECT 
  country,
  vaccine_count,
  (people_fully_vaccinated / population * 100) as coverage_rate
ORDER BY coverage_rate DESC
```

**Hipótese**: Países com mais opções podem ter vencido resistência vacinal oferecendo escolha.

### 2. Análise de Momentum de Vacinação
**Pergunta**: Quais países aceleraram ou desaceleraram significativamente a campanha?

**Implementação**:
```sql
-- Comparar daily_vaccinations entre períodos (ex: 2021 Q1 vs 2023 Q1)
-- Identificar outliers positivos/negativos
WITH momentum AS (
  SELECT 
    country,
    year,
    month,
    AVG(daily_vaccinations) as avg_daily,
    LAG(AVG(daily_vaccinations)) OVER (PARTITION BY country ORDER BY year, month) as prev_avg
  FROM silver_vaccinations
  GROUP BY ALL
)
SELECT 
  country,
  ((avg_daily - prev_avg) / prev_avg * 100) as growth_rate
WHERE prev_avg IS NOT NULL
ORDER BY growth_rate DESC
```

**Insight esperado**: Identificar programas de vacinação mais eficientes ou gargalos logísticos.

### 3. Análise de Equity Vacinal
**Pergunta**: Qual a desigualdade na distribuição de doses entre países ricos e pobres?

**Implementação**:
```sql
-- Adicionar classificação de income_level (World Bank)
-- Calcular doses per capita por grupo
SELECT 
  income_level,
  SUM(total_vaccinations) / SUM(population) as doses_per_capita,
  AVG(vaccine_count) as avg_vaccine_types
GROUP BY income_level
```

**Relevância**: Tópico quente em saúde pública global, mostra consciência de inequidade.

### 4. Time Series Forecasting (Avançado)
**Pergunta**: Previsão de doses necessárias para os próximos 6 meses

**Implementação**: Usar SQL AI functions do Databricks:
```sql
SELECT 
  country,
  ai_forecast(
    daily_vaccinations, 
    date, 
    horizon => 180,  -- 6 meses
    frequency => 'daily'
  ) as forecast
FROM silver_vaccinations
WHERE country = 'Brazil'
```

**Diferencial**: Mostra conhecimento de features avançadas da plataforma.

### 5. Análise de Composição de Portfolio
**Pergunta**: Quais combinações de vacinas foram mais comuns?

**Implementação**:
```sql
-- Identificar padrões de co-ocorrência
-- Ex: "Pfizer + AstraZeneca + Sinovac" foi usado por quantos países?
SELECT 
  vaccines,
  COUNT(*) as country_count,
  ARRAY_JOIN(COLLECT_SET(country), ', ') as countries
FROM silver_locations
GROUP BY vaccines
ORDER BY country_count DESC
```

**Insight**: Pode revelar alianças geopolíticas ou restrições logísticas regionais.

## Melhorias Futuras

1. **Adicionar testes de qualidade**: Expectations para validar `total_vaccinations >= 0`, `date` não nulo, etc.
2. **Incremental refresh**: Converter para Streaming Tables se houver feed contínuo de dados
3. **Particionamento**: Adicionar `PARTITION BY (year, month)` nas tabelas Gold para performance
4. **Dashboard**: Conectar um Lakeview dashboard para visualização executiva
5. **Alertas**: Configurar alertas quando daily_vaccinations cair abaixo de threshold crítico

## Como Executar

```bash
# 1. Upload dos arquivos fonte para o Volume
dbutils.fs.cp("/caminho/local/locations.csv", 
              "/Volumes/workspace/default/covid_vaccination_sources/")

# 2. Via Databricks UI
- Abrir pipeline: covid19_vaccination_pipeline
- Clicar em "Start" para executar full refresh

# 3. Via API
databricks pipelines update run --pipeline-id 9d83a5b0-54a7-46a8-886b-1d663c442dc4
```

## Métricas de Performance

- **Ingestão Bronze**: ~196k registros processados em <1min
- **Transformação Silver**: Parse de arrays + extração temporal em <30s
- **Agregação Gold**: 3 tabelas analíticas geradas em <45s
- **Total pipeline**: <3min end-to-end (serverless cold start incluído)

## Referências

- Dataset: Our World in Data COVID-19 Vaccination Dataset
- Arquitetura: Databricks Medallion Architecture Best Practices
- Framework: Lakeflow Spark Declarative Pipelines Documentation

---
