# Queries de Análise Avançada - COVID-19 Vaccination

## Queries Prontas para Demonstração

### 1. Análise de Aceleração da Campanha
**Objetivo**: Identificar países que ganharam ou perderam velocidade na vacinação

```sql
WITH monthly_pace AS (
  SELECT 
    country,
    year,
    month,
    SUM(daily_vaccinations) as total_doses_month,
    LAG(SUM(daily_vaccinations)) OVER (
      PARTITION BY country 
      ORDER BY year, month
    ) as prev_month_doses
  FROM workspace.default.silver_vaccinations
  WHERE daily_vaccinations IS NOT NULL
  GROUP BY country, year, month
)
SELECT 
  country,
  year,
  month,
  total_doses_month,
  prev_month_doses,
  ROUND(
    ((total_doses_month - prev_month_doses) / prev_month_doses * 100), 
    2
  ) as growth_rate_pct
FROM monthly_pace
WHERE prev_month_doses > 0
  AND year >= 2023
ORDER BY growth_rate_pct DESC
LIMIT 20;
```

**Insight esperado**: Países com growth_rate negativo podem ter atingido saturação de cobertura ou enfrentado problemas logísticos.

---

### 2. Comparação Regional - Oriente Médio vs Europa
**Objetivo**: Entender diferenças estratégicas entre regiões

```sql
-- Criar lista de países por região (simplificado)
WITH regions AS (
  SELECT 
    country,
    iso_code,
    CASE 
      WHEN iso_code IN ('IRN', 'IRQ', 'SAU', 'ARE', 'KWT', 'BHR', 'OMN', 'QAT', 'JOR', 'LBN', 'SYR', 'YEM') 
        THEN 'Oriente Médio'
      WHEN iso_code IN ('DEU', 'FRA', 'ITA', 'ESP', 'POL', 'ROU', 'NLD', 'BEL', 'CZE', 'GRC', 'PRT', 'HUN', 'SWE', 'AUT', 'BGR', 'DNK', 'FIN', 'SVK', 'NOR', 'IRL', 'HRV', 'SVN', 'LTU', 'LVA', 'EST', 'LUX', 'MLT', 'CYP', 'ISL')
        THEN 'Europa'
      ELSE 'Outras'
    END as region
  FROM workspace.default.silver_locations
)
SELECT 
  r.region,
  COUNT(DISTINCT r.country) as total_countries,
  ROUND(AVG(l.vaccine_count), 1) as avg_vaccine_types,
  MAX(l.vaccine_count) as max_vaccine_types,
  STRING_AGG(DISTINCT l.location, ', ') as top_diverse_countries
FROM regions r
JOIN workspace.default.gold_countries_vaccine_types l 
  ON r.iso_code = l.iso_code
WHERE r.region IN ('Oriente Médio', 'Europa')
  AND l.vaccine_count >= 8  -- apenas países com alta diversidade
GROUP BY r.region;
```

**Insight esperado**: Oriente Médio deve ter média mais alta (geopolítica + COVAX).

---

### 3. Análise de Consistência dos Dados
**Objetivo**: Identificar gaps ou anomalias nos dados reportados

```sql
-- Encontrar países com saltos suspeitos nas doses totais
WITH daily_changes AS (
  SELECT 
    country,
    date,
    total_vaccinations,
    LAG(total_vaccinations) OVER (
      PARTITION BY country 
      ORDER BY date
    ) as prev_total,
    total_vaccinations - LAG(total_vaccinations) OVER (
      PARTITION BY country 
      ORDER BY date
    ) as daily_change
  FROM workspace.default.silver_vaccinations
  WHERE total_vaccinations IS NOT NULL
)
SELECT 
  country,
  date,
  total_vaccinations,
  prev_total,
  daily_change,
  CASE 
    WHEN daily_change < 0 THEN 'ANOMALIA: Redução'
    WHEN daily_change > 10000000 THEN 'ANOMALIA: Spike muito alto'
    WHEN daily_change = 0 THEN 'Estagnação'
    ELSE 'Normal'
  END as status
FROM daily_changes
WHERE daily_change < 0 OR daily_change > 10000000
ORDER BY ABS(daily_change) DESC
LIMIT 50;
```

**Por que isso importa**: Mostra senso crítico sobre qualidade de dados. Em entrevista, você pode mencionar que implementaria Expectations do Databricks para automatizar essa validação.

---

### 4. Ranking de Eficiência Operacional
**Objetivo**: Quais países vacinaram mais com menos tipos de vacina?

```sql
-- Eficiência = doses totais / número de vacinas usadas
WITH latest_data AS (
  SELECT 
    country,
    MAX(date) as last_date
  FROM workspace.default.silver_vaccinations
  GROUP BY country
),
final_counts AS (
  SELECT 
    v.country,
    v.total_vaccinations,
    l.vaccine_count
  FROM workspace.default.silver_vaccinations v
  JOIN latest_data ld 
    ON v.country = ld.country AND v.date = ld.last_date
  JOIN workspace.default.gold_countries_vaccine_types l
    ON v.country = l.location
  WHERE v.total_vaccinations IS NOT NULL
    AND l.vaccine_count > 0
)
SELECT 
  country,
  total_vaccinations,
  vaccine_count,
  ROUND(total_vaccinations / vaccine_count, 0) as doses_per_vaccine_type,
  CASE 
    WHEN vaccine_count = 1 THEN 'Estratégia Focada'
    WHEN vaccine_count BETWEEN 2 AND 4 THEN 'Estratégia Balanceada'
    ELSE 'Estratégia Diversificada'
  END as strategy_type
FROM final_counts
WHERE total_vaccinations > 1000000  -- apenas países com volume significativo
ORDER BY doses_per_vaccine_type DESC
LIMIT 30;
```

**Conversa para entrevista**: "Países como China e Índia podem aparecer no topo - não porque são mais eficientes operacionalmente, mas porque produziram localmente 1-2 vacinas em escala massiva. Já países menores precisaram diversificar via importação."

---

### 5. Análise de Janela Temporal - Primeiras vs Últimas Doses
**Objetivo**: Velocidade de rollout inicial vs manutenção de longo prazo

```sql
-- Comparar primeiros 90 dias vs últimos 90 dias de campanha
WITH campaign_phases AS (
  SELECT 
    country,
    date,
    daily_vaccinations,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY date) as day_rank_asc,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY date DESC) as day_rank_desc
  FROM workspace.default.silver_vaccinations
  WHERE daily_vaccinations IS NOT NULL
)
SELECT 
  country,
  ROUND(AVG(CASE WHEN day_rank_asc <= 90 THEN daily_vaccinations END), 0) as avg_first_90_days,
  ROUND(AVG(CASE WHEN day_rank_desc <= 90 THEN daily_vaccinations END), 0) as avg_last_90_days,
  ROUND(
    (AVG(CASE WHEN day_rank_desc <= 90 THEN daily_vaccinations END) - 
     AVG(CASE WHEN day_rank_asc <= 90 THEN daily_vaccinations END)) /
    AVG(CASE WHEN day_rank_asc <= 90 THEN daily_vaccinations END) * 100,
    1
  ) as pct_change
FROM campaign_phases
GROUP BY country
HAVING avg_first_90_days IS NOT NULL AND avg_last_90_days IS NOT NULL
ORDER BY pct_change DESC
LIMIT 25;
```

**Insight**: 
- `pct_change` **positivo** = país acelerou campanha ao longo do tempo (boa logística)
- `pct_change` **negativo** = campanha perdeu momentum (saturação ou fadiga vacinal)

---

### 6. Correlação entre Diversidade e Volume Total
**Objetivo**: Mais vacinas = mais doses aplicadas?

```sql
-- Scatter plot data: vaccine_count vs total_vaccinations
WITH latest_totals AS (
  SELECT 
    country,
    MAX(date) as last_date
  FROM workspace.default.silver_vaccinations
  GROUP BY country
)
SELECT 
  l.location as country,
  l.vaccine_count,
  v.total_vaccinations,
  v.people_vaccinated,
  CASE 
    WHEN l.vaccine_count >= 8 THEN 'Alto'
    WHEN l.vaccine_count >= 5 THEN 'Médio'
    ELSE 'Baixo'
  END as diversity_level
FROM workspace.default.gold_countries_vaccine_types l
JOIN workspace.default.silver_vaccinations v 
  ON l.location = v.country
JOIN latest_totals lt
  ON v.country = lt.country AND v.date = lt.last_date
WHERE v.total_vaccinations > 100000  -- filtrar países muito pequenos
ORDER BY l.vaccine_count DESC, v.total_vaccinations DESC;
```

**Como apresentar**: "Exportaria isso para Python e faria um seaborn scatter plot com regressão linear. Minha hipótese é que a correlação é fraca - volume depende mais de população e PIB do que de diversidade de vacinas."

---

## Visualizações Sugeridas (para Dashboard)

### 1. Mapa de Calor Temporal
- **Eixo X**: Meses (Jan/2021 → Ago/2024)
- **Eixo Y**: Top 30 países
- **Cor**: Daily vaccinations (escala log)
- **Insight visual**: Identificar "ondas" de vacinação sincronizadas globalmente

### 2. Sankey Diagram
- **Fluxo**: Tipo de vacina → País → Volume de doses
- **Mostra**: Quais vacinas dominaram em quais regiões

### 3. Racing Bar Chart
- **Animação**: Ranking de países por total_vaccinations ao longo dos meses
- **Efeito**: Mostra dramaticamente a ultrapassagem de Índia, China, etc.

### 4. Treemap
- **Hierarquia**: Região → País → Vaccine Types
- **Tamanho**: Total vaccinations
- **Cor**: Vaccine diversity

---