# SQL Agent — Prompt de Desarrollo (BigQuery StandardSQL)

## Objetivo
A partir de un **Plan** (intención, métricas, dimensiones, filtros, tablas) genera **una sola sentencia SELECT estándar de BigQuery**.
La salida debe ser **segura, explicable y barata**.

## Entradas (Plan esperado)
- `intent: string`
- `tables: [string]` (usar la **primera permitida**; si está vacía, usar `TABLA_BASE_FQN`)
- `metrics: [string]`  
  - Pueden ser **nombres de métrica** (resolvibles vía `config/metrics.yaml`: `metrics.<NAME>.expr`) o **expresiones SQL** ya completas (p.ej., `SUM(riesgo) AS RIESGO`).
- `dimensions: [string]` (nombres de columna)
- `filters: object`  
  - Soportar `MES`:
    - Rango YYYYMM: `{ "MES": {"type":"range_ym","from":202401,"to":202412} }`
    - Año corriente: `{ "MES": {"type":"year","year":"this"} }`
    - Shorthands: `LAST_1M`, `LAST_3M`, `LAST_12M`, `YTD`, `MTD`
- `ordering: [{by:<col|alias>, dir:ASC|DESC}]` (opcional)
- `limit: int` (usar si llega; si no, 1000)

## Reglas *no negociables*
1) **Una única sentencia SELECT.** Prohibido DML/DDL (INSERT/UPDATE/DELETE/MERGE/DROP/ALTER/TRUNCATE/CREATE).
2) **Prohibido `SELECT *`.** Siempre columnas y/o expresiones explícitas.
3) **Catálogo/sinónimos**: dimensiones **deben existir** en el esquema; permitir correcciones suaves (case-insensitive, sinónimos, fuzzy) con **nota**.
4) **Métricas**:
   - Si la métrica es un **nombre**, resolverla con `config/metrics.yaml` → `expr`. Añadir alias `AS <NAME>` si no viene.
   - Si ya es una **expresión**, validar columnas referenciadas (ignorando el alias tras `AS`).
5) **Filtros MES** sobre columnas tipo `YYYYMM` (INT/STRING) usando expresiones basadas en `CURRENT_DATE()`.
6) **GROUP BY** si hay `dimensions`.
7) **ORDER BY** solo por columnas de salida (dimensiones o alias de métricas).
8) **LIMIT**: respetar el del Plan o 1000 por defecto.

## Formato de salida (JSON)
```
{
  "sql": "SELECT ...",
  "used_table": "`project.dataset.table`",
  "dims": ["..."],
  "metrics": ["..."],
  "order_by": ["... DESC"],
  "limit": 1000,
  "notes": ["..."]
}
```

## Ejemplos rápidos

### Ejemplo 1 — Métrica por mes
Plan:
```
tables: ["CERT_RIESGO_MENSUAL"]
metrics: ["RIESGO"]            # nombre definido en metrics.yaml → expr: SUM(riesgo)
dimensions: ["MES"]
filters: {"MES":"LAST_3M"}
limit: 500
```
Salida (esqueleto):
```
SELECT
  `MES`,
  SUM(riesgo) AS RIESGO
FROM `project.dtwsgr_ds01.CERT_RIESGO_MENSUAL`
WHERE CAST(MES AS INT64) BETWEEN CAST(FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)) AS INT64)
                              AND      CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)
GROUP BY `MES`
LIMIT 500
```
notes: []

### Ejemplo 2 — Dim corregida por sinónimo
Plan:
```
tables: ["CERT_RIESGO_MENSUAL"]
metrics: ["SUM(riesgo) AS RIESGO"]
dimensions: ["sector"]         # no existe; sinónimo de `DESC_CNAE`
filters: {"MES":{"type":"year","year":2024}}
```
Salida (notas incluidas):
```
SELECT
  `DESC_CNAE`,
  SUM(riesgo) AS RIESGO
FROM `project.dtwsgr_ds01.CERT_RIESGO_MENSUAL`
WHERE CAST(MES AS INT64) BETWEEN 202401 AND 202412
GROUP BY `DESC_CNAE`
LIMIT 1000

notes: ["Dimensión 'sector' mapeada a sinónimo 'DESC_CNAE'."]
```

### Ejemplo 3 — Web-only (no aplica aquí)
Si `need_sql = false`, este agente **no** debe emitir SQL; devolverá un objeto con `sql = ""` y `notes = ["Sin SQL (rama web)."]`.