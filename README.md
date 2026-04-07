# grid-pulse-spain

Plataforma end-to-end para ingesta y análisis de datos de electricidad y clima en España. El objetivo es captar datos reales, cargarlos en BigQuery, transformarlos con dbt y ofrecer visualizaciones mediante Streamlit.

## Stack

- Python
- Apache Airflow
- dbt
- Google BigQuery
- Streamlit
- APIs de datos de electricidad y clima

## Estructura del repositorio

- `.github/workflows/` - workflows de CI/CD
- `airflow/dags/` - DAGs de orquestación
- `dbt_project/models/staging/` - modelos base de dbt
- `dbt_project/models/marts/` - modelos de negocio finales
- `dbt_project/seeds/` - datos semilla para dbt
- `dbt_project/tests/` - pruebas de dbt
- `docs/` - documentación del proyecto
- `scripts/` - scripts auxiliares
- `src/extract/redata/` - extracción de datos de electricidad
- `src/extract/weather/` - extracción de datos meteorológicos
- `src/load/` - carga a BigQuery u otros destinos
- `src/config/` - configuración y constantes
- `src/utils/` - utilidades compartidas
- `streamlit_app/pages/` - páginas de la app Streamlit
- `streamlit_app/components/` - componentes de UI de Streamlit
- `tests/` - pruebas unitarias e integración

## Roadmap en fases

1. Fase 1: Estructura e infraestructura
   - Definir carpetas y archivos base
   - Configurar entorno y dependencias
   - Preparar esquema inicial de BigQuery

2. Fase 2: Ingesta de datos
   - Conectar fuentes de electricidad y clima
   - Implementar extractores y carga inicial
   - Asegurar ingestión periódica con Airflow

3. Fase 3: Modelado y transformación
   - Construir modelos dbt básicos
   - Crear tablas de staging y marts
   - Añadir tests dbt

4. Fase 4: Visualización y entrega
   - Desarrollar la app Streamlit
   - Presentar métricas clave y gráficos
   - Preparar despliegue y documentación
