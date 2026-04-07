# grid-pulse-spain

Plataforma end-to-end de data engineering para analizar el sistema eléctrico español a partir de datos reales de electricidad y clima.

El objetivo del proyecto es ingerir datos horarios desde fuentes públicas, cargarlos en BigQuery, transformarlos con dbt y exponer métricas y visualizaciones a través de una aplicación en Streamlit.

## Stack

- Python
- Google BigQuery
- dbt
- Apache Airflow
- Streamlit
- APIs públicas de electricidad y clima

## Estructura del repositorio

- `.github/workflows/` - workflows de automatización y CI
- `airflow/dags/` - DAGs de orquestación
- `dbt_project/models/staging/` - modelos de staging en dbt
- `dbt_project/models/marts/` - modelos analíticos finales
- `dbt_project/seeds/` - datos semilla para dbt
- `dbt_project/tests/` - pruebas de calidad en dbt
- `docs/` - documentación del proyecto
- `scripts/` - scripts auxiliares y runners
- `src/extract/redata/` - extracción de datos eléctricos
- `src/extract/weather/` - extracción de datos meteorológicos
- `src/load/` - carga de datos a BigQuery
- `src/config/` - configuración y constantes
- `src/utils/` - utilidades compartidas
- `streamlit_app/pages/` - páginas de la app Streamlit
- `streamlit_app/components/` - componentes reutilizables de la interfaz
- `tests/` - pruebas unitarias e integración

## Roadmap en fases

1. **Fase 1: Base del proyecto**
   - Definir la estructura del repositorio
   - Configurar entorno y dependencias
   - Preparar configuración inicial del proyecto

2. **Fase 2: Ingesta de datos**
   - Conectar con APIs reales de electricidad y clima
   - Extraer datos históricos iniciales
   - Guardar y validar los datos crudos

3. **Fase 3: Carga y modelado**
   - Cargar datos raw en BigQuery
   - Construir modelos de staging y marts con dbt
   - Implementar modelos incrementales

4. **Fase 4: Calidad y automatización**
   - Añadir tests básicos de calidad
   - Crear un runner reproducible
   - Orquestar la ejecución del pipeline

5. **Fase 5: Visualización**
   - Desarrollar la aplicación en Streamlit
   - Explorar demanda, generación renovable, clima y precio
   - Publicar una versión final del proyecto para portfolio