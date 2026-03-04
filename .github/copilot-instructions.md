# Copilot Instructions for GESTOR DE CAMPANHAS

## Project Overview

This repository contains the **Optimizador de Distribuição de Campanhas** — an intelligent campaign distribution and scarcity-management system built as a Python Jupyter Notebook. The algorithm:

1. Analyzes future scarcity of each marketing campaign
2. Prioritizes critical campaigns (e.g. `iXS_PCDPMTOP`)
3. Preserves customers for future campaigns, avoiding greedy over-allocation
4. Maximizes total eligible customer coverage

The workflow reads customer eligibility data from a SQL Server database ("Diomedes", `tempdb`), processes it with pandas/numpy, and exports the final campaign distribution as CSV files.

## Tech Stack

| Component | Details |
|---|---|
| Language | Python 3 |
| Notebook format | Jupyter Notebook (`.ipynb`) |
| Data processing | `pandas`, `numpy` |
| Database access | `pyodbc` → SQL Server (`Driver={SQL Server}`) |
| Date handling | `datetime`, `timedelta` (standard library) |
| File I/O | `os`, `csv` (standard library) |

No `requirements.txt` or `pyproject.toml` exists yet. Key dependencies to install manually:

```bash
pip install pandas numpy pyodbc jupyter
```

## Key Configuration Constants

These are defined near the top of the main notebook and should be updated per environment:

| Constant | Purpose |
|---|---|
| `PATH_PLANO` | Path to the input campaign plan CSV |
| `FOLDER_OUTPUT` | Folder where exported CSVs are written |
| `SEPARADOR` | CSV field separator (default `;`) |
| `ENCODING` | File encoding (default `utf-8`) |
| `CAMPANHA_PRIORITARIA` | Name of the highest-priority campaign |
| `RANDOM_SEED` | Seed for reproducible random sampling (default `42`) |

## Data Files

The CSV files in the repository root are sample/reference datasets:

- `df_baseenvio.csv` — Customer eligibility base
- `df_envios.csv` — Historical send records
- `df_plano.csv` — Campaign plan
- `df_resultados.csv` / `df_resultados_campanha.csv` — Results output
- `df_CCR.csv`, `df_PMS.csv`, `df_objetivos.csv` — Supporting datasets

## Coding Standards

- Follow **PEP 8** for Python code style.
- Use **snake_case** for variables, functions, and file names.
- Prefix temporary/intermediate DataFrames with `df_` (e.g. `df_baseenvio`).
- Section headings in the notebook should follow the existing `# === BLOCO N: TITLE ===` style.
- Always set `RANDOM_SEED` before any random sampling to ensure reproducibility.
- Keep SQL queries as multi-line strings with clear inline comments.

## Restrictions

- **Never** hardcode database credentials, passwords, or connection strings with real credentials in source code. Use environment variables or a config file excluded via `.gitignore`.
- **Never** commit real customer PII (Personally Identifiable Information) or production data files.
- **Never** modify the CSV files in the repository root without team approval — they serve as reference samples.
- **Do not** alter `RANDOM_SEED` without documenting the reason, as it affects reproducibility.
- Avoid adding new pip dependencies without updating the installation instructions above.

## Running the Project

1. Open the notebook in Jupyter:
   ```bash
   jupyter notebook "Projecto Campanhas vf.ipynb"
   ```
2. Update the configuration constants (`PATH_PLANO`, `FOLDER_OUTPUT`, connection string) for your local environment.
3. Ensure the SQL Server "Diomedes" is reachable and the ODBC driver is installed.
4. Run all cells in order (Kernel → Restart & Run All).

## Agent Behaviour

- Always ask for clarification if a prompt is ambiguous about which campaign or dataset is being targeted.
- When adding new algorithm logic, maintain the existing block (`BLOCO N`) structure inside the notebook.
- When generating SQL queries, follow the style already present in the notebook (CTEs with `/* ===… === */` section banners, `DROP TABLE IF EXISTS` cleanup at the top, `INNER JOIN` over implicit joins).
- Prefer vectorised pandas operations over Python loops for performance.
