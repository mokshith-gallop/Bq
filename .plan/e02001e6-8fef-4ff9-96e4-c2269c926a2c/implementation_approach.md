# Implementation Approach

Build a robust, production-grade Python deployment script (`/workspace/project/scripts/deploy_ddl.py`) to manage the schema conversion deployment lifecycle.

### Summary of Technical Choices
- **Dataset Orchestration**: The script automatically checks for and creates target BigQuery datasets across two separate projects, enforcing geographic placement rules:
  - `PROJECT_US` (`acme-analytics`): Enforces `US` multi-region for `raw`, `staging`, `retail`, and `udfs` datasets.
  - `PROJECT_EU` (`acme-analytics-eu`): Enforces `EU` multi-region for `regional_eu` and `udfs` datasets.
- **Dynamic Variable Substitution**: The script dynamically loads variable templates from `/workspace/project/ddl/variables.env`, parses SQL statements, replaces placeholder strings (e.g. `${PROJECT_US}`, `${DS_RAW}`, `${DS_RETAIL}`) with environment-substituted values, and executes them as legacy-free standard SQL query jobs.
- **Dependency Ordering**: Deploy tables before views, following `/workspace/project/ddl/deploy_order.txt` topological sort sequence.
- **UDF Registration Integration**: Registers standard JS and remote UDFs using pre-configured target connection locations. Handles potential remote-connection/function dependency gaps.
