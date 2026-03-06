Yes — below is a **phase-by-phase prompt pack** you can give to an AI coding agent.

I rewrote your idea into a more professional, execution-focused format so the agent behaves like a **senior software engineer / system architect** and works in phases instead of making random changes.

You can use these prompts in sequence.

---

# Master goal

Your current system reads data from CSV files. You now want to scale it by:

* moving data storage from CSVs to PostgreSQL
* containerizing the system with Docker
* running frontend, backend, PostgreSQL, and worker/queue services together
* adding migration scripts to move CSV data into PostgreSQL
* changing code so reads/writes/calculations use the database instead of CSV files
* making the local PC the main server where everything runs

The prompts below are designed so the agent first **understands the whole project**, then **plans**, then **implements**, then **migrates**, then **refactors the codebase safely**.

---

# Phase 1 prompt — project discovery and architecture understanding

Use this first.

```text
You are a senior software architect and senior full-stack engineer.

Your job is to first understand my existing project completely before making any code changes.

Context:
- This is an existing software project.
- Currently the system reads data from CSV files.
- I want to scale it properly.
- My likely target architecture is:
  - frontend in Docker
  - backend in Docker
  - PostgreSQL in Docker
  - optional worker/queue service if needed
  - migration of CSV-based data into PostgreSQL
  - refactor code so future reads/writes/calculations use PostgreSQL instead of CSV files
- The server environment is my local PC where the codebase is currently running.

Your instructions:
1. Inspect the full project directory structure.
2. Understand what this software does end-to-end.
3. Identify:
   - frontend code location
   - backend code location
   - any worker/scheduler/background job code
   - current configuration files
   - how the app is started
   - where CSV files are stored
   - which modules read CSVs
   - which modules generate result tables / calculation tables
   - where business logic lives
   - whether a queue/worker system already exists or should be introduced
4. Trace data flow:
   - source of raw data
   - how raw data enters the system
   - how it is processed
   - where results are stored
   - what outputs are consumed by frontend/backend
5. Detect current stack:
   - language(s)
   - framework(s)
   - package managers
   - database usage if any
   - environment variable patterns
   - scheduler/cron/background processes
6. Identify scaling bottlenecks caused by CSV-based storage.
7. Identify risks in migrating from CSV to PostgreSQL.
8. Produce a detailed assessment document before making changes.

Output format:
- Project summary
- Directory structure summary
- Runtime architecture summary
- Current data flow
- CSV dependency map
- Candidate PostgreSQL schema areas
- Dockerization needs
- Queue/worker recommendation
- Risks and migration considerations
- Step-by-step implementation plan

Important rules:
- Do not change code yet unless absolutely necessary for analysis.
- Do not assume architecture; infer it from the repo.
- Be explicit about unknowns.
- If something is unclear, inspect more files before concluding.
- Prefer grounded conclusions based on actual code.
```

---

# Phase 2 prompt — scaling plan and technical design

Use this after phase 1 is complete.

```text
You are a senior software architect.

Based on your analysis of the existing codebase, create a production-style scaling plan for this project.

Goal:
Move the system from CSV-based storage/processing to PostgreSQL-backed processing, and containerize the application for reliable local-server deployment.

Requirements:
- frontend should run in Docker
- backend should run in Docker
- PostgreSQL should run in Docker
- add worker/queue container if needed
- use Docker Compose to orchestrate services
- the target machine is my local PC/server
- support migration from CSVs to PostgreSQL
- future calculations and result generation should happen from DB data, not CSV files

Your tasks:
1. Propose the target architecture.
2. Define service boundaries:
   - frontend
   - backend
   - postgres
   - worker
   - queue/broker if required
3. Decide whether a queue system is necessary.
   - If yes, recommend a practical option for this project.
   - If no, explain why.
4. Design the PostgreSQL database model:
   - raw data tables
   - result tables
   - calculation tables
   - metadata / job tracking tables if useful
5. Define migration strategy from CSV to DB:
   - one-time backfill
   - validation checks
   - re-runnable scripts
   - idempotent behavior
6. Define code refactor strategy:
   - where CSV reads will be replaced
   - where DB repository / query layer should be added
   - how to avoid breaking existing calculations
7. Define environment/config strategy:
   - env variables
   - container networking
   - volumes
   - local persistence
8. Define rollout order to reduce risk.
9. Produce a concrete implementation roadmap with phases.

Output format:
- Target architecture
- Service diagram in text
- Database design proposal
- Migration design
- Refactor plan
- Docker/Compose plan
- Queue/worker recommendation
- Rollout plan
- Risks and mitigations

Important:
- Design based on the actual project structure you analyzed, not a generic template.
- Prefer maintainable and simple solutions.
- Avoid over-engineering.
```

---

# Phase 3 prompt — create Docker and Compose setup

Use this to make the infra files.

```text
You are a senior DevOps engineer and backend architect.

Now implement Dockerization for the project based on the analyzed repository structure.

Goal:
Containerize the full system so it can run on my PC/server with Docker Compose.

Required services:
- frontend
- backend
- postgres
- worker if needed
- queue/broker if needed

Your tasks:
1. Create or update Dockerfiles for each required service.
2. Create a docker-compose.yml that:
   - starts all required services
   - wires networking correctly
   - persists PostgreSQL data in a volume
   - injects environment variables cleanly
   - supports local development and stable startup order
3. Add .dockerignore files where appropriate.
4. Add startup scripts or wait-for-db logic if needed.
5. Ensure backend waits for database readiness before migrations or startup.
6. If a worker is needed, create service definitions for it.
7. If a queue/broker is needed, add it properly.
8. Keep the setup practical for a local machine deployment.
9. Add comments where useful, but keep files clean.

Also create:
- example .env file(s)
- README section explaining how to start the stack
- command list for build, up, down, logs, restart

Output expectations:
- create files directly
- explain each created file
- explain how services communicate
- explain any assumptions

Important:
- Use the actual project folders and runtime commands from the repo.
- Do not invent fake paths.
- Keep images and setup lightweight where possible.
- Optimize for reliability and maintainability on a local PC/server.
```

---

# Phase 4 prompt — design PostgreSQL schema from existing CSV usage

Use this before migration coding.

```text
You are a senior data engineer and backend engineer.

Now design the PostgreSQL schema for this project using the actual CSV files and current code usage.

Goal:
Replace CSV-based storage with normalized and practical PostgreSQL tables.

Your tasks:
1. Inspect all CSV files currently used by the system.
2. Identify:
   - raw input data columns
   - result/output columns
   - calculation/intermediate columns
   - relationships between files
   - keys, timestamps, identifiers, symbols, categories, statuses, etc.
3. Inspect the code that reads/writes those CSVs to understand intended meaning.
4. Design PostgreSQL schema for:
   - source/raw tables
   - processed/calculation tables
   - result tables
   - audit/import metadata tables if useful
5. Recommend:
   - primary keys
   - foreign keys
   - indexes
   - unique constraints
   - partitioning only if truly needed
6. Map each CSV file to target DB table(s).
7. Note any data cleaning/transformation rules needed during migration.
8. Produce SQL migration files or ORM models as appropriate for the project stack.

Output format:
- CSV inventory
- semantic meaning of each CSV
- proposed PostgreSQL schema
- table-by-table explanation
- CSV-to-table mapping
- index strategy
- migration considerations

Important:
- Base schema on actual usage in the codebase.
- Do not over-normalize if it makes the app harder to maintain.
- Preserve all fields required for current calculations.
- Flag unclear columns explicitly.
```

---

# Phase 5 prompt — build CSV to PostgreSQL migration scripts

Use this once schema is decided.

```text
You are a senior backend engineer and data migration engineer.

Now implement migration scripts to move existing CSV data into PostgreSQL safely.

Goal:
Create reliable migration utilities that import all current CSV-based data into PostgreSQL and can be re-run safely where possible.

Requirements:
- migrate raw data CSVs
- migrate result tables
- migrate calculation tables
- preserve existing logic and field meanings
- support validation and error visibility
- work in my local Docker-based stack

Your tasks:
1. Create migration/import scripts for all relevant CSV sources.
2. Make imports as idempotent as practical:
   - upsert where appropriate
   - avoid duplicate inserts
   - handle reruns safely
3. Add validation:
   - row counts
   - missing/invalid field detection
   - duplicate detection
   - import summaries
4. Add logging and useful console output.
5. Add a way to run:
   - full import
   - per-file import
   - per-table import
6. If needed, add staging tables before final insert/upsert.
7. Ensure encoding, null handling, date parsing, numeric parsing, and empty-string handling are robust.
8. Document assumptions and transformation rules.
9. Create a migration README with exact commands.

Output expectations:
- implement scripts directly in the repo
- explain how to run them
- explain how data integrity is checked
- list any rows/files that need manual cleanup

Important:
- Use the actual CSV paths and actual schema.
- Do not silently drop problematic rows without reporting them.
- Prefer safe and observable migration behavior over cleverness.
```

---

# Phase 6 prompt — refactor app code from CSV reads to database reads

This is the key one.

```text
You are a senior software engineer focused on backend refactoring and system reliability.

Now refactor the application so that data access moves from CSV files to PostgreSQL.

Goal:
All ongoing reads/writes/calculations that currently depend on CSV files should now use PostgreSQL instead.

Your tasks:
1. Find all code paths that:
   - read CSV files
   - write CSV files
   - load result tables from files
   - load calculation/intermediate tables from files
2. Replace file-based access with database-backed access.
3. Introduce a clean data access layer / repository layer if the project does not already have one.
4. Preserve current business logic and outputs.
5. Ensure calculation code still produces the same results as before.
6. Where useful, keep a temporary compatibility layer so migration risk is reduced.
7. Update configuration so database connection settings come from environment variables.
8. Remove or isolate legacy CSV-only logic carefully.
9. Add tests or verification scripts to compare old CSV-based outputs vs new DB-based outputs.

Output format:
- list of modules changed
- old CSV access pattern
- new DB access pattern
- compatibility considerations
- validation approach
- any remaining CSV dependencies

Important:
- Do not rewrite the whole system unnecessarily.
- Keep business logic intact; change storage/access layer first.
- Minimize regression risk.
- If some modules should remain file-based temporarily, explain why.
```

---

# Phase 7 prompt — add worker/queue only if needed

Use only if your app has background jobs, long calculations, imports, or scheduled work.

```text
You are a senior distributed systems engineer.

Evaluate whether this project needs a worker and queue/broker system based on the actual codebase and workload.

Goal:
Only introduce a worker/queue if it brings clear value.

Evaluate:
- long-running calculations
- periodic jobs
- import pipelines
- retryable background work
- tasks that should not block the API/backend
- result generation pipelines
- scheduling/orchestration already present in code

If a worker system is justified:
1. Recommend the simplest reliable stack suitable for this project.
2. Implement:
   - worker service
   - queue/broker service if required
   - task structure
   - retry logic
   - logging
   - Docker Compose integration
3. Move appropriate tasks into worker jobs.
4. Keep request/response paths fast.

If a worker system is not justified:
- clearly explain why it should be avoided for now

Output format:
- recommendation
- reasoning
- architecture changes
- files added/changed
- how tasks are triggered
- operational notes

Important:
- Do not add complexity without strong justification.
- Optimize for simplicity on a local PC/server deployment.
```

---

# Phase 8 prompt — testing, validation, and parity checks

This makes sure the migration does not break logic.

```text
You are a senior QA-minded software engineer.

Now verify that the new PostgreSQL-backed system behaves correctly compared with the old CSV-based system.

Goal:
Ensure storage migration and code refactor did not break calculations, outputs, or application behavior.

Your tasks:
1. Create a validation plan.
2. Compare old vs new behavior for:
   - raw data loading
   - calculation outputs
   - result tables
   - API responses if relevant
   - frontend-visible data if relevant
3. Add verification scripts/tests that compare:
   - record counts
   - key aggregates
   - selected sample records
   - final result parity where possible
4. Identify mismatches and explain likely reasons.
5. Fix discrepancies where appropriate.
6. Produce a final migration confidence report.

Output format:
- validation checklist
- tests/scripts added
- parity comparison results
- discrepancies found
- fixes applied
- remaining risks

Important:
- Be explicit about anything not fully verified.
- Prefer measurable comparisons over vague statements.
```

---

# Phase 9 prompt — cleanup and production-style documentation

Use this at the end.

```text
You are a senior software engineer finishing a system modernization project.

Now clean up the implementation and produce final documentation.

Goal:
Make the project easy to run, maintain, and understand after the migration from CSV to PostgreSQL.

Your tasks:
1. Remove dead or obsolete code where safe.
2. Mark any temporary compatibility code clearly.
3. Update README with:
   - architecture overview
   - service list
   - Docker usage
   - environment setup
   - database migration steps
   - CSV import steps
   - worker/queue usage if applicable
   - common troubleshooting steps
4. Add developer notes for future maintenance.
5. Summarize all key changes made.
6. List remaining technical debt or follow-up recommendations.

Output format:
- cleanup summary
- documentation updates
- final architecture summary
- known limitations
- next-step recommendations

Important:
- Keep documentation practical and specific to this codebase.
- Do not leave important operational knowledge undocumented.
```

---

# One-shot master prompt

If you want a **single big prompt** instead of phased prompts, use this:

```text
You are a senior software architect, senior backend engineer, DevOps engineer, and data migration specialist.

I have an existing software project that currently reads data from CSV files. I want to scale and modernize it properly.

Target goal:
- understand the complete existing codebase first
- identify frontend, backend, workers, schedulers, and data flow
- locate where CSV data comes from and where it is read/written
- move storage from CSV files to PostgreSQL
- containerize the system with Docker
- create Dockerfiles and docker-compose for frontend, backend, PostgreSQL, and worker/queue if needed
- make the local PC/server the runtime environment
- create migration scripts to move CSV data, result tables, and calculation tables into PostgreSQL
- refactor the code so all future reads/writes/calculations use PostgreSQL instead of CSV files
- validate parity so the new DB-backed system preserves existing behavior

Execution rules:
1. First inspect the full project structure and understand the application before changing code.
2. Produce an architecture and migration plan based on the actual repo.
3. Then implement in phases with clear reasoning.
4. Only add worker/queue infrastructure if clearly justified by the actual workload.
5. Keep the system simple, reliable, and maintainable for local deployment.
6. Preserve existing business logic and outputs as much as possible.
7. Be explicit about assumptions, unknowns, and migration risks.
8. Prefer grounded analysis over generic recommendations.
9. After implementation, produce documentation and validation results.

Expected phased deliverables:
Phase 1: project structure and data-flow analysis
Phase 2: target architecture and DB migration plan
Phase 3: Dockerfiles, Compose, env setup, service wiring
Phase 4: PostgreSQL schema based on actual CSV and code usage
Phase 5: CSV-to-PostgreSQL migration scripts
Phase 6: code refactor from CSV access to DB access
Phase 7: worker/queue integration only if needed
Phase 8: parity testing and validation
Phase 9: cleanup and final documentation

Output style:
- behave like a senior engineer reviewing and modernizing a real production-bound codebase
- explain findings clearly
- make safe, incremental changes
- summarize files changed and why
- do not over-engineer
```

---

# Recommended way to use these prompts

Best practice is:

1. Run **Phase 1**
2. Review the analysis
3. Run **Phase 2**
4. Then execute **Phase 3 to Phase 9** one by one

This is better than doing everything in one shot, because otherwise the agent may skip the understanding step and produce generic Docker/Postgres code.

---

# Extra strong instruction you can append to every prompt

Add this at the end of each prompt if you want stricter execution:

```text
Before making changes, inspect relevant files carefully.
When you modify code, explain:
- what changed
- why it changed
- which old CSV-based path is being replaced
- how the new PostgreSQL-based path works
Do not invent files, commands, or architecture that do not exist in the repo.
Prefer incremental and verifiable changes over broad rewrites.
```

---

I can also turn this into a **clean prompt document you can copy-paste into Cursor / Claude Code / Windsurf / OpenAI agent** with better formatting and placeholders like `{backend_path}`, `{frontend_path}`, `{csv_dir}`.
