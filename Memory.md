MASTER PROMPT — PROJECT MEMORY RECOVERY & PERSISTENCE

You are a senior AI infrastructure engineer, context engineer, and developer-tooling specialist.

We are working on an existing, active software project with approximately 20–25 previous Claude Code sessions, possibly additional Codex sessions, and substantial development history.

Our organization uses Claude Code, Codex, Git, and an office NAS/file server.

Your objective: Recover all available historical project conversations, preserve their important technical context as portable Markdown files, configure automatic memory for future sessions, and ensure that a fresh Claude Code or Codex session can continue this project without access to the original AI account.

1. Technology and environment

Evaluate these official open-source projects:

- Claude-Mem: https://github.com/thedotmack/claude-mem
- Mem0: https://github.com/mem0ai/mem0
- Basic Memory: https://github.com/basicmachines-co/basic-memory

Prefer Claude-Mem for future automatic session capture.

Use native Claude Code hooks, supported MCP integrations, project instructions, and skills where they materially improve reliability.

Do not install unnecessary agent frameworks or dependencies. Do not assume an MCP server or plugin is already installed.

Check current documentation, compatibility, maintenance status, and security implications before recommending installation.

2. Historical session discovery — mandatory

Start with a READ-ONLY audit of the current project and available session history.

Inspect the appropriate locations, including:

Claude Code:

- "~/.claude/projects/"
- "~/.claude/history.jsonl", if present
- Existing project-related auto-memory, session indexes, and subagent records
- Alternative locations configured through "CLAUDE_CONFIG_DIR"

Codex:

- "$CODEX_HOME/sessions/"
- "$CODEX_HOME/archived_sessions/"
- Default to "~/.codex/" when CODEX_HOME is unset
- Other supported session indexes or metadata

First identify the current repository's canonical path, Git remote, working directories, and relevant worktrees.

Match historical sessions to this specific project using available metadata, original working directories, Git information, and transcript content.

Do not rely only on directory-name similarity.

Inspect all plausible matching session directories, including old project paths and renamed repositories.

Where available, use official session export or script interfaces. JSONL parsing must tolerate version differences, missing fields, and malformed records.

Include available main-agent sessions and relevant subagent history without duplicating content.

Important requirements:

- Discover every recoverable matching session, not just the most recent one.
- Preserve chronological order.
- Create a manifest containing source path, session ID, timestamps, size, matching confidence, and processing status.
- Compute hashes to prevent duplicate imports.
- Clearly distinguish complete, partial, missing, and inaccessible transcripts.
- Do not assume all historical conversations are still available.
- Do not read unrelated projects or other users' sessions.

Never delete, truncate, rewrite, or modify the original conversation files.

3. Secure historical archive

Create the following structure inside the repository:

"docs/ai-memory/"

Use this organization:

docs/ai-memory/
├── INDEX.md
├── PROJECT.md
├── ARCHITECTURE.md
├── CURRENT_STATE.md
├── DECISIONS.md
├── KNOWN_ISSUES.md
├── NEXT_ACTIONS.md
├── SESSION_HISTORY.md
├── RECOVERY_REPORT.md
├── manifest.json
└── sessions/
    ├── <date>_<session-id>.md
    └── ...

Create a separate Markdown summary for every recoverable historical session.

Each session summary must capture:

- Session identifier and timestamp
- Original source reference
- User requests and intended outcomes
- Architecture or design decisions
- Code changes and relevant files
- Bugs, errors, and their solutions
- Important commands and configuration changes
- Work completed
- Unfinished work and blockers
- Follow-up tasks
- Relevant Git commit hashes, where available

Use links to source files and historical session identifiers for traceability.

Never invent conversations or decisions.

Do not copy passwords, API tokens, private keys, financial secrets, or confidential customer information into general-purpose memory.

Keep raw transcripts out of Git unless explicitly authorized.

4. Consolidate project knowledge

Read all recovered session summaries, then reconstruct the project's development history.

Inspect the current source code, README, Git history, tests, and configuration to validate historical claims.

Resolve contradictions carefully:

- The current repository is authoritative for what is implemented now.
- Older conversations explain historical decisions, not necessarily current behavior.
- Record unresolved contradictions explicitly.
- Do not mark an implementation as complete without evidence.

Populate all project-level Markdown files.

"CURRENT_STATE.md" must accurately explain what currently works, what is partially implemented, and what remains unfinished.

"NEXT_ACTIONS.md" must identify the immediate next development task.

Create a lightweight "INDEX.md" linking the important project files and historical session summaries.

5. Claude-Mem installation and integration

Inspect the currently installed Claude-Mem version, if any.

Follow its official installation and integration documentation.

Verify:

- Compatibility with the installed Claude Code and Codex versions
- Availability of local memory capture
- Observer provider and associated costs
- Memory search capabilities
- MCP integration, where supported
- Privacy settings and external data transmission
- Session-start context injection
- Session-end capture and persistence

Important: Do not assume Claude-Mem can automatically import old Claude Code JSONL transcripts.

If the installed version offers a documented, supported historical-backfill mechanism, evaluate it and test it on a small copied sample.

Otherwise, retain the recovered Markdown files as the portable historical memory and make them accessible through project instructions or a local search interface.

Never write records directly into undocumented Claude-Mem database tables.

Do not install software, enable paid observers, change telemetry settings, or connect external services without authorization.

6. Office NAS storage and backups

Our shared file server contains approximately 12 TB of storage.

The NAS mount path is:

"YOUR_NAS_MOUNT_PATH"

Organize the project backup under:

"YOUR_NAS_MOUNT_PATH/ai-memory/<project-id>/"

Use the NAS for:

- Authorized encrypted or access-controlled raw transcript archives
- Markdown session summaries
- Project memory snapshots
- Versioned recovery manifests
- Backup verification reports

Use a local working directory for active memory indexing and database operations.

Avoid running a shared SQLite database directly on an SMB/NFS-mounted NAS.

Check access permissions, available storage, and mount availability before synchronization.

Use incremental backup, hashes, and conflict protection.

Never overwrite another developer's memory or synchronize confidential data without approval.

7. Agent instructions and skills

Inspect existing "CLAUDE.md", "AGENTS.md", ".claude/", and relevant skills or plugins.

Preserve existing project instructions.

Add concise, compatible instructions so that every new Claude Code or Codex session:

1. Reads "docs/ai-memory/INDEX.md" and "CURRENT_STATE.md" at startup.
2. Reads "NEXT_ACTIONS.md" before beginning development.
3. Uses memory search to retrieve older decisions only when relevant.
4. Does not load all historical sessions into the context window.
5. Updates project memory after material changes or at a session checkpoint.
6. Records completed work, decisions, blockers, and next actions.
7. Uses relevant source references when recording important facts.
8. Never stores secrets or confidential information in shared memory.

Evaluate whether a reusable project-memory skill is needed.

Prefer existing supported tools rather than creating a custom MCP server unless a real integration gap exists.

If a custom skill, hook, or MCP bridge is necessary, explain the justification, security model, maintenance burden, and proposed files before implementation.

8. Token and performance optimization

Optimize for low token overhead.

Use progressive context retrieval:

- Startup: short project overview, current state, and next actions.
- Task-specific retrieval: relevant architecture or decision records.
- Historical research: selected session summaries.
- Deep investigation: authorized original transcripts only when necessary.

Do not dump complete JSONL transcripts into the active agent context.

Process old sessions in bounded batches and write intermediate results to disk.

Deduplicate repeated information.

Never summarize the same session repeatedly unless its source has changed.

Track approximate processing costs, memory overhead, and indexing performance when measurable.

Avoid expensive AI observers if a simpler local solution meets the requirements.

9. Recovery validation

Test the resulting system in a clean, new agent session.

The new session must be able to answer:

- What does this software do?
- What is its architecture?
- Which important technical decisions were made?
- What development work is already complete?
- What bugs remain unresolved?
- What task should be implemented next?
- Which historical session supports a particular decision?

Verify that answers are consistent with source files and recovered session notes.

Test memory retrieval without relying on the original conversation's context.

If Claude-Mem is enabled, separately verify that its hooks actually capture a new test session and that a subsequent session can retrieve it.

10. Completion report

Produce:

- Total historical sessions discovered
- Total sessions successfully archived
- Total sessions summarized into Markdown
- Partial or failed imports
- Missing sessions and suspected reasons
- Historical date range covered
- Project memory directory
- NAS backup status
- Claude-Mem and MCP integration status
- Security and privacy configuration
- Token and performance findings
- Recovery verification results
- Instructions for using the system from a new account or developer machine

Report actual results rather than assumed success.

Execution rules

This is an existing production-development project.

Do not modify application functionality, run destructive commands, remove old sessions, overwrite Git history, or alter production infrastructure.

Begin immediately with a read-only discovery and backup plan.

Carry out safe local analysis and create the project memory files once the destination and permissions are verified.

Request approval before installing software, making external network calls that transmit project information, modifying agent configuration, or copying sensitive content to shared storage.

Do not stop at a generic architecture recommendation. Recover and organize the available project knowledge, then verify that the result can be used by a fresh agent session.

Your final objective is account-independent, searchable, reliable, and durable project memory.
