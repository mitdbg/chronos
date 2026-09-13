# Chronos Multi-Store Branching Abstract

**Status:** Research abstract; not an API specification

Chronos is a state management layer for agentic applications. Its
vision is to provide one unified branching abstraction across many state stores:
relational databases, filesystems, sandboxes, vector stores, object stores, and
other application-managed state. Agents should be able to fork, mutate,
evaluate, compare, discard, and merge speculative execution paths. Chronos is
bolt-on: existing systems keep their normal APIs, such as SQL for relational
data and POSIX for files.

This matters because useful agent workflows rarely fit inside a single state store. 
In data science, trying a new feature pipeline may change notebooks and
scripts, produce intermediate files, update experiment metadata, and write new
rows into feature or evaluation tables. Similar patterns appear in data cleaning, auto research,simulation, and monte-carlo-tree-search-based optimizations. A failed attempt
should not leave stale artifacts or polluted database state behind. 

Current tools do not provide this capability. Git isolates files but not live
database state. Database transactions provide only transient isolation, and
holding them open across long-latency LLM inference or tool execution reduces concurrency.
Database-native branching systems such as Neon and Doltgres provide stronger
database isolation, but they are heavyweight and do not cover non-database
state. Copying the whole workspace and database is simple, but too slow for broad speculation.

Chronos supplies the shared branch lifecycle across these stores. It can create
and checkout branches instantly. It can discard failed attempts and support
review before promotion. Our preliminary benchmarks show that Chronos provides instant
branching and strong query performance while adding branch semantics to stores
that do not natively support them.
