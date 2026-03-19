# ADR-0001: Domain-Agnostic ETL Core

## Status
Accepted

## Context
The previous codebase coupled core pipeline logic to a specific beneficiaries/gifts domain.

## Decision
We introduced a src-layout domain-agnostic core under `src/etl_pipeline`, with:
- configurable N-source extraction
- declarative joins
- optional hooks/middleware/schema/checkpoint features
- sync + async execution modes

## Consequences
- library users can build arbitrary pipelines without editing source files
- examples preserve legacy domain use cases outside core
