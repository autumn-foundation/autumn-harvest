# 🔭 Vantage: Spec for Workflow Search

## 👤 User Story
As an Operations Engineer or Customer Support Rep, I want to query workflows by business-specific custom attributes (e.g., `customer_id`, `order_status`) using a flexible query syntax, so that I can quickly find, debug, or perform bulk actions on specific subsets of running or completed workflows without scanning the entire database.

## 📈 The "So What?" (Business Value)
When systems scale, finding a specific workflow by its auto-generated UUID is impossible without external indices. Customer support needs to find "all failed workflows for tenant X" instantly to resolve tickets. Without this, engineers waste hours writing ad-hoc database queries or building custom indexing solutions outside of the workflow engine. Native, structured search turns the workflow engine from a black box into a manageable, observable system, drastically reducing Mean Time to Resolution (MTTR) for operational issues.

**Metric Definition:**
- Success = P99 query latency for complex searches across millions of workflows is under 500ms.
- 0 manual database queries required by support teams to locate workflows by business identifiers.

## 🕵️ Gap Analysis
- **Temporal:** Provides visibility via Elasticsearch, supporting SQL-like queries on `search_attributes`.
- **Current State (`autumn-harvest`):** Workflows are stored in Postgres. There is an index on `search_attrs` (`USING GIN (search_attrs)`), but no API or query language to take advantage of it safely without direct SQL access.

## ✅ Acceptance Criteria
- Must define a schema for standardizing `search_attrs` (e.g., string, integer, boolean, datetime).
- Must provide a robust, SQL-like query syntax (e.g., `Status = 'Failed' AND customer_id = '123'`) exposed through the Management HTTP API.
- Must parse the query language safely and translate it into optimized Postgres queries utilizing the existing GIN index.
- Must allow workflows to upsert (update/insert) their custom search attributes during execution.
- Must support pagination for large result sets.

## 🚫 Out of Scope
- Full-text search on unstructured event history or log payloads.
- Integration with external search engines like Elasticsearch (we will rely exclusively on Postgres GIN indexing for Phase 4).
- Real-time subscriptions or alerts based on search queries.
