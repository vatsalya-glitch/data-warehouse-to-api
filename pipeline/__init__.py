"""Real, runnable implementation of the ecommerce order lookup pipeline.

Uses DuckDB as the warehouse and SQLite as the serving database so the
whole pipeline runs locally with no external services -- see
include/README.md for why, and docs/design-pattern.md for the pattern
this implements in production terms (BigQuery/Snowflake + Postgres).
"""
