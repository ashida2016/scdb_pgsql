"""scdb_pgsql — 高性能 PostgreSQL 数据库操作包.

本包提供了基于连接池的 PostgreSQL 数据库操作接口，
支持多格式查询结果、分页、批量操作和事务管理。

Example:
    >>> from scdb_pgsql import SCDBPgSQL, SCDBPgSQLMeta
    >>> meta = SCDBPgSQLMeta(
    ...     database="mydb", user="admin", password="secret"
    ... )
    >>> with SCDBPgSQL(meta) as db:
    ...     rows = db.fetch_all("SELECT * FROM users")
"""

__version__ = "0.3.0"

from scdb_pgsql.core import SCDBPgSQL
from scdb_pgsql.exceptions import (
    SCDBConnectionError,
    SCDBPgSQLError,
    SCDBQueryError,
    SCDBTransactionError,
)
from scdb_pgsql.meta import SCDBPgSQLMeta

__all__ = [
    "SCDBPgSQL",
    "SCDBPgSQLMeta",
    "SCDBPgSQLError",
    "SCDBConnectionError",
    "SCDBQueryError",
    "SCDBTransactionError",
]
