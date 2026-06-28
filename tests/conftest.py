"""pytest 全局配置与 fixtures.

为测试提供通用的 mock 对象和 fixture，避免依赖真实数据库。
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from scdb_pgsql.meta import SCDBPgSQLMeta


@pytest.fixture()
def meta() -> SCDBPgSQLMeta:
    """创建测试用的 SCDBPgSQLMeta 实例."""
    return SCDBPgSQLMeta(
        host="localhost",
        port=5432,
        database="testdb",
        user="testuser",
        password="testpass",
        min_connections=1,
        max_connections=5,
    )


@pytest.fixture()
def mock_pool():
    """创建 mock 的 ThreadedConnectionPool.

    Returns:
        tuple: (mock_pool_instance, mock_pool_class_patcher)
    """
    with patch("scdb_pgsql.core.psycopg2.pool.ThreadedConnectionPool") as mock_cls:
        mock_instance = MagicMock()
        mock_instance.closed = False
        mock_cls.return_value = mock_instance
        yield mock_instance


@pytest.fixture()
def db(meta, mock_pool):
    """创建带有 mock 连接池的 SCDBPgSQL 实例.

    Args:
        meta: 测试用元数据.
        mock_pool: mock 连接池.

    Returns:
        已初始化的 SCDBPgSQL 实例.
    """
    from scdb_pgsql.core import SCDBPgSQL

    instance = SCDBPgSQL(meta)
    return instance
