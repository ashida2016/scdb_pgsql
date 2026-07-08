"""SCDBPgSQL 核心模块单元测试.

基于 unittest.mock 的单元测试，无需真实数据库即可验证
全部功能路径。覆盖连接管理、查询、批量操作、事务和异常处理。
"""

from __future__ import annotations

import json
import math
import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch, call

import pytest

from scdb_pgsql import (
    SCDBPgSQL,
    SCDBPgSQLMeta,
    SCDBConnectionError,
    SCDBPgSQLError,
    SCDBQueryError,
    SCDBTransactionError,
)

import psycopg2.pool


# ====================================================================
# SCDBPgSQLMeta 测试
# ====================================================================


class TestSCDBPgSQLMeta:
    """SCDBPgSQLMeta 数据类测试."""

    def test_creation_with_defaults(self):
        """验证使用默认值创建实例."""
        meta = SCDBPgSQLMeta(database="db", user="u", password="p")
        assert meta.host == "localhost"
        assert meta.port == 5432
        assert meta.min_connections == 1
        assert meta.max_connections == 10
        assert meta.connect_timeout == 5
        assert meta.options is None
        assert meta.sslmode is None

    def test_creation_with_custom_values(self):
        """验证使用自定义值创建实例."""
        meta = SCDBPgSQLMeta(
            host="db.example.com",
            port=5433,
            database="prod",
            user="admin",
            password="s3cret",
            min_connections=5,
            max_connections=50,
            connect_timeout=10,
            options="-c search_path=myschema",
            sslmode="require",
        )
        assert meta.host == "db.example.com"
        assert meta.port == 5433
        assert meta.max_connections == 50
        assert meta.sslmode == "require"

    def test_immutability(self):
        """验证实例不可变 (frozen=True)."""
        meta = SCDBPgSQLMeta(database="db", user="u", password="p")
        with pytest.raises(AttributeError):
            meta.host = "other"  # type: ignore[misc]

    def test_validation_empty_database(self):
        """验证 database 为空时抛出异常."""
        with pytest.raises(ValueError, match="database 不能为空"):
            SCDBPgSQLMeta(database="", user="u", password="p")

    def test_validation_empty_user(self):
        """验证 user 为空时抛出异常."""
        with pytest.raises(ValueError, match="user 不能为空"):
            SCDBPgSQLMeta(database="db", user="", password="p")

    def test_validation_empty_password(self):
        """验证 password 为空时抛出异常."""
        with pytest.raises(ValueError, match="password 不能为空"):
            SCDBPgSQLMeta(database="db", user="u", password="")

    def test_validation_negative_min_connections(self):
        """验证 min_connections 为负数时抛出异常."""
        with pytest.raises(ValueError, match="min_connections 不能为负数"):
            SCDBPgSQLMeta(
                database="db", user="u", password="p", min_connections=-1
            )

    def test_validation_zero_max_connections(self):
        """验证 max_connections 为 0 时抛出异常."""
        with pytest.raises(ValueError, match="max_connections 必须至少为 1"):
            SCDBPgSQLMeta(
                database="db", user="u", password="p", max_connections=0
            )

    def test_validation_min_exceeds_max(self):
        """验证 min_connections > max_connections 时抛出异常."""
        with pytest.raises(
            ValueError, match="min_connections 不能大于 max_connections"
        ):
            SCDBPgSQLMeta(
                database="db",
                user="u",
                password="p",
                min_connections=10,
                max_connections=5,
            )

    def test_validation_negative_timeout(self):
        """验证 connect_timeout 为负数时抛出异常."""
        with pytest.raises(ValueError, match="connect_timeout 不能为负数"):
            SCDBPgSQLMeta(
                database="db", user="u", password="p", connect_timeout=-1
            )

    def test_to_kwargs_basic(self):
        """验证 to_kwargs 返回正确的基础参数."""
        meta = SCDBPgSQLMeta(database="db", user="u", password="p")
        kwargs = meta.to_kwargs()
        assert kwargs["host"] == "localhost"
        assert kwargs["port"] == 5432
        assert kwargs["database"] == "db"
        assert kwargs["user"] == "u"
        assert kwargs["password"] == "p"
        assert kwargs["connect_timeout"] == 5
        assert "options" not in kwargs
        assert "sslmode" not in kwargs

    def test_to_kwargs_with_optional(self):
        """验证 to_kwargs 包含可选参数."""
        meta = SCDBPgSQLMeta(
            database="db",
            user="u",
            password="p",
            options="-c search_path=public",
            sslmode="require",
        )
        kwargs = meta.to_kwargs()
        assert kwargs["options"] == "-c search_path=public"
        assert kwargs["sslmode"] == "require"

    def test_to_dsn_basic(self):
        """验证 to_dsn 生成正确的基础 DSN."""
        meta = SCDBPgSQLMeta(database="db", user="u", password="p")
        dsn = meta.to_dsn()
        assert dsn.startswith("postgresql://u:p@localhost:5432/db?")
        assert "connect_timeout=5" in dsn

    def test_to_dsn_with_ssl(self):
        """验证 to_dsn 包含 SSL 参数."""
        meta = SCDBPgSQLMeta(
            database="db", user="u", password="p", sslmode="verify-full"
        )
        dsn = meta.to_dsn()
        assert "sslmode=verify-full" in dsn


# ====================================================================
# 异常层次结构测试
# ====================================================================


class TestExceptions:
    """异常类层次结构测试."""

    def test_base_exception_hierarchy(self):
        """验证异常继承关系."""
        assert issubclass(SCDBConnectionError, SCDBPgSQLError)
        assert issubclass(SCDBQueryError, SCDBPgSQLError)
        assert issubclass(SCDBTransactionError, SCDBPgSQLError)
        assert issubclass(SCDBPgSQLError, Exception)

    def test_exception_message(self):
        """验证异常消息传递."""
        exc = SCDBPgSQLError("test message")
        assert str(exc) == "test message"
        assert exc.message == "test message"

    def test_exception_catch_by_base(self):
        """验证可通过基类统一捕获."""
        with pytest.raises(SCDBPgSQLError):
            raise SCDBConnectionError("conn error")


# ====================================================================
# SCDBPgSQL 连接与初始化测试
# ====================================================================


class TestSCDBPgSQLInit:
    """SCDBPgSQL 初始化与连接管理测试."""

    def test_init_creates_pool(self, meta):
        """验证初始化时创建连接池."""
        with patch(
            "scdb_pgsql.core.psycopg2.pool.ThreadedConnectionPool"
        ) as mock_cls:
            mock_cls.return_value = MagicMock(closed=False)
            db = SCDBPgSQL(meta)
            mock_cls.assert_called_once()
            call_kwargs = mock_cls.call_args
            assert call_kwargs[1]["minconn"] == 1
            assert call_kwargs[1]["maxconn"] == 5

    def test_init_pool_failure(self, meta):
        """验证连接池创建失败时抛出 SCDBConnectionError."""
        import psycopg2

        with patch(
            "scdb_pgsql.core.psycopg2.pool.ThreadedConnectionPool"
        ) as mock_cls:
            mock_cls.side_effect = psycopg2.OperationalError("connection refused")
            with pytest.raises(SCDBConnectionError, match="连接池初始化失败"):
                SCDBPgSQL(meta)

    def test_context_manager(self, db, mock_pool):
        """验证上下文管理器正确关闭连接池."""
        with db as instance:
            assert instance is db
        mock_pool.closeall.assert_called_once()

    def test_close(self, db, mock_pool):
        """验证 close 方法关闭连接池."""
        db.close()
        mock_pool.closeall.assert_called_once()

    def test_close_already_closed(self, db, mock_pool):
        """验证重复关闭是安全的."""
        mock_pool.closed = True
        db.close()  # 不应抛出异常
        mock_pool.closeall.assert_not_called()


# ====================================================================
# test_connection 测试
# ====================================================================


class TestTestConnection:
    """连接测试方法测试."""

    def test_connection_success(self, db, mock_pool):
        """验证连接测试成功."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn

        assert db.test_connection() is True
        mock_pool.putconn.assert_called_once_with(mock_conn)

    def test_connection_failure(self, db, mock_pool):
        """验证连接测试失败返回 False."""
        import psycopg2

        mock_pool.getconn.side_effect = psycopg2.OperationalError("fail")

        assert db.test_connection() is False

    def test_connection_pool_closed(self, meta):
        """验证连接池关闭后测试返回 False."""
        with patch(
            "scdb_pgsql.core.psycopg2.pool.ThreadedConnectionPool"
        ) as mock_cls:
            mock_instance = MagicMock()
            mock_instance.closed = False
            mock_cls.return_value = mock_instance
            db = SCDBPgSQL(meta)
            mock_instance.closed = True
            assert db.test_connection() is False


# ====================================================================
# fetch_all 测试
# ====================================================================


class TestFetchAll:
    """fetch_all 查询测试."""

    def _setup_cursor(self, mock_pool, rows, description):
        """辅助方法：设置 mock cursor 的返回值."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = rows
        mock_cursor.description = description
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn
        return mock_conn, mock_cursor

    def test_fetch_all_tuple_format(self, db, mock_pool):
        """验证 tuple 格式返回结果."""
        rows = [(1, "Alice"), (2, "Bob")]
        desc = [("id",), ("name",)]
        self._setup_cursor(mock_pool, rows, desc)

        result = db.fetch_all("SELECT * FROM users")
        assert result == rows

    def test_fetch_all_dictionary_format(self, db, mock_pool):
        """验证 dictionary 格式返回结果."""
        rows = [(1, "Alice"), (2, "Bob")]
        desc = [("id",), ("name",)]
        self._setup_cursor(mock_pool, rows, desc)

        result = db.fetch_all(
            "SELECT * FROM users", result_format="dict"
        )
        assert result == [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

    def test_fetch_all_json_format(self, db, mock_pool):
        """验证 json 格式返回结果."""
        rows = [(1, "Alice")]
        desc = [("id",), ("name",)]
        self._setup_cursor(mock_pool, rows, desc)

        result = db.fetch_all(
            "SELECT * FROM users", result_format="json"
        )
        parsed = json.loads(result)
        assert parsed == [{"id": 1, "name": "Alice"}]

    def test_fetch_all_dataframe_format(self, db, mock_pool):
        """验证 dataframe 格式返回结果."""
        rows = [(1, "Alice"), (2, "Bob")]
        desc = [("id",), ("name",)]
        self._setup_cursor(mock_pool, rows, desc)

        result = db.fetch_all(
            "SELECT * FROM users", result_format="df"
        )
        import pandas as pd

        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["id", "name"]
        assert len(result) == 2

    def test_fetch_all_with_params(self, db, mock_pool):
        """验证带参数的查询."""
        rows = [(1, "Alice")]
        desc = [("id",), ("name",)]
        mock_conn, mock_cursor = self._setup_cursor(mock_pool, rows, desc)

        db.fetch_all(
            "SELECT * FROM users WHERE id = %s", params=(1,)
        )
        mock_cursor.execute.assert_called_once_with(
            "SELECT * FROM users WHERE id = %s", (1,)
        )

    def test_fetch_all_empty_result(self, db, mock_pool):
        """验证空结果集."""
        self._setup_cursor(mock_pool, [], [("id",), ("name",)])
        result = db.fetch_all("SELECT * FROM users")
        assert result == []

    def test_fetch_all_query_error(self, db, mock_pool):
        """验证查询错误时抛出 SCDBQueryError."""
        import psycopg2

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg2.ProgrammingError(
            "syntax error"
        )
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn

        with pytest.raises(SCDBQueryError, match="查询执行失败"):
            db.fetch_all("INVALID SQL")

    def test_fetch_all_xml_format(self, db, mock_pool):
        """验证 xml 格式返回结果."""
        rows = [(1, "Alice"), (2, "Bob")]
        desc = [("id",), ("name",)]
        self._setup_cursor(mock_pool, rows, desc)

        result = db.fetch_all(
            "SELECT * FROM users", result_format="xml"
        )
        root = ET.fromstring(result)
        assert root.tag == "results"
        row_elems = root.findall("row")
        assert len(row_elems) == 2
        assert row_elems[0].find("id").text == "1"
        assert row_elems[0].find("name").text == "Alice"

    def test_fetch_all_yaml_format(self, db, mock_pool):
        """验证 yaml 格式返回结果."""
        import yaml

        rows = [(1, "Alice"), (2, "Bob")]
        desc = [("id",), ("name",)]
        self._setup_cursor(mock_pool, rows, desc)

        result = db.fetch_all(
            "SELECT * FROM users", result_format="yaml"
        )
        parsed = yaml.safe_load(result)
        assert parsed == [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

    def test_fetch_all_csv_format(self, db, mock_pool):
        """验证 csv 格式返回结果."""
        rows = [(1, "Alice"), (2, "Bob")]
        desc = [("id",), ("name",)]
        self._setup_cursor(mock_pool, rows, desc)

        result = db.fetch_all(
            "SELECT * FROM users", result_format="csv"
        )
        lines = result.strip().splitlines()
        assert lines[0] == "id,name"
        assert lines[1] == "1,Alice"
        assert lines[2] == "2,Bob"

    def test_fetch_all_invalid_format(self, db, mock_pool):
        """验证不支持的格式抛出 SCDBQueryError."""
        rows = [(1,)]
        desc = [("id",)]
        self._setup_cursor(mock_pool, rows, desc)

        with pytest.raises(SCDBQueryError, match="不支持的结果格式"):
            db.fetch_all("SELECT 1", result_format="invalid_fmt")  # type: ignore[arg-type]

    def test_fetch_all_conn_returned(self, db, mock_pool):
        """验证连接在查询后被归还到连接池."""
        rows = [(1,)]
        desc = [("id",)]
        mock_conn, _ = self._setup_cursor(mock_pool, rows, desc)

        db.fetch_all("SELECT 1")
        mock_pool.putconn.assert_called_once_with(mock_conn)


# ====================================================================
# fetch_page 测试
# ====================================================================


class TestFetchPaginated:
    """fetch_page 分页查询测试."""

    def _setup_paginated_cursor(self, mock_pool, rows, description):
        """辅助方法：设置分页查询的 mock cursor."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = rows
        mock_cursor.description = description
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn
        return mock_conn, mock_cursor

    def test_paginated_first_page(self, db, mock_pool):
        """验证第一页分页查询."""
        # 模拟 3 条记录中取第一页 (page_size=2)
        rows = [(1, "Alice", 3), (2, "Bob", 3)]
        desc = [("id",), ("name",), ("_scdb_total",)]
        self._setup_paginated_cursor(mock_pool, rows, desc)

        result = db.fetch_page(
            "SELECT id, name FROM users", page=1, page_size=2
        )
        assert result["page"] == 1
        assert result["page_size"] == 2
        assert result["total"] == 3
        assert result["total_pages"] == 2
        assert result["data"] == [(1, "Alice"), (2, "Bob")]

    def test_paginated_last_page(self, db, mock_pool):
        """验证最后一页分页查询."""
        rows = [(3, "Charlie", 3)]
        desc = [("id",), ("name",), ("_scdb_total",)]
        self._setup_paginated_cursor(mock_pool, rows, desc)

        result = db.fetch_page(
            "SELECT id, name FROM users", page=2, page_size=2
        )
        assert result["page"] == 2
        assert result["total"] == 3
        assert result["total_pages"] == 2
        assert len(result["data"]) == 1

    def test_paginated_empty_result_with_count(self, db, mock_pool):
        """验证空结果页仍返回正确的总数."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # 第一次 fetchall 返回空 (分页查询)
        # 第二次 fetchone 返回总数 (COUNT 查询)
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = (5,)
        mock_cursor.description = [("id",), ("name",), ("_scdb_total",)]
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn

        result = db.fetch_page(
            "SELECT id, name FROM users", page=10, page_size=2
        )
        assert result["total"] == 5
        assert result["total_pages"] == 3
        assert result["page"] == 10

    def test_paginated_dictionary_format(self, db, mock_pool):
        """验证分页查询支持 dictionary 格式."""
        rows = [(1, "Alice", 1)]
        desc = [("id",), ("name",), ("_scdb_total",)]
        self._setup_paginated_cursor(mock_pool, rows, desc)

        result = db.fetch_page(
            "SELECT id, name FROM users",
            page=1,
            page_size=10,
            result_format="dict",
        )
        assert result["data"] == [{"id": 1, "name": "Alice"}]

    def test_paginated_invalid_page(self, db):
        """验证 page < 1 时抛出 ValueError."""
        with pytest.raises(ValueError, match="page 必须 >= 1"):
            db.fetch_page("SELECT 1", page=0)

    def test_paginated_invalid_page_size(self, db):
        """验证 page_size < 1 时抛出 ValueError."""
        with pytest.raises(ValueError, match="page_size 必须 >= 1"):
            db.fetch_page("SELECT 1", page_size=0)

    def test_paginated_sql_contains_cte(self, db, mock_pool):
        """验证分页 SQL 使用 CTE 包装."""
        rows = [(1, 1)]
        desc = [("id",), ("_scdb_total",)]
        mock_conn, mock_cursor = self._setup_paginated_cursor(
            mock_pool, rows, desc
        )

        db.fetch_page("SELECT id FROM t", page=1, page_size=10)
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "WITH _scdb_base AS" in executed_sql
        assert "COUNT(*) OVER()" in executed_sql
        assert "LIMIT" in executed_sql
        assert "OFFSET" in executed_sql


# ====================================================================
# execute 测试
# ====================================================================


class TestExecute:
    """execute 单条执行测试."""

    def _setup_execute_cursor(self, mock_pool, rowcount=1):
        """辅助方法：设置 execute 的 mock cursor."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = rowcount
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn
        return mock_conn, mock_cursor

    def test_execute_insert(self, db, mock_pool):
        """验证 INSERT 执行和自动提交."""
        mock_conn, mock_cursor = self._setup_execute_cursor(mock_pool, 1)

        result = db.execute(
            "INSERT INTO users (name) VALUES (%s)", ("Alice",)
        )
        assert result == 1
        mock_conn.commit.assert_called_once()

    def test_execute_update(self, db, mock_pool):
        """验证 UPDATE 返回受影响行数."""
        self._setup_execute_cursor(mock_pool, 5)
        result = db.execute(
            "UPDATE users SET active = true WHERE age > %s", (18,)
        )
        assert result == 5

    def test_execute_delete(self, db, mock_pool):
        """验证 DELETE 操作."""
        self._setup_execute_cursor(mock_pool, 3)
        result = db.execute("DELETE FROM users WHERE active = false")
        assert result == 3

    def test_execute_error_rollback(self, db, mock_pool):
        """验证执行错误时回滚."""
        import psycopg2

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg2.IntegrityError(
            "duplicate key"
        )
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn

        with pytest.raises(SCDBQueryError, match="SQL 执行失败"):
            db.execute("INSERT INTO users (id) VALUES (%s)", (1,))
        mock_conn.rollback.assert_called_once()


# ====================================================================
# execute_many 测试
# ====================================================================


class TestExecuteMany:
    """execute_many 批量执行测试."""

    def test_execute_many_success(self, db, mock_pool):
        """验证批量执行成功."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 3
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn

        with patch("scdb_pgsql.core.psycopg2.extras.execute_batch") as mock_eb:
            params = [("Alice",), ("Bob",), ("Charlie",)]
            result = db.execute_many(
                "INSERT INTO users (name) VALUES (%s)", params
            )
            mock_eb.assert_called_once()
            assert result == 3
            mock_conn.commit.assert_called_once()

    def test_execute_many_custom_page_size(self, db, mock_pool):
        """验证自定义 page_size."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn

        with patch("scdb_pgsql.core.psycopg2.extras.execute_batch") as mock_eb:
            db.execute_many(
                "INSERT INTO t (a) VALUES (%s)",
                [(1,)],
                page_size=500,
            )
            _, kwargs = mock_eb.call_args
            assert kwargs["page_size"] == 500

    def test_execute_many_error_rollback(self, db, mock_pool):
        """验证批量执行错误时回滚."""
        import psycopg2

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn

        with patch("scdb_pgsql.core.psycopg2.extras.execute_batch") as mock_eb:
            mock_eb.side_effect = psycopg2.IntegrityError("constraint")
            with pytest.raises(SCDBQueryError, match="批量执行失败"):
                db.execute_many(
                    "INSERT INTO t (a) VALUES (%s)", [(1,)]
                )
            mock_conn.rollback.assert_called_once()


# ====================================================================
# execute_values 测试
# ====================================================================


class TestExecuteValues:
    """execute_values 批量插入测试."""

    def test_execute_values_success(self, db, mock_pool):
        """验证 execute_values 批量插入成功."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 3
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn

        with patch(
            "scdb_pgsql.core.psycopg2.extras.execute_values"
        ) as mock_ev:
            values = [(1, "a"), (2, "b"), (3, "c")]
            result = db.execute_values(
                "INSERT INTO t (id, name) VALUES %s", values
            )
            mock_ev.assert_called_once()
            assert result == 3
            mock_conn.commit.assert_called_once()

    def test_execute_values_with_template(self, db, mock_pool):
        """验证自定义值模板."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn

        with patch(
            "scdb_pgsql.core.psycopg2.extras.execute_values"
        ) as mock_ev:
            db.execute_values(
                "INSERT INTO geo (pt) VALUES %s",
                [(1.0, 2.0)],
                template="(ST_Point(%s, %s))",
            )
            _, kwargs = mock_ev.call_args
            assert kwargs["template"] == "(ST_Point(%s, %s))"

    def test_execute_values_error_rollback(self, db, mock_pool):
        """验证 execute_values 错误时回滚."""
        import psycopg2

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.getconn.return_value = mock_conn

        with patch(
            "scdb_pgsql.core.psycopg2.extras.execute_values"
        ) as mock_ev:
            mock_ev.side_effect = psycopg2.DataError("invalid data")
            with pytest.raises(SCDBQueryError, match="execute_values 失败"):
                db.execute_values(
                    "INSERT INTO t (a) VALUES %s", [(1,)]
                )
            mock_conn.rollback.assert_called_once()


# ====================================================================
# 事务管理测试
# ====================================================================


class TestTransaction:
    """transaction 事务管理测试."""

    def test_transaction_commit(self, db, mock_pool):
        """验证事务正常提交."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_conn

        with db.transaction() as tx:
            tx.execute("INSERT INTO t (a) VALUES (%s)", (1,))

        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_pool.putconn.assert_called_once_with(mock_conn)

    def test_transaction_rollback_on_error(self, db, mock_pool):
        """验证事务异常时回滚."""
        import psycopg2

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg2.IntegrityError(
            "duplicate"
        )
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_conn

        with pytest.raises(SCDBTransactionError):
            with db.transaction() as tx:
                tx.execute("INSERT INTO t (a) VALUES (%s)", (1,))

        mock_conn.rollback.assert_called()
        mock_cursor.close.assert_called_once()

    def test_transaction_execute_many(self, db, mock_pool):
        """验证事务内批量执行."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 3
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_conn

        with patch("scdb_pgsql.core.psycopg2.extras.execute_batch"):
            with db.transaction() as tx:
                result = tx.execute_many(
                    "INSERT INTO t (a) VALUES (%s)",
                    [(1,), (2,), (3,)],
                )
                assert result == 3

        mock_conn.commit.assert_called_once()

    def test_transaction_execute_values(self, db, mock_pool):
        """验证事务内 execute_values."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 2
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_conn

        with patch("scdb_pgsql.core.psycopg2.extras.execute_values"):
            with db.transaction() as tx:
                result = tx.execute_values(
                    "INSERT INTO t (a, b) VALUES %s",
                    [(1, "x"), (2, "y")],
                )
                assert result == 2

        mock_conn.commit.assert_called_once()

    def test_transaction_rollback_on_generic_exception(self, db, mock_pool):
        """验证非数据库异常也触发回滚."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_conn

        with pytest.raises(SCDBTransactionError, match="事务执行失败，已回滚"):
            with db.transaction() as tx:
                raise RuntimeError("application error")

        mock_conn.rollback.assert_called()

    def test_transaction_autocommit_disabled(self, db, mock_pool):
        """验证事务中 autocommit 被禁用."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_conn

        with db.transaction():
            pass

        # autocommit 应被设置为 False
        assert mock_conn.autocommit is False

    def test_transaction_connection_returned(self, db, mock_pool):
        """验证事务结束后连接被归还."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_conn

        with db.transaction():
            pass

        mock_pool.putconn.assert_called_once_with(mock_conn)


# ====================================================================
# 连接获取/归还测试
# ====================================================================


class TestConnectionManagement:
    """连接获取和归还测试."""

    def test_get_conn_pool_closed(self, db, mock_pool):
        """验证连接池关闭后获取连接失败."""
        mock_pool.closed = True
        with pytest.raises(SCDBConnectionError, match="连接池未初始化或已关闭"):
            db._get_conn()

    def test_get_conn_returns_none(self, db, mock_pool):
        """验证获取到 None 连接时抛出异常."""
        mock_pool.getconn.return_value = None
        with pytest.raises(SCDBConnectionError, match="无法从连接池获取连接"):
            db._get_conn()

    def test_get_conn_pool_error(self, db, mock_pool):
        """验证连接池错误时抛出异常."""
        mock_pool.getconn.side_effect = psycopg2.pool.PoolError(
            "pool exhausted"
        )
        with pytest.raises(SCDBConnectionError, match="从连接池获取连接失败"):
            db._get_conn()

    def test_put_conn_pool_error(self, db, mock_pool):
        """验证归还连接失败时不抛出异常."""
        mock_conn = MagicMock()
        mock_pool.putconn.side_effect = psycopg2.pool.PoolError("error")

        # 不应抛出异常
        db._put_conn(mock_conn)
        mock_conn.close.assert_called_once()


# ====================================================================
# _format_results 静态方法测试
# ====================================================================


class TestFormatResults:
    """_format_results 结果格式转换测试."""

    def test_format_tuple(self):
        """验证 tuple 格式直接返回原始数据."""
        rows = [(1, "a"), (2, "b")]
        result = SCDBPgSQL._format_results(rows, None, "tuple")
        assert result is rows

    def test_format_dictionary(self):
        """验证 dictionary 格式转换."""
        rows = [(1, "a")]
        desc = [("id",), ("name",)]
        result = SCDBPgSQL._format_results(rows, desc, "dict")
        assert result == [{"id": 1, "name": "a"}]

    def test_format_json(self):
        """验证 json 格式序列化."""
        rows = [(1, "a")]
        desc = [("id",), ("name",)]
        result = SCDBPgSQL._format_results(rows, desc, "json")
        parsed = json.loads(result)
        assert parsed == [{"id": 1, "name": "a"}]

    def test_format_dataframe(self):
        """验证 dataframe 格式转换."""
        import pandas as pd

        rows = [(1, "a"), (2, "b")]
        desc = [("id",), ("name",)]
        result = SCDBPgSQL._format_results(rows, desc, "df")
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["id", "name"]

    def test_format_invalid(self):
        """验证无效格式抛出异常."""
        with pytest.raises(SCDBQueryError, match="不支持的结果格式"):
            SCDBPgSQL._format_results([], None, "invalid_fmt")  # type: ignore[arg-type]

    def test_format_xml(self):
        """验证 xml 格式转换."""
        rows = [(1, "a"), (2, "b")]
        desc = [("id",), ("name",)]
        result = SCDBPgSQL._format_results(rows, desc, "xml")
        root = ET.fromstring(result)
        assert root.tag == "results"
        assert len(root.findall("row")) == 2
        assert root.find("row/id").text == "1"
        assert root.find("row/name").text == "a"

    def test_format_xml_none_value(self):
        """验证 xml 格式处理 None 值."""
        rows = [(1, None)]
        desc = [("id",), ("name",)]
        result = SCDBPgSQL._format_results(rows, desc, "xml")
        root = ET.fromstring(result)
        name_text = root.find("row/name").text
        assert name_text is None or name_text == ""

    def test_format_yaml(self):
        """验证 yaml 格式转换."""
        import yaml

        rows = [(1, "a")]
        desc = [("id",), ("name",)]
        result = SCDBPgSQL._format_results(rows, desc, "yaml")
        parsed = yaml.safe_load(result)
        assert parsed == [{"id": 1, "name": "a"}]

    def test_format_csv(self):
        """验证 csv 格式转换."""
        rows = [(1, "a"), (2, "b")]
        desc = [("id",), ("name",)]
        result = SCDBPgSQL._format_results(rows, desc, "csv")
        lines = result.strip().splitlines()
        assert lines[0] == "id,name"
        assert lines[1] == "1,a"
        assert lines[2] == "2,b"

    def test_format_csv_none_value(self):
        """验证 csv 格式处理 None 值."""
        rows = [(1, None)]
        desc = [("id",), ("name",)]
        result = SCDBPgSQL._format_results(rows, desc, "csv")
        lines = result.strip().splitlines()
        assert lines[1] == "1,"

    def test_format_empty_rows(self):
        """验证空行集的格式转换."""
        desc = [("id",), ("name",)]
        result = SCDBPgSQL._format_results([], desc, "dict")
        assert result == []
