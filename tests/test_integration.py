"""scdb_pgsql 集成测试 — 基于真实 PostgreSQL 数据库.

测试前提:
    1. 执行 create_test_db.sql 初始化测试数据库:
       psql -h pgpool.lan -U postgres -f create_test_db.sql

    2. 运行集成测试:
       pytest tests/test_integration.py -v

测试数据库信息:
    - Host: pgpool.lan
    - Database: test_5002
    - User: test5002
    - Password: Love2026
    - Table: t_test5002 (id SERIAL, name VARCHAR)
"""

from __future__ import annotations

import json

import pytest

from scdb_pgsql import (
    SCDBPgSQL,
    SCDBPgSQLMeta,
    SCDBConnectionError,
    SCDBQueryError,
    SCDBTransactionError,
)

# ====================================================================
# 测试配置
# ====================================================================

# 标记本文件所有测试为集成测试
pytestmark = pytest.mark.integration

TEST_META = SCDBPgSQLMeta(
    host="pgpool.lan",
    port=5432,
    database="test_5002",
    user="test5002",
    password="Love2026",
    min_connections=1,
    max_connections=5,
)

TABLE = "t_test5002"


# ====================================================================
# Fixtures
# ====================================================================


@pytest.fixture(scope="module")
def db():
    """创建模块级别的数据库连接，所有测试共享.

    Yields:
        SCDBPgSQL 实例.
    """
    instance = SCDBPgSQL(TEST_META)
    yield instance
    instance.close()


@pytest.fixture(autouse=True)
def clean_table(db):
    """每个测试前后清空测试表.

    确保每个测试从干净的状态开始。
    """
    # db.execute(f"TRUNCATE TABLE {TABLE} RESTART IDENTITY CASCADE")
    db.execute(f"DELETE FROM {TABLE}")
    yield
    # db.execute(f"TRUNCATE TABLE {TABLE} RESTART IDENTITY CASCADE")
    db.execute(f"DELETE FROM {TABLE}")


# ====================================================================
# 连接测试
# ====================================================================


class TestConnection:
    """数据库连接测试."""

    def test_connection_success(self, db):
        """验证连接测试成功."""
        assert db.test_connection() is True

    def test_connection_failure(self):
        """验证错误连接信息导致连接失败."""
        bad_meta = SCDBPgSQLMeta(
            host="pgpool.lan",
            port=5432,
            database="nonexistent_db_xyz",
            user="test5002",
            password="Love2026",
        )
        with pytest.raises(SCDBConnectionError):
            SCDBPgSQL(bad_meta)

    def test_context_manager(self):
        """验证上下文管理器正常工作."""
        with SCDBPgSQL(TEST_META) as db:
            assert db.test_connection() is True


# ====================================================================
# 单条 CRUD 测试
# ====================================================================


class TestCRUD:
    """基础增删改查操作测试."""

    def test_insert_and_fetch(self, db):
        """验证插入并查询单条记录."""
        db.execute(
            f"INSERT INTO {TABLE} (name) VALUES (%s)", ("Alice",)
        )
        rows = db.fetch_all(f"SELECT id, name FROM {TABLE}")
        assert len(rows) == 1
        assert rows[0][1] == "Alice"

    def test_update(self, db):
        """验证更新操作."""
        db.execute(
            f"INSERT INTO {TABLE} (name) VALUES (%s)", ("Alice",)
        )
        affected = db.execute(
            f"UPDATE {TABLE} SET name = %s WHERE name = %s",
            ("Bob", "Alice"),
        )
        assert affected == 1

        rows = db.fetch_all(f"SELECT name FROM {TABLE}")
        assert rows[0][0] == "Bob"

    def test_delete(self, db):
        """验证删除操作."""
        db.execute(
            f"INSERT INTO {TABLE} (name) VALUES (%s)", ("Alice",)
        )
        affected = db.execute(
            f"DELETE FROM {TABLE} WHERE name = %s", ("Alice",)
        )
        assert affected == 1

        rows = db.fetch_all(f"SELECT * FROM {TABLE}")
        assert len(rows) == 0

    def test_insert_returns_rowcount(self, db):
        """验证 execute 返回受影响的行数."""
        result = db.execute(
            f"INSERT INTO {TABLE} (name) VALUES (%s)", ("Alice",)
        )
        assert result == 1


# ====================================================================
# 多格式输出测试
# ====================================================================


class TestResultFormats:
    """查询结果格式测试."""

    def _insert_test_data(self, db):
        """插入测试数据."""
        db.execute_many(
            f"INSERT INTO {TABLE} (name) VALUES (%s)",
            [("Alice",), ("Bob",), ("Charlie",)],
        )

    def test_tuple_format(self, db):
        """验证 tuple 格式 (默认)."""
        self._insert_test_data(db)
        rows = db.fetch_all(f"SELECT id, name FROM {TABLE} ORDER BY id")
        assert isinstance(rows, list)
        assert isinstance(rows[0], tuple)
        assert len(rows) == 3
        assert rows[0][1] == "Alice"

    def test_dictionary_format(self, db):
        """验证 dictionary 格式."""
        self._insert_test_data(db)
        rows = db.fetch_all(
            f"SELECT id, name FROM {TABLE} ORDER BY id",
            result_format="dict",
        )
        assert isinstance(rows, list)
        assert isinstance(rows[0], dict)
        assert rows[0]["name"] == "Alice"
        assert "id" in rows[0]

    def test_json_format(self, db):
        """验证 json 格式."""
        self._insert_test_data(db)
        result = db.fetch_all(
            f"SELECT id, name FROM {TABLE} ORDER BY id",
            result_format="json",
        )
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert len(parsed) == 3
        assert parsed[0]["name"] == "Alice"

    def test_dataframe_format(self, db):
        """验证 dataframe 格式."""
        import pandas as pd

        self._insert_test_data(db)
        df = db.fetch_all(
            f"SELECT id, name FROM {TABLE} ORDER BY id",
            result_format="df",
        )
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["id", "name"]
        assert len(df) == 3
        assert df.iloc[0]["name"] == "Alice"

    def test_empty_result_all_formats(self, db):
        """验证空结果集在所有格式下正常工作."""
        import pandas as pd

        # tuple
        assert db.fetch_all(f"SELECT * FROM {TABLE}") == []

        # dictionary
        assert db.fetch_all(
            f"SELECT * FROM {TABLE}", result_format="dict"
        ) == []

        # json
        assert json.loads(
            db.fetch_all(f"SELECT * FROM {TABLE}", result_format="json")
        ) == []

        # dataframe
        df = db.fetch_all(
            f"SELECT * FROM {TABLE}", result_format="df"
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


# ====================================================================
# 分页查询测试
# ====================================================================


class TestPagination:
    """分页查询测试."""

    def _insert_bulk_data(self, db, count=25):
        """插入批量测试数据."""
        db.execute_values(
            f"INSERT INTO {TABLE} (name) VALUES %s",
            [(f"user_{i:03d}",) for i in range(1, count + 1)],
        )

    def test_first_page(self, db):
        """验证第一页数据."""
        self._insert_bulk_data(db)
        result = db.fetch_page(
            f"SELECT id, name FROM {TABLE} ORDER BY id",
            page=1,
            page_size=10,
        )
        assert result["page"] == 1
        assert result["page_size"] == 10
        assert result["total"] == 25
        assert result["total_pages"] == 3
        assert len(result["data"]) == 10

    def test_middle_page(self, db):
        """验证中间页数据."""
        self._insert_bulk_data(db)
        result = db.fetch_page(
            f"SELECT id, name FROM {TABLE} ORDER BY id",
            page=2,
            page_size=10,
        )
        assert result["page"] == 2
        assert len(result["data"]) == 10

    def test_last_page(self, db):
        """验证最后一页数据 (不满页)."""
        self._insert_bulk_data(db)
        result = db.fetch_page(
            f"SELECT id, name FROM {TABLE} ORDER BY id",
            page=3,
            page_size=10,
        )
        assert result["page"] == 3
        assert len(result["data"]) == 5
        assert result["total"] == 25

    def test_empty_page(self, db):
        """验证超出范围的页码."""
        self._insert_bulk_data(db, count=5)
        result = db.fetch_page(
            f"SELECT id, name FROM {TABLE} ORDER BY id",
            page=10,
            page_size=10,
        )
        assert result["total"] == 5
        assert len(result["data"]) == 0

    def test_page_with_dictionary_format(self, db):
        """验证分页查询支持 dictionary 格式."""
        self._insert_bulk_data(db, count=5)
        result = db.fetch_page(
            f"SELECT id, name FROM {TABLE} ORDER BY id",
            page=1,
            page_size=3,
            result_format="dict",
        )
        assert isinstance(result["data"][0], dict)
        assert "name" in result["data"][0]
        assert result["total"] == 5
        assert result["total_pages"] == 2

    def test_page_with_params(self, db):
        """验证带参数的分页查询."""
        self._insert_bulk_data(db, count=20)
        result = db.fetch_page(
            f"SELECT id, name FROM {TABLE} WHERE id > %s ORDER BY id",
            params=(0,),
            page=1,
            page_size=10,
        )
        assert result["total"] == 20
        assert len(result["data"]) == 10


# ====================================================================
# 批量操作测试
# ====================================================================


class TestBatchOperations:
    """批量操作测试."""

    def test_execute_many(self, db):
        """验证 execute_many 批量插入."""
        result = db.execute_many(
            f"INSERT INTO {TABLE} (name) VALUES (%s)",
            [("Alice",), ("Bob",), ("Charlie",)],
        )
        # execute_batch 的 rowcount 行为：返回最后一批的 rowcount
        rows = db.fetch_all(f"SELECT * FROM {TABLE}")
        assert len(rows) == 3

    def test_execute_values(self, db):
        """验证 execute_values 批量插入."""
        result = db.execute_values(
            f"INSERT INTO {TABLE} (name) VALUES %s",
            [("Alice",), ("Bob",), ("Charlie",), ("David",), ("Eve",)],
        )
        assert result == 5
        rows = db.fetch_all(f"SELECT * FROM {TABLE}")
        assert len(rows) == 5

    def test_execute_many_large_batch(self, db):
        """验证大批量插入."""
        data = [(f"user_{i}",) for i in range(500)]
        db.execute_many(
            f"INSERT INTO {TABLE} (name) VALUES (%s)", data
        )
        rows = db.fetch_all(f"SELECT COUNT(*) FROM {TABLE}")
        assert rows[0][0] == 500

    def test_execute_values_large_batch(self, db):
        """验证 execute_values 大批量插入."""
        data = [(f"user_{i}",) for i in range(1000)]
        result = db.execute_values(
            f"INSERT INTO {TABLE} (name) VALUES %s", data
        )
        assert result == 1000


# ====================================================================
# 事务测试
# ====================================================================


class TestTransaction:
    """事务管理测试."""

    def test_transaction_commit(self, db):
        """验证事务正常提交."""
        with db.transaction() as tx:
            tx.execute(
                f"INSERT INTO {TABLE} (name) VALUES (%s)", ("Alice",)
            )
            tx.execute(
                f"INSERT INTO {TABLE} (name) VALUES (%s)", ("Bob",)
            )

        rows = db.fetch_all(f"SELECT * FROM {TABLE}")
        assert len(rows) == 2

    def test_transaction_rollback(self, db):
        """验证事务异常回滚."""
        # 先插入一条基准数据
        db.execute(
            f"INSERT INTO {TABLE} (name) VALUES (%s)", ("Baseline",)
        )

        with pytest.raises(SCDBTransactionError):
            with db.transaction() as tx:
                tx.execute(
                    f"INSERT INTO {TABLE} (name) VALUES (%s)", ("Alice",)
                )
                # 故意触发异常
                raise RuntimeError("模拟业务异常")

        # 事务回滚后只保留基准数据
        rows = db.fetch_all(f"SELECT * FROM {TABLE}")
        assert len(rows) == 1
        assert rows[0][1] == "Baseline"

    def test_transaction_execute_many(self, db):
        """验证事务内批量执行."""
        with db.transaction() as tx:
            tx.execute_many(
                f"INSERT INTO {TABLE} (name) VALUES (%s)",
                [("Alice",), ("Bob",), ("Charlie",)],
            )

        rows = db.fetch_all(f"SELECT * FROM {TABLE}")
        assert len(rows) == 3

    def test_transaction_execute_values(self, db):
        """验证事务内 execute_values."""
        with db.transaction() as tx:
            tx.execute_values(
                f"INSERT INTO {TABLE} (name) VALUES %s",
                [("Alice",), ("Bob",)],
            )

        rows = db.fetch_all(f"SELECT * FROM {TABLE}")
        assert len(rows) == 2

    def test_transaction_mixed_operations(self, db):
        """验证事务内混合操作 (INSERT + UPDATE + DELETE)."""
        with db.transaction() as tx:
            tx.execute(
                f"INSERT INTO {TABLE} (name) VALUES (%s)", ("Alice",)
            )
            tx.execute(
                f"INSERT INTO {TABLE} (name) VALUES (%s)", ("Bob",)
            )
            tx.execute(
                f"UPDATE {TABLE} SET name = %s WHERE name = %s",
                ("Alice_Updated", "Alice"),
            )
            tx.execute(
                f"DELETE FROM {TABLE} WHERE name = %s", ("Bob",)
            )

        rows = db.fetch_all(
            f"SELECT name FROM {TABLE} ORDER BY id"
        )
        assert len(rows) == 1
        assert rows[0][0] == "Alice_Updated"

    def test_transaction_rollback_preserves_previous_data(self, db):
        """验证事务回滚不影响已提交的数据."""
        # 第一个事务：正常提交
        with db.transaction() as tx:
            tx.execute(
                f"INSERT INTO {TABLE} (name) VALUES (%s)", ("Committed",)
            )

        # 第二个事务：回滚
        with pytest.raises(SCDBTransactionError):
            with db.transaction() as tx:
                tx.execute(
                    f"INSERT INTO {TABLE} (name) VALUES (%s)", ("Rollback",)
                )
                raise RuntimeError("故意回滚")

        # 只有第一个事务的数据保留
        rows = db.fetch_all(f"SELECT name FROM {TABLE}")
        assert len(rows) == 1
        assert rows[0][0] == "Committed"
