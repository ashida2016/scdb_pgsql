"""PostgreSQL 数据库操作核心模块.

本模块实现了 SCDBPgSQL 类，提供基于连接池的高性能 PostgreSQL
数据库操作接口，支持多种查询结果格式、分页查询、批量操作和
事务管理。

性能优化措施:
    - 使用 ThreadedConnectionPool 实现线程安全连接池化
    - 使用 execute_batch 替代原生 executemany (3-10x 性能提升)
    - 使用 execute_values 进行高性能批量 INSERT
    - 分页查询使用 COUNT(*) OVER() 窗口函数减少数据库往返
"""

from __future__ import annotations

import json
import logging
import math
from contextlib import contextmanager
from typing import Any, Generator, Literal, Sequence

import psycopg2
import psycopg2.extras
import psycopg2.pool

from scdb_pgsql.exceptions import (
    SCDBConnectionError,
    SCDBQueryError,
    SCDBTransactionError,
)
from scdb_pgsql.meta import SCDBPgSQLMeta

logger = logging.getLogger(__name__)

# 结果格式类型别名
ResultFormat = Literal["tuple", "df", "json", "dict"]

# execute_batch 默认批次大小
_DEFAULT_PAGE_SIZE = 1000


class _TransactionContext:
    """事务上下文对象.

    在 ``SCDBPgSQL.transaction()`` 上下文管理器内部使用，
    提供事务作用域的 execute / execute_many / execute_values 方法。

    该类不应被外部直接实例化。

    Attributes:
        _conn: 事务专用的数据库连接.
        _cursor: 事务专用的游标.
    """

    def __init__(self, conn: Any, cursor: Any) -> None:
        self._conn = conn
        self._cursor = cursor

    def execute(self, sql: str, params: tuple | dict | None = None) -> int:
        """在事务中执行单条 SQL 语句.

        Args:
            sql: 要执行的 SQL 语句.
            params: SQL 参数 (位置参数元组或命名参数字典).

        Returns:
            受影响的行数.

        Raises:
            SCDBTransactionError: SQL 执行失败时.
        """
        try:
            self._cursor.execute(sql, params)
            return self._cursor.rowcount
        except psycopg2.Error as exc:
            raise SCDBTransactionError(
                f"事务内 SQL 执行失败: {exc}"
            ) from exc

    def execute_many(
        self,
        sql: str,
        params_list: Sequence[tuple | dict],
        page_size: int = _DEFAULT_PAGE_SIZE,
    ) -> int:
        """在事务中批量执行 SQL 语句.

        内部使用 ``psycopg2.extras.execute_batch`` 以获得比原生
        ``executemany`` 更好的性能。

        Args:
            sql: 要执行的 SQL 语句模板.
            params_list: 参数列表，每个元素对应一次执行.
            page_size: 每批次发送的语句数量.

        Returns:
            受影响的总行数 (近似值，基于最终 rowcount).

        Raises:
            SCDBTransactionError: 批量执行失败时.
        """
        try:
            psycopg2.extras.execute_batch(
                self._cursor, sql, params_list, page_size=page_size
            )
            return self._cursor.rowcount
        except psycopg2.Error as exc:
            raise SCDBTransactionError(
                f"事务内批量执行失败: {exc}"
            ) from exc

    def execute_values(
        self,
        sql: str,
        values_list: Sequence[tuple],
        template: str | None = None,
        page_size: int = _DEFAULT_PAGE_SIZE,
    ) -> int:
        """在事务中使用 execute_values 进行高性能批量插入.

        Args:
            sql: INSERT 语句模板 (使用 ``%s`` 占位符标记 VALUES 位置).
            values_list: 值列表，每个元素为一行的元组.
            template: 自定义值模板 (如 ``(%s, %s, ST_Point(%s, %s))``).
            page_size: 每批次发送的行数.

        Returns:
            受影响的总行数.

        Raises:
            SCDBTransactionError: 批量插入失败时.
        """
        try:
            psycopg2.extras.execute_values(
                self._cursor,
                sql,
                values_list,
                template=template,
                page_size=page_size,
            )
            return self._cursor.rowcount
        except psycopg2.Error as exc:
            raise SCDBTransactionError(
                f"事务内 execute_values 失败: {exc}"
            ) from exc


class SCDBPgSQL:
    """高性能 PostgreSQL 数据库操作类.

    基于连接池的 PostgreSQL 数据库操作接口，提供增删改查、
    分页查询、批量操作和事务管理功能。

    Attributes:
        _meta: 数据库连接元数据.
        _pool: 线程安全连接池.

    Example:
        >>> from scdb_pgsql import SCDBPgSQL, SCDBPgSQLMeta
        >>> meta = SCDBPgSQLMeta(
        ...     database="mydb", user="admin", password="secret"
        ... )
        >>> with SCDBPgSQL(meta) as db:
        ...     if db.test_connection():
        ...         rows = db.fetch_all("SELECT * FROM users")
    """

    def __init__(self, meta: SCDBPgSQLMeta) -> None:
        """初始化 SCDBPgSQL 实例并创建连接池.

        Args:
            meta: 包含数据库连接参数的元数据实例.

        Raises:
            SCDBConnectionError: 连接池初始化失败时.
        """
        self._meta = meta
        self._pool: psycopg2.pool.ThreadedConnectionPool | None = None
        self._init_pool()

    def _init_pool(self) -> None:
        """初始化线程安全连接池.

        Raises:
            SCDBConnectionError: 连接池创建失败时.
        """
        try:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self._meta.min_connections,
                maxconn=self._meta.max_connections,
                **self._meta.to_kwargs(),
            )
            logger.info(
                "连接池已创建 (min=%d, max=%d, db=%s)",
                self._meta.min_connections,
                self._meta.max_connections,
                self._meta.database,
            )
        except psycopg2.Error as exc:
            raise SCDBConnectionError(
                f"连接池初始化失败: {exc}"
            ) from exc

    def __enter__(self) -> SCDBPgSQL:
        """进入上下文管理器.

        Returns:
            当前 SCDBPgSQL 实例.
        """
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """退出上下文管理器并关闭连接池."""
        self.close()

    def close(self) -> None:
        """关闭连接池并释放所有连接.

        调用后该实例不应再被使用。重复调用是安全的。
        """
        if self._pool is not None and not self._pool.closed:
            self._pool.closeall()
            logger.info("连接池已关闭 (db=%s)", self._meta.database)

    def _get_conn(self) -> Any:
        """从连接池获取一个连接.

        Returns:
            数据库连接对象.

        Raises:
            SCDBConnectionError: 连接池未初始化或获取连接失败时.
        """
        if self._pool is None or self._pool.closed:
            raise SCDBConnectionError("连接池未初始化或已关闭")
        try:
            conn = self._pool.getconn()
            if conn is None:
                raise SCDBConnectionError("无法从连接池获取连接")
            return conn
        except psycopg2.pool.PoolError as exc:
            raise SCDBConnectionError(
                f"从连接池获取连接失败: {exc}"
            ) from exc

    def _put_conn(self, conn: Any) -> None:
        """将连接归还到连接池.

        Args:
            conn: 要归还的数据库连接对象.
        """
        if self._pool is not None and not self._pool.closed:
            try:
                self._pool.putconn(conn)
            except psycopg2.pool.PoolError:
                logger.warning("归还连接到连接池失败，尝试关闭连接")
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass

    # ----------------------------------------------------------------
    # 连接测试
    # ----------------------------------------------------------------

    def test_connection(self) -> bool:
        """测试数据库连接是否可用.

        通过执行 ``SELECT 1`` 验证连接池中的连接是否正常工作。

        Returns:
            连接可用返回 True，否则返回 False.
        """
        conn = None
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                return result is not None and result[0] == 1
        except (psycopg2.Error, SCDBConnectionError) as exc:
            logger.error("连接测试失败: %s", exc)
            return False
        finally:
            if conn is not None:
                conn.rollback()
                self._put_conn(conn)

    # ----------------------------------------------------------------
    # 查询操作 (SELECT)
    # ----------------------------------------------------------------

    @staticmethod
    def _format_results(
        rows: list[tuple],
        description: Any,
        result_format: ResultFormat,
    ) -> Any:
        """将原始查询结果转换为指定格式.

        Args:
            rows: 原始查询结果行列表.
            description: 游标的 description 属性 (列元信息).
            result_format: 目标格式.

        Returns:
            根据 result_format 返回:
            - ``"tuple"``: ``list[tuple]``
            - ``"dict"``: ``list[dict]``
            - ``"json"``: JSON 字符串
            - ``"df"``: ``pandas.DataFrame``

        Raises:
            SCDBQueryError: 格式转换失败时.
        """
        if result_format == "tuple":
            return rows

        columns = [desc[0] for desc in description] if description else []

        if result_format == "dict":
            return [dict(zip(columns, row)) for row in rows]

        if result_format == "json":
            dict_rows = [dict(zip(columns, row)) for row in rows]
            try:
                return json.dumps(dict_rows, ensure_ascii=False, default=str)
            except (TypeError, ValueError) as exc:
                raise SCDBQueryError(
                    f"JSON 序列化失败: {exc}"
                ) from exc

        if result_format == "df":
            try:
                import pandas as pd  # noqa: PLC0415

                return pd.DataFrame(rows, columns=columns)
            except ImportError as exc:
                raise SCDBQueryError(
                    "使用 df 格式需要安装 pandas: "
                    "pip install pandas"
                ) from exc

        raise SCDBQueryError(f"不支持的结果格式: {result_format}")

    def fetch_all(
        self,
        sql: str,
        params: tuple | dict | None = None,
        result_format: ResultFormat = "tuple",
    ) -> Any:
        """执行查询并一次性返回全部结果.

        Args:
            sql: SELECT 查询语句.
            params: SQL 参数.
            result_format: 结果格式，支持 ``"tuple"`` (默认)、
                ``"df"``、``"json"``、``"dict"``.

        Returns:
            查询结果，格式由 result_format 决定.

        Raises:
            SCDBQueryError: 查询执行或结果转换失败时.
        """
        conn = None
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                return self._format_results(rows, cur.description, result_format)
        except SCDBQueryError:
            raise
        except psycopg2.Error as exc:
            raise SCDBQueryError(f"查询执行失败: {exc}") from exc
        finally:
            if conn is not None:
                conn.rollback()
                self._put_conn(conn)

    def fetch_page(
        self,
        sql: str,
        params: tuple | dict | None = None,
        page: int = 1,
        page_size: int = 50,
        result_format: ResultFormat = "tuple",
    ) -> dict[str, Any]:
        """执行分页查询.

        使用 ``COUNT(*) OVER()`` 窗口函数在单次查询中同时获取
        总行数和分页数据，避免额外的 COUNT 查询开销。

        Args:
            sql: SELECT 查询语句 (不应包含 LIMIT/OFFSET).
            params: SQL 参数.
            page: 页码 (从 1 开始).
            page_size: 每页行数.
            result_format: 结果格式.

        Returns:
            包含分页信息的字典::

                {
                    "data": <格式化后的结果>,
                    "page": <当前页码>,
                    "page_size": <每页行数>,
                    "total": <总行数>,
                    "total_pages": <总页数>,
                }

        Raises:
            ValueError: page 或 page_size 参数无效时.
            SCDBQueryError: 查询执行失败时.
        """
        if page < 1:
            raise ValueError("page 必须 >= 1")
        if page_size < 1:
            raise ValueError("page_size 必须 >= 1")

        offset = (page - 1) * page_size

        # 使用 CTE + 窗口函数实现单次查询分页
        paginated_sql = (
            f"WITH _scdb_base AS ({sql}) "
            f"SELECT *, COUNT(*) OVER() AS _scdb_total "
            f"FROM _scdb_base "
            f"LIMIT {page_size} OFFSET {offset}"
        )

        conn = None
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(paginated_sql, params)
                rows = cur.fetchall()

                if not rows:
                    # 空结果时仍需查询总数
                    count_sql = f"SELECT COUNT(*) FROM ({sql}) AS _scdb_cnt"
                    cur.execute(count_sql, params)
                    total = cur.fetchone()[0]
                    total_pages = math.ceil(total / page_size) if total > 0 else 0

                    # 返回空结果，格式仍需正确
                    empty_data = self._format_results(
                        [], cur.description, result_format
                    )
                    return {
                        "data": empty_data,
                        "page": page,
                        "page_size": page_size,
                        "total": total,
                        "total_pages": total_pages,
                    }

                # 提取总行数 (最后一列为 _scdb_total)
                total = rows[0][-1]
                total_pages = math.ceil(total / page_size)

                # 移除 _scdb_total 列
                cleaned_rows = [row[:-1] for row in rows]

                # 移除 description 中的 _scdb_total
                cleaned_desc = cur.description[:-1] if cur.description else None

                data = self._format_results(
                    cleaned_rows, cleaned_desc, result_format
                )

                return {
                    "data": data,
                    "page": page,
                    "page_size": page_size,
                    "total": total,
                    "total_pages": total_pages,
                }
        except (SCDBQueryError, ValueError):
            raise
        except psycopg2.Error as exc:
            raise SCDBQueryError(f"分页查询执行失败: {exc}") from exc
        finally:
            if conn is not None:
                conn.rollback()
                self._put_conn(conn)

    # ----------------------------------------------------------------
    # 增删改操作 (INSERT / UPDATE / DELETE)
    # ----------------------------------------------------------------

    def execute(
        self,
        sql: str,
        params: tuple | dict | None = None,
    ) -> int:
        """执行单条 SQL 语句 (INSERT / UPDATE / DELETE).

        操作在独立事务中执行，成功后自动提交，
        失败时自动回滚。

        Args:
            sql: 要执行的 SQL 语句.
            params: SQL 参数.

        Returns:
            受影响的行数.

        Raises:
            SCDBQueryError: SQL 执行失败时.
        """
        conn = None
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rowcount = cur.rowcount
            conn.commit()
            return rowcount
        except psycopg2.Error as exc:
            if conn is not None:
                conn.rollback()
            raise SCDBQueryError(f"SQL 执行失败: {exc}") from exc
        finally:
            if conn is not None:
                self._put_conn(conn)

    def execute_many(
        self,
        sql: str,
        params_list: Sequence[tuple | dict],
        page_size: int = _DEFAULT_PAGE_SIZE,
    ) -> int:
        """批量执行 SQL 语句.

        内部使用 ``psycopg2.extras.execute_batch``，将多条 SQL 语句
        合并发送，性能远优于原生 ``executemany``。操作在独立事务中
        执行。

        Args:
            sql: SQL 语句模板.
            params_list: 参数列表.
            page_size: 每批次发送的语句数量，默认 1000.

        Returns:
            受影响的总行数 (基于最终 rowcount).

        Raises:
            SCDBQueryError: 批量执行失败时.
        """
        conn = None
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur, sql, params_list, page_size=page_size
                )
                rowcount = cur.rowcount
            conn.commit()
            return rowcount
        except psycopg2.Error as exc:
            if conn is not None:
                conn.rollback()
            raise SCDBQueryError(f"批量执行失败: {exc}") from exc
        finally:
            if conn is not None:
                self._put_conn(conn)

    def execute_values(
        self,
        sql: str,
        values_list: Sequence[tuple],
        template: str | None = None,
        page_size: int = _DEFAULT_PAGE_SIZE,
    ) -> int:
        """使用 execute_values 进行高性能批量插入.

        利用 ``psycopg2.extras.execute_values`` 将多行数据合并为
        单条 INSERT 语句发送，在大批量插入场景下性能最优。操作在
        独立事务中执行。

        Args:
            sql: INSERT 语句模板，使用 ``%s`` 标记 VALUES 位置.
                例如: ``INSERT INTO t (a, b) VALUES %s``
            values_list: 值列表，每个元素为一行数据的元组.
            template: 自定义值模板.
                例如: ``(%s, %s, ST_Point(%s, %s))``
            page_size: 每批次发送的行数，默认 1000.

        Returns:
            受影响的总行数.

        Raises:
            SCDBQueryError: 批量插入失败时.
        """
        conn = None
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    sql,
                    values_list,
                    template=template,
                    page_size=page_size,
                )
                rowcount = cur.rowcount
            conn.commit()
            return rowcount
        except psycopg2.Error as exc:
            if conn is not None:
                conn.rollback()
            raise SCDBQueryError(
                f"execute_values 失败: {exc}"
            ) from exc
        finally:
            if conn is not None:
                self._put_conn(conn)

    # ----------------------------------------------------------------
    # 事务管理
    # ----------------------------------------------------------------

    @contextmanager
    def transaction(self) -> Generator[_TransactionContext, None, None]:
        """创建事务上下文管理器.

        在 ``with`` 块中提供 ``_TransactionContext`` 对象，支持
        在同一事务中执行多条 SQL 操作。正常退出时自动提交，
        发生异常时自动回滚。

        Yields:
            事务上下文对象，提供 execute / execute_many / execute_values
            方法.

        Raises:
            SCDBTransactionError: 事务提交或回滚失败时.

        Example:
            >>> with db.transaction() as tx:
            ...     tx.execute("INSERT INTO t (a) VALUES (%s)", (1,))
            ...     tx.execute("UPDATE t SET a = %s WHERE a = %s", (2, 1))
        """
        conn = None
        try:
            conn = self._get_conn()
            conn.autocommit = False
            cursor = conn.cursor()
            tx = _TransactionContext(conn, cursor)
            try:
                yield tx
                conn.commit()
                logger.debug("事务已提交")
            except SCDBTransactionError:
                conn.rollback()
                logger.warning("事务已回滚 (SCDBTransactionError)")
                raise
            except Exception as exc:
                conn.rollback()
                logger.warning("事务已回滚: %s", exc)
                raise SCDBTransactionError(
                    f"事务执行失败，已回滚: {exc}"
                ) from exc
            finally:
                cursor.close()
        except SCDBTransactionError:
            raise
        except psycopg2.Error as exc:
            raise SCDBTransactionError(
                f"事务管理失败: {exc}"
            ) from exc
        finally:
            if conn is not None:
                self._put_conn(conn)
