"""自定义异常模块.

本模块定义了 scdb_pgsql 包的异常层次结构，用于区分不同类型
的数据库操作错误，便于调用方进行针对性的异常捕获和处理。
"""


class SCDBPgSQLError(Exception):
    """scdb_pgsql 基础异常类.

    所有 scdb_pgsql 相关异常的基类，可用于统一捕获本包
    抛出的所有异常。

    Attributes:
        message: 错误描述信息.
    """

    def __init__(self, message: str = "") -> None:
        self.message = message
        super().__init__(self.message)


class SCDBConnectionError(SCDBPgSQLError):
    """数据库连接异常.

    在以下场景中抛出:
    - 无法建立到数据库服务器的连接
    - 连接池初始化失败
    - 连接超时
    - 连接测试失败
    """


class SCDBQueryError(SCDBPgSQLError):
    """查询执行异常.

    在以下场景中抛出:
    - SQL 语法错误
    - 查询执行过程中发生错误
    - 结果集处理失败
    """


class SCDBTransactionError(SCDBPgSQLError):
    """事务操作异常.

    在以下场景中抛出:
    - 事务提交失败
    - 事务回滚失败
    - 事务上下文中的操作异常
    """
