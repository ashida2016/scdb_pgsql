"""PostgreSQL 连接元数据定义模块.

本模块定义了 SCDBPgSQLMeta 数据类，用于封装连接 PostgreSQL 数据库
所需的全部参数，并提供 DSN 字符串和关键字参数的转换方法。
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class SCDBPgSQLMeta:
    """PostgreSQL 数据库连接元数据.

    通过此数据类传入数据库连接所需的全部参数。使用 ``frozen=True``
    确保实例不可变，``slots=True`` 优化内存占用和属性访问速度。

    Attributes:
        host: 数据库服务器主机地址.
        port: 数据库服务器端口.
        database: 目标数据库名称.
        user: 连接用户名.
        password: 连接密码.
        min_connections: 连接池最小连接数.
        max_connections: 连接池最大连接数.
        connect_timeout: 连接超时时间 (秒).
        options: libpq 连接选项字符串 (如 ``-c search_path=public``).
        sslmode: SSL 连接模式 (如 ``require``, ``verify-full``).

    Example:
        >>> meta = SCDBPgSQLMeta(
        ...     database="mydb",
        ...     user="admin",
        ...     password="secret",
        ...     max_connections=20,
        ... )
        >>> meta.to_kwargs()
        {'host': 'localhost', 'port': 5432, 'database': 'mydb', ...}
    """

    database: str
    user: str
    password: str
    host: str = "localhost"
    port: int = 5432
    min_connections: int = 1
    max_connections: int = 10
    connect_timeout: int = 5
    options: str | None = None
    sslmode: str | None = None

    def __post_init__(self) -> None:
        """验证参数有效性.

        Raises:
            ValueError: 当必填参数为空或连接池参数无效时.
        """
        if not self.database:
            raise ValueError("database 不能为空")
        if not self.user:
            raise ValueError("user 不能为空")
        if not self.password:
            raise ValueError("password 不能为空")
        if self.min_connections < 0:
            raise ValueError("min_connections 不能为负数")
        if self.max_connections < 1:
            raise ValueError("max_connections 必须至少为 1")
        if self.min_connections > self.max_connections:
            raise ValueError(
                "min_connections 不能大于 max_connections"
            )
        if self.connect_timeout < 0:
            raise ValueError("connect_timeout 不能为负数")

    def to_kwargs(self) -> dict[str, str | int]:
        """转换为 psycopg2.connect() 可接受的关键字参数字典.

        Returns:
            包含连接参数的字典，仅含非 None 值的参数.
        """
        kwargs: dict[str, str | int] = {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "connect_timeout": self.connect_timeout,
        }
        if self.options is not None:
            kwargs["options"] = self.options
        if self.sslmode is not None:
            kwargs["sslmode"] = self.sslmode
        return kwargs

    def to_dsn(self) -> str:
        """生成 libpq 格式的 DSN 连接字符串.

        Returns:
            格式为 ``postgresql://user:password@host:port/database`` 的
            连接字符串，附带查询参数.
        """
        base = (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
        params: list[str] = [
            f"connect_timeout={self.connect_timeout}",
        ]
        if self.sslmode is not None:
            params.append(f"sslmode={self.sslmode}")
        if self.options is not None:
            params.append(f"options={self.options}")
        return f"{base}?{'&'.join(params)}"
