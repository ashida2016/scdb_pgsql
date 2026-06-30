# scdb_pgsql

一个高性能、工业级的操作 PostgreSQL 数据库的 Python 包。

基于 `psycopg2-binary` 驱动，提供连接池化、事务管理、多格式查询输出和批量操作支持。

## 特性

- **连接池化** — 基于 `ThreadedConnectionPool` 的线程安全连接池
- **多格式输出** — 查询结果支持 `tuple`、`dict`、`json`、`df` 四种格式
- **分页查询** — 使用窗口函数 `COUNT(*) OVER()` 实现高效单次查询分页
- **批量操作** — 基于 `execute_batch` 和 `execute_values` 的高性能批量写入
- **事务管理** — 上下文管理器风格的事务支持，自动提交/回滚
- **连接测试** — 内置 `test_connection()` 方法验证数据库连通性

## 安装

```bash
pip install -e .

# 安装开发依赖 (pytest, pytest-cov, pytest-mock)
pip install -e ".[dev]"
```

## 快速开始

### 1. 建立连接

```python
from scdb_pgsql import SCDBPgSQL, SCDBPgSQLMeta

# 创建连接元数据
meta = SCDBPgSQLMeta(
    host="localhost",
    port=5432,
    database="mydb",
    user="admin",
    password="secret",
    min_connections=2,
    max_connections=20,
)

# 使用上下文管理器 (推荐)
with SCDBPgSQL(meta) as db:
    if db.test_connection():
        print("连接成功!")
```

### 2. 查询操作

```python
with SCDBPgSQL(meta) as db:
    # 默认返回 tuple 列表
    rows = db.fetch_all("SELECT id, name FROM users WHERE age > %s", (18,))

    # 返回字典列表
    dicts = db.fetch_all(
        "SELECT id, name FROM users",
        result_format="dict"
    )
    # [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    # 返回 JSON 字符串
    json_str = db.fetch_all(
        "SELECT id, name FROM users",
        result_format="json"
    )

    # 返回 pandas DataFrame
    df = db.fetch_all(
        "SELECT id, name, age FROM users",
        result_format="df"
    )
```

### 3. 分页查询

```python
with SCDBPgSQL(meta) as db:
    result = db.fetch_page(
        "SELECT id, name FROM users",
        page=2,
        page_size=20,
        result_format="dict",
    )
    print(result["data"])         # 当前页数据
    print(result["page"])         # 2
    print(result["page_size"])    # 20
    print(result["total"])        # 总行数
    print(result["total_pages"])  # 总页数
```

### 4. 增删改操作

```python
with SCDBPgSQL(meta) as db:
    # 单条插入
    db.execute(
        "INSERT INTO users (name, age) VALUES (%s, %s)",
        ("Alice", 30)
    )

    # 单条更新
    affected = db.execute(
        "UPDATE users SET age = %s WHERE name = %s",
        (31, "Alice")
    )

    # 单条删除
    db.execute("DELETE FROM users WHERE age < %s", (18,))
```

### 5. 批量操作

```python
with SCDBPgSQL(meta) as db:
    # execute_many — 批量执行 (基于 execute_batch，性能优于 executemany)
    db.execute_many(
        "INSERT INTO users (name, age) VALUES (%s, %s)",
        [("Alice", 30), ("Bob", 25), ("Charlie", 35)],
    )

    # execute_values — 最高性能的批量插入
    db.execute_values(
        "INSERT INTO users (name, age) VALUES %s",
        [("Alice", 30), ("Bob", 25), ("Charlie", 35)],
    )
```

### 6. 事务操作

```python
with SCDBPgSQL(meta) as db:
    # 使用事务上下文管理器
    with db.transaction() as tx:
        tx.execute(
            "INSERT INTO orders (user_id, amount) VALUES (%s, %s)",
            (1, 99.99)
        )
        tx.execute(
            "UPDATE accounts SET balance = balance - %s WHERE user_id = %s",
            (99.99, 1)
        )
        # 正常退出: 自动 COMMIT
        # 异常退出: 自动 ROLLBACK
```

## API 参考

### SCDBPgSQLMeta

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `database` | `str` | 必填 | 数据库名称 |
| `user` | `str` | 必填 | 用户名 |
| `password` | `str` | 必填 | 密码 |
| `host` | `str` | `"localhost"` | 主机地址 |
| `port` | `int` | `5432` | 端口号 |
| `min_connections` | `int` | `1` | 连接池最小连接数 |
| `max_connections` | `int` | `10` | 连接池最大连接数 |
| `connect_timeout` | `int` | `5` | 连接超时 (秒) |
| `options` | `str \| None` | `None` | libpq 连接选项 |
| `sslmode` | `str \| None` | `None` | SSL 模式 |

### SCDBPgSQL

| 方法 | 说明 |
|------|------|
| `test_connection()` | 测试数据库连接，返回 `bool` |
| `fetch_all(sql, params, result_format)` | 一次性查询全部结果 |
| `fetch_page(sql, params, page, page_size, result_format)` | 分页查询 |
| `execute(sql, params)` | 执行单条 SQL，返回受影响行数 |
| `execute_many(sql, params_list, page_size)` | 批量执行 (`execute_batch`) |
| `execute_values(sql, values_list, template, page_size)` | 高性能批量插入 |
| `transaction()` | 返回事务上下文管理器 |
| `close()` | 关闭连接池 |

### 结果格式 (result_format)

| 格式 | 返回类型 | 说明 |
|------|----------|------|
| `"tuple"` | `list[tuple]` | 默认格式 |
| `"dict"` | `list[dict]` | 字典列表 |
| `"json"` | `str` | JSON 字符串 |
| `"df"` | `pandas.DataFrame` | DataFrame (需安装 pandas) |

## 运行测试

```bash
# 运行测试并生成覆盖率报告
pytest --cov=scdb_pgsql --cov-report=term-missing --cov-report=html tests/

# 覆盖率 HTML 报告位于 htmlcov/index.html
```

## 许可证

MIT
