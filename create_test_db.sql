-- ==========================================================================
-- scdb_pgsql 测试数据库初始化脚本
-- Sample1: psql -h pgpool.lan -U postgres -f create_test_db.sql
-- Sample2: PGPASSWORD='管理员postgres的密码' psql -h pgpool.lan -U postgres -d postgres -f create_test_db.sql
-- Sample2: PGPASSWORD=$PGPOOL_PASWD psql -h pgpool.lan -U $PGPOOL_ADMIN -d $PGPOOL_DEFDB -f create_test_db.sql

-- Note:
-- To use "psql" tool , need install pgsql client tools first as follow
/*
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget -qO- https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg

sudo apt update
sudo apt install postgresql-client-18
sudo apt install postgresql-client-common
*/
-- ==========================================================================

-- 1. 创建测试数据库
-- 如果数据库已存在，先断开所有连接再删除
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'test_5002'
  AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS test_5002;

-- 【已修复】修改了 LC_COLLATE 和 LC_CTYPE 的写法，匹配你系统的实际名称
CREATE DATABASE test_5002
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8';

-- 2. 创建测试用户并授权
DROP ROLE IF EXISTS test5002;
CREATE ROLE test5002 WITH LOGIN PASSWORD 'Love2026';
-- 注意：如果数据库没创建成功，这一步也会报错，现在上面修复了，这里就没问题了
GRANT ALL PRIVILEGES ON DATABASE test_5002 TO test5002;

-- 3. 连接到测试数据库并创建表
-- 在连接前加入 n 秒延迟，清除 PgBouncer 连接池缓存重试计数
SELECT pg_sleep(10);
\connect test_5002

-- 将 public schema 的权限授予测试用户
GRANT ALL ON SCHEMA public TO test5002;

CREATE TABLE IF NOT EXISTS t_test5002 (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(255)
);

-- 将表的所有权限授予测试用户
GRANT ALL PRIVILEGES ON TABLE t_test5002 TO test5002;
GRANT USAGE, SELECT ON SEQUENCE t_test5002_id_seq TO test5002;
