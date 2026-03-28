# Encrypted Data Manager

FastAPI backend for encrypted identity data query and management.

当前工程主数据后端固定为 `ClickHouse`。姓名与年份明文入库，身份证仅用 `Fernet` 加密，按 `(birth_year, name, id)` 排序，适合 2000 万行级别导入与查询。

## Quick Start

1. Create the Conda environment (Python 3.10):

```bash
conda env create -f environment.yml
conda activate data-manager
```

2. Create key file:

```bash
cp .env.example .env
python scripts/generate_keys.py
```

3. Initialize metadata database and record store:

```bash
python scripts/init_db.py
```

4. Run service:

```bash
uvicorn app.main:app --reload
```

5. Open UI:

```bash
http://127.0.0.1:8000/
```

## Run Tests

Activate the Conda environment and run:

```bash
python -m pytest -q
```

当前也可以直接运行内置回归测试：

```bash
python -m unittest tests.test_api_e2e tests.test_import_recovery tests.test_query_mask_strategy tests.test_openapi_contract tests.test_security_hashing
```

## One-Command Startup

Use the startup script to auto-check environment, initialize key/database, and launch service:

```bash
conda activate data-manager
./scripts/start_local.sh
```

Optional environment variables:
- `PYTHON_BIN`: python executable, default `python3`
- `HOST`: bind host, default `0.0.0.0`
- `PORT`: bind port, default `8000`
- `CHECK_ONLY=1`: run checks and init only, do not start server

## ClickHouse

推荐把 `.env.example` 中以下配置改成你的实际值：

```bash
CLICKHOUSE_URL=http://127.0.0.1:8123
CLICKHOUSE_DATABASE=data_manager
CLICKHOUSE_RECORDS_TABLE=person_records
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_NATIVE_PORT=9000
CLICKHOUSE_PREFER_NATIVE_CLIENT=true
```

ClickHouse 模式下：
- Excel/CSV 导入使用 `Polars` 清洗与批量处理。
- 如果主机装有 `clickhouse-client`，导入会优先走 native 协议批量写入；否则走 HTTP `JSONEachRow` 批量写入。
- 姓名、年份明文存储，支持姓名模糊查询和年份范围查询。
- 身份证只存 `Fernet` 密文，另存不可逆指纹仅用于去重和整证精确匹配。
- 旧前端接口 `/api/v1/data/import`、`/api/v1/query`、`/api/v1/stats/summary` 不变。

环境迁移时，直接携带项目代码、`.env` 和 `environment.yml`，目标主机执行 `conda env create -f environment.yml` 即可重建运行环境。

身份证片段模糊查询在 “仅身份证密文 + 不增加额外身份证检索索引” 的约束下无法在 2000 万行规模保持高性能，因此 ClickHouse 主路径已优化为“姓名模糊 + 年份范围”，整证身份证查询仍可通过指纹快速命中。

## Default Admin Initialization

First startup will create super admin using env values:
- `BOOTSTRAP_SUPERADMIN_USERNAME`
- `BOOTSTRAP_SUPERADMIN_PASSWORD`

## Operations Docs

- HTTPS reverse proxy deployment: `docs/05_nginx_https_deployment.md`
- Key backup/recovery/rotation SOP: `docs/06_key_backup_recovery_rotation_sop.md`
- Frontend interaction UAT checklist: `docs/07_frontend_interaction_uat_checklist.md`
- ClickHouse deployment for macOS / AlmaLinux 9: `docs/09_clickhouse_mac_almalinux9.md`
