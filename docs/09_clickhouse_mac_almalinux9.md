# ClickHouse Deployment For macOS And AlmaLinux 9

## 架构落地

- 应用元数据：继续使用 SQLite，保存用户、JWT 黑名单、导入任务、审计日志、配额。
- 主数据：使用 ClickHouse 表 `person_records`。
- 字段策略：
  - `name`：明文
  - `birth_year`：明文
  - `id_no_cipher`：`Fernet` 密文
  - `id_no_digest`：不可逆 `SHA-256` 指纹，仅用于去重和整证精确匹配

ClickHouse 建表顺序：

```sql
CREATE DATABASE IF NOT EXISTS `data_manager`;

CREATE TABLE IF NOT EXISTS `data_manager`.`person_records` (
  id UInt64,
  name String,
  birth_year UInt16,
  id_no_cipher String,
  id_no_digest FixedString(64),
  created_by UInt64,
  created_at DateTime DEFAULT now(),
  INDEX idx_name_ngram name TYPE ngrambf_v1(2, 4096, 2, 0) GRANULARITY 4,
  INDEX idx_id_digest id_no_digest TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY intDiv(toUInt32(birth_year), 10)
ORDER BY (birth_year, name, id)
SETTINGS index_granularity = 8192;
```

## macOS

安装 ClickHouse：

```bash
curl https://clickhouse.com/ | sh
mkdir -p "$HOME/.local/clickhouse"
./clickhouse install --prefix "$HOME/.local/clickhouse" \
  --binary-path bin \
  --config-path etc/clickhouse-server \
  --log-path var/log/clickhouse-server \
  --data-path var/lib/clickhouse \
  --pid-path var/run/clickhouse-server \
  -y
```

创建 Conda 环境：

```bash
conda env create -f environment.yml
conda activate data-manager
ln -sf "$HOME/.local/clickhouse/bin/clickhouse" "$CONDA_PREFIX/bin/clickhouse"
ln -sf "$HOME/.local/clickhouse/bin/clickhouse-client" "$CONDA_PREFIX/bin/clickhouse-client"
ln -sf "$HOME/.local/clickhouse/bin/clickhouse-server" "$CONDA_PREFIX/bin/clickhouse-server"
```

配置 `.env`：

```bash
CLICKHOUSE_URL=http://127.0.0.1:8123
CLICKHOUSE_DATABASE=data_manager
CLICKHOUSE_RECORDS_TABLE=person_records
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_NATIVE_PORT=9000
CLICKHOUSE_PREFER_NATIVE_CLIENT=true
```

初始化：

```bash
"$HOME/.local/clickhouse/bin/clickhouse" server \
  --config-file "$HOME/.local/clickhouse/etc/clickhouse-server/config.xml" \
  --daemon
cp .env.example .env
python scripts/generate_keys.py
python scripts/init_db.py
./scripts/start_local.sh
```

## AlmaLinux 9

AlmaLinux 9 使用 `dnf`/`yum` 安装，兼容 RHEL 9 生态。

安装系统依赖：

```bash
sudo dnf install -y bzip2 curl findutils gcc gcc-c++ git
```

安装 Miniconda：

```bash
curl -fsSL -o /tmp/miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash /tmp/miniconda.sh -b -p "$HOME/miniconda3"
eval "$("$HOME/miniconda3/bin/conda" shell.bash hook)"
```

写入 ClickHouse repo：

```bash
sudo dnf install -y dnf-plugins-core
sudo dnf config-manager --add-repo https://packages.clickhouse.com/rpm/clickhouse.repo
```

安装 ClickHouse：

```bash
sudo dnf install -y clickhouse-server clickhouse-client
sudo systemctl enable --now clickhouse-server
```

确认 HTTP 端口：

```bash
curl -sS http://127.0.0.1:8123/ping
```

创建 Conda 环境并启动应用：

```bash
conda env create -f environment.yml
conda activate data-manager
cp .env.example .env
python3 scripts/generate_keys.py
python3 scripts/init_db.py
./scripts/start_local.sh
```

## 导入行为

- 支持 `.xlsx` 和 `.csv`
- `.xlsx` 会自动读取所有 Sheet
- 自动清洗：
  - 去空行
  - 去重身份证
  - 过滤无效身份证
- 导入批次优先通过本机 `clickhouse-client` 走 native 协议写入；不可用时走 HTTP `JSONEachRow` 批量写入

## 查询行为

- 高性能主路径：
  - 姓名模糊查询
  - 年份范围查询
- 身份证：
  - 整证精确查询：支持
  - 片段模糊查询：只适合小数据量或先用姓名/年份缩小候选集后再做解密过滤

## 运行前检查

建议先执行：

```bash
CHECK_ONLY=1 ./scripts/start_local.sh
```

启动脚本会强制检查 `CLICKHOUSE_URL` 是否已配置。

## 迁移

- Python 运行环境只保留 `environment.yml` 这一份定义文件。
- 迁移到新主机时：
  - 安装 Conda
  - 安装 ClickHouse
  - 拷贝项目代码、`.env`、`data/keys.json`
  - 在项目根目录执行 `conda env create -f environment.yml`
- 如果目标机已有同名环境，改用 `conda env update -n data-manager -f environment.yml --prune`。
