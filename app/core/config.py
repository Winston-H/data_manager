from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Encrypted Data Manager"
    env: str = "dev"
    host: str = "0.0.0.0"
    port: int = 8000

    jwt_secret: str = "replace_this_with_at_least_32_chars_jwt_secret"
    jwt_expire_seconds: int = 7200
    token_revocation_cleanup_interval_seconds: int = 60

    db_path: str = "./data/app.db"
    key_file: str = "./data/keys.json"
    clickhouse_url: str = ""
    clickhouse_database: str = "data_manager"
    clickhouse_records_table: str = "person_records"
    clickhouse_username: str = ""
    clickhouse_password: str = ""
    clickhouse_timeout_seconds: float = 60.0
    clickhouse_native_port: int = 9000
    clickhouse_prefer_native_client: bool = True
    clickhouse_insert_batch_size: int = 100000
    clickhouse_dedup_probe_batch_size: int = 10000
    clickhouse_dedup_probe_max_query_bytes: int = 200000
    clickhouse_query_candidate_limit: int = 5000
    clickhouse_partial_id_scan_limit: int = 50000

    import_polars_excel_engine: str = "calamine"
    import_sheet_read_workers: int = 4
    import_encrypt_workers: int = 4
    import_speed_mode: str = "fast"
    import_progress_flush_every: int = 5000
    import_live_progress_every: int = 200
    import_cancel_check_every: int = 200
    import_fast_progress_flush_every: int = 50000
    import_fast_live_progress_every: int = 1000
    import_fast_cancel_check_every: int = 1000
    sqlite_busy_timeout_ms: int = 60000

    bootstrap_superadmin_username: str = "admin"
    bootstrap_superadmin_password: str = "ChangeMe123!"
    query_mask_roles: str = "USER"
    hidden_usernames: str = ""
    audit_log_retention_days: int = 3
    audit_log_cleanup_interval_seconds: int = 300

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
