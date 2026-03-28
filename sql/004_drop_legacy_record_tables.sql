DROP INDEX IF EXISTS idx_records_dedup_hash;
DROP INDEX IF EXISTS idx_tokens_hash_field_gram;
DROP INDEX IF EXISTS idx_tokens_record_id;
DROP INDEX IF EXISTS idx_records_created_at;
DROP INDEX IF EXISTS idx_records_created_by;

DROP TABLE IF EXISTS record_search_tokens;
DROP TABLE IF EXISTS encrypted_records;
