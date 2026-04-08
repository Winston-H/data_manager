CREATE TABLE IF NOT EXISTS token_revocations (
  jti TEXT PRIMARY KEY,
  expires_at INTEGER NOT NULL,
  revoked_by INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
  FOREIGN KEY (revoked_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_token_revocations_expires_at
  ON token_revocations(expires_at);
