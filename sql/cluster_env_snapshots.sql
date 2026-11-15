-- 快照 / 公有资源 / 归档
BEGIN;

CREATE TABLE cluster_env.public_config (
    id                  bigserial PRIMARY KEY,
    name                text NOT NULL UNIQUE,
    current_snapshot_id bigint,
    created_by          bigint,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE cluster_env.config_snapshot (
    id                  bigserial PRIMARY KEY,
    scope               text NOT NULL CHECK (scope IN ('public', 'profile', 'project')),
    scope_key           text NOT NULL,
    public_config_id    bigint REFERENCES cluster_env.public_config(id) ON DELETE RESTRICT,
    profile_id          bigint REFERENCES cluster_env.profile(id) ON DELETE RESTRICT,
    project_id          varchar(36) REFERENCES public.admin_project(project_id) ON DELETE SET NULL,
    version             integer NOT NULL CHECK (version >= 1),
    bundle_hash         char(64) NOT NULL,
    manifest            jsonb NOT NULL DEFAULT '{}'::jsonb,
    source              text NOT NULL,
    parent_snapshot_id  bigint REFERENCES cluster_env.config_snapshot(id) ON DELETE SET NULL,
    status              text NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'superseded', 'purged')),
    pinned              boolean NOT NULL DEFAULT false,
    actor               bigint,
    reason              text NOT NULL DEFAULT '',
    created_at          timestamptz NOT NULL DEFAULT now(),
    retained_until      timestamptz,
    content_purged_at   timestamptz,
    CONSTRAINT config_snapshot_scope_version_uk UNIQUE (scope_key, version),
    CONSTRAINT config_snapshot_hash_ck CHECK (bundle_hash ~ '^[0-9a-f]{64}$')
);

ALTER TABLE cluster_env.public_config
    ADD CONSTRAINT public_config_current_snapshot_fk
    FOREIGN KEY (current_snapshot_id) REFERENCES cluster_env.config_snapshot(id) ON DELETE SET NULL;

ALTER TABLE cluster_env.profile
    ADD COLUMN IF NOT EXISTS current_snapshot_id bigint
        REFERENCES cluster_env.config_snapshot(id) ON DELETE SET NULL;

ALTER TABLE cluster_env.project_env
    ADD COLUMN IF NOT EXISTS public_snapshot_id bigint
        REFERENCES cluster_env.config_snapshot(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS profile_snapshot_id bigint
        REFERENCES cluster_env.config_snapshot(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS materialized_snapshot_id bigint
        REFERENCES cluster_env.config_snapshot(id) ON DELETE SET NULL;

CREATE TABLE cluster_env.config_snapshot_file (
    id              bigserial PRIMARY KEY,
    snapshot_id     bigint NOT NULL REFERENCES cluster_env.config_snapshot(id) ON DELETE CASCADE,
    filename        text NOT NULL,
    sha256          char(64) NOT NULL,
    size_bytes      bigint NOT NULL CHECK (size_bytes >= 0),
    mime_type       text NOT NULL DEFAULT 'application/octet-stream',
    content         bytea,
    content_omitted boolean NOT NULL DEFAULT false,
    storage_path    text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT config_snapshot_file_uk UNIQUE (snapshot_id, filename),
    CONSTRAINT config_snapshot_file_sha_ck CHECK (sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE cluster_env.config_snapshot_archive (
    id              bigserial PRIMARY KEY,
    snapshot_id     bigint NOT NULL UNIQUE
                    REFERENCES cluster_env.config_snapshot(id) ON DELETE RESTRICT,
    scope           text NOT NULL,
    path            text NOT NULL UNIQUE,
    sha256          char(64) NOT NULL,
    size_bytes      bigint NOT NULL CHECK (size_bytes > 0),
    status          text NOT NULL DEFAULT 'archived'
                    CHECK (status IN ('archived', 'deleted', 'missing')),
    created_by      bigint,
    created_at      timestamptz NOT NULL DEFAULT now(),
    deleted_by      bigint,
    deleted_at      timestamptz,
    note            text NOT NULL DEFAULT '',
    CONSTRAINT config_snapshot_archive_hash_ck CHECK (sha256 ~ '^[0-9a-f]{64}$')
);

ALTER TABLE cluster_env.env_event DROP CONSTRAINT IF EXISTS env_event_event_type_check;
ALTER TABLE cluster_env.env_event ADD CONSTRAINT env_event_event_type_check CHECK (event_type IN (
    'init', 'reinit', 'validate', 'apply', 'rollback', 'refresh', 'upload', 'edit',
    'lock', 'unlock', 'archive', 'gc', 'purge', 'public_scan', 'public_rollback',
    'restore', 'materialize', 'drift', 'keep'
));

CREATE INDEX public_config_current_idx ON cluster_env.public_config(current_snapshot_id);
CREATE INDEX config_snapshot_scope_idx ON cluster_env.config_snapshot(scope_key, created_at DESC);
CREATE INDEX config_snapshot_status_idx ON cluster_env.config_snapshot(status, created_at);
CREATE INDEX config_snapshot_public_idx ON cluster_env.config_snapshot(public_config_id, version);
CREATE INDEX config_snapshot_profile_idx ON cluster_env.config_snapshot(profile_id, version);
CREATE INDEX config_snapshot_project_idx ON cluster_env.config_snapshot(project_id, version);
CREATE INDEX config_snapshot_file_snapshot_idx ON cluster_env.config_snapshot_file(snapshot_id);
CREATE INDEX config_snapshot_archive_status_idx ON cluster_env.config_snapshot_archive(status, created_at);

CREATE TRIGGER public_config_touch_updated_at
BEFORE UPDATE ON cluster_env.public_config
FOR EACH ROW EXECUTE FUNCTION cluster_env.touch_updated_at();

COMMENT ON TABLE cluster_env.public_config IS 'Shared public config pointer';
COMMENT ON TABLE cluster_env.config_snapshot IS 'Immutable config snapshots; DB content authoritative for stored files';
COMMENT ON TABLE cluster_env.config_snapshot_file IS 'Snapshot files; large files may be hash-only; content purged after archive';
COMMENT ON TABLE cluster_env.config_snapshot_archive IS 'Archive files outside DB; admin-managed';

COMMIT;
