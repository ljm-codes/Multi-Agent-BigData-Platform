-- 集群环境 profile / 契约 / 项目绑定
-- 依赖 public.admin_project(project_id) 已存在
BEGIN;

CREATE SCHEMA IF NOT EXISTS cluster_env;

COMMENT ON SCHEMA cluster_env IS 'Cluster profiles, resolved environment contracts and project bindings';

-- ---------------------------------------------------------------------------
-- 1. Cluster identity
-- ---------------------------------------------------------------------------
CREATE TABLE cluster_env.cluster_info (
    id                  bigserial PRIMARY KEY,
    name                text NOT NULL,
    identity_hash       char(64) NOT NULL,
    fs_default_fs       text,
    hive_metastore_uris text,
    kafka_bootstrap     text,
    source              text NOT NULL DEFAULT 'manual'
                        CHECK (source IN ('auto_pull', 'manual', 'import')),
    status              text NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'disabled')),
    description         text NOT NULL DEFAULT '',
    created_by          bigint,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT cluster_info_identity_uk UNIQUE (identity_hash),
    CONSTRAINT cluster_info_name_uk     UNIQUE (name),
    CONSTRAINT cluster_info_identity_ck CHECK (identity_hash ~ '^[0-9a-f]{64}$')
);

COMMENT ON TABLE  cluster_env.cluster_info IS 'Cluster identity; profiles and contracts reference it';
COMMENT ON COLUMN cluster_env.cluster_info.identity_hash IS 'sha256 over canonical cluster identity';
COMMENT ON COLUMN cluster_env.cluster_info.created_by IS 'admin_info.id';

-- ---------------------------------------------------------------------------
-- 2. Profile
-- ---------------------------------------------------------------------------
CREATE TABLE cluster_env.profile (
    id                  bigserial PRIMARY KEY,
    cluster_id          bigint NOT NULL REFERENCES cluster_env.cluster_info(id) ON DELETE RESTRICT,
    owner_admin_id      bigint NOT NULL,
    name                text NOT NULL,
    source              text NOT NULL DEFAULT 'upload'
                        CHECK (source IN ('upload', 'auto_pull', 'manual', 'import')),
    status              text NOT NULL DEFAULT 'draft'
                        CHECK (status IN ('draft', 'validated', 'active', 'archived', 'deleted')),
    current_revision    integer NOT NULL DEFAULT 1 CHECK (current_revision >= 1),
    draft_revision      integer NOT NULL DEFAULT 1 CHECK (draft_revision >= 1),
    draft_bundle_hash   char(64),
    description         text NOT NULL DEFAULT '',
    created_by          bigint,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT profile_owner_name_uk UNIQUE (owner_admin_id, name),
    CONSTRAINT profile_revision_ck   CHECK (draft_revision >= current_revision),
    CONSTRAINT profile_draft_hash_ck CHECK (draft_bundle_hash IS NULL OR draft_bundle_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX profile_cluster_idx ON cluster_env.profile(cluster_id);
CREATE INDEX profile_owner_idx   ON cluster_env.profile(owner_admin_id);
CREATE INDEX profile_status_idx  ON cluster_env.profile(status);

COMMENT ON TABLE  cluster_env.profile IS 'User-managed cluster profile';
COMMENT ON COLUMN cluster_env.profile.current_revision IS 'Revision currently bound/validated';
COMMENT ON COLUMN cluster_env.profile.draft_revision IS 'Latest uploaded/edited revision';
COMMENT ON COLUMN cluster_env.profile.owner_admin_id IS 'admin_info.id';

-- ---------------------------------------------------------------------------
-- 3. Profile files (immutable per revision)
-- ---------------------------------------------------------------------------
CREATE TABLE cluster_env.profile_file (
    id              bigserial PRIMARY KEY,
    profile_id      bigint NOT NULL REFERENCES cluster_env.profile(id) ON DELETE RESTRICT,
    revision        integer NOT NULL CHECK (revision >= 1),
    filename        text NOT NULL,
    sha256          char(64) NOT NULL,
    size_bytes      bigint NOT NULL CHECK (size_bytes > 0),
    mime_type       text NOT NULL DEFAULT 'application/octet-stream',
    storage_path    text NOT NULL,
    created_by      bigint,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT profile_file_uk UNIQUE (profile_id, revision, filename),
    CONSTRAINT profile_file_storage_uk UNIQUE (storage_path),
    CONSTRAINT profile_file_filename_ck
        CHECK (filename <> '.' AND filename <> '..' AND filename ~ '^[A-Za-z0-9._-]+$'),
    CONSTRAINT profile_file_storage_ck
        CHECK (storage_path !~ '^/' AND storage_path !~ '\.\.' AND
               storage_path ~ '^[0-9]+/[0-9]+/[A-Za-z0-9._-]+$'),
    CONSTRAINT profile_file_sha_ck CHECK (sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX profile_file_revision_idx ON cluster_env.profile_file(profile_id, revision);
CREATE INDEX profile_file_sha_idx      ON cluster_env.profile_file(sha256);

COMMENT ON TABLE  cluster_env.profile_file IS 'Raw XML / properties / yaml file per profile revision';
COMMENT ON COLUMN cluster_env.profile_file.storage_path IS 'Relative path in platform file store';
COMMENT ON COLUMN cluster_env.profile_file.sha256 IS 'Content hash; upload/edit ETag';

-- ---------------------------------------------------------------------------
-- 4. Resolved contract (immutable content; status can change)
-- ---------------------------------------------------------------------------
CREATE TABLE cluster_env.contract (
    id                  bigserial PRIMARY KEY,
    profile_id          bigint NOT NULL REFERENCES cluster_env.profile(id) ON DELETE RESTRICT,
    cluster_id          bigint NOT NULL REFERENCES cluster_env.cluster_info(id) ON DELETE RESTRICT,
    profile_revision    integer NOT NULL CHECK (profile_revision >= 1),
    profile_bundle_hash char(64) NOT NULL,
    env_version         integer NOT NULL CHECK (env_version >= 1),
    contract_hash       char(64) NOT NULL,
    contract_data       jsonb NOT NULL,
    provenance          jsonb NOT NULL DEFAULT '{}'::jsonb,
    conflicts           jsonb NOT NULL DEFAULT '[]'::jsonb,
    unknowns            jsonb NOT NULL DEFAULT '[]'::jsonb,
    status              text NOT NULL DEFAULT 'valid'
                        CHECK (status IN ('valid', 'stale', 'unreachable', 'superseded')),
    probed_at           timestamptz NOT NULL DEFAULT now(),
    expires_at          timestamptz,
    created_by          bigint,
    created_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT contract_profile_version_uk UNIQUE (profile_id, env_version),
    CONSTRAINT contract_hash_ck       CHECK (contract_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT contract_bundle_hash_ck CHECK (profile_bundle_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX contract_cluster_idx ON cluster_env.contract(cluster_id, status);
CREATE INDEX contract_hash_idx    ON cluster_env.contract(contract_hash);
CREATE INDEX contract_expires_idx ON cluster_env.contract(expires_at)
    WHERE expires_at IS NOT NULL;

COMMENT ON TABLE  cluster_env.contract IS 'Resolved environment facts; content immutable, status mutable';
COMMENT ON COLUMN cluster_env.contract.contract_data IS 'Normalized ClusterEnv JSON';
COMMENT ON COLUMN cluster_env.contract.provenance IS 'Per-field source: profile/live/prompt/unknown';
COMMENT ON COLUMN cluster_env.contract.conflicts IS 'Prompt vs actual environment conflicts';

-- ---------------------------------------------------------------------------
-- 5. Project binding (one active env per project)
-- ---------------------------------------------------------------------------
CREATE TABLE cluster_env.project_env (
    project_id          varchar(36) PRIMARY KEY
                        REFERENCES public.admin_project(project_id) ON DELETE CASCADE,
    profile_id          bigint NOT NULL REFERENCES cluster_env.profile(id) ON DELETE RESTRICT,
    profile_revision    integer NOT NULL CHECK (profile_revision >= 1),
    profile_bundle_hash char(64) NOT NULL,
    active_contract_id  bigint NOT NULL REFERENCES cluster_env.contract(id) ON DELETE RESTRICT,
    active_env_version  integer NOT NULL CHECK (active_env_version >= 1),
    status              text NOT NULL DEFAULT 'active'
                        CHECK (status IN (
                            'active', 'reinit_pending', 'validating', 'conflict_hold',
                            'applying', 'stale', 'repairing', 'verifying',
                            'failed', 'rolled_back'
                        )),
    locked              boolean NOT NULL DEFAULT false,
    lock_owner          bigint,
    lock_expires_at     timestamptz,
    activated_at        timestamptz NOT NULL DEFAULT now(),
    activated_by        bigint,
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT project_env_bundle_hash_ck CHECK (profile_bundle_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX project_env_contract_idx ON cluster_env.project_env(active_contract_id);
CREATE INDEX project_env_profile_idx  ON cluster_env.project_env(profile_id);
CREATE INDEX project_env_status_idx   ON cluster_env.project_env(status);

COMMENT ON TABLE  cluster_env.project_env IS 'Active environment version bound to a project';
COMMENT ON COLUMN cluster_env.project_env.status IS 'Reinit/rollback state machine';
COMMENT ON COLUMN cluster_env.project_env.locked IS 'Project-level env lock';

-- ---------------------------------------------------------------------------
-- 6. Environment event audit
-- ---------------------------------------------------------------------------
CREATE TABLE cluster_env.env_event (
    id                  bigserial PRIMARY KEY,
    project_id          varchar(36)
                        REFERENCES public.admin_project(project_id) ON DELETE SET NULL,
    profile_id          bigint
                        REFERENCES cluster_env.profile(id) ON DELETE SET NULL,
    from_contract_id    bigint REFERENCES cluster_env.contract(id) ON DELETE SET NULL,
    to_contract_id      bigint REFERENCES cluster_env.contract(id) ON DELETE SET NULL,
    event_type          text NOT NULL
                        CHECK (event_type IN (
                            'init', 'reinit', 'validate', 'apply', 'rollback',
                            'refresh', 'upload', 'edit', 'lock', 'unlock'
                        )),
    actor               bigint,
    reason              text NOT NULL DEFAULT '',
    diff                jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX env_event_project_idx ON cluster_env.env_event(project_id, created_at DESC);
CREATE INDEX env_event_profile_idx ON cluster_env.env_event(profile_id, created_at DESC);

COMMENT ON TABLE  cluster_env.env_event IS 'Audit: profile edits, validation, apply, rollback';
COMMENT ON COLUMN cluster_env.env_event.actor IS 'admin_info.id';

-- ---------------------------------------------------------------------------
-- 7. Conflicts
-- ---------------------------------------------------------------------------
CREATE TABLE cluster_env.env_conflict (
    id              bigserial PRIMARY KEY,
    contract_id     bigint NOT NULL REFERENCES cluster_env.contract(id) ON DELETE CASCADE,
    field           text NOT NULL,
    prompt_value    jsonb,
    actual_value    jsonb,
    source          text NOT NULL,
    resolution      text NOT NULL DEFAULT 'pending'
                    CHECK (resolution IN ('pending', 'use_prompt', 'use_env', 'custom')),
    resolved_by     bigint,
    resolved_at     timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT env_conflict_uk UNIQUE (contract_id, field)
);

CREATE INDEX env_conflict_pending_idx ON cluster_env.env_conflict(contract_id, resolution);

COMMENT ON TABLE  cluster_env.env_conflict IS 'Prompt vs actual environment conflict';
COMMENT ON COLUMN cluster_env.env_conflict.source IS 'e.g. hive desc, hdfs ls, kafka sample';

-- ---------------------------------------------------------------------------
-- 8. Verification runs
-- ---------------------------------------------------------------------------
CREATE TABLE cluster_env.verification_run (
    id              bigserial PRIMARY KEY,
    project_id      varchar(36)
                    REFERENCES public.admin_project(project_id) ON DELETE SET NULL,
    contract_id     bigint NOT NULL REFERENCES cluster_env.contract(id) ON DELETE RESTRICT,
    env_version     integer NOT NULL CHECK (env_version >= 1),
    gate            text NOT NULL CHECK (gate IN ('env', 'deploy', 'e2e')),
    status          text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'running', 'passed', 'failed', 'error', 'stale')),
    command         text NOT NULL DEFAULT '',
    exit_code       integer,
    evidence        jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at      timestamptz,
    finished_at     timestamptz,
    created_by      bigint,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX verification_run_project_idx  ON cluster_env.verification_run(project_id, env_version, gate);
CREATE INDEX verification_run_contract_idx ON cluster_env.verification_run(contract_id);

COMMENT ON TABLE  cluster_env.verification_run IS 'Verification evidence per environment version';
COMMENT ON COLUMN cluster_env.verification_run.gate IS 'env / deploy / e2e';

-- ---------------------------------------------------------------------------
-- 9. Probe snapshots
-- ---------------------------------------------------------------------------
CREATE TABLE cluster_env.probe_snapshot (
    id              bigserial PRIMARY KEY,
    contract_id     bigint NOT NULL REFERENCES cluster_env.contract(id) ON DELETE CASCADE,
    node            text NOT NULL DEFAULT '',
    command         text NOT NULL,
    kind            text NOT NULL
                    CHECK (kind IN ('hive', 'hdfs', 'flink', 'kafka', 'yarn', 'xml')),
    output_hash     char(64),
    summary         jsonb NOT NULL DEFAULT '{}'::jsonb,
    storage_path    text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT probe_snapshot_hash_ck
        CHECK (output_hash IS NULL OR output_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX probe_snapshot_contract_idx ON cluster_env.probe_snapshot(contract_id, created_at DESC);

COMMENT ON TABLE  cluster_env.probe_snapshot IS 'Live probe commands and summaries';
COMMENT ON COLUMN cluster_env.probe_snapshot.storage_path IS 'Optional raw output file';

-- ---------------------------------------------------------------------------
-- 10. updated_at triggers
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION cluster_env.touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER cluster_info_touch_updated_at
BEFORE UPDATE ON cluster_env.cluster_info
FOR EACH ROW EXECUTE FUNCTION cluster_env.touch_updated_at();

CREATE TRIGGER profile_touch_updated_at
BEFORE UPDATE ON cluster_env.profile
FOR EACH ROW EXECUTE FUNCTION cluster_env.touch_updated_at();

CREATE TRIGGER project_env_touch_updated_at
BEFORE UPDATE ON cluster_env.project_env
FOR EACH ROW EXECUTE FUNCTION cluster_env.touch_updated_at();

COMMIT;
