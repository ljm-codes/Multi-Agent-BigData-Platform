-- 项目资源 materialize / drift
BEGIN;

ALTER TABLE public.admin_project DROP CONSTRAINT IF EXISTS admin_project_verify_status_check;
ALTER TABLE public.admin_project ADD CONSTRAINT admin_project_verify_status_check
    CHECK (verify_status IN ('unknown', 'passed', 'failed', 'error', 'stale'));

CREATE TABLE cluster_env.project_resource (
    project_id              varchar(36) PRIMARY KEY
                            REFERENCES public.admin_project(project_id) ON DELETE CASCADE,
    profile_id              bigint REFERENCES cluster_env.profile(id) ON DELETE SET NULL,
    profile_revision        integer,
    public_snapshot_id      bigint REFERENCES cluster_env.config_snapshot(id) ON DELETE SET NULL,
    profile_snapshot_id     bigint REFERENCES cluster_env.config_snapshot(id) ON DELETE SET NULL,
    materialized_snapshot_id bigint REFERENCES cluster_env.config_snapshot(id) ON DELETE SET NULL,
    effective_hash          char(64) NOT NULL,
    managed_files           jsonb NOT NULL DEFAULT '{}'::jsonb,
    target_path             text NOT NULL DEFAULT '',
    created_by              bigint,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT project_resource_hash_ck CHECK (effective_hash ~ '^[0-9a-f]{64}$')
);

CREATE TABLE cluster_env.config_drift (
    id                  bigserial PRIMARY KEY,
    scope               text NOT NULL CHECK (scope IN ('public', 'project')),
    project_id          varchar(36) REFERENCES public.admin_project(project_id) ON DELETE CASCADE,
    public_config_id    bigint REFERENCES cluster_env.public_config(id) ON DELETE CASCADE,
    expected_snapshot_id bigint REFERENCES cluster_env.config_snapshot(id) ON DELETE SET NULL,
    expected_hash       char(64),
    actual_files        jsonb NOT NULL DEFAULT '{}'::jsonb,
    actual_hash         char(64),
    status              text NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'kept', 'rolled_back', 'dismissed')),
    detected_at         timestamptz NOT NULL DEFAULT now(),
    resolved_at         timestamptz,
    resolved_by         bigint,
    note                text NOT NULL DEFAULT '',
    CONSTRAINT config_drift_expected_hash_ck
        CHECK (expected_hash IS NULL OR expected_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT config_drift_actual_hash_ck
        CHECK (actual_hash IS NULL OR actual_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX project_resource_profile_idx ON cluster_env.project_resource(profile_id);
CREATE INDEX project_resource_materialized_idx ON cluster_env.project_resource(materialized_snapshot_id);
CREATE INDEX config_drift_project_idx ON cluster_env.config_drift(project_id, detected_at DESC);
CREATE INDEX config_drift_public_idx ON cluster_env.config_drift(public_config_id, detected_at DESC);
CREATE INDEX config_drift_pending_idx ON cluster_env.config_drift(status, detected_at);

CREATE TRIGGER project_resource_touch_updated_at
BEFORE UPDATE ON cluster_env.project_resource
FOR EACH ROW EXECUTE FUNCTION cluster_env.touch_updated_at();

COMMENT ON TABLE cluster_env.project_resource IS 'Materialized project resources and source snapshots';
COMMENT ON TABLE cluster_env.config_drift IS 'Detected config drift requiring keep/rollback';

COMMIT;
