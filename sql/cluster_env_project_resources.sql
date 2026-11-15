-- 项目 resources 允许任意相对路径；大文件只存哈希，不存 content
BEGIN;

ALTER TABLE cluster_env.config_snapshot_file
    DROP CONSTRAINT IF EXISTS config_snapshot_file_name_ck;

ALTER TABLE cluster_env.config_snapshot_file
    ADD COLUMN IF NOT EXISTS content_omitted boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN cluster_env.config_snapshot_file.content_omitted
    IS 'true = 文件超过内容快照阈值，只保留 sha256/size，不存 content';

-- storage_root 已经是 ./cluster_profiles，storage_path 不再重复带 cluster_profiles/ 前缀
ALTER TABLE cluster_env.profile_file
    DROP CONSTRAINT IF EXISTS profile_file_storage_ck;
ALTER TABLE cluster_env.profile_file
    ADD CONSTRAINT profile_file_storage_ck CHECK (
        storage_path !~ '^/' AND storage_path !~ '\.\.' AND
        storage_path ~ '^[0-9]+/[0-9]+/[A-Za-z0-9._-]+$'
    );

ALTER TABLE cluster_env.profile
    DROP CONSTRAINT IF EXISTS profile_status_check;
ALTER TABLE cluster_env.profile
    ADD CONSTRAINT profile_status_check CHECK (
        status IN ('draft', 'validated', 'active', 'archived', 'deleted')
    );

COMMIT;
