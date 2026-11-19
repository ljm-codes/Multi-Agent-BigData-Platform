-- 建库入口：按依赖顺序依次引入建表脚本
-- 依赖关系（后者引用前者建的表）：
--   postgresql.sql              -> log/code schema 与 public/log/code 表（admin_info、admin_project 等）
--   cluster_env.sql             -> cluster_env schema 与 cluster_info/profile/profile_file/contract/project_env 等
--   cluster_env_snapshots.sql   -> public_config/config_snapshot 等，引 cluster_env.profile
--   cluster_env_materialize.sql -> project_resource/config_drift，引 config_snapshot/profile/admin_project
--   cluster_env_project_resources.sql -> 迁移：改 config_snapshot_file/profile_file/profile
-- 注意：cluster_env*.sql 均自带 BEGIN/COMMIT 事务，这里不再额外包一层外层事务。

\ir postgresql.sql
\ir cluster_env.sql
\ir cluster_env_snapshots.sql
\ir cluster_env_materialize.sql
\ir cluster_env_project_resources.sql