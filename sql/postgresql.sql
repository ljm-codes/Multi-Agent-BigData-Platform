create database MultiAgentBigDataPlatform;

create schema log;

create schema code;


create table log.checker_log
(
    id             bigserial primary key,

    app_id         varchar(255)                                       not null,
    app_type       varchar(20) check (app_type in ('SPARK', 'FLINK')) null,

    dedup_key      varchar(64)                                        not null,               -- md5 算法计算得到的唯一键

    status         varchar(20)                                                 default 'PENDING'
        check (status in ('PENDING', 'PROCESSING', 'DONE', 'ESCALATED', 'FAILED')) not null,

    checker_error  text                                               null     default '',
    -- 逻辑删除 -> 当到了一定时间后才真正的删除
    is_solved      boolean                                                     default false,
    -- 数据
    check_type     varchar(200)                                       not null,
    error_type     varchar(200)                                       null,                   -- error / warning
    check_describe text                                               null     default '',
    check_data     jsonb                                                       default '{}',

    repeat_count   integer                                            not null default 1,
    -- 时间
    check_time     timestamptz                                                 default now(), -- 检测出这条数据的时间
    create_time    timestamptz                                                 default now(), -- 创建这个记录的时间
    update_time    timestamptz                                                 default now()  -- 更新这个记录的时间
);

-- 取某 app 的待处理错误
create index idx_checker_log_app_pending on log.checker_log (app_id, check_time) where status = 'PENDING';
-- 去重
create unique index uk_checker_log_dedup_pending on log.checker_log (dedup_key) where status = 'PENDING';
-- 清理旧数据
create index idx_checker_log_done_update_time on log.checker_log (update_time) where status = 'DONE';
-- 人工排查
create index idx_checker_log_app_time on log.checker_log (app_id, check_time desc);


create table app_leases
(
    app_id       varchar(255) primary key,
    session_id   varchar(255) not null,     -- 每次抢锁生成的唯一 ID / 也是 Agent 会话ID
    acquired_at  timestamptz default now(), -- 获取锁的时间
    heartbeat_at timestamptz default now()  -- 最后一次心跳的时间
);

-- 运维 Agent 运行历史：app_leases 是锁表（会删除），这张表负责归档
create table if not exists log.agent_runs
(
    id          bigserial primary key,
    app_id      varchar(255) not null,
    session_id  varchar(255) not null unique,
    app_type    varchar(20),
    status      varchar(20)  not null default 'RUNNING',
    error_count int          not null default 0,
    started_at  timestamptz  not null default now(),
    finished_at timestamptz,
    out_msg     text,
    error       text,
    constraint agent_run_status_check
        check (status in ('RUNNING', 'PAUSED', 'DONE', 'ERROR', 'ABANDONED'))
);

-- 已有库不会因为 create table if not exists 更新约束，单独迁移一次
alter table log.agent_runs drop constraint if exists agent_run_status_check;
alter table log.agent_runs add constraint agent_run_status_check
    check (status in ('RUNNING', 'PAUSED', 'DONE', 'ERROR', 'ABANDONED'));

create index if not exists idx_agent_runs_app_time on log.agent_runs (app_id, started_at desc);
create index if not exists idx_agent_runs_status_time on log.agent_runs (status, started_at desc);

drop table if exists admin_info;

create table admin_info
(
    id          bigserial primary key,
    username    varchar(255)                                            not null,
    password    varchar(255)                                            not null,
    -- 0: 初级开发人员；1: 中级开发人员；2: 高级开发人员
    permission  int         default 0 check ( permission in (0, 1, 2) ) not null,
    email       varchar(255) unique,
    -- 是否禁用
    disabled    boolean     default false                               not null,
    create_time timestamptz default now(),
    update_time timestamptz default now()
);

alter table admin_info
    alter column email set not null;

create index idx_name_email on admin_info (username, email) where disabled = false;

drop table if exists admin_thread;

create table admin_thread
(
    id          bigserial primary key,
    thread_id   varchar(255) unique not null,
    thread_name varchar(20)         not null,
    admin_id    bigint references admin_info (id),
    is_archive  boolean     default false,
    is_top      boolean     default false, -- 是否置顶
    create_time timestamptz default now(),
    update_time timestamptz default now()
);

drop table if exists log.recommend_approval_log;

create table log.recommend_approval_log
(
    id                bigserial primary key,
    recommend_id      bigint  not null,                                                  -- 推荐人ID
    reason            text    not null default '',                                       -- 审核原因
    level             int     not null default 0 check (level in (0, 1, 2)),             -- 推荐人等级
    recommended_level int     not null default 0 check (recommended_level in (0, 1, 2)), -- 被推荐人等级
    is_pass           boolean not null default false,                                    -- 是否通过审核
    create_time       timestamptz      default now(),                                    -- 创建这个记录的时间
    update_time       timestamptz      default now()                                     -- 更新这个记录的时间
);

create table code.app_file_idx
(
    id          bigserial primary key,
    app_type    varchar(20) check ( app_type in ('SPARK', 'FLINK'))                  not null,
    app_name    varchar(255)                                                         not null,
    source_type varchar(10) check ( source_type in ('hdfs', 'local', 'git', 'ssh') ) not null,
    source_dir  text                                                                 not null,
    commit_cmd  text                                                                 not null,
    create_time timestamptz                                                          not null default now(),
    update_time timestamptz                                                          not null default now()
); -- 提交作业时，指向 源码 映射表 -> 用于 快速定位到应用文件路径（ops-Agent用）

create unique index uk_app_file_app on code.app_file_idx (app_type, app_name);

-- 以下 SQL 均为 未开发 完整
create table log.AIOps_log
(

)
