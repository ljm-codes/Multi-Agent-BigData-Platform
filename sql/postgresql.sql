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

alter table admin_info alter column email set not null ;

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


-- 以下 SQL 均为 未开发 完整
create table code.ast
(
    id       bigserial primary key,
    language varchar(20) not null
);


create table log.AIOps_log
(

)
