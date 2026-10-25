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
    check_data     jsonb                                                        default '{}',

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
    session_id   varchar(255) not null, -- 每次抢锁生成的唯一 ID / 也是 Agent 会话ID
    acquired_at  timestamptz default now(),  -- 获取锁的时间
    heartbeat_at timestamptz default now()  -- 最后一次心跳的时间
);

-- 以下 SQL 均为 未开发 完整
create table code.ast(
    id             bigserial primary key,
    language varchar(20) not null
);


create table log.AIOps_log
(

)
