import base64
import os
import re
import secrets
from datetime import datetime, timezone, timedelta
from typing import Any, Literal

import bcrypt
import jwt
from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import func, select, and_, update, true, false
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert

from SPO.postgresql_model import AdminInfo, RecommendApprovalTable
from SPO.route_results import RouteResponse, ResultCode
from Tools import LogSetting
from Tools.db import SessionLocal, AsyncSessionLocal
from Tools.share_check_tool import redis_con

route = APIRouter()
email_re = r"\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*"
logger = LogSetting.create(__name__)


class AdminInputInfo(BaseModel):
    id: int = Field(default=0, description="管理员ID")
    username: str = Field(default="", description="用户名")
    password: str = Field(default="", description="密码")
    email: str = Field(default="", description="邮箱")
    reason: str = Field(default="", description="审批原因")
    permission: int = Field(default=0, description="权限等级")
    recommend_token: str = Field(default="", description="推荐人令牌")
    code: str = Field(default="", description="推荐码（推荐链接里的短码）")
    disabled: bool = Field(default=False, description="是否禁用")


class RecommendInfo(BaseModel):
    recommend_id: int = Field(description="推荐人ID")
    recommend_name: str = Field(description="推荐人姓名")
    recommend_reason: str = Field(default="", description="推荐原因")
    recommended_person_level: int = Field(default=0, description="被推荐人权限等级")


class AccountStatusInfo(BaseModel):
    """启用 / 封禁账号的入参"""
    id: int = Field(description="要操作的管理员ID")
    disabled: bool = Field(description="true=封禁，false=启用")
    reason: str = Field(default="", description="操作原因（可选，只记日志）")


class AccountUpdateInfo(BaseModel):
    """修改账号资料的入参：只改传了的字段"""
    id: int = Field(description="要修改的管理员ID")
    username: str | None = Field(default=None, description="新用户名；不传表示不修改")
    email: str | None = Field(default=None, description="新邮箱；不传表示不修改")
    permission: int | None = Field(default=None, description="新权限等级 0/1/2；不传表示不修改")


def encode_password(password: str) -> str:
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode(), salt)
    return hashed.decode()


def check_password(password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed_password.encode())


def _generate_temp_key() -> str:
    raw_key = os.urandom(32)
    base64_key = base64.b64encode(raw_key).decode('utf-8')
    return base64_key


def _parse_temp_token(token: str, recommend_id: int):
    """
    解析临时token，返回token中的信息
    """
    try:
        temp_key = redis_con.hget("jwt_key", f"temp-{recommend_id}")
    except Exception as e:
        logger.error(f"在尝试从Redis中获取临时key时出错 {type(e).__name__}: {e}")
        return None, "Token 无效"
    if not temp_key:
        return None, "Token 无效"
    try:
        decoded = jwt.decode(token, temp_key.decode('utf-8'), algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        return None, "JWT 自身已过期"
    except jwt.InvalidTokenError:
        return None, "JWT 签名无效或格式错误"

    return decoded, None


@route.post("/build/link")
async def build_recommend_link(request: Request, recommend_info: RecommendInfo) -> RouteResponse:
    """
    构建推荐链接，提供者必须是管理员，接收者可以是新用户
    -> 该端口会将管理员的信息封存到token中，后续在注册时需要验证该token
    """
    token_info = request.state.info
    if not token_info or not isinstance(token_info, dict):
        return RouteResponse.error(msg="推荐人信息不能为空，或格式错误", code=ResultCode.NOT_VALUES_ERROR)
    permission = token_info.get("permission", 0)
    if permission <= 0:
        return RouteResponse.error(msg="推荐人权限等级不足，不能进行推荐", code=ResultCode.PERMISSION_ERROR)
    if permission < recommend_info.recommended_person_level:
        return RouteResponse.error(msg="不允许出现被推荐人的权限等级高于自己", code=ResultCode.PERMISSION_ERROR)
    recommend_id = token_info.get("id")
    if recommend_id != recommend_info.recommend_id or token_info.get(
            "username") != recommend_info.recommend_name:
        return RouteResponse.error(msg="推荐人信息与token不匹配", code=ResultCode.UNAUTHORIZED)
    # 构建token
    payload = {
        "username": token_info.get("username"),
        "recommended_person_permission": recommend_info.recommended_person_level,
        "permission": permission,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=20),
        "iat": datetime.now(timezone.utc),
    }
    # 生成token -> 时限 20分钟
    temp_key = _generate_temp_key()
    redis_con.hset("jwt_key", f"temp-{recommend_id}", temp_key)  # 3.2 没有 hsetex；有效期由 token 的 exp 管
    token = jwt.encode(payload, temp_key, algorithm="HS256")
    # 短码：链接里只放随机码，token 留在 Redis
    code = secrets.token_urlsafe(16)
    redis_con.hset("temp_code", code, f"{recommend_id}:{token}")
    # 记录推荐人信息
    try:
        async with AsyncSessionLocal() as session:
            stmt = insert(RecommendApprovalTable).values(
                id=recommend_id,
                recommend_id=recommend_id,
                reason=recommend_info.recommend_reason,
                level=permission,
                recommended_level=recommend_info.recommended_person_level,
                is_pass=False,
            )
            await session.execute(stmt)
            await session.commit()
    except Exception as e:
        logger.error(f"在尝试记录推荐人信息时出错 {type(e).__name__}: {e}")
        return RouteResponse.error(msg="数据库操作失败，但推荐链接构建成功", code=ResultCode.DB_ERROR,
                                   data={"code": code,
                                         "link": f"/recommend/register?code={code}"})
    return RouteResponse.ok(
        msg="推荐链接构建成功",
        data={"code": code, "link": f"/recommend/register?code={code}"}
    )


@route.post("/recommend/register")
async def register_admin(admin_info: AdminInputInfo) -> RouteResponse:
    """
    由权利更高或者同等权利的人推荐某个人进行注册
    -> 会先对该推荐人进行验证，确认其是否有足够的权利进行注册
    -> 注册并不是立马通过的，而是需要等待对应的推荐人确认，并调整该用户的权限（同时不能超过推荐人自己的权限）
    -> 期间该用户处于禁用状态，无法进行任何操作
    """
    email = admin_info.email
    if not re.match(email_re, email):
        return RouteResponse.error(msg="邮箱格式错误", code=ResultCode.NOT_VALUES_ERROR)
    username = admin_info.username
    password = admin_info.password
    if not username or not password:
        return RouteResponse.error(msg="用户名或密码不能为空", code=ResultCode.NOT_VALUES_ERROR)
    # 链接里只有短码，token 和推荐人 id 都从 Redis 取
    code = admin_info.code
    if not code:
        return RouteResponse.error(msg="推荐码不能为空", code=ResultCode.NOT_VALUES_ERROR)
    raw = redis_con.hget("temp_code", code)
    if not raw:
        return RouteResponse.error(msg="推荐码无效、已使用或已过期", code=ResultCode.UNAUTHORIZED)
    redis_con.hdel("temp_code", code)  # 用完就删
    recommend_id_str, _, recommend_token = raw.decode("utf-8").partition(":")
    recommend_id = int(recommend_id_str or 0)
    if not recommend_token or not recommend_id:
        return RouteResponse.error(msg="推荐码内容损坏，请让推荐人重新生成", code=ResultCode.UNAUTHORIZED)
    decoded, msg = _parse_temp_token(recommend_token, recommend_id)
    if not decoded:
        return RouteResponse.error(msg=msg, code=ResultCode.UNAUTHORIZED)
    recommended_person_permission = decoded.get("recommended_person_permission", 0)
    permission = decoded.get("permission", 0)
    if permission < recommended_person_permission:
        return RouteResponse.error(msg="出现了权限不匹配现状，请联系管理员", code=ResultCode.PERMISSION_ERROR)
    # 注册用户
    hash_password = encode_password(password)
    try:
        with SessionLocal() as session:
            stmt = insert(AdminInfo).values(
                username=username,
                password=hash_password,
                email=email,
                permission=recommended_person_permission,
                disabled=False,  # 对于被推荐的用户，默认是启用状态
                create_time=func.now(),
                update_time=func.now(),
            )
            session.execute(stmt)
            session.commit()
    except Exception as e:
        logger.error(f"注册用户 {username} 失败：{e}")
        return RouteResponse.error(msg=f"注册失败：{e}", code=ResultCode.DB_ERROR)
    try:
        async with AsyncSessionLocal() as session:
            # 更新日志
            stmt = update(RecommendApprovalTable).where(
                RecommendApprovalTable.recommend_id == recommend_id
            ).values(
                is_pass=True,
                update_time=func.now(),
            )
            await session.execute(stmt)
            await session.commit()
    except Exception as e:
        logger.error(f"更新推荐人日志失败：{e}")
        return RouteResponse.error(msg=f"更新推荐人日志失败：{e}，但注册成功，请手动进行日志的更改",
                                   code=ResultCode.DB_ERROR)
    return RouteResponse.ok(msg="注册成功")


@route.get("/recommend/register")
async def recommend_register_info(code: str = Query(default="")) -> RouteResponse:
    """
    推荐链接的只读校验：推荐码是否有效、给了几级权限、还剩多久
    -> 不消费码；真正注册仍然走 POST /recommend/register
    """
    if not code:
        return RouteResponse.error(msg="缺少推荐码", code=ResultCode.NOT_VALUES_ERROR)
    raw = redis_con.hget("temp_code", code)
    if not raw:
        return RouteResponse.error(msg="推荐码无效、已被使用或已过期", code=ResultCode.RECOMMEND_CODE_ERROR)
    recommend_id_str, _, recommend_token = raw.decode("utf-8").partition(":")
    decoded, msg = _parse_temp_token(recommend_token, int(recommend_id_str or 0))
    if not decoded:
        return RouteResponse.error(msg=msg or "推荐码无效", code=ResultCode.RECOMMEND_CODE_ERROR)
    expire_in = int(decoded.get("exp", 0) - datetime.now(timezone.utc).timestamp())
    return RouteResponse.ok(data={
        "recommended_level": decoded.get("recommended_person_permission", 0),
        "expire_in": max(expire_in, 0),
    })


@route.post("/account/status")
async def set_account_status(request: Request, body: AccountStatusInfo) -> RouteResponse:
    """
    启用 / 封禁账号，只有 permission == 2 可以调用

    -> 中间件每个请求都会查库比对 disabled，所以封禁即时生效，不需要额外的黑名单
    -> 不能操作自己：唯一的高权限账号把自己封了，就没人能解开了
    -> 同权限之间可以互相操作，否则超管之间无法互相约束
    """
    token_info = request.state.info
    if not token_info or not isinstance(token_info, dict):
        return RouteResponse.error(msg="操作人信息不能为空，或格式错误", code=ResultCode.INVALID_VALUE_ERROR)
    if token_info.get("permission") != 2:
        return RouteResponse.error(msg="只有最高权限（permission=2）可以修改账号状态",
                                   code=ResultCode.PERMISSION_ERROR)
    if str(token_info.get("id")) == str(body.id):
        return RouteResponse.error(msg="不能修改自己的账号状态", code=ResultCode.PERMISSION_ERROR)
    try:
        with SessionLocal() as session:
            exists = session.scalar(select(func.count(AdminInfo.id)).where(AdminInfo.id == body.id)) or 0
            if not exists:
                return RouteResponse.error(msg=f"管理员 {body.id} 不存在", code=ResultCode.NOT_FOUND)
            session.execute(
                update(AdminInfo).where(AdminInfo.id == body.id).values(
                    disabled=body.disabled,
                    update_time=func.now(),
                )
            )
            session.commit()
    except Exception as e:
        logger.error(f"修改管理员 {body.id} 的状态失败：{e}")
        return RouteResponse.error(msg=f"修改账号状态失败：{e}", code=ResultCode.DB_ERROR)

    action = "封禁" if body.disabled else "启用"
    logger.info(f"{action}账号：target={body.id}, operator={token_info.get('id')}, reason={body.reason or '无'}")
    return RouteResponse.ok(msg=f"已{action}管理员 {body.id}")


@route.post("/account/update")
async def update_account(request: Request, body: AccountUpdateInfo) -> RouteResponse:
    """
    修改账号资料（用户名 / 邮箱 / 权限）

    -> 只有 permission == 2 可以调用
    -> 不能修改自己（避免把自己降级或改坏）
    -> 同级（permission == 2）只能改资料，不能改权限
    """
    token_info = request.state.info
    if not token_info or not isinstance(token_info, dict):
        return RouteResponse.error(msg="操作人信息不能为空，或格式错误", code=ResultCode.INVALID_VALUE_ERROR)
    if token_info.get("permission") != 2:
        return RouteResponse.error(msg="只有最高权限（permission=2）可以修改账号资料",
                                   code=ResultCode.PERMISSION_ERROR)
    if str(token_info.get("id")) == str(body.id):
        return RouteResponse.error(msg="不能修改自己的账号", code=ResultCode.PERMISSION_ERROR)

    values: dict[str, Any] = {}
    username = (body.username or "").strip()
    email = (body.email or "").strip()
    if username:
        values["username"] = username
    if email:
        if not re.match(email_re, email):
            return RouteResponse.error(msg="邮箱格式错误", code=ResultCode.NOT_VALUES_ERROR)
        values["email"] = email
    if body.permission is not None:
        if body.permission not in (0, 1, 2):
            return RouteResponse.error(msg="权限等级只能是 0/1/2", code=ResultCode.INVALID_VALUE_ERROR)
        values["permission"] = body.permission
    if not values:
        return RouteResponse.error(msg="没有需要修改的字段", code=ResultCode.NOT_VALUES_ERROR)

    try:
        with SessionLocal() as session:
            target = session.execute(
                select(AdminInfo).where(AdminInfo.id == body.id)
            ).scalars().first()
            if target is None:
                return RouteResponse.error(msg=f"管理员 {body.id} 不存在", code=ResultCode.NOT_FOUND)
            if "permission" in values and target.permission == 2:
                return RouteResponse.error(msg="不能修改同级账号的权限", code=ResultCode.PERMISSION_ERROR)

            session.execute(
                update(AdminInfo)
                .where(AdminInfo.id == body.id)
                .values(update_time=func.now(), **values)
            )
            session.commit()
            updated = session.execute(
                select(AdminInfo).where(AdminInfo.id == body.id)
            ).scalars().first()
            data = {
                "id": updated.id,
                "username": updated.username,
                "email": updated.email,
                "permission": updated.permission,
                "disabled": updated.disabled,
            }
    except IntegrityError:
        return RouteResponse.error(msg="用户名或邮箱已存在", code=ResultCode.DB_ERROR)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"修改管理员 {body.id} 资料失败：{exc}")
        return RouteResponse.error(msg=f"修改账号失败：{exc}", code=ResultCode.DB_ERROR)

    logger.info(f"修改账号：target={body.id}, fields={sorted(values)}, operator={token_info.get('id')}")
    return RouteResponse.ok(msg="账号已更新", data=data)


@route.get("/log_info")
async def get_log_info(
        request: Request, limit: int = 10, offset: int = 0,
        log_id: int | None = None, recommend_id: int | None = None,
        start_time: datetime | None = None, end_time: datetime | None = None,
) -> RouteResponse:
    """
    获取审批日志详情
    """
    token_info = request.state.info
    if not token_info or not isinstance(token_info, dict):
        return RouteResponse.error(msg="推荐人信息不能为空，或格式错误", code=ResultCode.INVALID_VALUE_ERROR)
    admin_id = token_info.get("id")
    admin_name = token_info.get("username")
    permission = token_info.get("permission")
    if not admin_id or not admin_name or not permission:
        return RouteResponse.error(msg="token 信息错误，请重新登录获取", code=ResultCode.INVALID_VALUE_ERROR)
    payload = {
        "limit": limit,
        "offset": offset,
        "id": admin_id,
        "name": admin_name,
        "permission": permission,
    }
    stmt = select(RecommendApprovalTable).limit(limit).offset(offset)
    if log_id:
        stmt = stmt.where(RecommendApprovalTable.id == log_id)
    if recommend_id:
        stmt = stmt.where(RecommendApprovalTable.recommend_id == recommend_id)
    if start_time and end_time:
        stmt = stmt.where(RecommendApprovalTable.create_time.between(start_time, end_time))
    try:
        with SessionLocal() as session:
            result = session.execute(stmt).mappings().all()
        if not result:
            payload["log_data"] = []
            payload["total"] = 0
            return RouteResponse.ok(msg="获取审批日志成功", data=payload)
        payload["log_data"] = result
        payload["total"] = len(result)
        return RouteResponse.ok(msg="获取审批日志成功", data=payload)
    except Exception as e:
        logger.error(f"获取审批日志失败：{e}")
        return RouteResponse.error(msg=f"获取审批日志失败：{e}", code=ResultCode.DB_ERROR)


@route.get("/admin_info")
async def get_admin_info(
        request: Request, limit: int = 10, offset: int = 0,
        disabled: bool = False, s_permission: Literal[0, 1, 2] = None,
        a_id: int | None = None,
) -> RouteResponse:
    """
    获取管理员信息，只能获取比自己权限等级低或者等于自己权限等级的管理员信息
    permission 会优先使用 token_info里面的等级，超出自身的直接采用自身的权限等级
    """
    token_info = request.state.info
    if not token_info or not isinstance(token_info, dict):
        return RouteResponse.error(msg="推荐人信息不能为空，或格式错误", code=ResultCode.INVALID_VALUE_ERROR)
    admin_id = token_info.get("id")
    admin_name = token_info.get("username")
    permission = token_info.get("permission")
    if not admin_id or not admin_name or not permission:
        return RouteResponse.error(msg="token 信息错误，请重新登录获取", code=ResultCode.INVALID_VALUE_ERROR)
    if not s_permission or s_permission > permission:
        s_permission = permission
    payload = {
        "s_permission": s_permission,
        "limit": limit,
        "offset": offset,
        "disabled": disabled,
    }
    stmt = select(
        AdminInfo.id,
        AdminInfo.username,
        AdminInfo.permission,
        AdminInfo.disabled,
        AdminInfo.email,
        AdminInfo.create_time,
    ).limit(limit).offset(offset).where(
        and_(
            AdminInfo.disabled == (false() if not disabled else true()),
            AdminInfo.permission <= s_permission,
        )
    )
    if a_id:
        stmt = stmt.where(AdminInfo.id == a_id)
    try:
        with SessionLocal() as session:
            result = session.execute(stmt).mappings().all()
        if not result:
            payload["admin_data"] = []
            payload["total"] = 0
            return RouteResponse.ok(msg="获取管理员信息成功", data=payload)
        # 对 email 进行脱敏
        mutable_results = [dict(row) for row in result]
        for item in mutable_results:
            email = item.get("email", "")
            email = email.strip()
            if not email:
                item['email'] = '<unknown>'
                continue
            if '@' not in email:
                item['email'] = '<unknown>'
                continue
            local, domain = email.rsplit('@', 1)
            if len(local) <= 2:
                masked_local = local
            else:
                masked_local = local[:2] + '*' * (len(local) - 2)
            item["email"] = masked_local + '@' + domain
        payload["admin_data"] = mutable_results
        payload["total"] = len(mutable_results)
        return RouteResponse.ok(msg="获取管理员信息成功", data=payload)
    except Exception as e:
        logger.error(f"获取管理员信息失败：{e}")
        return RouteResponse.error(msg=f"获取管理员信息失败：{e}", code=ResultCode.DB_ERROR)


@route.post("/login")
async def login(admin_info: AdminInputInfo) -> RouteResponse:
    """
    登录接口，返回token
    -> 登录成功后，会将管理员的信息封存到token中，后续在注册时需要验证该token
    """
    email = admin_info.email
    if not re.match(email_re, email):
        return RouteResponse.error(msg="邮箱格式错误", code=ResultCode.NOT_VALUES_ERROR)
    password = admin_info.password
    if not email or not password:
        return RouteResponse.error(msg="邮箱或密码不能为空", code=ResultCode.NOT_VALUES_ERROR)
    try:
        with SessionLocal() as session:
            stmt = select(AdminInfo).where(
                and_(
                    AdminInfo.email == email,
                    AdminInfo.disabled == false(),
                )
            )
            # 进行数量校验
            admin_info = session.execute(stmt).scalars().first()
            if not admin_info:
                return RouteResponse.error(msg="用户名或密码错误", code=ResultCode.NOT_VALUES_ERROR)
        user_db_password = admin_info.password
        if not check_password(password, user_db_password):
            return RouteResponse.error(msg="用户名或密码错误", code=ResultCode.NOT_VALUES_ERROR)
        # 封装token
        payload = {
            "id": admin_info.id,
            "username": admin_info.username,
            "permission": admin_info.permission,
            "disabled": admin_info.disabled,
        }
        # 无需手动设置过期时间，因为密钥会轮训更新
        token = jwt.encode(
            payload,
            key=redis_con.hget('jwt_key', 'new').encode('utf-8'),
            algorithm='HS256'
        )
        return RouteResponse.ok(msg="登录成功", data={
            "token": token,
            "payload": payload,
            "time": datetime.now(),
        })
    except Exception as e:
        logger.error(f"登录失败：{e}")
        return RouteResponse.error(msg=f"登录失败：{e}", code=ResultCode.DB_ERROR)


@route.get("/token")
async def update_token(request: Request, email: str, old_token: str) -> RouteResponse:
    """
    更新老旧的token
    """
    # 优先验证 old_token 是否真的过期
    try:
        # 若确实过期了，那么将会理应来说，会成功解析到 old_key
        old_key = redis_con.hget('jwt_key', 'old').encode('utf-8')
        jwt.decode(old_token, old_key, algorithms=['HS256'])
    except jwt.InvalidTokenError:
        logger.warning(f"token无效，或者该token还没过期，不用更新：{old_token}")
        return RouteResponse.error(msg="token无效，或者该token还没过期，不用更新", code=ResultCode.INVALID_VALUE_ERROR)
    except Exception as e:
        logger.error(f"token无效失败：{e}")
        return RouteResponse.error(msg=f"token无效，或者该token还没过期，不用更新，出错信息：{e}",
                                   code=ResultCode.INVALID_VALUE_ERROR)

    token_info = request.state.info
    # 先验证token信息是否有效
    if not token_info or not isinstance(token_info, dict):
        return RouteResponse.error(msg="token信息无效", code=ResultCode.NOT_VALUES_ERROR)
    if not re.match(email_re, email):
        return RouteResponse.error(msg="邮箱格式错误, token更新失败", code=ResultCode.NOT_VALUES_ERROR)
    try:
        # 一开始已经验证过，这里将会使用 request.state.info 中的 token_info
        token_info = request.state.info
        # 封装token
        payload = {
            "id": token_info.get("id"),
            "username": token_info.get("username"),
            "permission": token_info.get("permission"),
            "disabled": token_info.get("disabled"),
        }
        # 无需手动设置过期时间，因为密钥会轮训更新
        token = jwt.encode(
            payload,
            key=redis_con.hget('jwt_key', 'new').encode('utf-8'),
            algorithm='HS256'
        )
        return RouteResponse.ok(msg="token更新成功", data={
            "token": token,
            "payload": payload,
            "time": datetime.now(),
        })
    except Exception as e:
        logger.error(f"token更新失败：{e}")
        return RouteResponse.error(msg=f"token更新失败：{e}", code=ResultCode.DB_ERROR)
