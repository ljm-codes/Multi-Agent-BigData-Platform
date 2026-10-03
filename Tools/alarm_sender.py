import json
import os
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.utils import formataddr
from typing import Optional

import requests

# from langchain_core.tools import tool

from Tools import LogSetting
from load_config.config import config

logger = LogSetting.create(__name__)

_email_config = config.get('email')
_feishu_uri = config.get("feishu", {}).get("uri", "")
if not _email_config:
    raise ValueError('邮箱配置不能为空')
_stmp = _email_config.get('smtp')
_port = int(_email_config.get('port'))
_sender = _email_config.get('sender')
_sender_name = _email_config.get('sender_name')
_receiver = _email_config.get('receiver')

if isinstance(_receiver, list):
    if len(_receiver) == 1:
        _receiver = _receiver[0]
    else:
        _receiver = ','.join(_receiver)


def send_error_email(
        subject: str,
        content: str,
):
    """
    发送错误消息的邮件

    :param subject: 邮件主题
    :param content: 邮件内容
    """
    msg = MIMEText(_text=content, _subtype="plain")
    msg['From'] = formataddr((_sender_name, _sender))
    msg['To'] = Header(_receiver, charset='utf-8')
    msg['Subject'] = Header(f"错误信息：{subject}", charset='utf-8')

    try:
        with smtplib.SMTP_SSL(_stmp, _port, timeout=10) as server:
            server.login(_sender, os.getenv("EMAIL_AUTH_CODE"))
            server.sendmail(_sender, _receiver, msg.as_string())
            logger.info("邮件发送成功")
        return True
    except smtplib.SMTPException as e:
        logger.error(f"邮件发送失败，错误：{e}")
        return False


# @tool
# def send_error_email_tool(
#         subject: str,
#         content: str,
# ):
#     """
#     该工具用于将当前的发生错误消息发送到技术人员的邮箱中，若在运行当中引发的错误，可以通过此工具发送错误信息
#     参数：
#         subject: 主题（标题） - 用于概要当前错误的简单标题
#         content: 错误的详细消息
#     """
#     return "邮件发送成功" if send_error_email(subject, content) else "邮件发送失败"


def send_warning_msg(
        app_id: str, description: str,
        error_id: Optional[list[int]] = None,
):
    """
    不向 Agent 进行提供该工具， 采用手动 告警
    向人员发送警告消息
    Args:
        error_id: 错误ID
        app_id: YARN 应用ID
        description: 警告简单描述
    Returns:

    """
    try:
        error_id_str = ','.join(map(str, error_id)) if error_id else "None"
        resp = requests.post(_feishu_uri, headers={"Content-Type": "application/json"},
                             data=json.dumps({
                                 "msg_type": "text",
                                 "content": {
                                     "text": f"错误ids: {error_id_str} 来自 {app_id}，{description}"
                                 }
                             }), timeout=10)
        if resp.status_code != 200:
            logger.error(f"发送消息失败，状态码 {resp.status_code}，响应体 {resp.text}")
            return False, f"发送消息失败，状态码 {resp.status_code}，响应体 {resp.text}"
        content_json = resp.json()
        code = content_json.get("code")
        if code in (9499, 19002):
            return False, f"发送消息失败，状态码 {code}，请检查JSON 结构是否符合对应消息类型的规范。"
        if code == 19021:
            return False, f"发送消息失败，状态码 {code}，签名校验失败，响应体 {content_json.get('msg')}"
        if code == 11232:
            return False, f"发送消息过于频繁，状态码 {code}，请稍后重试。"
        if code == 230051:
            return False, f"发送的内存存在敏感词，状态码 {code}，响应体 {content_json.get('msg')}"
        if code in (10003, 10004, 99999):
            return False, f"可能是 群不存在 未启用 权限不足，状态码 {code}，响应体 {content_json.get('msg')}"
        return True, "success"
    except requests.exceptions.Timeout:
        return False, f"请求超时(10s)"
    except requests.exceptions.ConnectionError as e:
        return False, f"连不上: {e}"
    except requests.exceptions.RequestException as e:
        return False, f"{type(e).__name__}: {e}"


if __name__ == '__main__':
    send_error_email(subject="测试错误", content="这是一条测试错误消息")
