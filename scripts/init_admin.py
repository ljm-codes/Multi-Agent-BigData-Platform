"""
初始化管理员账号

数据库里没有 admin_info 记录时，谁也登不进去（登录查的是邮箱）。
这个脚本负责创建第一个账号，默认权限 2。

用法：
    python scripts/init_admin.py --email admin@example.com --password Admin@123
"""
import argparse
import sys
from pathlib import Path

import bcrypt
import psycopg
import yaml

ROOT_DIR = Path(__file__).resolve().parent.parent


def main() -> int:
    parser = argparse.ArgumentParser(description="创建平台管理员账号")
    parser.add_argument("--username", default="admin", help="用户名，默认 admin")
    parser.add_argument("--email", required=True, help="登录用的邮箱（登录时填这个）")
    parser.add_argument("--password", required=True, help="登录密码")
    parser.add_argument("--permission", type=int, default=2, choices=(0, 1, 2),
                        help="权限等级，默认 2（最高）")
    args = parser.parse_args()

    config_path = ROOT_DIR / "config.yaml"
    if not config_path.is_file():
        print(f"[失败] 找不到配置文件：{config_path}")
        return 1

    pg = yaml.safe_load(config_path.read_text(encoding="utf-8"))["databases"]["postgres"]
    password_hash = bcrypt.hashpw(args.password.encode(), bcrypt.gensalt()).decode()

    with psycopg.connect(host=pg["host"], port=pg["port"], user=pg["user"],
                         password=pg["password"], dbname=pg["db"]) as conn:
        exists = conn.execute(
            "select 1 from admin_info where email = %s", (args.email,)
        ).fetchone()
        if exists:
            print(f"[跳过] 邮箱已存在：{args.email}")
            return 1

        conn.execute(
            "insert into admin_info (username, password, email, permission, disabled) "
            "values (%s, %s, %s, %s, false)",
            (args.username, password_hash, args.email, args.permission),
        )
        conn.commit()

    print("[完成] 管理员已创建")
    print(f"  邮箱: {args.email}")
    print(f"  密码: {args.password}")
    print(f"  权限: {args.permission}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
