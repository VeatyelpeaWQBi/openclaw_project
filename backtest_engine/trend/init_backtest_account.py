#!/usr/bin/env python3
"""
回测模拟账户初始化脚本

用法:
  python backtest_engine/trend/init_backtest_account.py --capital 100000 --name "趋势回测1号"
  python backtest_engine/trend/init_backtest_account.py --capital 200000 --name "趋势回测2号"
"""

import sys
import os

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_QT_ROOT = os.path.join(_PROJECT_ROOT, 'quant_trading')
if _QT_ROOT not in sys.path:
    sys.path.insert(0, _QT_ROOT)

import argparse
from infra.account_manager import AccountManager


def init_account(capital, name):
    """
    创建回测模拟账户

    参数:
        capital: 初始资金
        name: 账户昵称

    返回:
        dict: 账户信息
    """
    am = AccountManager()

    # 用昵称作为bind_id，避免重复创建
    bind_id = f"backtest_{name}"

    # 检查是否已存在
    existing = am.get_account_by_bind_id(bind_id)
    if existing:
        print(f"账户已存在: ID={existing['id']}, 昵称={existing.get('nickname')}, 资金={existing.get('total_capital')}")
        return existing

    # 创建新账户
    account = am.init_account_by_bind_id(
        bind_id=bind_id,
        capital=capital,
        nickname=name,
        simulator=1,  # 模拟账户
    )

    print(f"模拟账户创建成功!")
    print(f"  账户ID: {account['id']}")
    print(f"  昵称: {name}")
    print(f"  初始资金: {capital:,.2f}")
    print(f"  类型: 模拟账户(simulator=1)")
    print(f"  bind_id: {bind_id}")

    return account


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='创建回测模拟账户')
    parser.add_argument('--capital', type=float, required=True, help='初始资金')
    parser.add_argument('--name', type=str, required=True, help='账户昵称')
    args = parser.parse_args()

    account = init_account(args.capital, args.name)
    print(f"\n回测时使用 account_id={account['id']}")
