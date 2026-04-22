#!/usr/bin/env python3
"""
恐贪指数数据获取模块
网站: https://app.jiucaishuo.com/pagesE/tool/fear_greed

运行时间: ~10秒

使用方法:
    from fetch_fear_index import get_fear_index
    data = get_fear_index()

返回数据结构:
    {
        'score': 75,
        'score_decimal': 74.64,
        'update_time': '2026-04-21',
        'status': '贪婪',
        'indicators': {
            'volatility': {'value': 15.15, 'status': '近一年较低'},
            'volume': {'value': 24096.94, 'status': '近一年较高'},
            ...
        },
        'history': {
            '1day': {'status': '贪婪'},
            '1week': {'status': '中立'},
            ...
        },
        'elapsed_seconds': 9.5,
        'fetch_time': '2026-04-22 02:06:40',
    }
"""

import json
import re
import time
from datetime import datetime


def get_fear_index(timeout=30):
    """
    获取恐贪指数数据

    Args:
        timeout: 超时时间(秒)

    Returns:
        dict: 恐贪指数数据
    """
    from playwright.sync_api import sync_playwright

    start_time = time.time()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080},
        )
        page = context.new_page()

        page.goto('https://app.jiucaishuo.com/pagesE/tool/fear_greed', timeout=timeout * 1000)
        page.wait_for_timeout(8000)

        page_text = page.evaluate('() => document.body.innerText')
        browser.close()

    elapsed = time.time() - start_time

    data = parse_fear_index(page_text)
    data['elapsed_seconds'] = round(elapsed, 2)
    data['fetch_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return data


def parse_fear_index(text):
    """
    从页面文本解析恐贪指数数据
    """
    data = {
        'score': None,
        'score_decimal': None,
        'update_time': None,
        'status': None,
        'indicators': {},
        'history': {},
    }

    # 恐贪指数整数
    match = re.search(r'指数数值\s*(\d+)', text)
    if match:
        data['score'] = int(match.group(1))

    # 精确值
    match = re.search(r'恐惧贪婪指数\s*(\d+\.\d+)', text)
    if match:
        data['score_decimal'] = float(match.group(1))

    # 更新时间
    match = re.search(r'更新时间[:\s]*(\d{4}[-]\d{1,2}[-]\d{1,2})', text)
    if match:
        data['update_time'] = match.group(1)

    # 状态 (贪婪/恐惧等)
    match = re.search(r'指数属性\s*(极度恐惧|恐惧|中立|贪婪|极度贪婪)', text)
    if match:
        data['status'] = match.group(1)

    # 六大指标
    indicators_map = {
        'volatility': (r'50ETF期权波动率\s*(\d+\.\d+)%', r'指数波动率\s*(近一年[^月\n]+)'),
        'volume': (r'两市成交量\s*(\d+\.\d+)亿', r'两市成交量\s*(近一年[^月\n]+)'),
        'price_strength': (r'创新高个股占比\s*(\d+\.\d+)%', r'股价强度\s*(近一年[^月\n]+)'),
        'futures_spread': (r'股指期货升贴水率\s*(-?\d+\.\d+)%', r'升贴水率\s*(近一年[^月\n]+)'),
        'safe_haven': (r'股债回报差\s*(\d+\.\d+)%', r'避险天堂\s*(近一年[^月\n]+)'),
        'leverage': (r'融资买入占比\s*(\d+\.\d+)%', r'杠杆水平\s*(近一年[^月\n]+)'),
    }

    for name, (value_pattern, status_pattern) in indicators_map.items():
        value_match = re.search(value_pattern, text)
        status_match = re.search(status_pattern, text)

        value = float(value_match.group(1)) if value_match else None
        status = status_match.group(1).strip() if status_match else None

        if value is not None or status:
            data['indicators'][name] = {
                'value': value,
                'status': status,
            }
            if name == 'leverage':
                data['indicators'][name]['note'] = '不计入指数'

    # 历史对比
    history_patterns = [
        ('1day', r'1日前\s*(极度恐惧|恐惧|中立|贪婪|极度贪婪)'),
        ('1week', r'1周前\s*(极度恐惧|恐惧|中立|贪婪|极度贪婪)'),
        ('1month', r'1月前\s*(极度恐惧|恐惧|中立|贪婪|极度贪婪)'),
        ('1year', r'1年前\s*(极度恐惧|恐惧|中立|贪婪|极度贪婪)'),
    ]

    for period, pattern in history_patterns:
        match = re.search(pattern, text)
        if match:
            data['history'][period] = {'status': match.group(1)}

    return data


def save_to_file(data, filepath='fear_index_latest.json'):
    """保存数据到文件"""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return filepath


def main():
    """主函数"""
    print(f"=== 获取恐贪指数 @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")

    start = time.time()
    data = get_fear_index(timeout=30)
    total_time = time.time() - start

    print(f"\n恐贪指数: {data['score']} ({data.get('score_decimal', 'N/A')})")
    print(f"状态: {data['status']}")
    print(f"更新时间: {data['update_time']}")
    print(f"获取耗时: {data['elapsed_seconds']}秒")

    print("\n六大情绪指标:")
    for name, ind in data['indicators'].items():
        note = f" ({ind.get('note', '')})" if ind.get('note') else ""
        print(f"  {name}: {ind['value']} - {ind['status']}{note}")

    print("\n历史对比:")
    for period, hist in data['history'].items():
        print(f"  {period}: {hist['status']}")

    print(f"总运行时间: {total_time:.2f}秒")

    return data


if __name__ == '__main__':
    main()