#!/usr/bin/env python3
"""
风上忍小说文本清洗 - 第二轮补丁
处理第一轮遗漏的拼音替代模式
"""
import os
import re
import csv
import json
from datetime import datetime

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

def clean_all_pinyin(line):
    """用更通用的规则清洗所有拼音替代"""
    original = line

    # --- sè → 色 (通用) ---
    # 匹配: 中文字符 + sè 或 sè + 中文字符
    line = re.sub(r'([\u4e00-\u9fff])sè', r'\1色', line)
    line = re.sub(r'sè([\u4e00-\u9fff])', r'色\1', line)
    # 形形sèsè → 形形色色
    line = line.replace('sèsè', '色色')
    # 面sècháo → 面色潮
    line = re.sub(r'sècháo', '色潮', line)
    # 单独 sè 后跟非字母
    line = re.sub(r'sè([^a-zA-Z])', r'色\1', line)
    # sè 在行尾
    line = re.sub(r'sè$', '色', line)

    # --- xìng → 性 (通用，但排除"姓"的情况) ---
    # 常见后缀模式
    line = re.sub(r'([\u4e00-\u9fff])xìng', r'\1性', line)  
    line = re.sub(r'xìng([\u4e00-\u9fff])', r'性\1', line)
    # 单独 xìng 后跟非字母
    line = re.sub(r'xìng([^a-zA-Z])', r'性\1', line)
    # xìng 在行尾
    line = re.sub(r'xìng$', '性', line)

    # --- jīng → 精/经 (通用) ---
    line = re.sub(r'([\u4e00-\u9fff])jīng', r'\1精', line)
    line = re.sub(r'jīng([\u4e00-\u9fff])', r'精\1', line)
    line = re.sub(r'jīng([^a-zA-Z])', r'精\1', line)
    line = re.sub(r'jīng$', '精', line)

    # --- yīn → 阴 (通用) ---
    line = re.sub(r'([\u4e00-\u9fff])yīn', r'\1阴', line)
    line = re.sub(r'yīn([\u4e00-\u9fff])', r'阴\1', line)
    line = re.sub(r'yīn([^a-zA-Z])', r'阴\1', line)
    line = re.sub(r'yīn$', '阴', line)

    # --- shè → 射 (通用) ---
    line = re.sub(r'([\u4e00-\u9fff])shè', r'\1射', line)
    line = re.sub(r'shè([\u4e00-\u9fff])', r'射\1', line)
    line = re.sub(r'shè([^a-zA-Z])', r'射\1', line)
    line = re.sub(r'shè$', '射', line)

    # --- rì → 日 (通用) ---
    line = re.sub(r'([\u4e00-\u9fff])rì', r'\1日', line)
    line = re.sub(r'rì([\u4e00-\u9fff])', r'日\1', line)
    line = re.sub(r'(\d)rì', r'\1日', line)
    line = re.sub(r'rì([^a-zA-Z])', r'日\1', line)
    line = re.sub(r'rì$', '日', line)

    # --- yín → 淫 (通用) ---
    line = re.sub(r'([\u4e00-\u9fff])yín', r'\1淫', line)
    line = re.sub(r'yín([\u4e00-\u9fff])', r'淫\1', line)
    line = re.sub(r'yín([^a-zA-Z])', r'淫\1', line)

    # --- jī活 → 激活 ---
    line = line.replace('jī活', '激活')

    # --- cháo红 → 潮红 ---
    line = line.replace('cháo红', '潮红')

    # --- 其他常见拼音 ---
    # àn → 暗/案 etc
    line = re.sub(r'yīnàn', '阴暗', line)

    # --- se (without tone) → 色 ---
    line = re.sub(r'([\u4e00-\u9fff])se\b', r'\1色', line)
    line = re.sub(r'\bse([\u4e00-\u9fff])', r'色\1', line)

    return line, (line != original)


def main():
    print("风上忍小说文本清洗 - 第二轮补丁")
    print("=" * 60)

    files = [
        '第七脑域_utf8.txt',
        '末日咆哮1_utf8.txt',
        '末日咆哮2_utf8.txt',
        '时空之头号玩家_utf8.txt',
        '异体_utf8.txt',
    ]

    report_entries = []
    all_stats = []

    for fname in files:
        fpath = os.path.join(PROJECT_DIR, fname)
        if not os.path.exists(fpath):
            continue

        with open(fpath, 'r', encoding='utf-8') as f:
            content = f.read()

        lines = content.split('\n')
        new_lines = []
        modified_count = 0

        for i, line in enumerate(lines):
            cleaned, modified = clean_all_pinyin(line)
            if modified:
                modified_count += 1
                report_entries.append({
                    'file': fname,
                    'line': i + 1,
                    'type': 'pinyin_v2',
                    'original': line[:100],
                    'cleaned': cleaned[:100],
                })
            new_lines.append(cleaned)

        with open(fpath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(new_lines))

        print(f"  {fname}: {modified_count} 行修正")
        all_stats.append({'file': fname, 'lines_modified': modified_count})

    # 追加到CSV报告
    csv_path = os.path.join(PROJECT_DIR, 'cleaning_report.csv')
    with open(csv_path, 'a', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['file', 'line', 'type', 'original', 'cleaned'])
        for entry in report_entries:
            writer.writerow(entry)

    print(f"\n第二轮修正: {len(report_entries)} 条记录")
    print(f"已追加到: {csv_path}")

    # 更新结果JSON
    result_path = os.path.join(PROJECT_DIR, 'cleaning_result.json')
    with open(result_path, 'r', encoding='utf-8') as f:
        result = json.load(f)
    result['v2_timestamp'] = datetime.now().isoformat()
    result['v2_lines_modified'] = len(report_entries)
    result['v2_statistics'] = all_stats
    with open(result_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print("完成!")


if __name__ == '__main__':
    main()
