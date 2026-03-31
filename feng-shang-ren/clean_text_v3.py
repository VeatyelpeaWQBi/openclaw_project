#!/usr/bin/env python3
"""
精准清洗脚本 v3
关键原则：宁可不洗，不可误删。只删确定是脏数据的内容。
"""
import re
import csv
import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 书籍配置
BOOKS = {
    "第七脑域": os.path.join(BASE_DIR, "第七脑域_utf8.txt"),
    "末日咆哮1": os.path.join(BASE_DIR, "末日咆哮1_utf8.txt"),
    "末日咆哮2": os.path.join(BASE_DIR, "末日咆哮2_utf8.txt"),
    "时空之头号玩家": os.path.join(BASE_DIR, "时空之头号玩家_utf8.txt"),
    "异体": os.path.join(BASE_DIR, "异体_utf8.txt"),
}

# ============ A. 独立声明/分隔行（整行删除） ============
# 只匹配整行就是这些内容的情况，不匹配包含正文的行
FULL_LINE_PATTERNS = [
    # 网站声明
    re.compile(r'^声明：本书为TXT图书下载网.*$'),
    re.compile(r'^申明:本书由TXT图书下载网.*$'),
    # 用户上传分隔线
    re.compile(r'^-{10,}用户上传之内容开始-{10,}$'),
    re.compile(r'^-{10,}用户上传之内容结束-{10,}$'),
    # 分节阅读
    re.compile(r'^分节阅读\s*\d+\s*$'),
    # 独立的"------------"行（至少10个横线）
    re.compile(r'^-{10,}\s*$'),
    # TXT图书下载网推广行
    re.compile(r'^更多精彩好书.*TXT图书下载网.*$'),
]

# ============ B. 行尾/行中网站水印（只删除水印部分） ============
# 每个模式：(regex, replacement, description)
WATERMARK_PATTERNS = [
    # (顶点小说手打小说) - 行尾或行中，有括号
    (re.compile(r'\(顶点小说手打小说\)'), '', '(顶点小说手打小说)'),
    # 顶点小说手打小说 - 无括号变体
    (re.compile(r'顶点小说手打小说'), '', '顶点小说手打小说'),
    # レwww.shuyaya.com&spades;思&hearts;路&clubs;客レ - 完整水印
    (re.compile(r'レwww\.shuyaya\.com&spades;思&hearts;路&clubs;客レ'), '', 'レwww.shuyaya.com水印'),
    # 小^说^无广告的~顶点*小说~网www.shuyaya.com - 行尾
    (re.compile(r'小\^说\^无广告的~顶点\*小说~网www\.shuyaya\.com'), '', 'www.shuyaya.com水印'),
    # ['www.shuyaya.com']免费文字更新! - 行尾
    (re.compile(r"\['www\.shuyaya\.com'\]免费文字更新!"), '', 'shuyaya.com推广'),
]

# ============ C. 章节标题行中的广告（只删除标记部分） ============
AD_IN_TITLE_PATTERNS = [
    # (本章免费)
    (re.compile(r'\(本章免费\)'), '', '(本章免费)'),
    # 【顶点小说】
    (re.compile(r'【顶点小说】'), '', '【顶点小说】'),
]


def clean_line(line):
    """对单行进行清洗，返回 (cleaned_line, action, detail)"""
    original = line.rstrip('\n\r')
    stripped = original.strip()

    # A. 检查是否是独立的声明/分隔行 → 整行删除
    for pattern in FULL_LINE_PATTERNS:
        if pattern.match(stripped):
            return None, 'full_line_removed', pattern.pattern

    cleaned = original
    actions = []

    # B. 行尾/行中网站水印 → 只删除水印部分
    for pattern, replacement, desc in WATERMARK_PATTERNS:
        if pattern.search(cleaned):
            cleaned = pattern.sub(replacement, cleaned)
            actions.append(desc)

    # C. 章节标题中的广告 → 只删除标记部分
    for pattern, replacement, desc in AD_IN_TITLE_PATTERNS:
        if pattern.search(cleaned):
            cleaned = pattern.sub(replacement, cleaned)
            actions.append(desc)

    if actions:
        # 清理可能产生的多余空格
        cleaned = re.sub(r'\s{3,}', '  ', cleaned)
        return cleaned, 'watermark_removed', '; '.join(actions)

    return cleaned, 'kept', ''


def process_book(name, input_path, output_path, csv_writer):
    """处理一本书"""
    with open(input_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    cleaned_lines = []
    stats = {'total': 0, 'kept': 0, 'full_line_removed': 0, 'watermark_removed': 0}

    for i, line in enumerate(lines, 1):
        stats['total'] += 1
        cleaned, action, detail = clean_line(line)

        if cleaned is None:
            # 整行删除
            stats['full_line_removed'] += 1
            csv_writer.writerow([name, i, action, line.rstrip('\n\r'), '', detail])
        elif action == 'watermark_removed':
            stats['watermark_removed'] += 1
            cleaned_lines.append(cleaned + '\n')
            csv_writer.writerow([name, i, action, line.rstrip('\n\r'), cleaned, detail])
        else:
            stats['kept'] += 1
            cleaned_lines.append(line)
            # 只记录删除/修改的行，保留的行太多不记录

    with open(output_path, 'w', encoding='utf-8') as f:
        f.writelines(cleaned_lines)

    return stats


def main():
    # 创建输出目录
    clean_dir = os.path.join(BASE_DIR, "cleaned_v3")
    os.makedirs(clean_dir, exist_ok=True)

    csv_path = os.path.join(clean_dir, "cleaning_report_v3.csv")
    all_stats = {}

    with open(csv_path, 'w', encoding='utf-8', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['book', 'line_number', 'action', 'original', 'cleaned', 'detail'])

        for name, input_path in BOOKS.items():
            output_path = os.path.join(clean_dir, f"{name}_clean.txt")
            print(f"处理: {name} ...")
            stats = process_book(name, input_path, output_path, csv_writer)
            all_stats[name] = stats
            print(f"  总行数: {stats['total']}, 保留: {stats['kept']}, "
                  f"整行删除: {stats['full_line_removed']}, "
                  f"水印清理: {stats['watermark_removed']}")

    # 保存结果
    result = {
        "version": "v3",
        "description": "精准清洗，宁可不洗不可误删",
        "stats": all_stats,
        "output_dir": clean_dir,
        "report": csv_path,
    }

    result_path = os.path.join(BASE_DIR, "cleaning_result_v3.json")
    with open(result_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n清洗完成！报告: {csv_path}")
    print(f"结果: {result_path}")


if __name__ == '__main__':
    main()
