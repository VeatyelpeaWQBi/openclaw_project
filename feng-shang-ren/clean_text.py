#!/usr/bin/env python3
"""
风上忍小说文本清洗脚本
清洗5本小说中的脏数据：拼音替代、广告水印、乱码等
"""
import os
import re
import csv
import json
from datetime import datetime

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

# ============ 清洗规则 ============

# 1. 拼音声调替代还原字典
# 格式: (正则模式, 替换为)
PINYIN_REPLACEMENTS = [
    # --- 性 (xìng) ---
    # 需要非常谨慎，xìng 可能是"性"或"姓"
    # 在以下语境中几乎肯定是"性":
    (r'女xìng', '女性'),
    (r'男xìng', '男性'),
    (r'女xìng', '女性'),
    (r'xìng格', '性格'),
    (r'xìng取向', '性取向'),
    (r'xìng攻击', '性攻击'),
    (r'xìng虐', '性虐'),
    (r'xìng感', '性感'),
    (r'xìng福', '幸福'),  # 谐音
    (r'xìng别', '性别'),
    (r'xìng欲', '性欲'),
    (r'xìng生活', '性生活'),
    (r'xìng质', '性质'),
    (r'xìng命', '性命'),
    (r'xìng能', '性能'),
    (r'xìng爱', '性爱'),
    (r'xìng行为', '性行为'),
    (r'xìng骚扰', '性骚扰'),
    (r'xìng工作者', '性工作者'),
    (r'xìng教育', '性教育'),
    (r'灭绝xìng', '灭绝性'),
    (r'压倒xìng', '压倒性'),
    (r'观赏xìng', '观赏性'),
    (r'纪律xìng', '纪律性'),
    (r'弹xìng', '弹性'),
    (r'象征xìng', '象征性'),
    (r'针对xìng', '针对性'),
    (r'突发xìng', '突发性'),
    (r'可能xìng', '可能性'),
    (r'重要xìng', '重要性'),
    (r'特殊xìng', '特殊性'),
    (r'灵活xìng', '灵活性'),
    (r'实用xìng', '实用性'),
    (r'戏剧xìng', '戏剧性'),
    (r'全面xìng', '全面性'),
    (r'一致xìng', '一致性'),
    (r'属xìng', '属性'),
    (r'理xìng', '理性'),
    (r'小xìng子', '小性子'),
    (r'耍.*?xìng子', lambda m: m.group(0).replace('xìng', '性')),
    (r'弱受的xìng格', '弱受的性格'),
    (r'软软的xìng格', '软软的性格'),
    (r'爽朗.*?xìng', lambda m: m.group(0).replace('xìng', '性') if '男xìng向' in m.group(0) else m.group(0)),
    (r'男xìng向', '男性向'),
    (r'xìng格', '性格'),  # catch-all for remaining xìng格

    # --- 色 (sè) ---
    (r'红sè', '红色'),
    (r'蓝sè', '蓝色'),
    (r'青sè', '青色'),
    (r'白sè', '白色'),
    (r'黑sè', '黑色'),
    (r'绿sè', '绿色'),
    (r'紫sè', '紫色'),
    (r'灰sè', '灰色'),
    (r'黄sè', '黄色'),
    (r'颜sè', '颜色'),
    (r'特sè', '特色'),
    (r'白se', '白色'),
    (r'红se', '红色'),
    (r'蓝se', '蓝色'),
    (r'青se', '青色'),
    (r'黑se', '黑色'),
    (r'绿se', '绿色'),
    (r'紫se', '紫色'),
    (r'灰se', '灰色'),
    (r'黄se', '黄色'),
    (r'颜se', '颜色'),
    (r'特se', '特色'),
    (r'白sé', '白色'),

    # --- 精/经 (jīng) ---
    (r'jīng神', '精神'),
    (r'jīng神力', '精神力'),
    (r'jīng神屏障', '精神屏障'),
    (r'jīng英', '精英'),
    (r'jīng通', '精通'),
    (r'jīng明', '精明'),
    (r'jīng心', '精心'),
    (r'jīng致', '精致'),
    (r'jīng打采', '精打采'),
    (r'蜈蚣jīng', '蜈蚣精'),
    (r'jīng锐', '精锐'),

    # --- 阴 (yīn) ---
    (r'yīn影', '阴影'),
    (r'yīn招', '阴招'),
    (r'yīn差阳错', '阴差阳错'),
    (r'yīn凉', '阴凉'),
    (r'yīn暗', '阴暗'),
    (r'yīn谋', '阴谋'),

    # --- 射/涉 (shè) ---
    (r'反shè', '反射'),
    (r'反shè点', '反射点'),
    (r'神经反shè', '神经反射'),
    (r'jīng神反shè', '精神反射'),
    (r'shè击', '射击'),
    (r'shè出', '射出'),
    (r'发shè', '发射'),

    # --- 日 (rì) ---
    (r'末rì', '末日'),
    (r'末rì幻境', '末日幻境'),
    (r'当rì', '当日'),
    (r'rì幻境', '日幻境'),
    (r'11rì', '11日'),
    (r'\d+rì', lambda m: m.group(0).replace('rì', '日')),

    # --- 淫 (yín) ---
    (r'yín乐', '淫乐'),
    (r'yín笑', '淫笑'),
    (r'yín荡', '淫荡'),

    # --- 阴部/下体相关 (censored with ***) ---
    # 这些需要根据上下文判断，单独处理
]

# 2. 广告/水印行模式
AD_LINE_PATTERNS = [
    # bookdown.com 完整行
    re.compile(r'^\s*(声明|申明).*?bookdown\.com\.cn.*$'),
    re.compile(r'^.*?TXT图书下载网.*?bookdown\.com\.cn.*$'),
    re.compile(r'^.*?更多精彩好书.*?TXT图书下载网.*$'),
    re.compile(r'^-{10,}.*?用户上传.*?-{10,}$'),
    re.compile(r'^分节阅读\s*\d+\s*$'),
    # shuyaya.com 水印行
    re.compile(r'^.*?顶点小说手打小说.*$'),
    re.compile(r'^.*?小\^说\^无广告.*$'),
    re.compile(r'^.*?レwww\.shuyaya\.com.*$'),
    # 章节标题行中的 (本章免费)
    re.compile(r'\(本章免费\)'),
]

# 3. 行内水印模式（需要从行内移除，保留正文）
INLINE_WATERMARKS = [
    # bookdown.com 变体
    re.compile(r'ｗωｗwww\.bookdown書com网'),
    re.compile(r'ｗｗｗwww\.bookdown書com网'),
    re.compile(r'www\.bookdown\.(сom|com)\.cn'),
    # shuyaya.com 变体
    re.compile(r'小\^说\^无广告的~顶点\*小说~网www\.shuyaya\.com'),
    re.compile(r"顶点小说手打小说\['www\.shuyaya\.com'\]免费文字更新!"),
    re.compile(r'レwww\.shuyaya\.com&spades;思&hearts;路&clubs;客レ'),
    re.compile(r'レwww\.shuyaya\.com.*?レ'),
    # 残留的 https://
    re.compile(r'https://$'),
]

# 4. 行内 *** 恢复上下文字典
# 基于上下文推断被***遮蔽的字
STAR_RESTORE_PATTERNS = [
    # 末日咆哮1 中的 *** 恢复
    (re.compile(r'蹲\*\*\*'), '蹲下身'),
    (re.compile(r'嘴唇\*\*\*了'), '嘴唇蹭了'),
    (re.compile(r'保留\*\*\*的高尚'), '保留贞操的高尚'),
    (re.compile(r'夺取了她的\*\*\*'), '夺取了她的贞操'),
    (re.compile(r'空间\*\*\*的瞬间逃出对手的\*\*\*'),
     '空间限制的瞬间逃出对手的攻击范围'),
    (re.compile(r'碎片\*\*\*交易'), '碎片黑市交易'),
    (re.compile(r'碎片\*\*\*房间'), '碎片训练房间'),
    (re.compile(r'时间\*\*\*空'), '时间内打空'),
    (re.compile(r'范围内\*\*\*选择'), '范围内自由选择'),
    (re.compile(r'华夏\*\*\*方'), '华夏军方'),
    (re.compile(r'华夏\*\*\*方是真的'), '华夏军方是真的'),
    (re.compile(r'首都京城的\*\*\*们'), '首都京城的权贵们'),
    (re.compile(r'镜头\*\*\*现'), '镜头中出现'),
    (re.compile(r'你\*\*\*那份闲心'), '你操那份闲心'),
    (re.compile(r'这最后的\*\*\*阶段'), '这最后的冲刺阶段'),
    (re.compile(r'如同\*\*\*般'), '如同花蕊般'),
    (re.compile(r'借过联盟中的\*\*\*保密'), '借过联盟中的内部保密'),
    (re.compile(r'不过\*\*\*上的事情'), '不过政治上的事情'),
    (re.compile(r'\*\*\*交易区'), '黑市交易区'),
    (re.compile(r'青龙、\*\*\*、朱雀'), '青龙、白虎、朱雀'),
    (re.compile(r'许默忽然有种想.*?错误的\*\*\*'), None),  # skip, too ambiguous
    (re.compile(r'\*\*\*\s*$'), None),  # skip standalone *** at end of line

    # 末日咆哮2 中的 *** 恢复
    (re.compile(r'你\*\*\*不会'), '他妈不会'),
    (re.compile(r'你们现在早\*\*\*喂了'), '你们现在早他妈喂了'),
    (re.compile(r'我们\*\*\*该你们的'), '我们他妈该你们的'),
    (re.compile(r'谁\*\*\*把我挤出去的'), '他妈谁把我挤出去的'),
    (re.compile(r'看你下次还\*\*\*敢不敢'), '看你下次还他妈敢不敢'),
    (re.compile(r'你这个小婊子想\*\*\*撞死我'), '你这个小婊子想故意撞死我'),
    (re.compile(r'求爷爷告\*\*\*'), '求爷爷告奶奶'),
    (re.compile(r'爷的面，他不敢'), '爷的面，他不敢'),  # skip
    (re.compile(r'姑\*\*\*面'), '姑奶奶面'),
    (re.compile(r'青白色\*\*\*\*'), '青白色肌肤'),
]

# 5. 乱码修复
MOJIBAKE_FIXES = [
    (r'雪��', '雪花'),
    (r'购���中心', '购物中心'),
]


def clean_line(line, line_num, filename, report_entries):
    """清洗单行文本，返回(清洗后行, 是否被修改)"""
    original = line
    modified = False

    # 1. 行内水印移除
    for wm_pattern in INLINE_WATERMARKS:
        new_line, count = wm_pattern.subn('', line)
        if count > 0:
            line = new_line
            modified = True
            report_entries.append({
                'file': filename,
                'line': line_num,
                'type': 'inline_watermark',
                'pattern': wm_pattern.pattern[:40],
                'original': original.strip()[:80],
                'cleaned': line.strip()[:80],
            })

    # 2. 乱码修复
    for old, new in MOJIBAKE_FIXES:
        if old in line:
            line = line.replace(old, new)
            modified = True
            report_entries.append({
                'file': filename,
                'line': line_num,
                'type': 'mojibake',
                'pattern': old,
                'original': original.strip()[:80],
                'cleaned': line.strip()[:80],
            })

    # 3. 拼音声调替代还原
    for pattern, replacement in PINYIN_REPLACEMENTS:
        if callable(replacement):
            new_line = re.sub(pattern, replacement, line)
        else:
            new_line = re.sub(pattern, replacement, line)
        if new_line != line:
            line = new_line
            modified = True

    # 4. 行内 *** 恢复
    for pattern, replacement in STAR_RESTORE_PATTERNS:
        if replacement is None:
            continue  # 跳过无法确定的模式
        if pattern.search(line):
            new_line = pattern.sub(replacement, line)
            if new_line != line:
                line = new_line
                modified = True
                report_entries.append({
                    'file': filename,
                    'line': line_num,
                    'type': 'star_restore',
                    'pattern': pattern.pattern[:40],
                    'original': original.strip()[:80],
                    'cleaned': line.strip()[:80],
                })

    # 5. (本章免费) 移除
    if '(本章免费)' in line:
        line = line.replace('(本章免费)', '')
        modified = True

    if modified:
        report_entries.append({
            'file': filename,
            'line': line_num,
            'type': 'pinyin_or_other',
            'pattern': '',
            'original': original.strip()[:80],
            'cleaned': line.strip()[:80],
        })

    return line, modified


def is_ad_line(line):
    """判断是否为广告/水印行"""
    stripped = line.strip()
    for pattern in AD_LINE_PATTERNS:
        if pattern.search(stripped):
            return True
    return False


def clean_file(filepath, report_entries):
    """清洗单个文件"""
    filename = os.path.basename(filepath)
    print(f"\n{'='*60}")
    print(f"清洗: {filename}")

    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    total_lines = len(lines)
    cleaned_lines = []
    ad_lines_removed = 0
    lines_modified = 0
    header_footer_removed = 0

    # 找到头尾的声明块边界
    # 头部: 从第一行到 "---------------------------用户上传之内容开始--------------------------------"
    # 尾部: 从 "---------------------------用户上传之内容结束--------------------------------" 到末尾
    # 或者从 "更多精彩好书" 开始到末尾

    # 先处理头部
    header_end = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if '用户上传之内容开始' in stripped:
            header_end = i + 1
            header_footer_removed += (i + 1)
            break
        if stripped.startswith('《') and '》' in stripped and len(stripped) < 30:
            # 找到书名行，保留从此开始
            header_end = i
            header_footer_removed += i
            break

    # 如果没找到标记，跳过头部处理
    if header_end == 0:
        # 检查前15行是否有声明行
        for i in range(min(15, len(lines))):
            if is_ad_line(lines[i]):
                pass  # 标记但先不移除，留给后面的逻辑

    # 处理尾部
    footer_start = len(lines)
    for i in range(len(lines) - 1, -1, -1):
        stripped = lines[i].strip()
        if '用户上传之内容结束' in stripped:
            footer_start = i
            header_footer_removed += (len(lines) - i)
            break
        if '更多精彩好书' in stripped and 'TXT图书下载网' in stripped:
            footer_start = i
            header_footer_removed += (len(lines) - i)
            break
        if '声明' in stripped and 'bookdown.com.cn' in stripped:
            footer_start = i
            header_footer_removed += (len(lines) - i)
            break

    # 主处理循环
    for i in range(total_lines):
        line_num = i + 1
        line = lines[i]

        # 跳过头部声明区
        if i < header_end:
            report_entries.append({
                'file': filename,
                'line': line_num,
                'type': 'header_removed',
                'pattern': '',
                'original': line.strip()[:80],
                'cleaned': '',
            })
            continue

        # 跳过尾部声明区
        if i >= footer_start:
            report_entries.append({
                'file': filename,
                'line': line_num,
                'type': 'footer_removed',
                'pattern': '',
                'original': line.strip()[:80],
                'cleaned': '',
            })
            continue

        # 移除广告行
        if is_ad_line(line):
            ad_lines_removed += 1
            report_entries.append({
                'file': filename,
                'line': line_num,
                'type': 'ad_line_removed',
                'pattern': '',
                'original': line.strip()[:80],
                'cleaned': '',
            })
            continue

        # 清洗行内容
        cleaned, modified = clean_line(line, line_num, filename, report_entries)
        if modified:
            lines_modified += 1

        cleaned_lines.append(cleaned)

    # 清理多余空行（连续3个以上空行缩减为2个）
    final_lines = []
    empty_count = 0
    for line in cleaned_lines:
        if line.strip() == '':
            empty_count += 1
            if empty_count <= 2:
                final_lines.append(line)
        else:
            empty_count = 0
            final_lines.append(line)

    # 移除CRLF，统一为LF
    final_lines = [line.replace('\r\n', '\n').replace('\r', '\n') for line in final_lines]

    # 确保文件以换行符结尾
    if final_lines and not final_lines[-1].endswith('\n'):
        final_lines[-1] += '\n'

    # 写入清洗后的文件
    with open(filepath, 'w', encoding='utf-8') as f:
        f.writelines(final_lines)

    stats = {
        'file': filename,
        'total_lines': total_lines,
        'ad_lines_removed': ad_lines_removed,
        'header_footer_removed': header_footer_removed,
        'lines_modified': lines_modified,
        'final_lines': len(final_lines),
    }

    print(f"  总行数: {total_lines}")
    print(f"  头尾声明移除: {header_footer_removed} 行")
    print(f"  广告行移除: {ad_lines_removed} 行")
    print(f"  内容修正行: {lines_modified} 行")
    print(f"  最终行数: {len(final_lines)}")

    return stats


def main():
    print("风上忍小说文本清洗工具")
    print("=" * 60)
    print(f"项目目录: {PROJECT_DIR}")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

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
            print(f"警告: 文件不存在 {fpath}")
            continue
        stats = clean_file(fpath, report_entries)
        all_stats.append(stats)

    # 生成CSV报告
    csv_path = os.path.join(PROJECT_DIR, 'cleaning_report.csv')
    with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['file', 'line', 'type', 'pattern', 'original', 'cleaned'])
        writer.writeheader()
        writer.writerows(report_entries)
    print(f"\n清洗报告已保存到: {csv_path}")
    print(f"报告记录数: {len(report_entries)}")

    # 生成结果JSON
    result = {
        'timestamp': datetime.now().isoformat(),
        'files_processed': len(all_stats),
        'statistics': all_stats,
        'total_ad_lines_removed': sum(s['ad_lines_removed'] for s in all_stats),
        'total_header_footer_removed': sum(s['header_footer_removed'] for s in all_stats),
        'total_lines_modified': sum(s['lines_modified'] for s in all_stats),
        'total_report_entries': len(report_entries),
    }

    result_path = os.path.join(PROJECT_DIR, 'cleaning_result.json')
    with open(result_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"清洗结果已保存到: {result_path}")

    print(f"\n完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    return result


if __name__ == '__main__':
    main()
