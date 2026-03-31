#!/usr/bin/env python3
"""
风上忍小说 RAG 索引构建器 v2
技术栈：jieba分词 + gensim Word2Vec + faiss向量检索
"""

import os
import re
import json
import math
import numpy as np
from collections import Counter, defaultdict

import jieba
import jieba.analyse
from gensim.models import Word2Vec
import faiss

# ============================================================
# 配置
# ============================================================
PROJECT_DIR = "***REMOVED***/feng-shang-ren"
BOOKS = [
    {"file": "异体_utf8.txt", "name": "异体", "pov": "first", "genre": "校园超能力"},
    {"file": "第七脑域_utf8.txt", "name": "第七脑域", "pov": "third", "genre": "校园超能力"},
    {"file": "时空之头号玩家_utf8.txt", "name": "时空之头号玩家", "pov": "third", "genre": "游戏流"},
    {"file": "末日咆哮2_utf8.txt", "name": "末日咆哮2", "pov": "mixed", "genre": "末日超能力"},
    {"file": "末日咆哮1_utf8.txt", "name": "末日咆哮1", "pov": "third", "genre": "末日超能力"},
]
OUTPUT_DIR = os.path.join(PROJECT_DIR, "rag")
CHUNK_SIZE = 600   # 每个块的目标字数（缩小以提高精度）
CHUNK_OVERLAP = 100
W2V_DIM = 128      # Word2Vec 向量维度
W2V_EPOCHS = 15    # 训练轮数

# ============================================================
# 停用词
# ============================================================
STOPWORDS = set("的 了 在 是 我 有 和 就 不 人 都 一 一个 上 也 很 到 说 要 去 你 会 着 没有 看 好 自己 这 他 她 它 们 那 被 从 里 与 但 而 为 对 以 就是 还 把 能 这个 那个 什么 怎么 吗 吧 呢 啊 哦 嗯 哈 嘿 呀 啦 嘛".split())

# ============================================================
# 特征标签定义
# ============================================================
SCENE_TYPES = {
    "日常对话": ["说", "道", "问", "答", "聊", "告诉", "问道", "说道"],
    "内心独白": ["心想", "想道", "暗想", "觉得", "感觉", "认为", "看来", "难道"],
    "幽默吐槽": ["无语", "囧", "汗", "冏", "忍了", "算了", "暴殄", "天打雷劈", "传说中的", "教练"],
    "动作战斗": ["攻击", "战斗", "拳", "踢", "闪", "躲", "打", "轰", "爆发", "力量", "能力", "剑", "斩"],
    "场景描写": ["天空", "阳光", "房间", "街道", "城市", "校园", "教室", "走廊", "树木"],
    "感情互动": ["喜欢", "爱", "脸红", "心跳", "拥抱", "牵手", "亲", "温柔", "微笑", "感动"],
    "ACG文化": ["宅", "二次元", "三次元", "手办", "新番", "萌", "后宫", "萝莉", "正太", "御姐", "cos"],
    "能力觉醒": ["觉醒", "异能", "超能力", "能力", "力量", "特殊", "天赋", "脑域", "碎片"],
    "回忆闪回": ["想起", "回忆", "曾经", "以前", "那时候", "当年", "过去"],
    "悬疑紧张": ["突然", "危险", "恐惧", "紧张", "神秘", "诡异", "暗", "阴"],
}

# ============================================================
# 文本处理
# ============================================================
def read_book(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

def detect_chapters(text):
    chapters = []
    pattern = re.compile(r'^(第[\d一二三四五六七八九十百千]+章\s*.+)$', re.MULTILINE)
    for match in pattern.finditer(text):
        chapters.append({"title": match.group(1).strip(), "start": match.start()})
    for i in range(len(chapters) - 1):
        chapters[i]["end"] = chapters[i+1]["start"]
    if chapters:
        chapters[-1]["end"] = len(text)
    return chapters

def split_paragraphs(text):
    paragraphs = re.split(r'\n\s*\n|\n(?=[^\n]{20,})', text)
    return [p.strip() for p in paragraphs if len(p.strip()) > 15]

def smart_chunk(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    paragraphs = split_paragraphs(text)
    chunks = []
    current_chunk = []
    current_len = 0

    for para in paragraphs:
        para_len = len(para)
        if current_len + para_len > chunk_size and current_chunk:
            chunk_text = "\n\n".join(current_chunk)
            chunks.append(chunk_text)
            if overlap > 0:
                overlap_chunks = []
                overlap_len = 0
                for p in reversed(current_chunk):
                    if overlap_len + len(p) <= overlap:
                        overlap_chunks.insert(0, p)
                        overlap_len += len(p)
                    else:
                        break
                current_chunk = overlap_chunks + [para]
                current_len = sum(len(p) for p in current_chunk)
            else:
                current_chunk = [para]
                current_len = para_len
        else:
            current_chunk.append(para)
            current_len += para_len

    if current_chunk:
        chunks.append("\n\n".join(current_chunk))
    return chunks

# ============================================================
# jieba 分词
# ============================================================
def tokenize(text):
    """用jieba分词，过滤停用词和单字"""
    words = jieba.lcut(text)
    return [w for w in words if len(w) >= 2 and w not in STOPWORDS and not w.isspace()]

# ============================================================
# 特征提取
# ============================================================
def extract_features(chunk_text, book_info):
    features = {
        "book": book_info["name"],
        "genre": book_info["genre"],
        "scene_types": [],
        "emotion_tone": [],
        "char_count": len(chunk_text),
        "has_dialogue": bool(re.search(r'["""「」].*?["""「」]', chunk_text)),
        "keywords": [],
    }

    for scene_type, markers in SCENE_TYPES.items():
        score = sum(1 for m in markers if m in chunk_text)
        if score >= 2:
            features["scene_types"].append(scene_type)

    # 用jieba提取关键词
    keywords = jieba.analyse.extract_tags(chunk_text, topK=8)
    features["keywords"] = keywords

    if not features["scene_types"]:
        features["scene_types"] = ["日常对话"]

    return features

# ============================================================
# 主构建流程
# ============================================================
def build_index():
    print("=" * 60)
    print("风上忍小说 RAG 索引构建器 v2")
    print("jieba + Word2Vec + faiss")
    print("=" * 60)

    all_chunks = []
    all_tokens = []  # 用于训练Word2Vec
    chunk_id = 0

    # 第1步：分块 + 分词
    for book in BOOKS:
        filepath = os.path.join(PROJECT_DIR, book["file"])
        if not os.path.exists(filepath):
            print(f"⚠️  文件不存在: {filepath}")
            continue

        print(f"\n📖 处理: {book['name']}")
        text = read_book(filepath)
        print(f"   总字数: {len(text)}")

        chapters = detect_chapters(text)
        chunks = smart_chunk(text)
        print(f"   章节数: {len(chapters)}, 分块数: {len(chunks)}")

        for chunk_text in chunks:
            features = extract_features(chunk_text, book)
            tokens = tokenize(chunk_text)

            all_chunks.append({
                "id": chunk_id,
                "book": book["name"],
                "text": chunk_text,
                "tokens": tokens,
                "features": features,
            })
            all_tokens.append(tokens)
            chunk_id += 1

        print(f"   ✅ {len(chunks)} 块")

    print(f"\n📊 总计: {len(all_chunks)} 个文本块")

    # 第2步：训练 Word2Vec
    print("\n🔧 训练 Word2Vec 模型...")
    w2v_model = Word2Vec(
        sentences=all_tokens,
        vector_size=W2V_DIM,
        window=8,
        min_count=3,
        workers=4,
        epochs=W2V_EPOCHS,
        sg=1,  # skip-gram
    )
    print(f"   词汇表大小: {len(w2v_model.wv)}")
    print(f"   向量维度: {W2V_DIM}")

    # 第3步：将文本块转换为向量
    print("\n🔧 生成文本向量...")
    def text_to_vector(tokens):
        """将token列表转换为平均词向量"""
        vectors = []
        for token in tokens:
            if token in w2v_model.wv:
                vectors.append(w2v_model.wv[token])
        if not vectors:
            return np.zeros(W2V_DIM, dtype=np.float32)
        vec = np.mean(vectors, axis=0)
        # L2归一化
        norm = np.linalg.norm(vec)
        if norm > 0:
            vec = vec / norm
        return vec.astype(np.float32)

    vectors = np.zeros((len(all_chunks), W2V_DIM), dtype=np.float32)
    for i, chunk in enumerate(all_chunks):
        vectors[i] = text_to_vector(chunk["tokens"])
        if (i + 1) % 2000 == 0:
            print(f"   已处理 {i+1}/{len(all_chunks)}")

    # 第4步：构建 faiss 索引
    print("\n🔧 构建 faiss 索引...")
    # 使用 IVF 索引（如果数据量大）或 Flat 索引
    if len(all_chunks) > 10000:
        nlist = min(256, len(all_chunks) // 40)
        quantizer = faiss.IndexFlatIP(W2V_DIM)
        faiss_index = faiss.IndexIVFFlat(quantizer, W2V_DIM, nlist, faiss.METRIC_INNER_PRODUCT)
        faiss_index.train(vectors)
        faiss_index.nprobe = min(16, nlist)
    else:
        faiss_index = faiss.IndexFlatIP(W2V_DIM)

    faiss_index.add(vectors)
    print(f"   索引向量数: {faiss_index.ntotal}")

    # 第5步：保存所有数据
    print("\n💾 保存索引文件...")

    # 保存 faiss 索引
    faiss.write_index(faiss_index, os.path.join(OUTPUT_DIR, "faiss_index.bin"))

    # 保存原始向量（用于混合模式中的精确排序）
    np.save(os.path.join(OUTPUT_DIR, "vectors.npy"), vectors)

    # 保存 Word2Vec 模型
    w2v_model.save(os.path.join(OUTPUT_DIR, "w2v_model.bin"))

    # 保存chunks元数据
    chunks_meta = []
    for chunk in all_chunks:
        meta = {
            "id": chunk["id"],
            "book": chunk["book"],
            "text_preview": chunk["text"][:100] + "...",
            "char_count": len(chunk["text"]),
            "features": chunk["features"],
        }
        chunks_meta.append(meta)

    with open(os.path.join(OUTPUT_DIR, "chunks_meta.json"), 'w', encoding='utf-8') as f:
        json.dump(chunks_meta, f, ensure_ascii=False, indent=2)

    # 保存完整文本块（按book分组）
    chunks_by_book = defaultdict(list)
    for chunk in all_chunks:
        chunks_by_book[chunk["book"]].append({
            "id": chunk["id"],
            "text": chunk["text"],
        })

    for book_name, book_chunks in chunks_by_book.items():
        filepath = os.path.join(OUTPUT_DIR, f"chunks_{book_name}.json")
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(book_chunks, f, ensure_ascii=False)

    # 保存特征索引
    feature_index = {"by_scene": defaultdict(list), "by_book": defaultdict(list)}
    for chunk in all_chunks:
        fid = chunk["id"]
        for st in chunk["features"]["scene_types"]:
            feature_index["by_scene"][st].append(fid)
        feature_index["by_book"][chunk["book"]].append(fid)
    feature_index = {k: dict(v) for k, v in feature_index.items()}
    with open(os.path.join(OUTPUT_DIR, "feature_index.json"), 'w', encoding='utf-8') as f:
        json.dump(feature_index, f, ensure_ascii=False, indent=2)

    # 保存摘要
    summary = {
        "version": 2,
        "total_chunks": len(all_chunks),
        "total_books": len(BOOKS),
        "w2v_vocab_size": len(w2v_model.wv),
        "w2v_dim": W2V_DIM,
        "faiss_type": "IVFFlat" if len(all_chunks) > 10000 else "FlatIP",
        "scene_types": list(SCENE_TYPES.keys()),
        "chunks_per_book": {b["name"]: len(chunks_by_book[b["name"]]) for b in BOOKS},
        "build_time": "2026-03-30-v2",
    }
    with open(os.path.join(OUTPUT_DIR, "build_summary.json"), 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("\n✅ 索引构建完成！")
    print(f"   总文本块: {len(all_chunks)}")
    print(f"   Word2Vec 词汇: {len(w2v_model.wv)}")
    print(f"   faiss 向量数: {faiss_index.ntotal}")

if __name__ == "__main__":
    build_index()
