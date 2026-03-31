#!/usr/bin/env python3
"""
风上忍小说 RAG 索引构建器 v3
技术栈：jieba分词 + sentence-transformers + faiss向量检索
模型：paraphrase-multilingual-MiniLM-L12-v2（384维，多语言）
"""

import os
import re
import json
import time
import numpy as np
from collections import Counter, defaultdict

# 离线模式，避免网络请求（模型已本地缓存）
os.environ.setdefault("HF_HUB_OFFLINE", "1")

import jieba
import jieba.analyse
from sentence_transformers import SentenceTransformer
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
CHUNK_SIZE = 600
CHUNK_OVERLAP = 100
EMBEDDING_MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2'
EMBEDDING_DIM = 384
BATCH_SIZE = 64

# ============================================================
# 停用词
# ============================================================
STOPWORDS = set("的 了 在 是 我 有 和 就 不 人 都 一 一个 上 也 很 到 说 要 去 你 会 着 没有 看 好 自己 这 他 她 它 们 那 被 从 里 与 而 为 对 以 就是 还 把 能 这个 那个 什么 怎么 吗 吧 呢 啊 哦 嗯 哈 嘿 呀 啦 嘛".split())

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

    keywords = jieba.analyse.extract_tags(chunk_text, topK=8)
    features["keywords"] = keywords

    if not features["scene_types"]:
        features["scene_types"] = ["日常对话"]

    return features

# ============================================================
# sentence-transformers 编码
# ============================================================
def encode_chunks(model, chunks, batch_size=BATCH_SIZE):
    """用jieba分词后，送入sentence-transformers编码"""
    print(f"   使用 jieba 分词预处理...")
    processed_texts = [' '.join(jieba.lcut(chunk)) for chunk in chunks]
    print(f"   使用 sentence-transformers 批量编码（batch_size={batch_size}）...")
    embeddings = model.encode(
        processed_texts,
        batch_size=batch_size,
        show_progress_bar=True,
        normalize_embeddings=True,
    )
    return embeddings  # shape: (n_chunks, 384)

# ============================================================
# 主构建流程
# ============================================================
def build_index():
    start_time = time.time()

    print("=" * 60)
    print("风上忍小说 RAG 索引构建器 v3")
    print("jieba + sentence-transformers + faiss")
    print(f"模型: {EMBEDDING_MODEL_NAME} ({EMBEDDING_DIM}维)")
    print("=" * 60)

    # 第1步：加载 sentence-transformers 模型
    print(f"\n🔧 加载 sentence-transformers 模型: {EMBEDDING_MODEL_NAME}")
    model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    print(f"   模型加载完成，输出维度: {model.get_sentence_embedding_dimension()}")

    # 第2步：分块 + 特征提取
    all_chunks = []
    chunk_id = 0

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
            all_chunks.append({
                "id": chunk_id,
                "book": book["name"],
                "text": chunk_text,
                "features": features,
            })
            chunk_id += 1

        print(f"   ✅ {len(chunks)} 块")

    print(f"\n📊 总计: {len(all_chunks)} 个文本块")

    # 第3步：批量编码
    print(f"\n🔧 使用 sentence-transformers 编码文本向量...")
    raw_texts = [chunk["text"] for chunk in all_chunks]
    embeddings = encode_chunks(model, raw_texts, batch_size=BATCH_SIZE)
    print(f"   向量形状: {embeddings.shape}")

    # 确保是float32
    embeddings = embeddings.astype(np.float32)

    # 第4步：构建 faiss 索引
    print(f"\n🔧 构建 faiss 索引（FlatIP, dim={EMBEDDING_DIM}）...")
    faiss_index = faiss.IndexFlatIP(EMBEDDING_DIM)
    faiss_index.add(embeddings)
    print(f"   索引向量数: {faiss_index.ntotal}")

    # 第5步：保存所有数据
    print("\n💾 保存索引文件...")

    # 保存 faiss 索引
    faiss.write_index(faiss_index, os.path.join(OUTPUT_DIR, "faiss_index.bin"))

    # 保存原始向量（用于混合模式中的精确排序）
    np.save(os.path.join(OUTPUT_DIR, "vectors.npy"), embeddings)

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

    build_time = round(time.time() - start_time, 1)

    # 保存摘要
    summary = {
        "version": 3,
        "total_chunks": len(all_chunks),
        "total_books": len(BOOKS),
        "embedding_model": EMBEDDING_MODEL_NAME,
        "embedding_dim": EMBEDDING_DIM,
        "faiss_type": "FlatIP",
        "scene_types": list(SCENE_TYPES.keys()),
        "chunks_per_book": {b["name"]: len(chunks_by_book[b["name"]]) for b in BOOKS},
        "build_time_seconds": build_time,
        "build_date": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    with open(os.path.join(OUTPUT_DIR, "build_summary.json"), 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 索引构建完成！（耗时 {build_time}s）")
    print(f"   总文本块: {len(all_chunks)}")
    print(f"   向量维度: {EMBEDDING_DIM}")
    print(f"   faiss 向量数: {faiss_index.ntotal}")

    return summary

if __name__ == "__main__":
    build_index()
