#!/usr/bin/env python3
"""
风上忍小说 RAG 检索查询器 v3
基于 jieba + sentence-transformers + faiss 的语义向量检索
模型：paraphrase-multilingual-MiniLM-L12-v2（384维）
"""

import os
import sys
import json
import re
import numpy as np
import argparse
from collections import Counter

# 离线模式，避免网络请求（模型已本地缓存）
os.environ.setdefault("HF_HUB_OFFLINE", "1")

import jieba
import jieba.analyse
from sentence_transformers import SentenceTransformer
import faiss

PROJECT_DIR = "***REMOVED***/feng-shang-ren"
RAG_DIR = os.path.join(PROJECT_DIR, "rag")
EMBEDDING_MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2'
STOPWORDS = set("的 了 在 是 我 有 和 就 不 人 都 一 一个 上 也 很 到 说 要 去 你 会 着 没有 看 好 自己 这 他 她 它 们 那 被 从 里 与 而 为 对 以 就是 还 把 能 这个 那个 什么 怎么 吗 吧 呢 啊 哦 嗯 哈 嘿 呀 啦 嘛".split())

class RAGQueryV3:
    def __init__(self):
        self.chunks_meta = []
        self.chunks_by_book = {}
        self.feature_index = {}
        self.model = None
        self.faiss_index = None
        self.vectors = None
        self._load_all()

    def _load_all(self):
        # 加载 sentence-transformers 模型
        print(f"加载 sentence-transformers 模型: {EMBEDDING_MODEL_NAME}")
        self.model = SentenceTransformer(EMBEDDING_MODEL_NAME)
        print(f"  模型加载完成，维度: {self.model.get_sentence_embedding_dimension()}")

        # 加载 faiss 索引
        faiss_path = os.path.join(RAG_DIR, "faiss_index.bin")
        if os.path.exists(faiss_path):
            self.faiss_index = faiss.read_index(faiss_path)
            print(f"  faiss 索引已加载，向量数: {self.faiss_index.ntotal}")

        # 加载原始向量
        vectors_path = os.path.join(RAG_DIR, "vectors.npy")
        if os.path.exists(vectors_path):
            self.vectors = np.load(vectors_path)
            print(f"  向量矩阵已加载，形状: {self.vectors.shape}")

        # 加载chunks元数据
        meta_path = os.path.join(RAG_DIR, "chunks_meta.json")
        if os.path.exists(meta_path):
            with open(meta_path, 'r', encoding='utf-8') as f:
                self.chunks_meta = json.load(f)

        # 加载每个book的完整文本块
        for book_file in os.listdir(RAG_DIR):
            if book_file.startswith("chunks_") and book_file.endswith(".json") and book_file != "chunks_meta.json":
                book_name = book_file[7:-5]
                with open(os.path.join(RAG_DIR, book_file), 'r', encoding='utf-8') as f:
                    self.chunks_by_book[book_name] = json.load(f)

        # 加载特征索引
        fi_path = os.path.join(RAG_DIR, "feature_index.json")
        if os.path.exists(fi_path):
            with open(fi_path, 'r', encoding='utf-8') as f:
                self.feature_index = json.load(f)

    def text_to_vector(self, text):
        """用sentence-transformers编码查询文本"""
        processed = ' '.join(jieba.lcut(text))
        vec = self.model.encode(
            [processed],
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        return vec.astype(np.float32)  # shape: (1, 384)

    def filter_by_features(self, scene_types=None, books=None):
        candidate_ids = set(range(len(self.chunks_meta)))
        if scene_types:
            scene_ids = set()
            for st in scene_types:
                if st in self.feature_index.get("by_scene", {}):
                    scene_ids.update(self.feature_index["by_scene"][st])
            candidate_ids &= scene_ids
        if books:
            book_ids = set()
            for b in books:
                if b in self.feature_index.get("by_book", {}):
                    book_ids.update(self.feature_index["by_book"][b])
            candidate_ids &= book_ids
        return candidate_ids

    def query(self, text=None, scene_types=None, books=None, top_k=5, method="hybrid"):
        if not text and not scene_types and not books:
            return []

        results = []

        if method == "text" and text:
            query_vec = self.text_to_vector(text)
            scores, indices = self.faiss_index.search(query_vec, top_k)
            for score, idx in zip(scores[0], indices[0]):
                if idx >= 0:
                    results.append({
                        "id": int(idx),
                        "score": float(score),
                        "source": "semantic",
                        "meta": self.chunks_meta[idx] if idx < len(self.chunks_meta) else {},
                    })

        elif method == "feature":
            candidate_ids = self.filter_by_features(scene_types, books)
            import random
            candidate_list = sorted(candidate_ids)
            if len(candidate_list) > top_k:
                candidate_list = random.sample(candidate_list, top_k)
            for doc_id in candidate_list:
                results.append({
                    "id": doc_id,
                    "score": 1.0,
                    "source": "feature_only",
                    "meta": self.chunks_meta[doc_id] if doc_id < len(self.chunks_meta) else {},
                })

        elif method == "hybrid":
            if text:
                if scene_types or books:
                    candidate_ids = self.filter_by_features(scene_types, books)
                    if candidate_ids:
                        query_vec_full = self.text_to_vector(text).flatten()
                        scored = []
                        for cid in candidate_ids:
                            if self.vectors is not None and cid < len(self.vectors):
                                sim = float(np.dot(query_vec_full, self.vectors[cid]))
                                scored.append((cid, sim))
                        scored.sort(key=lambda x: x[1], reverse=True)
                        for cid, sim in scored[:top_k]:
                            results.append({
                                "id": cid,
                                "score": sim,
                                "source": "hybrid+feature",
                                "meta": self.chunks_meta[cid] if cid < len(self.chunks_meta) else {},
                            })
                    if len(results) < top_k:
                        remain = top_k - len(results)
                        existing_ids = {r["id"] for r in results}
                        query_vec = self.text_to_vector(text)
                        scores, indices = self.faiss_index.search(query_vec, remain * 3)
                        for score, idx in zip(scores[0], indices[0]):
                            if idx >= 0 and idx not in existing_ids:
                                results.append({
                                    "id": int(idx),
                                    "score": float(score),
                                    "source": "semantic",
                                    "meta": self.chunks_meta[idx] if idx < len(self.chunks_meta) else {},
                                })
                                if len(results) >= top_k:
                                    break
                else:
                    query_vec = self.text_to_vector(text)
                    scores, indices = self.faiss_index.search(query_vec, top_k)
                    for score, idx in zip(scores[0], indices[0]):
                        if idx >= 0:
                            results.append({
                                "id": int(idx),
                                "score": float(score),
                                "source": "semantic",
                                "meta": self.chunks_meta[idx] if idx < len(self.chunks_meta) else {},
                            })
            else:
                return self.query(text=None, scene_types=scene_types, books=books,
                                top_k=top_k, method="feature")

        return results

    def get_chunk_text(self, doc_id):
        meta = self.chunks_meta[doc_id] if doc_id < len(self.chunks_meta) else None
        if not meta:
            return None
        book = meta["book"]
        if book in self.chunks_by_book:
            for chunk in self.chunks_by_book[book]:
                if chunk["id"] == doc_id:
                    return chunk["text"]
        return None

    def get_stats(self):
        summary_path = os.path.join(RAG_DIR, "build_summary.json")
        summary = {}
        if os.path.exists(summary_path):
            with open(summary_path, 'r', encoding='utf-8') as f:
                summary = json.load(f)
        return {
            "version": summary.get("version", "?"),
            "total_chunks": len(self.chunks_meta),
            "total_books": len(self.chunks_by_book),
            "embedding_model": summary.get("embedding_model", "?"),
            "embedding_dim": summary.get("embedding_dim", "?"),
            "faiss_type": summary.get("faiss_type", "?"),
            "build_time_seconds": summary.get("build_time_seconds", "?"),
            "scene_types": list(self.feature_index.get("by_scene", {}).keys()),
            "books": list(self.chunks_by_book.keys()),
        }


def format_results(results, engine, show_text=False):
    if not results:
        print("未找到匹配结果")
        return

    print(f"\n找到 {len(results)} 个匹配结果：")
    print("=" * 60)

    for i, r in enumerate(results, 1):
        meta = r["meta"]
        print(f"\n【结果 {i}】 相关度: {r['score']:.4f} | 方式: {r['source']}")
        print(f"  来源: {meta.get('book', '?')}")
        print(f"  场景: {', '.join(meta.get('features', {}).get('scene_types', []))}")
        print(f"  字数: {meta.get('char_count', '?')}")
        print(f"  关键词: {', '.join(meta.get('features', {}).get('keywords', [])[:5])}")

        if show_text:
            text = engine.get_chunk_text(r["id"])
            if text:
                print(f"\n  --- 原文片段 ---")
                print(f"  {text[:600]}...")
                print(f"  --- 片段结束 ---")

        print("-" * 40)


def main():
    parser = argparse.ArgumentParser(description="风上忍小说RAG检索 v3")
    parser.add_argument("text", nargs="?", help="搜索文本")
    parser.add_argument("--scene", nargs="+", help="场景类型过滤",
                       choices=["日常对话", "内心独白", "幽默吐槽", "动作战斗", "场景描写",
                               "感情互动", "ACG文化", "能力觉醒", "回忆闪回", "悬疑紧张"])
    parser.add_argument("--book", nargs="+", help="指定作品过滤")
    parser.add_argument("--top", type=int, default=5, help="返回结果数")
    parser.add_argument("--method", choices=["text", "feature", "hybrid"], default="hybrid")
    parser.add_argument("--show-text", action="store_true", help="显示原文片段")
    parser.add_argument("--stats", action="store_true", help="显示索引统计")

    args = parser.parse_args()

    engine = RAGQueryV3()

    if args.stats:
        stats = engine.get_stats()
        print("📊 RAG索引统计 v3：")
        for k, v in stats.items():
            print(f"  {k}: {v}")
        return

    results = engine.query(
        text=args.text,
        scene_types=args.scene,
        books=args.book,
        top_k=args.top,
        method=args.method,
    )

    format_results(results, engine, show_text=args.show_text)


if __name__ == "__main__":
    main()
