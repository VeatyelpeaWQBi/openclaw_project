-- ============================================
-- 技术指标表添加OBV能量潮列
-- 执行时间: 2026-05-17
-- 说明: 为technical_indicators表添加obv和ma_obv字段
-- ============================================

-- 添加obv列（REAL类型，存储能量潮值）
ALTER TABLE technical_indicators ADD COLUMN obv REAL;

-- 添加ma_obv列（REAL类型，存储OBV的10日均线）
ALTER TABLE technical_indicators ADD COLUMN ma_obv REAL;

-- 验证列是否添加成功
PRAGMA table_info(technical_indicators);