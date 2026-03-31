#!/bin/bash
# 代码质量扫描脚本
# Push前运行，检查所有warning和error

echo "🔍 开始代码质量扫描..."
echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

PROJECT_DIR="***REMOVED***"
ERRORS=0
WARNINGS=0

# 扫描tail_trading
echo "=== 扫描 tail_trading ==="
RESULT=$(cd "$PROJECT_DIR" && pylint tail_trading/src/*.py \
    --disable=C0114,C0115,C0116 \
    --max-line-length=120 \
    --exit-zero 2>&1)
    
PYLINT_SCORE=$(echo "$RESULT" | grep "Your code has been rated" | tail -1)
echo "$RESULT" | grep -E "^[A-Z]:" | head -20
echo "$PYLINT_SCORE"
echo ""

# 扫描backtest_engine
echo "=== 扫描 backtest_engine ==="
RESULT2=$(cd "$PROJECT_DIR" && pylint backtest_engine/src/*.py \
    --disable=C0114,C0115,C0116 \
    --max-line-length=120 \
    --exit-zero 2>&1)
    
PYLINT_SCORE2=$(echo "$RESULT2" | grep "Your code has been rated" | tail -1)
echo "$RESULT2" | grep -E "^[A-Z]:" | head -20
echo "$PYLINT_SCORE2"
echo ""

# 统计
echo "=== 扫描完成 ==="
echo "tail_trading: $PYLINT_SCORE"
echo "backtest_engine: $PYLINT_SCORE2"
echo ""

# 检查是否有E(rror)级别的问题
ERRORS=$(cd "$PROJECT_DIR" && pylint tail_trading/src/*.py backtest_engine/src/*.py \
    --disable=C0114,C0115,C0116 \
    --max-line-length=120 2>&1 | grep -c "^[E]:")

if [ "$ERRORS" -gt 0 ]; then
    echo "❌ 发现 $ERRORS 个错误，请修复后再Push"
    exit 1
else
    echo "✅ 无Error级别问题，可以Push"
    exit 0
fi
