#!/usr/bin/env python3
"""A股板块数据源调研 - 使用signal超时"""
import time, json, random, signal, sys, traceback
sys.path.insert(0, '/home/drizztbi/openclaw_project/tail_trading')

results = []

class TimeoutError(Exception): pass

def handler(signum, frame):
    raise TimeoutError("Timed out")

def test_api(name, func_str, timeout=30):
    """Test API with signal-based timeout"""
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print(f"{'='*60}", flush=True)
    
    start = time.time()
    old_handler = signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout)
    
    try:
        # Execute the function
        result = eval(func_str)
        signal.alarm(0)
        elapsed = round(time.time() - start, 2)
        
        if result is None:
            print(f"  ✗ 返回 None")
            results.append({"name": name, "available": False, "error": "返回None", "elapsed": elapsed})
            return None
        
        if hasattr(result, 'shape'):
            rows, cols = result.shape
            fields = list(result.columns)
            print(f"  ✓ Shape: {rows} x {cols}")
            print(f"  Fields: {fields[:12]}")
            sample = {}
            if rows > 0:
                row = result.iloc[0].to_dict()
                for k, v in row.items():
                    if hasattr(v, 'item'): sample[k] = v.item()
                    elif hasattr(v, 'isoformat'): sample[k] = v.isoformat()
                    else: sample[k] = str(v)
                print(f"  Sample: {json.dumps(sample, ensure_ascii=False)[:300]}")
            results.append({"name": name, "available": True, "rows": rows, "cols": cols,
                          "fields": fields, "elapsed": elapsed, "sample": json.dumps(sample, ensure_ascii=False)[:400]})
            return result
            
        elif isinstance(result, (list, dict)):
            tp = type(result).__name__
            ln = len(result)
            print(f"  ✓ Type: {tp}, Len: {ln}")
            if isinstance(result, list) and ln > 0:
                print(f"  First: {str(result[0])[:200]}")
            elif isinstance(result, dict):
                print(f"  Keys: {list(result.keys())[:10]}")
            results.append({"name": name, "available": True, "rows": ln, "type": tp, "elapsed": elapsed,
                          "sample": str(result[:2] if isinstance(result, list) else list(result.keys())[:5])[:400]})
            return result
        else:
            print(f"  ✓ Type: {type(result).__name__}")
            results.append({"name": name, "available": True, "elapsed": elapsed, "type": type(result).__name__})
            return result
            
    except TimeoutError:
        signal.alarm(0)
        elapsed = round(time.time() - start, 2)
        print(f"  ✗ TIMEOUT ({timeout}s)")
        results.append({"name": name, "available": False, "error": "Timeout", "elapsed": elapsed})
        return None
    except Exception as e:
        signal.alarm(0)
        elapsed = round(time.time() - start, 2)
        err = str(e)[:200]
        is_ban = any(x in err.lower() for x in ['connection refused', 'remotedisconnected', '10054', '104'])
        print(f"  ✗ ERROR: {err}")
        results.append({"name": name, "available": False, "error": err, "banned": is_ban, "elapsed": elapsed})
        return None
    finally:
        signal.signal(signal.SIGALRM, old_handler)

def sleep_rand(lo=2, hi=4):
    t = random.uniform(lo, hi)
    print(f"  ⏳ {t:.1f}s", flush=True)
    time.sleep(t)

# Import libraries
import akshare as ak
import adata

# ============================================================
print("\n" + "#"*60)
print("# PART 1: 板块排名接口")
print("#"*60, flush=True)

# 1. ak.stock_sector_spot
for ind in ['新浪行业', '概念']:
    test_api(f"ak.stock_sector_spot('{ind}') [新浪]", f"ak.stock_sector_spot('{ind}')", timeout=25)
    sleep_rand(3, 5)

# 2-3. EM boards
test_api("ak.stock_board_concept_name_em() [东财概念]", "ak.stock_board_concept_name_em()", timeout=25)
sleep_rand(3, 5)
test_api("ak.stock_board_industry_name_em() [东财行业]", "ak.stock_board_industry_name_em()", timeout=25)
sleep_rand(3, 5)

# 4-5. THS boards
test_api("ak.stock_board_concept_name_ths() [同花顺概念]", "ak.stock_board_concept_name_ths()", timeout=25)
sleep_rand(3, 5)
test_api("ak.stock_board_industry_name_ths() [同花顺行业]", "ak.stock_board_industry_name_ths()", timeout=25)
sleep_rand(3, 5)

# 6-7. THS summary
test_api("ak.stock_board_concept_summary_ths() [同花顺概念摘要]", "ak.stock_board_concept_summary_ths()", timeout=25)
sleep_rand(3, 5)
test_api("ak.stock_board_industry_summary_ths() [同花顺行业摘要]", "ak.stock_board_industry_summary_ths()", timeout=25)
sleep_rand(3, 5)

# 8. SZSE
test_api("ak.stock_szse_sector_summary() [深交所]", "ak.stock_szse_sector_summary()", timeout=25)
sleep_rand(3, 5)

# adata
test_api("adata.stock.info.all_concept_code_ths() [同花顺概念代码]", "adata.stock.info.all_concept_code_ths()", timeout=25)
sleep_rand(3, 5)
test_api("adata.stock.info.all_concept_code_east() [东财概念代码]", "adata.stock.info.all_concept_code_east()", timeout=25)
sleep_rand(3, 5)
test_api("adata.stock.info.get_concept_ths() [同花顺概念]", "adata.stock.info.get_concept_ths()", timeout=25)
sleep_rand(3, 5)
test_api("adata.stock.info.get_concept_east() [东财概念]", "adata.stock.info.get_concept_east()", timeout=25)
sleep_rand(3, 5)
test_api("adata.stock.info.get_plate_east() [东财板块]", "adata.stock.info.get_plate_east()", timeout=25)
sleep_rand(3, 5)
test_api("adata.stock.info.market_rank_sina() [新浪排名]", "adata.stock.info.market_rank_sina()", timeout=40)
sleep_rand(3, 5)
test_api("adata.stock.info.get_industry_sw() [申万行业]", "adata.stock.info.get_industry_sw()", timeout=25)
sleep_rand(3, 5)

# ============================================================
print("\n" + "#"*60)
print("# PART 2: 板块成分股", flush=True)
print("#"*60)

test_api("ak.stock_board_concept_cons_ths('人工智能')", "ak.stock_board_concept_cons_ths('人工智能')", timeout=25)
sleep_rand(3, 5)
test_api("ak.stock_board_industry_cons_ths('半导体')", "ak.stock_board_industry_cons_ths('半导体')", timeout=25)
sleep_rand(3, 5)
test_api("ak.stock_board_concept_cons_em('人工智能')", "ak.stock_board_concept_cons_em('人工智能')", timeout=25)
sleep_rand(3, 5)
test_api("ak.stock_board_industry_cons_em('半导体')", "ak.stock_board_industry_cons_em('半导体')", timeout=25)
sleep_rand(3, 5)
test_api("adata.stock.info.concept_constituent_ths('人工智能')", "adata.stock.info.concept_constituent_ths('人工智能')", timeout=25)
sleep_rand(3, 5)
test_api("adata.stock.info.concept_constituent_east('人工智能')", "adata.stock.info.concept_constituent_east('人工智能')", timeout=25)
sleep_rand(3, 5)

# Sina related
test_api("ak.stock_sector_sectors('新浪行业')", "ak.stock_sector_sectors('新浪行业')", timeout=25)
sleep_rand(3, 5)
test_api("ak.stock_sector_fund_flow_rank(indicator='今日', sector_type='新浪行业')", 
         "ak.stock_sector_fund_flow_rank(indicator='今日', sector_type='新浪行业')", timeout=25)
sleep_rand(3, 5)

# ============================================================
print("\n" + "#"*60)
print("# PART 3: 个股行情", flush=True)
print("#"*60)

test_api("ak.stock_zh_a_hist('000001','daily','20260301','20260331') [东财]", 
         "ak.stock_zh_a_hist('000001','daily','20260301','20260331')", timeout=25)
sleep_rand(3, 5)
test_api("ak.stock_zh_a_hist_tx('000001','daily','20260301','20260331') [腾讯]", 
         "ak.stock_zh_a_hist_tx('000001','daily','20260301','20260331')", timeout=25)
sleep_rand(3, 5)
test_api("ak.stock_zh_a_daily('sz000001') [新浪]", "ak.stock_zh_a_daily('sz000001')", timeout=25)
sleep_rand(3, 5)
test_api("adata.stock.market.get_market('000001') [adata]", "adata.stock.market.get_market('000001')", timeout=25)
sleep_rand(3, 5)

# ============================================================
print("\n" + "#"*60)
print("# FINAL RESULTS", flush=True)
print("#"*60)
print(json.dumps(results, ensure_ascii=False, indent=2))

with open('/home/drizztbi/openclaw_project/tail_trading/research_results.json', 'w') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

avail = sum(1 for r in results if r.get('available'))
print(f"\n总计: {len(results)} 个接口, 可用: {avail}, 失败: {len(results)-avail}")
