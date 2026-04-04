"""
海龟交易法 — 消息解析器
解析主人的自然语言指令，转换为结构化操作
"""

import re
import logging

logger = logging.getLogger(__name__)


class MessageParser:
    """自然语言消息解析器"""

    def parse(self, text):
        """
        主入口，解析文本为结构化指令

        参数:
            text: 原始文本

        返回:
            dict: {action, code, name, shares, price, keyword, raw}
        """
        text = text.strip()
        result = {
            'action': 'unknown',
            'code': None,
            'name': None,
            'shares': None,
            'price': None,
            'keyword': None,
            'raw': text,
        }

        if not text:
            logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

        # 按优先级尝试解析
        for parser_fn in [
            self.parse_account,
            self.parse_watchlist,
            self.parse_query,
            self.parse_confirm,
            self.parse_sell,
        ]:
            parsed = parser_fn(text)
            if parsed['action'] != 'unknown':
                return parsed

        logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

    def parse_confirm(self, text):
        """
        解析确认买入/加仓指令

        匹配模式：
        - "买了600519 1850" / "买入600519 1850"
        - "确认买入 600519"
        - "加仓300750 197"
        """
        result = {
            'action': 'unknown', 'code': None, 'name': None,
            'shares': None, 'price': None, 'keyword': None, 'raw': text,
        }

        # 买入/确认买入
        m = re.search(r'(?:确认|已)?(?:买入?|建仓)\s*(\d{6})\s*(\d+(?:\.\d+)?)?', text)
        if m:
            result['action'] = 'buy'
            result['code'] = m.group(1)
            if m.group(2):
                result['price'] = float(m.group(2))
            logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

        # 加仓
        m = re.search(r'加仓\s*(\d{6})\s*(\d+(?:\.\d+)?)?', text)
        if m:
            result['action'] = 'add'
            result['code'] = m.group(1)
            if m.group(2):
                result['price'] = float(m.group(2))
            logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

        logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

    def parse_sell(self, text):
        """
        解析卖出/止损指令

        匹配模式：
        - "止损了300750 197"
        - "卖出600519"
        - "平仓300750"
        """
        result = {
            'action': 'unknown', 'code': None, 'name': None,
            'shares': None, 'price': None, 'keyword': None, 'raw': text,
        }

        m = re.search(r'(?:止损|卖出|平仓|清仓)\s*(?:了)?\s*(\d{6})\s*(\d+(?:\.\d+)?)?', text)
        if m:
            result['action'] = 'sell'
            result['code'] = m.group(1)
            if m.group(2):
                result['price'] = float(m.group(2))
            logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

        logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

    def parse_query(self, text):
        """
        解析查询指令

        匹配模式：
        - "持仓" / "查持仓" → 查询持仓
        - "账户" / "查账户" → 查询账户
        - "持仓600519" → 查询单只持仓
        """
        result = {
            'action': 'unknown', 'code': None, 'name': None,
            'shares': None, 'price': None, 'keyword': None, 'raw': text,
        }

        if re.search(r'(?:查?持仓|仓位|持股)', text):
            result['action'] = 'query_position'
            m = re.search(r'(\d{6})', text)
            if m:
                result['code'] = m.group(1)
            logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

        if re.search(r'(?:查?账户|资金|余额)', text):
            result['action'] = 'query_account'
            logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

        logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

    def parse_watchlist(self, text):
        """
        解析自选池操作

        匹配模式：
        - "自选加 AI" / "关注 新能源"
        - "自选删 AI"
        - "自选列表" / "看自选"
        """
        result = {
            'action': 'unknown', 'code': None, 'name': None,
            'shares': None, 'price': None, 'keyword': None, 'raw': text,
        }

        # 自选加
        m = re.search(r'(?:自选|关注|收藏)\s*加\s*(.+)', text)
        if m:
            result['action'] = 'watchlist_add'
            result['keyword'] = m.group(1).strip()
            logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

        # 自选删
        m = re.search(r'(?:自选|关注|收藏)\s*(?:删|移除|删除)\s*(.+)', text)
        if m:
            result['action'] = 'watchlist_delete'
            result['keyword'] = m.group(1).strip()
            logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

        # 自选列表
        if re.search(r'(?:自选|关注|收藏)\s*(?:列表|列表|清单|看看)', text):
            result['action'] = 'watchlist_list'
            logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

        # 暂停自选
        m = re.search(r'(?:自选|关注)\s*(?:暂停|停)\s*(.+)', text)
        if m:
            result['action'] = 'watchlist_pause'
            result['keyword'] = m.group(1).strip()
            logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

        logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

    def parse_account(self, text):
        """
        解析账户操作

        匹配模式：
        - "入金 100000"
        - "出金 50000"
        - "设置资金 200000"
        """
        result = {
            'action': 'unknown', 'code': None, 'name': None,
            'shares': None, 'price': None, 'keyword': None, 'raw': text,
        }

        m = re.search(r'入金\s*(\d+(?:\.\d+)?)', text)
        if m:
            result['action'] = 'deposit'
            result['price'] = float(m.group(1))
            logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

        m = re.search(r'出金\s*(\d+(?:\.\d+)?)', text)
        if m:
            result['action'] = 'withdraw'
            result['price'] = float(m.group(1))
            logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

        m = re.search(r'(?:设置|初始化)\s*(?:资金|资本|本金)\s*(\d+(?:\.\d+)?)', text)
        if m:
            result['action'] = 'account_set'
            result['price'] = float(m.group(1))
            logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result

        logger.info(f'[消息解析] "{text[:30]}..." → action={result.get("action", "unknown")}')
        return result
