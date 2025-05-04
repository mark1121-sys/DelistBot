import os
import re
import logging
import asyncio
import json
import math
import aiohttp
import platform
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Optional
from dotenv import load_dotenv

# 既存コードからインポート
from bybit_client import BybitClient
from telegram_notifier import TelegramNotifier

# ロギング設定
logger = logging.getLogger(__name__)

# Windowsの場合、イベントループポリシーを設定
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

@dataclass
class DelistEvent:
    """デリストイベントの情報を格納"""
    tokens: List[str]
    event_detected_time: datetime
    event_text: str

class TelegramChannelMonitor:
    """TelegramチャンネルからBinanceデリスト通知を監視（WebhookまたはLong Polling方式）"""
    
    def __init__(self, bot_token: str, channel_username: str):
        self.bot_token = bot_token
        self.channel_username = channel_username
        self.api_url = f"https://api.telegram.org/bot{bot_token}"
        self.offset = 0
        self.is_running = False
        self.last_processed_message_id = 0
        
    async def start(self):
        """監視を開始"""
        try:
            self.is_running = True
            logger.info(f"Started monitoring channel: {self.channel_username}")
            
            # Long Pollingループを開始
            while self.is_running:
                try:
                    # チャンネルからの更新を取得
                    updates = await self.get_updates()
                    
                    for update in updates:
                        # チャンネルポストかMessageクラスかを確認
                        if "channel_post" in update:
                            message = update["channel_post"]
                        elif "message" in update:
                            message = update["message"]
                        else:
                            continue
                        
                        # チャンネルからのメッセージか確認
                        if message.get("chat", {}).get("username") == self.channel_username.replace("@", ""):
                            message_text = message.get("text", "")
                            message_id = message.get("message_id", 0)
                            
                            # 重複処理を防止
                            if message_id <= self.last_processed_message_id:
                                continue
                            
                            self.last_processed_message_id = message_id
                            logger.info(f"[TELEGRAM] New message: {message_text}")
                            
                            # デリストメッセージを検出
                            delist_event = self.parse_delist_message(message_text)
                            if delist_event:
                                logger.info(f"[DELIST] Detected: {delist_event}")
                                await self.on_delist_detected(delist_event)
                    
                    # 短い待機時間を設定
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(f"[ERROR] Error in monitoring loop: {e}")
                    await asyncio.sleep(5)
                
        except Exception as e:
            logger.error(f"[ERROR] Error starting Telegram monitor: {e}")
            self.is_running = False
    
    async def get_updates(self):
        """Telegram Bot APIからアップデートを取得"""
        try:
            # aiohttpのセッションタイムアウト設定を追加（Windowsの問題を回避）
            timeout = aiohttp.ClientTimeout(total=60, connect=30)
            connector = aiohttp.TCPConnector(enable_cleanup_closed=True)
            
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                params = {
                    "offset": self.offset,
                    "timeout": 30,
                    "allowed_updates": ["channel_post", "message"]
                }
                async with session.get(f"{self.api_url}/getUpdates", params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("ok"):
                            updates = data.get("result", [])
                            if updates:
                                # オフセットを更新
                                self.offset = updates[-1]["update_id"] + 1
                            return updates
                        else:
                            logger.error(f"API Error: {data}")
                    else:
                        logger.error(f"HTTP Error: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"[ERROR] Error getting updates: {e}")
            return []
    
    def parse_delist_message(self, text: str) -> Optional[DelistEvent]:
        """デリストメッセージのパース"""
        try:
            # パターン例: "Binance EN: Binance Will Delist CVP, EPX, FOR, LOOM, REEF, VGX on 2024-08-19"
            # または "Binance Will Delist xxx on yyyy-mm-dd"
            delist_patterns = [
                r"Binance(?:\s+EN)?:?\s*Binance\s+Will\s+Delist\s+(.*?)\s+on\s+",
                r"Binance\s+Will\s+Delist\s+(.*?)(?:\s+on\s+|$)"
            ]
            
            for pattern in delist_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    token_str = match.group(1)
                    
                    # 複数トークンを処理
                    tokens = []
                    # カンマまたは"and"で区切られたトークンを分割
                    token_parts = re.split(r',\s*|and', token_str)
                    for token in token_parts:
                        # トークンシンボルを抽出（英字と数字のみ）
                        symbol_match = re.search(r'([A-Z0-9]+)', token.strip(), re.IGNORECASE)
                        if symbol_match:
                            base_token = symbol_match.group(1).upper()
                            tokens.append(base_token)
                    
                    if tokens:
                        return DelistEvent(
                            tokens=tokens,
                            event_detected_time=datetime.now(),
                            event_text=text
                        )
                        
            return None
            
        except Exception as e:
            logger.error(f"[ERROR] Error parsing delist message: {e}")
            return None
    
    async def on_delist_detected(self, event: DelistEvent):
        """デリストイベント検出時のコールバック"""
        # サブクラスでオーバーライド
        pass
    
    async def stop(self):
        """監視を停止"""
        self.is_running = False

class DelistTradingBot(TelegramChannelMonitor):
    """Binanceデリストイベントを基にした自動取引ボット"""
    
    def __init__(self, bot_token: str, channel_username: str):
        super().__init__(bot_token, channel_username)
        self.bybit_client = BybitClient()
        self.notifier = TelegramNotifier()
        self.leverage = 5
        self.position_percent = 1.0  # 資産の100%
        self.funding_threshold = -0.01  # -1%以下
        self.traded_events = set()  # 重複取引防止
        self.cooldown_hours = 24 # クールダウン期間
        self.max_trades_per_event = 1  # イベントごとの最大取引数
        
        # 銘柄マッピングリスト（キャッシュ）
        self.token_symbol_mapping = {}  # {トークン名: [可能なBybitシンボル]}
        self.symbol_update_interval = 3600  # 1時間ごとに更新
        self.last_symbol_update_time = None
        
        # 銘柄リスト更新タスク
        self._symbol_update_task = None
        
        # 起動時の初期化
        asyncio.create_task(self.initialize_symbol_cache())
    
    async def initialize_symbol_cache(self):
        """初期化時にBybit銘柄リストを取得"""
        try:
            logger.info("[DELIST] Initializing symbol cache...")
            await self.update_symbol_cache()
            logger.info(f"[DELIST] Symbol cache initialized. Total mappings: {len(self.token_symbol_mapping)}")
            
            # 定期的な更新を開始
            self._symbol_update_task = asyncio.create_task(self.periodic_symbol_update())
        except Exception as e:
            logger.error(f"[ERROR] Failed to initialize symbol cache: {e}")
    
    async def periodic_symbol_update(self):
        """定期的にBybit銘柄リストを更新"""
        try:
            while self.is_running:
                await asyncio.sleep(self.symbol_update_interval)
                if not self.is_running:
                    break
                
                logger.info("[DELIST] Updating symbol cache...")
                await self.update_symbol_cache()
                logger.info(f"[DELIST] Symbol cache updated. Total mappings: {len(self.token_symbol_mapping)}")
        except Exception as e:
            logger.error(f"[ERROR] Error in periodic symbol update: {e}")
    
    async def update_symbol_cache(self):
        """Bybit銘柄リストを更新"""
        try:
            self.token_symbol_mapping.clear()
            
            # Bybitから全銘柄を取得
            market_data = self.bybit_client.get_market_data()
            logger.info(f"[DELIST] Retrieved {len(market_data)} symbols from Bybit")
            
            # 銘柄パターンを解析してマッピングを作成
            for data in market_data:
                symbol = data["symbol"]
                
                # シンボルからトークンを抽出
                patterns = [
                    r"1000(\w+)USDT",     # 1000SHIBUSDT -> SHIB
                    r"(\w+)USDT",         # BTCUSDT -> BTC
                    r"(\w+)USD",          # BTCUSD -> BTC
                    r"(\w+)PERP",         # BTCPERP -> BTC
                    r"(\w+)USDC",         # BTCUSDC -> BTC
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, symbol, re.IGNORECASE)
                    if match:
                        token = match.group(1).upper()
                        
                        # マッピングに追加
                        if token not in self.token_symbol_mapping:
                            self.token_symbol_mapping[token] = []
                        self.token_symbol_mapping[token].append(symbol)
                        
                        logger.debug(f"[DELIST] Mapped: {token} -> {symbol}")
                        break
            
            self.last_symbol_update_time = datetime.now()
            logger.info(f"[DELIST] Symbol cache updated successfully at {self.last_symbol_update_time}")
            
        except Exception as e:
            logger.error(f"[ERROR] Error updating symbol cache: {e}")
    
    def find_trading_pairs(self, tokens: List[str]) -> List[str]:
        """キャッシュされた銘柄リストからトークンに対応する取引ペアを高速検索"""
        trading_pairs = []
        
        try:
            for token in tokens:
                token_upper = token.upper()
                if token_upper in self.token_symbol_mapping:
                    # キャッシュから直接取得
                    matching_symbols = self.token_symbol_mapping[token_upper]
                    for symbol in matching_symbols:
                        logger.info(f"[DELIST] Found cached trading pair: {symbol} for token {token}")
                        trading_pairs.append(symbol)
                else:
                    logger.warning(f"[DELIST] No trading pair found for token: {token}")
            
            return trading_pairs
            
        except Exception as e:
            logger.error(f"[ERROR] Error finding trading pairs: {e}")
            return []
    
    async def on_delist_detected(self, event: DelistEvent):
        """デリストイベント検出時の処理"""
        try:
            logger.info(f"[DELIST] Processing delist event for tokens: {event.tokens}")
            
            # 通知送信
            event_message = f"🚨 Binance デリストイベント検出:\n"
            event_message += f"トークン: {', '.join(event.tokens)}\n"
            event_message += f"検出時刻: {event.event_detected_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            event_message += f"元メッセージ: {event.event_text}"
            self.notifier.send_message(event_message)
            
            # 取引可能なペアを検索
            trading_pairs = self.find_trading_pairs(event.tokens)
            if not trading_pairs:
                logger.warning(f"[DELIST] No trading pairs found for {event.tokens}")
                self.notifier.send_message(f"警告: {event.tokens}の取引ペアが見つかりません")
                return
            
            # 資金調達率をチェック
            eligible_pairs = []
            funding_rates = self.bybit_client.get_funding_rates()
            
            for pair in trading_pairs:
                funding_rate = None
                for rate_info in funding_rates:
                    if rate_info["symbol"] == pair:
                        funding_rate = rate_info["rate"]
                        break
                
                if funding_rate is not None and funding_rate <= self.funding_threshold:
                    eligible_pairs.append((pair, funding_rate))
                    logger.info(f"[DELIST] Eligible pair: {pair}, funding rate: {funding_rate:.4%}")
            
            if not eligible_pairs:
                logger.info(f"[DELIST] No eligible pairs with funding rate <= {self.funding_threshold:.4%}")
                self.notifier.send_message(f"対象の銘柄が資金調達率条件を満たしていません")
                return
            
            # 最も資金調達率が低い銘柄を選択
            eligible_pairs.sort(key=lambda x: x[1])  # 資金調達率で昇順ソート
            most_eligible = eligible_pairs[:self.max_trades_per_event]
            
            # 取引実行
            for symbol, funding_rate in most_eligible:
                try:
                    event_key = f"{event.tokens[0]}_{event.event_detected_time.strftime('%Y%m%d')}"
                    if event_key in self.traded_events:
                        logger.info(f"[DELIST] Event already traded: {event_key}")
                        continue
                    
                    # 取引を実行
                    trade_result = await self.execute_trade(symbol, funding_rate, event)
                    
                    if trade_result.get("success", False):
                        # 取引成功時に記録
                        self.traded_events.add(event_key)
                        
                        # クールダウンを設定
                        asyncio.create_task(self.remove_traded_event(event_key, self.cooldown_hours))
                        
                        # 通知を送信
                        self.notifier.send_message(self.format_trade_notification(trade_result, event))
                        
                except Exception as e:
                    logger.error(f"[ERROR] Error trading {symbol}: {e}")
                    
        except Exception as e:
            logger.error(f"[ERROR] Error processing delist event: {e}")
    
    async def remove_traded_event(self, event_key: str, hours: float):
        """指定時間後に取引済みイベントから削除"""
        await asyncio.sleep(hours * 3600)
        if event_key in self.traded_events:
            self.traded_events.remove(event_key)
            logger.info(f"[DELIST] Cleared traded event: {event_key}")
    
    async def execute_trade(self, symbol: str, funding_rate: float, event: DelistEvent) -> Dict:
        """取引を実行"""
        try:
            logger.info(f"[DELIST] Executing trade for {symbol}")
            
            # 残高確認
            balance = self.bybit_client.get_wallet_balance("USDT")
            if balance <= 0:
                return {
                    "success": False,
                    "error": "Insufficient balance",
                    "symbol": symbol
                }
            
            # 証拠金計算（残高の100%使用）
            margin = balance * self.position_percent
            
            # レバレッジ設定
            leverage_set = self.bybit_client.set_leverage(symbol, self.leverage)
            if not leverage_set:
                logger.info(f"[DELIST] Leverage already set to {self.leverage}x for {symbol}")
            
            # 現在の価格を取得
            market_data = self.bybit_client.get_market_data()
            target_data = next((data for data in market_data if data["symbol"] == symbol), None)
            
            if not target_data:
                return {
                    "success": False,
                    "error": f"Market data not found for {symbol}",
                    "symbol": symbol
                }
            
            current_price = target_data["last_price"]
            
            # 注文数量計算
            order_value = margin * self.leverage
            qty = order_value / current_price
            
            # シンボル情報に基づいて数量を調整
            symbol_info = self.bybit_client.get_symbol_info(symbol)
            if symbol_info:
                qty_step = symbol_info['qtyStep']
                qty = math.floor(qty / qty_step) * qty_step
                
                # 最小注文量チェック
                min_order_qty = symbol_info['minOrderQty']
                if qty < min_order_qty:
                    qty = min_order_qty
            
            # 成行注文実行
            order_result = self.bybit_client.place_market_order(symbol, "Buy", qty)
            
            if order_result.get("success", False):
                return {
                    "success": True,
                    "symbol": symbol,
                    "side": "Buy",
                    "qty": qty,
                    "price": current_price,
                    "order_id": order_result.get("order_id"),
                    "funding_rate": funding_rate,
                    "leverage": self.leverage,
                    "delist_event": event
                }
            else:
                return {
                    "success": False,
                    "error": order_result.get("error", "Unknown error"),
                    "symbol": symbol
                }
                
        except Exception as e:
            logger.error(f"[ERROR] Error executing trade: {e}")
            return {
                "success": False,
                "error": str(e),
                "symbol": symbol
            }
    
    def format_trade_notification(self, trade_result: Dict, event: DelistEvent) -> str:
        """取引通知のフォーマット"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        message = f"🚀 デリストイベント取引実行 ({current_time}):\n\n"
        message += f"デリスト情報:\n"
        message += f"• トークン: <b>{', '.join(event.tokens)}</b>\n"
        message += f"• 元メッセージ: <b>{event.event_text}</b>\n\n"
        
        message += f"取引情報:\n"
        message += f"• 銘柄: <b>{trade_result['symbol']}</b>\n"
        message += f"• 方向: <b>{trade_result['side']}</b>\n"
        message += f"• 数量: <b>{trade_result['qty']}</b>\n"
        message += f"• 価格: <b>{trade_result['price']}</b>\n"
        message += f"• レバレッジ: <b>{trade_result['leverage']}x</b>\n"
        message += f"• 資金調達率: <b>{trade_result['funding_rate']:.4%}</b>\n"
        message += f"• 注文ID: <b>{trade_result['order_id']}</b>\n"
        
        return message
    
    async def stop(self):
        """監視を停止"""
        self.is_running = False
        if hasattr(self, '_symbol_update_task') and self._symbol_update_task:
            self._symbol_update_task.cancel()
        await super().stop()

async def main():
    """メインエントリーポイント"""
    # 環境変数のロード
    load_dotenv()
    
    # ロギング設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Telegram設定
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    channel_username = "BWEnews_JP"  # https://t.me/BWEnews_JP
    
    if not bot_token:
        logger.error("TELEGRAM_BOT_TOKEN not set in environment variables")
        return
    
    # ボットを初期化して実行
    bot = DelistTradingBot(bot_token, channel_username)
    
    try:
        logger.info("Starting Binance Delisting Trading Bot...")
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Stopping bot...")
        await bot.stop()
    except Exception as e:
        logger.error(f"Error running bot: {e}")
        await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())