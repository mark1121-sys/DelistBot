import os
import re
import logging
import asyncio
import json
import math
import platform
import tempfile
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError
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

# 不要なログをフィルタリング
class MessageFilter(logging.Filter):
    def filter(self, record):
        filtered_messages = [
            "Got difference",
            "Connecting to",
            "Connection complete",
            "Disconnecting from",
            "Disconnection complete",
            "Phone migrated to",
            "Reconnecting to new data center"
        ]
        return not any(msg in record.getMessage() for msg in filtered_messages)

for handler in logging.getLogger().handlers:
    handler.addFilter(MessageFilter())

logging.getLogger('telethon').setLevel(logging.WARNING)

@dataclass
class DelistEvent:
    """デリストイベントの情報を格納"""
    tokens: List[str]
    event_detected_time: datetime
    event_text: str

class DelistTradingBot:
    """Binanceデリストイベントを基にした自動取引ボット"""
    
    def __init__(self):
        # 環境変数の読み込み
        load_dotenv()
        
        # Telegram API設定
        self.api_id = os.getenv("TELEGRAM_API_ID")
        self.api_hash = os.getenv("TELEGRAM_API_HASH")
        self.phone = os.getenv("TELEGRAM_PHONE")
        self.channel_username = "BWEnews_JP"  # https://t.me/BWEnews_JP
        
        if not self.api_id or not self.api_hash:
            raise ValueError("TELEGRAM_API_ID or TELEGRAM_API_HASH not set in environment variables")
        
        # Bybit関連の設定
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
        
        # Telegramセッション関連
        self.client = None
        self.session_file = None
        
        # 初期化処理を非同期で開始
        asyncio.create_task(self.initialize_symbol_cache())
    
    async def initialize_symbol_cache(self):
        """初期化時にBybit銘柄リストを取得"""
        try:
            logger.info("[DELIST] Initializing symbol cache...")
            await self.update_symbol_cache()
            logger.info(f"[DELIST] Symbol cache initialized. Total mappings: {len(self.token_symbol_mapping)}")
            
            # 定期的な更新を開始
            asyncio.create_task(self.periodic_symbol_update())
        except Exception as e:
            logger.error(f"[ERROR] Failed to initialize symbol cache: {e}")
    
    async def periodic_symbol_update(self):
        """定期的にBybit銘柄リストを更新"""
        try:
            while True:
                await asyncio.sleep(self.symbol_update_interval)
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
    
    async def handle_authentication(self):
        """認証処理"""
        try:
            logger.info("Starting authentication process...")
            await self.client.send_code_request(self.phone)
            code = input('Please enter the verification code you received: ')
            logger.info("Attempting to sign in with the provided code...")
            
            try:
                await self.client.sign_in(self.phone, code)
            except SessionPasswordNeededError:
                logger.info("Two-factor authentication required")
                password = input('Please enter your 2FA password: ')
                await self.client.sign_in(password=password)
            
            logger.info("Authentication successful!")
            return True
            
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False
    
    async def initialize_client(self):
        """Telegramクライアントの初期化"""
        try:
            logger.info("Initializing Telegram client...")
            
            # 電話番号の確認と設定
            if not self.phone:
                self.phone = input("Please enter your phone number (with country code, e.g., +819012345678): ")
                if not self.phone.startswith('+'):
                    if self.phone.startswith('0'):
                        self.phone = '+81' + self.phone[1:]
                    else:
                        self.phone = '+' + self.phone
            
            # セッションファイルの作成
            temp_dir = tempfile.gettempdir()
            self.session_file = os.path.join(temp_dir, f'delist_bot_session_{os.getpid()}')
            
            # クライアントの初期化
            self.client = TelegramClient(self.session_file, self.api_id, self.api_hash)
            await self.client.connect()
            
            # 認証チェック
            if not await self.client.is_user_authorized():
                if not await self.handle_authentication():
                    raise Exception("Authentication failed")
            
            logger.info("Telegram client initialized successfully")
            
        except Exception as e:
            self.cleanup_session()
            logger.error(f"Failed to initialize Telegram client: {e}")
            raise
    
    async def setup_message_handler(self):
        """メッセージハンドラーの設定"""
        @self.client.on(events.NewMessage(incoming=True))
        async def message_handler(event):
            try:
                # チャンネルメッセージのみを処理
                if not hasattr(event.chat, 'username') or event.chat.username != self.channel_username:
                    return
                
                message_text = event.text
                logger.info(f"[TELEGRAM] New message from {self.channel_username}: {message_text}")
                
                # デリストメッセージを検出
                delist_event = self.parse_delist_message(message_text)
                if delist_event:
                    logger.info(f"[DELIST] Detected: {delist_event}")
                    await self.on_delist_detected(delist_event)
                    
            except Exception as e:
                logger.error(f"[ERROR] Error processing message: {e}")
    
    def cleanup_session(self):
        """セッションのクリーンアップ"""
        try:
            if self.session_file and os.path.exists(self.session_file):
                os.remove(self.session_file)
                logger.info(f"Cleaned up session file: {self.session_file}")
        except Exception as e:
            logger.error(f"Error cleaning up session: {e}")
    
    async def run(self):
        """メインループ"""
        try:
            # クライアントの初期化
            await self.initialize_client()
            
            # メッセージハンドラーの設定
            await self.setup_message_handler()
            
            logger.info(f"Started monitoring channel: {self.channel_username}")
            print(f"\nMonitoring started for channel: {self.channel_username}")
            print("Waiting for delist messages...")
            
            # クライアントを実行
            await self.client.run_until_disconnected()
            
        except Exception as e:
            logger.error(f"Monitor crashed: {e}")
        finally:
            if self.client:
                await self.client.disconnect()
            self.cleanup_session()

async def main():
    """メインエントリーポイント"""
    # ロギング設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    try:
        bot = DelistTradingBot()
        await bot.run()
    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user")
        print("\nMonitoring stopped by user. Goodbye!")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    asyncio.run(main())