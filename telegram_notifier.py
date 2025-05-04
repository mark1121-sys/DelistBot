import os
import logging
import requests
from dotenv import load_dotenv

# ロギング設定
logger = logging.getLogger(__name__)

# API設定
API_TIMEOUT = 30  # タイムアウトを30秒に延長

class TelegramNotifier:
    def __init__(self):
        # 環境変数からBotトークンとチャットIDを取得
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        # apiURLの構築
        if self.bot_token:
            self.api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        else:
            self.api_url = None
            logger.warning("Telegram bot token not set in environment variables")
        
        # チャットIDをログに記録（デバッグ用）
        logger.info(f"Initialized TelegramNotifier with chat_id: {self.chat_id}")

    def send_message(self, message: str) -> bool:
        """
        Telegramにメッセージを送信する
        
        Args:
            message: 送信するメッセージ（HTML形式可）
            
        Returns:
            bool: 送信成功の場合はTrue、失敗の場合はFalse
        """
        try:
            # APIキーまたはチャットIDが設定されていない場合は送信しない
            if not self.bot_token or not self.chat_id or not self.api_url:
                logger.error("Cannot send Telegram message: bot token or chat ID not set")
                return False
            
            # 送信データを準備
            payload = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML"  # HTMLタグを使用可能にする
            }
            
            # POSTリクエストを送信
            response = requests.post(self.api_url, json=payload, timeout=API_TIMEOUT)
            
            # レスポンスをチェック
            if response.status_code != 200:
                logger.error(f"Failed to send Telegram message: {response.text}")
                return False
                
            logger.info("Telegram message sent successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
            return False

# テスト用関数
def test_telegram_notification():
    # 環境変数をロード
    load_dotenv()
    
    # 通知クラスをインスタンス化
    notifier = TelegramNotifier()
    
    # 設定情報を出力
    print(f"Bot Token: {notifier.bot_token}")
    print(f"Chat ID: {notifier.chat_id}")
    
    # テストメッセージを送信
    test_message = "This is a test message from Market Monitor"
    result = notifier.send_message(test_message)
    
    print(f"Notification sent: {result}")

# スクリプトとして実行された場合にテスト関数を実行
if __name__ == "__main__":
    # ロギング設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # テスト実行
    test_telegram_notification()