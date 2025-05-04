import os
import time
import hmac
import json
import logging
import urllib.parse
import requests
import numpy as np
import math
from datetime import datetime
from dotenv import load_dotenv

# ロギング設定
logger = logging.getLogger(__name__)

# API設定
API_TIMEOUT = 30

class BybitClient:
    def __init__(self):
        self.base_url = "https://api.bybit.com"
        # 履歴データを保存するディクショナリ
        self.volume_history = {}
        self.oi_history = {}
        
        # API認証情報
        self.api_key = os.getenv("BYBIT_API")
        self.api_secret = os.getenv("BYBIT_SECRET")
        self.recv_window = "5000"  # ミリ秒単位
        
        if not self.api_key or not self.api_secret:
            logger.warning("Bybit API credentials not found in environment variables")
            logger.warning(f"API_KEY: {self.api_key is not None}, API_SECRET: {self.api_secret is not None}")

    def get_funding_rates(self) -> list:
        """全シンボルの資金調達率を取得する"""
        try:
            logger.info("Fetching Bybit funding rates...")
            url = f"{self.base_url}/v5/market/tickers"
            params = {
                "category": "linear"
            }
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0'
            }
            response = requests.get(url, params=params, headers=headers, timeout=API_TIMEOUT)
            logger.info(f"Bybit Response Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                rates = []
                if data.get("retCode") == 0 and "result" in data and "list" in data["result"]:
                    for item in data["result"]["list"]:
                        try:
                            if "fundingRate" in item and item["fundingRate"] != "":
                                rate = float(item["fundingRate"])
                                rates.append({
                                    "symbol": item["symbol"],
                                    "rate": rate,
                                    "exchange": "Bybit"
                                })
                        except (ValueError, KeyError) as e:
                            continue
                logger.info(f"Successfully fetched {len(rates)} rates from Bybit")
                return rates
            return []
        except Exception as e:
            logger.error(f"Error fetching Bybit funding rates: {e}")
            return []

    def get_market_data(self) -> list:
        """取引量データ、OIデータ、その他の市場データを取得する"""
        try:
            logger.info("Fetching Bybit market data...")
            
            # ティッカー情報取得（取引量や価格を含む）
            ticker_url = f"{self.base_url}/v5/market/tickers"
            ticker_params = {
                "category": "linear"
            }
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0'
            }
            
            ticker_response = requests.get(ticker_url, params=ticker_params, headers=headers, timeout=API_TIMEOUT)
            if ticker_response.status_code != 200:
                logger.error(f"Failed to get Bybit ticker data: {ticker_response.text}")
                return []
                
            ticker_data = ticker_response.json()
            
            # OIデータ取得
            oi_url = f"{self.base_url}/v5/market/open-interest"
            
            market_data = []
            current_time = datetime.now()
            
            if ticker_data.get("retCode") == 0 and "result" in ticker_data and "list" in ticker_data["result"]:
                for item in ticker_data["result"]["list"]:
                    try:
                        symbol = item["symbol"]
                        
                        # 取引量と価格変動データを抽出
                        if "volume24h" in item and item["volume24h"] != "":
                            volume_24h = float(item["volume24h"])
                            
                            # 現在の価格データを取得
                            last_price = float(item.get("lastPrice", 0))
                            price_change_percent = float(item.get("price24hPcnt", 0)) * 100  # パーセンテージに変換
                            
                            # OIデータを取得
                            oi_params = {
                                "category": "linear",
                                "symbol": symbol
                            }
                            
                            oi_response = requests.get(oi_url, params=oi_params, headers=headers, timeout=API_TIMEOUT)
                            oi_value = 0
                            
                            if oi_response.status_code == 200:
                                oi_data = oi_response.json()
                                if oi_data.get("retCode") == 0 and "result" in oi_data and "list" in oi_data["result"] and oi_data["result"]["list"]:
                                    oi_list = oi_data["result"]["list"]
                                    if oi_list and "openInterest" in oi_list[0]:
                                        oi_value = float(oi_list[0]["openInterest"])
                            
                            # 過去の取引量データを更新・保存
                            if symbol not in self.volume_history:
                                self.volume_history[symbol] = []
                            
                            # 過去のOIデータを更新・保存
                            if symbol not in self.oi_history:
                                self.oi_history[symbol] = []
                                
                            # 最大12個のサンプルを保持（約2時間分のデータ）
                            if len(self.volume_history[symbol]) >= 12:
                                self.volume_history[symbol].pop(0)
                            
                            if len(self.oi_history[symbol]) >= 12:
                                self.oi_history[symbol].pop(0)
                            
                            self.volume_history[symbol].append({
                                "timestamp": current_time,
                                "volume": volume_24h
                            })
                            
                            self.oi_history[symbol].append({
                                "timestamp": current_time,
                                "oi": oi_value
                            })
                            
                            # 過去データがある場合、異常検知を実行
                            volume_change_percent = 0
                            volume_alert = False
                            base_volume = 0
                            
                            # 取引量変化率の計算
                            if len(self.volume_history[symbol]) >= 2:
                                previous_volumes = [v["volume"] for v in self.volume_history[symbol][:-1]]
                                base_volume = np.mean(previous_volumes)
                                
                                if base_volume > 0:
                                    volume_change_percent = ((volume_24h - base_volume) / base_volume) * 100
                                    
                                    # 取引量が150%以上増加し、価格も5%以上変動している場合にアラート
                                    if volume_change_percent > 150 and abs(price_change_percent) > 5:
                                        volume_alert = True
                            
                            # OI変化率の計算
                            oi_change_percent = 0
                            oi_alert = False
                            base_oi = 0
                            
                            if len(self.oi_history[symbol]) >= 2:
                                previous_oi = [o["oi"] for o in self.oi_history[symbol][:-1]]
                                base_oi = np.mean(previous_oi)
                                
                                if base_oi > 0:
                                    oi_change_percent = ((oi_value - base_oi) / base_oi) * 100
                                    
                                    # OIが30%以上増加し、価格も5%以上変動している場合にアラート
                                    if oi_change_percent > 30 and abs(price_change_percent) > 5:
                                        oi_alert = True
                            
                            market_data.append({
                                "symbol": symbol,
                                "exchange": "Bybit",
                                "volume_24h": volume_24h,
                                "oi": oi_value,
                                "last_price": last_price,
                                "price_change_percent": price_change_percent,
                                "volume_change_percent": volume_change_percent,
                                "oi_change_percent": oi_change_percent,
                                "base_volume": base_volume,
                                "base_oi": base_oi,
                                "volume_alert": volume_alert,
                                "oi_alert": oi_alert
                            })
                            
                            # APIレート制限を避けるために少し待機
                            time.sleep(0.1)
                            
                    except (ValueError, KeyError) as e:
                        logger.error(f"Error processing market data for {symbol}: {e}")
                        continue
            
            logger.info(f"Successfully fetched market data for {len(market_data)} symbols from Bybit")
            return market_data
        except Exception as e:
            logger.error(f"Error fetching Bybit market data: {e}")
            return []

    def _generate_signature(self, timestamp: str, method: str, path: str, query_string: str = '', body_str: str = '') -> str:
        """Bybit API V5用の改良された署名生成メソッド"""
        # 署名対象の文字列を生成
        payload = timestamp + self.api_key + self.recv_window
        
        # GETリクエストの場合はクエリストリングを追加
        if method == 'GET' and query_string:
            payload += query_string
        # POSTリクエストの場合はボディを追加
        elif method == 'POST' and body_str:
            payload += body_str
            
        logger.debug(f"Signature payload: {payload}")

        # HMAC-SHA256で署名を生成
        signature = hmac.new(
            bytes(self.api_secret, "utf-8"),
            bytes(payload, "utf-8"),
            digestmod="sha256"
        ).hexdigest()
        
        return signature

    def _get_headers(self, method: str = 'GET', path: str = '', params: dict = None, body: dict = None) -> dict:
        """認証用ヘッダーを生成する（改良版）"""
        timestamp = str(int(time.time() * 1000))
        
        query_string = ''
        if params and method == 'GET':
            query_string = urllib.parse.urlencode(params)
            
        body_str = ''
        if body and method == 'POST':
            body_str = json.dumps(body)
        
        signature = self._generate_signature(timestamp, method, path, query_string, body_str)
        
        return {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": self.recv_window,
            "X-BAPI-SIGN": signature,
            "Content-Type": "application/json"
        }

    def get_wallet_balance(self, coin: str = "USDT") -> float:
        """ウォレット残高を取得する - 修正版"""
        try:
            path = "/v5/account/wallet-balance"
            url = f"{self.base_url}{path}"
            params = {"accountType": "UNIFIED", "coin": coin}
            
            # GETリクエスト用のヘッダーを作成
            headers = self._get_headers('GET', path, params=params)
            
            logger.info(f"Requesting wallet balance for {coin}...")
            response = requests.get(url, headers=headers, params=params, timeout=API_TIMEOUT)
            if response.status_code != 200:
                logger.error(f"Failed to get Bybit wallet balance: {response.text}")
                return 0.0
            
            logger.info(f"Wallet balance response status: {response.status_code}")
            data = response.json()
            logger.info(f"Wallet balance response: {data}")
            
            if data.get("retCode") != 0:
                logger.error(f"Error in Bybit wallet balance response: {data}")
                return 0.0
            
            # 改良された残高取得ロジック
            if "result" in data and "list" in data["result"] and data["result"]["list"]:
                account_info = data["result"]["list"][0]
                
                # coin配列から指定したコインを検索
                if "coin" in account_info:
                    for coin_info in account_info["coin"]:
                        if coin_info.get("coin") == coin:
                            balance = float(coin_info.get("walletBalance", 0))
                            logger.info(f"Found {coin} balance: {balance}")
                            return balance
                
                # 代替: 総残高を使用
                if "totalWalletBalance" in account_info:
                    balance = float(account_info["totalWalletBalance"])
                    logger.info(f"Using total wallet balance: {balance}")
                    return balance
            
            logger.error(f"Could not find {coin} balance in Bybit wallet")
            return 0.0
        except Exception as e:
            logger.error(f"Error getting Bybit wallet balance: {e}")
            return 0.0

    def get_symbol_info(self, symbol: str) -> dict:
        """シンボル情報を取得する"""
        try:
            url = f"{self.base_url}/v5/market/instruments-info"
            params = {"category": "linear", "symbol": symbol}
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0'
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=API_TIMEOUT)
            if response.status_code != 200:
                logger.error(f"Failed to get symbol info for {symbol}: {response.text}")
                return None
                
            data = response.json()
            if data.get("retCode") != 0:
                logger.error(f"Error in Bybit symbol info response: {data}")
                return None
                
            if "result" in data and "list" in data["result"] and data["result"]["list"]:
                symbol_info = data["result"]["list"][0]
                lot_size_filter = symbol_info.get("lotSizeFilter", {})
                return {
                    'minOrderQty': float(lot_size_filter.get("minOrderQty", 0)),
                    'qtyStep': float(lot_size_filter.get("qtyStep", 0.001)),
                    'minOrderVal': float(symbol_info.get("minOrderValue", 0))
                }
            
            logger.error(f"Could not find info for {symbol}")
            return None
        except Exception as e:
            logger.error(f"Error getting symbol info for {symbol}: {e}")
            return None


    def get_position(self, symbol: str) -> dict:
        """ポジション情報を取得する（エラー処理強化版）"""
        try:
            path = "/v5/position/list"
            url = f"{self.base_url}{path}"
            
            # カテゴリパラメータを追加（linearは先物取引）
            params = {
                "category": "linear",
                "symbol": symbol
            }
            
            # 認証用ヘッダーを生成
            headers = self._get_headers('GET', path, params=params)
            
            # デバッグ用にリクエスト情報をログに記録
            logger.debug(f"Position request URL: {url}")
            logger.debug(f"Position request params: {params}")
            
            # APIリクエスト送信
            response = requests.get(url, headers=headers, params=params, timeout=API_TIMEOUT)
            
            # レスポンスステータスをログに記録
            logger.debug(f"Position response status: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"Failed to get position for {symbol}: {response.text}")
                return {"side": "None", "size": "0"}
            
            # レスポンスJSONをログに記録（デバッグ用）
            response_data = response.json()
            logger.debug(f"Position response: {response_data}")
            
            # "symbol not exist" エラーの処理
            if response_data.get("retCode") == 10001 and "symbol not exist" in response_data.get("retMsg", "").lower():
                logger.warning(f"Symbol {symbol} does not exist on Bybit. Returning empty position.")
                return {"side": "None", "size": "0", "symbol": symbol}
            
            if response_data.get("retCode") != 0:
                logger.error(f"Error in Bybit position response: {response_data}")
                return {"side": "None", "size": "0"}
            
            # レスポンス構造の確認と対応
            if "result" in response_data and "list" in response_data["result"]:
                positions = response_data["result"]["list"]
                
                # 返されたポジションリストが空でないことを確認
                if positions:
                    # 最初のポジションを取得
                    position = positions[0]
                    position_size = float(position.get("size", 0))
                    position_side = position.get("side", "None")
                    
                    logger.info(f"Current position for {symbol}: size={position_size}, side={position_side}")
                    return position
                else:
                    logger.info(f"No position data found for {symbol}")
                    return {"side": "None", "size": "0", "symbol": symbol}
            
            logger.info(f"No position found for {symbol} in response structure")
            return {"side": "None", "size": "0", "symbol": symbol}
        
        except Exception as e:
            logger.error(f"Error getting position for {symbol}: {e}")
            # エラー時はNoneではなく空のポジション情報を返す
            return {"side": "None", "size": "0", "symbol": symbol}

    def get_all_positions(self) -> list:
        """
        すべてのアクティブポジションを取得する（シンプル版）
        
        Returns:
            アクティブポジションのリスト
        """
        try:
            path = "/v5/position/list"
            url = f"{self.base_url}{path}"
            
            # 重要: linearカテゴリでは、symbol または settleCoin のいずれかが必須
            # settleCoinを使用して、すべてのUSDTペアのポジションを取得
            params = {
                "category": "linear",
                "settleCoin": "USDT",  # 決済通貨としてUSDTを指定
                "limit": "50"  # 結果の最大数を指定（デフォルトは20）
            }
            
            # 認証用ヘッダーを生成
            headers = self._get_headers('GET', path, params=params)
            
            # デバッグログを追加
            logger.debug(f"Getting all positions with params: {params}")
            
            # APIリクエスト送信
            response = requests.get(url, headers=headers, params=params, timeout=API_TIMEOUT)
            
            # レスポンスステータスとボディをログに記録
            logger.debug(f"All positions response status: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"Failed to get all positions: {response.text}")
                return []
            
            data = response.json()
            logger.debug(f"All positions response: {data}")
            
            if data.get("retCode") != 0:
                logger.error(f"Error in Bybit positions response: {data}")
                
                # エラーコード10001（パラメータ不足）の場合、ドキュメント通りにsettleCoinパラメータを使用しても
                # 失敗する場合があるため、代替方法に切り替え
                if data.get("retCode") == 10001:
                    logger.warning("Error 10001 received. Parameter issue with Bybit API.")
                    return []
                
                return []
            
            # アクティブなポジションのみをフィルタリング
            active_positions = []
            
            if "result" in data and "list" in data["result"]:
                positions_list = data["result"]["list"]
                logger.info(f"Retrieved {len(positions_list)} positions from API")
                
                for position in positions_list:
                    try:
                        symbol = position.get("symbol", "")
                        size = float(position.get("size", 0))
                        side = position.get("side", "None")
                        
                        # サイズが0より大きく、サイドが"None"でないポジションのみを抽出
                        if size > 0 and side != "None":
                            logger.info(f"Found active position: {symbol}, size={size}, side={side}")
                            active_positions.append(position)
                    
                    except (ValueError, KeyError) as e:
                        logger.error(f"Error parsing position data: {e}")
                        continue
            
            logger.info(f"Total active positions found: {len(active_positions)}")
            
            # 次のページのカーソルがあるか確認（ページネーション対応）
            next_cursor = data.get("result", {}).get("nextPageCursor", "")
            if next_cursor and active_positions:
                logger.info(f"More positions available at cursor: {next_cursor}")
                # 次のページがある場合、必要に応じて実装可能
            
            return active_positions
        
        except Exception as e:
            logger.error(f"Error getting all positions: {e}")
            return []


            
    def set_leverage(self, symbol: str, leverage: int) -> bool:
        """レバレッジを設定する - 改良版"""
        try:
            # 現在のポジション情報を取得してレバレッジをチェック
            position = self.get_position(symbol)
            if position:
                current_leverage = position.get("leverage")
                if current_leverage and int(float(current_leverage)) == leverage:
                    logger.info(f"Leverage for {symbol} is already set to {leverage}x, skipping update")
                    return True
            
            # レバレッジ設定が必要な場合
            path = "/v5/position/set-leverage"
            url = f"{self.base_url}{path}"
            body = {
                "category": "linear",
                "symbol": symbol,
                "buyLeverage": str(leverage),
                "sellLeverage": str(leverage)
            }
            
            # POST用のヘッダーを生成
            headers = self._get_headers('POST', path, body=body)
            
            logger.info(f"Setting leverage to {leverage}x for {symbol}...")
            logger.debug(f"Leverage request URL: {url}")
            logger.debug(f"Leverage request body: {body}")
            
            response = requests.post(url, headers=headers, json=body, timeout=API_TIMEOUT)
            logger.info(f"Set leverage response status: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"Failed to set Bybit leverage: {response.text}")
                return False
                
            data = response.json()
            logger.debug(f"Set leverage response: {data}")
            
            # エラーコードのチェック
            if data.get("retCode") != 0:
                # エラーコード110043: レバレッジが既に設定されている
                if data.get("retCode") == 110043 or "leverage not modified" in data.get("retMsg", "").lower():
                    logger.info(f"Bybit leverage already set to {leverage} for {symbol} (110043)")
                    return True
                
                # その他のエラーの場合
                logger.error(f"Error in Bybit set leverage response: {data}")
                return False
                
            logger.info(f"Successfully set Bybit leverage to {leverage} for {symbol}")
            return True
        except Exception as e:
            logger.error(f"Error setting Bybit leverage: {e}")
            return False
    
    def place_market_order(self, symbol: str, side: str, qty: float) -> dict:
        """成行注文を発注する"""
        try:
            # シンボル情報を取得してロットサイズに合わせる
            symbol_info = self.get_symbol_info(symbol)
            if not symbol_info:
                logger.error(f"Failed to get symbol info for {symbol}")
                return {"success": False, "error": f"Failed to get symbol info for {symbol}"}
            
            qty_step = symbol_info['qtyStep']
            qty = math.floor(qty / qty_step) * qty_step
            
            # 最小注文量チェック
            if qty < symbol_info['minOrderQty']:
                logger.error(f"Order quantity {qty} is less than minimum allowed {symbol_info['minOrderQty']}")
                return {"success": False, "error": f"Order quantity too small: {qty} < {symbol_info['minOrderQty']}"}
            
            # 注文数量を適切な精度に調整
            qty_precision = len(str(qty_step).rstrip('0').split('.')[-1]) if '.' in str(qty_step) else 0
            qty_str = f"{qty:.{qty_precision}f}"
            
            path = "/v5/order/create"
            url = f"{self.base_url}{path}"
            body = {
                "category": "linear",
                "symbol": symbol,
                "side": side.capitalize(),  # "Buy" or "Sell"
                "orderType": "Market",
                "qty": qty_str,
                "timeInForce": "IOC",  # Immediate Or Cancel
                "positionIdx": 0,  # 0: One-Way Mode
                "reduceOnly": False
            }
            
            headers = self._get_headers('POST', path, body=body)
            
            logger.info(f"Placing {side} market order for {qty_str} {symbol}...")
            logger.info(f"Order params: {body}")
            
            response = requests.post(url, headers=headers, json=body, timeout=API_TIMEOUT)
            logger.info(f"Order response status: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"Failed to place Bybit market order: {response.text}")
                return {"success": False, "error": response.text}
                
            data = response.json()
            logger.info(f"Order response: {data}")
            
            if data.get("retCode") != 0:
                logger.error(f"Error in Bybit place order response: {data}")
                return {"success": False, "error": data.get("retMsg", "Unknown error")}
                
            order_id = data.get("result", {}).get("orderId", "")
            logger.info(f"Successfully placed Bybit market order: {order_id}")
            
            # 注文情報を返す
            return {
                "success": True,
                "exchange": "Bybit",
                "symbol": symbol,
                "side": side,
                "qty": qty_str,
                "order_id": order_id,
                "order_type": "Market",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        except Exception as e:
            logger.error(f"Error placing Bybit market order: {e}")
            return {"success": False, "error": str(e)}

    def close_position(self, symbol: str, qty: float = None) -> dict:
        """ポジションをクローズする（部分または全部）- 改良版"""
        try:
            # 現在のポジション情報を取得
            position = self.get_position(symbol)
            if not position:
                logger.error(f"No position found for {symbol}")
                return {"success": False, "error": f"No position found for {symbol}"}
            
            # ポジションの方向とサイズを取得
            side = position.get("side", "")
            if not side or side == "None":
                logger.error(f"No active position for {symbol}")
                return {"success": False, "error": f"No active position for {symbol}"}
            
            position_size = float(position.get("size", 0))
            if position_size <= 0:
                logger.error(f"Invalid position size for {symbol}: {position_size}")
                return {"success": False, "error": f"Invalid position size: {position_size}"}
            
            # クローズする数量を決定
            close_qty = qty if qty and qty < position_size else position_size
            
            # 反対方向の注文を出す
            close_side = "Sell" if side == "Buy" else "Buy"
            
            logger.info(f"Closing {close_side} position for {symbol} with qty {close_qty}")
            
            # 市場注文でポジションをクローズ（reduceOnly=Trueを設定）
            path = "/v5/order/create"
            url = f"{self.base_url}{path}"
            
            # シンボル情報を取得してロットサイズに合わせる
            symbol_info = self.get_symbol_info(symbol)
            if symbol_info:
                qty_step = symbol_info['qtyStep']
                close_qty = math.floor(close_qty / qty_step) * qty_step
                
                # 最小注文量チェック
                if close_qty < symbol_info['minOrderQty']:
                    logger.error(f"Close quantity {close_qty} is less than minimum allowed {symbol_info['minOrderQty']}")
                    return {"success": False, "error": f"Close quantity too small: {close_qty} < {symbol_info['minOrderQty']}"}
                
                # 注文数量を適切な精度に調整
                qty_precision = len(str(qty_step).rstrip('0').split('.')[-1]) if '.' in str(qty_step) else 0
                qty_str = f"{close_qty:.{qty_precision}f}"
                
                # 注文パラメータを設定
                body = {
                    "category": "linear",
                    "symbol": symbol,
                    "side": close_side,
                    "orderType": "Market",
                    "qty": qty_str,
                    "timeInForce": "IOC",  # Immediate Or Cancel
                    "positionIdx": 0,      # One-Way Mode
                    "reduceOnly": True     # ポジションを減らすためだけの注文
                }
                
                # ヘッダーを生成
                headers = self._get_headers('POST', path, body=body)
                
                logger.info(f"Sending close position request: {symbol}, qty={qty_str}, side={close_side}")
                logger.debug(f"Close position request body: {body}")
                
                response = requests.post(url, headers=headers, json=body, timeout=API_TIMEOUT)
                logger.debug(f"Close position response status: {response.status_code}")
                
                if response.status_code != 200:
                    logger.error(f"Failed to close position: {response.text}")
                    return {"success": False, "error": response.text}
                    
                data = response.json()
                logger.debug(f"Close position response: {data}")
                
                if data.get("retCode") != 0:
                    logger.error(f"Error in close position response: {data}")
                    return {"success": False, "error": data.get("retMsg", "Unknown error")}
                    
                order_id = data.get("result", {}).get("orderId", "")
                logger.info(f"Successfully closed position with order ID: {order_id}")
                
                # 現在の価格を取得
                market_data = self.get_market_data()
                close_price = 0
                for item in market_data:
                    if item["symbol"] == symbol:
                        close_price = item["last_price"]
                        break
                
                # クローズ結果を返す
                return {
                    "success": True,
                    "exchange": "Bybit",
                    "symbol": symbol,
                    "side": close_side,
                    "qty": qty_str,
                    "close_price": close_price,
                    "order_id": order_id,
                    "order_type": "Market",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            
            else:
                logger.error(f"Failed to get symbol info for {symbol}")
                return {"success": False, "error": f"Failed to get symbol info for {symbol}"}
        
        except Exception as e:
            logger.error(f"Error closing position for {symbol}: {e}")
            return {"success": False, "error": str(e)}
    

    def get_open_interest_history(self, symbol: str, interval: str = "5min", limit: int = 12) -> list:
        """
        指定されたシンボルのOIの履歴データを取得する（エラー対応強化版）
        
        Args:
            symbol: シンボル（例：BTCUSDT）
            interval: 時間間隔（5min, 15min, 30min, 1h, 4h, 1d）
            limit: 取得するデータの最大数
                
        Returns:
            OI履歴データのリスト
        """
        try:
            logger.info(f"Fetching OI history for {symbol}")
            url = f"{self.base_url}/v5/market/open-interest-hist"
            
            # API仕様に合わせて intervalTime パラメータを変換
            # 5min, 15min, 30min, 1h, 4h, 1d のいずれかである必要がある
            interval_mapping = {
                "5min": "5min",
                "15min": "15min",
                "30min": "30min",
                "1h": "1h",
                "4h": "4h",
                "1d": "1d"
            }
            
            # マッピングに存在するインターバルを使用、存在しない場合はデフォルトの5分を使用
            api_interval = interval_mapping.get(interval, "5min")
            
            params = {
                "category": "linear",
                "symbol": symbol,
                "intervalTime": api_interval,
                "limit": limit
            }
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0'
            }
            
            logger.info(f"OI history request params: {params}")
            response = requests.get(url, params=params, headers=headers, timeout=API_TIMEOUT)
            
            if response.status_code != 200:
                logger.error(f"Failed to get OI history for {symbol}: {response.text}")
                # エラーが発生しても空の配列を返す（クラッシュを避けるため）
                return []
                
            data = response.json()
            
            # "symbol not exist" エラーの処理
            if data.get("retCode") == 10001 and "symbol not exist" in data.get("retMsg", "").lower():
                logger.warning(f"Symbol {symbol} does not exist on Bybit. Returning empty OI history.")
                return []
                
            if data.get("retCode") != 0:
                logger.error(f"Error in Bybit OI history response: {data}")
                return []
            
            if "result" in data and "list" in data["result"]:
                oi_history = []
                for item in data["result"]["list"]:
                    try:
                        oi_value = float(item.get("openInterest", 0))
                        timestamp = item.get("timestamp", 0)
                        oi_history.append({
                            "timestamp": timestamp,
                            "oi": oi_value
                        })
                    except (ValueError, KeyError) as e:
                        logger.error(f"Error parsing OI history item: {e}")
                        continue
                
                # データが0件の場合の処理
                if not oi_history:
                    logger.warning(f"No OI history data returned for {symbol}")
                    # 空のデータを作成（現在のOIを1つだけ持つリスト）
                    current_oi = 0
                    try:
                        # 現在のOIを取得
                        market_data = self.get_market_data()
                        for data in market_data:
                            if data["symbol"] == symbol:
                                current_oi = data.get("oi", 0)
                                break
                    except Exception as e:
                        logger.error(f"Error getting current OI: {e}")
                    
                    # 現在時刻のタイムスタンプを取得
                    current_timestamp = int(time.time() * 1000)
                    
                    # 空データを作成（分析用に同じデータを複数作成）
                    for i in range(limit):
                        oi_history.append({
                            "timestamp": current_timestamp - (i * 3600000),  # 1時間ずつ遡る
                            "oi": current_oi
                        })
                
                # 時系列順にソート（古いものから新しいもの）
                oi_history.sort(key=lambda x: x["timestamp"])
                
                logger.info(f"Successfully fetched {len(oi_history)} OI history items for {symbol}")
                return oi_history
            
            logger.error(f"No OI history data found for {symbol}")
            return []
        except Exception as e:
            logger.error(f"Error fetching OI history for {symbol}: {e}")
            return []
    

    def analyze_oi_change(self, symbol: str, threshold: float = -20.0) -> dict:
        """
        ポジションのOI変化を分析し、急激な減少があれば警告する（エラー対応強化版）
        
        Args:
            symbol: 分析するシンボル
            threshold: OI減少の閾値（パーセント、例：-20.0は20%の減少）
            
        Returns:
            分析結果を含む辞書
        """
        try:
            # 現在のポジションを確認
            position = self.get_position(symbol)
            if not position or position.get("side", "None") == "None" or float(position.get("size", 0)) <= 0:
                # ポジションがない場合はスキップ
                return {
                    "symbol": symbol,
                    "has_position": False,
                    "warning": False,
                    "message": "No active position"
                }
            
            # OI履歴データを取得
            oi_history = self.get_open_interest_history(symbol, interval="1h", limit=12)
            
            # OI履歴データが不足している場合の処理を強化
            if not oi_history:
                # OIデータが全くない場合
                logger.warning(f"No OI history data available for {symbol}")
                return {
                    "symbol": symbol,
                    "has_position": True,
                    "warning": False,
                    "change_percent": 0.0,
                    "threshold": threshold,
                    "message": "No OI history data available"
                }
            
            if len(oi_history) < 2:
                # 2ポイント以上のデータがない場合（比較できない）
                logger.warning(f"Insufficient OI history data for {symbol}: {len(oi_history)} points")
                return {
                    "symbol": symbol,
                    "has_position": True,
                    "warning": False,
                    "change_percent": 0.0,
                    "threshold": threshold,
                    "message": "Insufficient OI history data"
                }
            
            # 最新のOI値と前回の平均値を比較
            latest_oi = oi_history[-1]["oi"]
            
            # データポイントが2つだけの場合は単純に前回の値と比較
            if len(oi_history) == 2:
                previous_oi = oi_history[0]["oi"]
                average_previous_oi = previous_oi
            else:
                # 3つ以上ある場合は過去の平均を計算
                previous_oi_values = [item["oi"] for item in oi_history[:-1]]
                average_previous_oi = sum(previous_oi_values) / len(previous_oi_values)
            
            # 変化率を計算（ゼロ除算を防止）
            if average_previous_oi > 0:
                change_percent = ((latest_oi - average_previous_oi) / average_previous_oi) * 100
            else:
                change_percent = 0
                logger.warning(f"Previous OI for {symbol} is zero or negative: {average_previous_oi}")
            
            # 急減かどうかを判断
            is_rapid_decrease = change_percent <= threshold
            
            position_side = position.get("side", "Unknown")
            position_size = float(position.get("size", 0))
            
            return {
                "symbol": symbol,
                "has_position": True,
                "position_side": position_side,
                "position_size": position_size,
                "latest_oi": latest_oi,
                "average_previous_oi": average_previous_oi,
                "change_percent": change_percent,
                "threshold": threshold,
                "warning": is_rapid_decrease,
                "message": f"OI decreased by {abs(change_percent):.2f}% (threshold: {abs(threshold):.2f}%)" if is_rapid_decrease else "OI change is within normal range"
            }
            
        except Exception as e:
            logger.error(f"Error analyzing OI change for {symbol}: {e}")
            return {
                "symbol": symbol,
                "has_position": True,  # ポジション情報はすでに確認済み
                "warning": False,
                "error": str(e),
                "change_percent": 0.0,
                "threshold": threshold,
                "message": f"Error analyzing OI: {str(e)}"
            }
        

    def analyze_position_profit(self, symbol: str, threshold: float = 50.0) -> dict:
        """
        ポジションの利益率を分析し、閾値を超えた場合に半分クローズを提案する
        
        Args:
            symbol: 分析するシンボル
            threshold: 利益率の閾値（パーセント、例：50.0は50%の利益）
            
        Returns:
            分析結果を含む辞書
        """
        try:
            # 現在のポジションを確認
            position = self.get_position(symbol)
            if not position or position.get("side", "None") == "None" or float(position.get("size", 0)) <= 0:
                # ポジションがない場合はスキップ
                return {
                    "symbol": symbol,
                    "has_position": False,
                    "should_partial_close": False,
                    "message": "No active position"
                }
            
            # ポジション情報の取得
            position_side = position.get("side", "Unknown")
            position_size = float(position.get("size", 0))
            unrealised_pnl = float(position.get("unrealisedPnl", 0))
            position_value = float(position.get("positionValue", 0))
            avg_price = float(position.get("avgPrice", 0))
            
            # 利益率の計算
            if position_value > 0:
                profit_percent = (unrealised_pnl / position_value) * 100
            else:
                profit_percent = 0
                
            # 閾値を超えているかチェック
            should_partial_close = profit_percent >= threshold
            
            return {
                "symbol": symbol,
                "has_position": True,
                "position_side": position_side,
                "position_size": position_size,
                "unrealised_pnl": unrealised_pnl,
                "position_value": position_value,
                "avg_price": avg_price,
                "profit_percent": profit_percent,
                "threshold": threshold,
                "should_partial_close": should_partial_close,
                "message": f"Profit {profit_percent:.2f}% exceeds threshold {threshold:.2f}%, partial close recommended" if should_partial_close else f"Profit {profit_percent:.2f}% below threshold {threshold:.2f}%"
            }
            
        except Exception as e:
            logger.error(f"Error analyzing position profit for {symbol}: {e}")
            return {
                "symbol": symbol,
                "has_position": False,
                "should_partial_close": False,
                "error": str(e),
                "message": f"Error analyzing position profit: {str(e)}"
            }

    def partial_close_position(self, symbol: str, close_percent: float = 50.0) -> dict:
        """
        ポジションの一部をクローズする
        
        Args:
            symbol: 対象シンボル
            close_percent: クローズする割合（パーセント、例：50.0は50%をクローズ）
            
        Returns:
            クローズ結果を含む辞書
        """
        try:
            # 現在のポジション情報を取得
            position = self.get_position(symbol)
            if not position:
                logger.error(f"No position found for {symbol}")
                return {"success": False, "error": f"No position found for {symbol}"}
            
            # ポジションの方向とサイズを取得
            side = position.get("side", "")
            if not side or side == "None":
                logger.error(f"No active position for {symbol}")
                return {"success": False, "error": f"No active position for {symbol}"}
            
            position_size = float(position.get("size", 0))
            if position_size <= 0:
                logger.error(f"Invalid position size for {symbol}: {position_size}")
                return {"success": False, "error": f"Invalid position size: {position_size}"}
            
            # クローズする数量を計算
            close_qty = position_size * (close_percent / 100.0)
            
            # 反対方向の注文を出す
            close_side = "Sell" if side == "Buy" else "Buy"
            
            logger.info(f"Partially closing {close_percent:.2f}% of {side} position for {symbol} with qty {close_qty}")
            result = self.place_market_order(symbol, close_side, close_qty)
            
            # クローズ成功時、価格を取得して結果に追加
            if result.get("success", False):
                # 現在の価格を取得
                ticker_url = f"{self.base_url}/v5/market/tickers"
                ticker_params = {"category": "linear", "symbol": symbol}
                headers = {'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0'}
                
                ticker_response = requests.get(ticker_url, params=ticker_params, headers=headers, timeout=API_TIMEOUT)
                close_price = 0
                
                if ticker_response.status_code == 200:
                    ticker_data = ticker_response.json()
                    if ticker_data.get("retCode") == 0 and "result" in ticker_data and "list" in ticker_data["result"]:
                        for item in ticker_data["result"]["list"]:
                            if item["symbol"] == symbol:
                                close_price = float(item.get("lastPrice", 0))
                                break
                
                result["close_price"] = close_price
                result["close_percent"] = close_percent
                result["remaining_position"] = position_size - close_qty
            
            return result
            
        except Exception as e:
            logger.error(f"Error partially closing position for {symbol}: {e}")
            return {"success": False, "error": str(e)}

    def get_kline_data(self, symbol: str, interval: str = "5min", limit: int = 200) -> list:
        """
        指定されたシンボルの価格履歴データを取得する
        
        Args:
            symbol: シンボル名（例: BTCUSDT）
            interval: 時間間隔（1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d, 1w, 1M）
            limit: 取得するデータの最大数（最大200）
            
        Returns:
            ローソク足データのリスト
        """
        try:
            # インターバル名をBybit APIの形式に変換
            interval_map = {
                "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
                "1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
                "1d": "D", "1w": "W", "1M": "M"
            }
            
            api_interval = interval_map.get(interval, "5")
            
            url = f"{self.base_url}/v5/market/kline"
            params = {
                "category": "linear",
                "symbol": symbol,
                "interval": api_interval,
                "limit": min(limit, 200)  # 最大200件
            }
            
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0'
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=API_TIMEOUT)
            if response.status_code != 200:
                logger.error(f"Failed to get kline data for {symbol}: {response.text}")
                return []
                
            data = response.json()
            if data.get("retCode") != 0:
                logger.error(f"Error in Bybit kline data response: {data}")
                return []
            
            result = []
            if "result" in data and "list" in data["result"]:
                # APIレスポンスを解析し、使いやすい形式に変換
                # Bybit APIは新しいものから古いものの順にデータを返すので、反転させる
                klines = data["result"]["list"]
                for kline in reversed(klines):
                    try:
                        # [timestamp, open, high, low, close, volume, turnover]
                        timestamp, open_price, high, low, close, volume, turnover = kline
                        
                        result.append({
                            "timestamp": int(timestamp),
                            "open": float(open_price),
                            "high": float(high),
                            "low": float(low),
                            "close": float(close),
                            "volume": float(volume),
                            "turnover": float(turnover)
                        })
                    except (ValueError, IndexError) as e:
                        logger.error(f"Error parsing kline: {e}, data: {kline}")
                
            return result
        except Exception as e:
            logger.error(f"Error fetching kline data for {symbol}: {e}")
            return []

    def calculate_rsi(self, symbol: str, interval: str = "1h", period: int = 14) -> float:
        """
        RSI（相対力指数）を計算する
        
        Args:
            symbol: シンボル
            interval: 時間間隔
            period: RSIの計算期間
                
        Returns:
            RSI値 (0-100)
        """
        try:
            # ローソク足データを取得
            klines = self.get_kline_data(symbol, interval, limit=period+50)
            
            if len(klines) < period + 1:
                logger.warning(f"[RSI] {symbol}のRSI計算に必要なデータが不足しています({len(klines)}本)")
                return 50.0  # デフォルト値を返す
            
            # 終値のリストを抽出
            closes = [k["close"] for k in klines]
            
            # 価格変化を計算
            deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
            
            # 上昇/下降を分ける
            gains = [delta if delta > 0 else 0 for delta in deltas]
            losses = [-delta if delta < 0 else 0 for delta in deltas]
            
            # 初期の平均上昇/下降を計算
            avg_gain = sum(gains[:period]) / period
            avg_loss = sum(losses[:period]) / period
            
            # 残りのデータを処理
            for i in range(period, len(deltas)):
                avg_gain = (avg_gain * (period - 1) + gains[i]) / period
                avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            
            # RSIを計算
            if avg_loss == 0:
                return 100.0
            
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            
            logger.debug(f"[RSI] {symbol} {interval} RSI: {rsi:.2f}")
            return rsi
        except Exception as e:
            logger.error(f"[ERROR] {symbol}のRSI計算エラー: {e}")
            return 50.0  # デフォルト値を返す
    
    
# テスト用関数
def test_bybit_auto_trade():
    # ロギング設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    load_dotenv()
    client = BybitClient()
    symbol = "BTCUSDT"  # テスト用シンボル
    
    # API認証情報の確認
    api_key = os.getenv("BYBIT_API")
    api_secret = os.getenv("BYBIT_SECRET")
    logger.info(f"API KEY exists: {api_key is not None and api_key != ''}")
    logger.info(f"API SECRET exists: {api_secret is not None and api_secret != ''}")
    
    # 残高確認
    balance = client.get_wallet_balance("USDT")
    logger.info(f"Bybit USDT Balance: {balance}")
    
    if balance <= 0:
        logger.error("Insufficient balance for testing")
        return
    
    # 現在のポジション情報を取得
    position = client.get_position(symbol)
    logger.info(f"Current position: {position}")
    
    # OI分析のテスト
    oi_analysis = client.analyze_oi_change(symbol, threshold=-20.0)
    logger.info(f"OI Analysis: {oi_analysis}")
    
    # ポジションがある場合、ポジションクローズのテスト
    if position and position.get("side") != "None" and float(position.get("size", 0)) > 0:
        logger.info(f"Testing position close for {symbol}")
        
        # 確認用プロンプト
        proceed = input(f"Close the current position for {symbol}? (yes/no): ")
        if proceed.lower() == "yes":
            close_result = client.close_position(symbol)
            logger.info(f"Position close result: {close_result}")
        else:
            logger.info("Position close cancelled by user")
        
        return
    
    # 証拠金計算（残高の2%をテスト用に使用）
    margin = balance * 0.02
    logger.info(f"Using margin: {margin} USDT (2% of balance)")
    
    # シンボル情報取得
    symbol_info = client.get_symbol_info(symbol)
    logger.info(f"Symbol info: {symbol_info}")
    
    # レバレッジ設定
    leverage = 3
    result = client.set_leverage(symbol, leverage)
    logger.info(f"Set leverage result: {result}")
    
    # 現在の価格を取得
    market_data = client.get_market_data()
    target_symbol_data = next((data for data in market_data if data["symbol"] == symbol), None)
    
    if target_symbol_data:
        current_price = target_symbol_data["last_price"]
        logger.info(f"Current price of {symbol}: {current_price}")
        
        # 注文数量計算（少額でテスト）
        order_value = margin * leverage
        qty = order_value / current_price
        
        # シンボル情報に基づいて数量調整
        if symbol_info:
            qty_step = symbol_info['qtyStep']
            qty = math.floor(qty / qty_step) * qty_step
            
            # 最小注文量チェック
            if qty < symbol_info['minOrderQty']:
                logger.error(f"Order quantity {qty} is less than minimum allowed {symbol_info['minOrderQty']}")
                return
        
        logger.info(f"Order quantity: {qty}")
        
        # 確認用プロンプト
        proceed = input(f"Place a real BUY order for {qty} {symbol} with {leverage}x leverage? (yes/no): ")
        if proceed.lower() == "yes":
            # 実際に注文を出す
            order_result = client.place_market_order(symbol, "Buy", qty)
            logger.info(f"Order result: {order_result}")
        else:
            logger.info("Order cancelled by user")
    else:
        logger.error(f"Could not find market data for {symbol}")

if __name__ == "__main__":
    test_bybit_auto_trade()