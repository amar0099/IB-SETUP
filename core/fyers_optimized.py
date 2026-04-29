"""
core/fyers_optimized.py

Fyers API optimization layer: WebSocket + Smart Caching
Replaces REST polling with WebSocket streaming (99% fewer API calls)
"""

import json
import websocket
import threading
import time as time_mod
from datetime import datetime, timedelta
from typing import Optional, Callable, Dict

class FyersWebSocketManager:
    """Manages WebSocket connection for live quotes (0 API calls)"""
    
    def __init__(self, access_token: str, app_id: str):
        self.access_token = access_token
        self.app_id = app_id
        self.ws = None
        self.subscribed_symbols = set()
        self.quote_cache = {}
        self.callbacks = []
        self.connected = False
        self.ws_thread = None
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        
    def connect(self):
        """Establish WebSocket connection"""
        if self.ws is not None:
            return
        
        try:
            self.ws = websocket.WebSocketApp(
                "wss://api-feed.fyers.in/socket/stream",
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close
            )
            
            self.ws_thread = threading.Thread(
                target=self.ws.run_forever,
                kwargs={"ping_interval": 30, "ping_timeout": 10},
                daemon=True,
                name="FyersWebSocket"
            )
            self.ws_thread.start()
            time_mod.sleep(1)
            
        except Exception as e:
            print(f"[FYERS WS] ❌ Connection failed: {e}")
    
    def subscribe(self, symbols: list):
        """Subscribe to quotes (updates arrive automatically)"""
        new_symbols = set(symbols) - self.subscribed_symbols
        
        for symbol in new_symbols:
            try:
                message = {"T": "SUB", "S": symbol, "RT": "MKTDATA"}
                if self.ws and self.connected:
                    self.ws.send(json.dumps(message))
                    self.subscribed_symbols.add(symbol)
                    print(f"[FYERS WS] Subscribed: {symbol}")
            except Exception as e:
                print(f"[FYERS WS] Subscribe error: {e}")
    
    def _on_open(self, ws):
        self.connected = True
        self.reconnect_attempts = 0
        print("[FYERS WS] ✓ Connected")
    
    def _on_message(self, ws, message):
        """Receive quote updates - NO API CALL MADE"""
        try:
            data = json.loads(message)
            symbol = data.get("S")
            if symbol:
                self.quote_cache[symbol] = {
                    "ltp": data.get("LTP"),
                    "bid": data.get("BID"),
                    "ask": data.get("ASK"),
                    "volume": data.get("VOL"),
                    "iv": data.get("IV"),
                    "timestamp": data.get("TM"),
                }
                # Trigger callbacks for real-time updates
                for callback in self.callbacks:
                    try:
                        callback(symbol, self.quote_cache[symbol])
                    except:
                        pass
        except:
            pass
    
    def _on_error(self, ws, error):
        self.connected = False
        print(f"[FYERS WS] ❌ Error: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        self.connected = False
        print(f"[FYERS WS] Closed: {close_status_code}")
        self._attempt_reconnect()
    
    def _attempt_reconnect(self):
        """Attempt to reconnect after disconnection"""
        if self.reconnect_attempts < self.max_reconnect_attempts:
            self.reconnect_attempts += 1
            wait_time = min(2 ** self.reconnect_attempts, 60)
            print(f"[FYERS WS] ⏳ Reconnecting in {wait_time}s (attempt {self.reconnect_attempts})")
            threading.Timer(wait_time, self.connect).start()
    
    def get_quote(self, symbol: str) -> dict:
        """Get cached quote (NO API CALL)"""
        return self.quote_cache.get(symbol, {})
    
    def get_quotes(self, symbols: list) -> dict:
        """Get cached quotes for multiple symbols (NO API CALLS)"""
        return {symbol: self.get_quote(symbol) for symbol in symbols}
    
    def close(self):
        if self.ws:
            self.ws.close()


class SmartCache:
    """Caches API responses with TTL"""
    
    def __init__(self):
        self.cache = {}
        self.cache_time = {}
    
    def get_with_ttl(self, key: str, fetch_func, ttl_seconds: int = 3600):
        """Get cached value or fetch if expired"""
        now = datetime.now()
        
        if key in self.cache:
            cache_age = (now - self.cache_time[key]).total_seconds()
            if cache_age < ttl_seconds:
                return self.cache[key]
        
        try:
            result = fetch_func()
            self.cache[key] = result
            self.cache_time[key] = now
            return result
        except Exception as e:
            print(f"[CACHE] Error: {e}")
            if key in self.cache:
                return self.cache[key]
            raise
    
    def invalidate(self, key: str):
        """Clear cache for a key"""
        self.cache.pop(key, None)
        self.cache_time.pop(key, None)
    
    def clear_all(self):
        """Clear all caches"""
        self.cache.clear()
        self.cache_time.clear()
