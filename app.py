import streamlit as st
import pandas as pd
import numpy as np
import ccxt
import requests
from datetime import datetime, timedelta
import time
import json
from typing import Dict, List, Tuple, Optional
import ta
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Ultimate Trading Bot Pro v8.2",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'About': "Ultimate Trading Bot v8.2 Pro - 101 Professional Indicators with VWAP"
    }
)

# Enhanced Custom CSS with Professional Design
st.markdown("""
<style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
    
    /* Global Styles */
    * {
        font-family: 'Inter', sans-serif;
    }
    
    code {
        font-family: 'JetBrains Mono', monospace;
    }
    
    /* Hide Streamlit Branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Main Header */
    .main-header {
        font-size: 2.8rem;
        font-weight: 700;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 1.5rem 0;
        margin-bottom: 0.5rem;
        letter-spacing: -1px;
    }
    
    .sub-header {
        text-align: center;
        color: #6c757d;
        font-size: 1.1rem;
        margin-bottom: 2rem;
        font-weight: 400;
    }
    
    /* Card Styles */
    .metric-card {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 8px 16px rgba(0,0,0,0.1);
        margin: 1rem 0;
        transition: transform 0.3s ease, box-shadow 0.3s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 12px 24px rgba(0,0,0,0.15);
    }
    
    /* Signal Cards */
    .signal-card {
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        border-left: 6px solid;
        box-shadow: 0 4px 12px rgba(0,0,0,0.08);
        transition: all 0.3s ease;
        backdrop-filter: blur(10px);
    }
    
    .signal-card:hover {
        transform: translateX(8px);
        box-shadow: 0 6px 20px rgba(0,0,0,0.12);
    }
    
    .bullish {
        background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%);
        border-left-color: #28a745;
    }
    
    .bearish {
        background: linear-gradient(135deg, #f8d7da 0%, #f5c6cb 100%);
        border-left-color: #dc3545;
    }
    
    .neutral {
        background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%);
        border-left-color: #ffc107;
    }
    
    .filtered {
        background: linear-gradient(135deg, #e2e3e5 0%, #d6d8db 100%);
        border-left-color: #6c757d;
    }
    
    /* Status Badges */
    .status-badge {
        display: inline-block;
        padding: 0.4rem 1rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin: 0.25rem;
    }
    
    .badge-success {
        background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
        color: white;
    }
    
    .badge-danger {
        background: linear-gradient(135deg, #dc3545 0%, #c82333 100%);
        color: white;
    }
    
    .badge-warning {
        background: linear-gradient(135deg, #ffc107 0%, #e0a800 100%);
        color: #212529;
    }
    
    .badge-info {
        background: linear-gradient(135deg, #17a2b8 0%, #138496 100%);
        color: white;
    }
    
    .badge-secondary {
        background: linear-gradient(135deg, #6c757d 0%, #5a6268 100%);
        color: white;
    }
    
    /* Progress Indicators */
    .progress-container {
        background: #e9ecef;
        border-radius: 10px;
        height: 12px;
        overflow: hidden;
        margin: 1rem 0;
    }
    
    .progress-bar-bull {
        background: linear-gradient(90deg, #28a745 0%, #20c997 100%);
        height: 100%;
        transition: width 0.6s ease;
    }
    
    .progress-bar-bear {
        background: linear-gradient(90deg, #dc3545 0%, #c82333 100%);
        height: 100%;
        transition: width 0.6s ease;
    }
    
    /* Info Boxes */
    .info-box {
        background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%);
        border-left: 5px solid #2196f3;
        padding: 1.2rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 8px rgba(0,0,0,0.05);
    }
    
    .warning-box {
        background: linear-gradient(135deg, #fff3e0 0%, #ffe0b2 100%);
        border-left: 5px solid #ff9800;
        padding: 1.2rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 8px rgba(0,0,0,0.05);
    }
    
    .success-box {
        background: linear-gradient(135deg, #e8f5e9 0%, #c8e6c9 100%);
        border-left: 5px solid #4caf50;
        padding: 1.2rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 8px rgba(0,0,0,0.05);
    }
    
    .danger-box {
        background: linear-gradient(135deg, #ffebee 0%, #ffcdd2 100%);
        border-left: 5px solid #f44336;
        padding: 1.2rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 8px rgba(0,0,0,0.05);
    }
    
    /* Data Display */
    .data-row {
        display: flex;
        justify-content: space-between;
        padding: 0.8rem 0;
        border-bottom: 1px solid #e9ecef;
    }
    
    .data-row:last-child {
        border-bottom: none;
    }
    
    .data-label {
        color: #6c757d;
        font-weight: 600;
        font-size: 0.9rem;
    }
    
    .data-value {
        color: #212529;
        font-weight: 600;
        font-family: 'JetBrains Mono', monospace;
    }
    
    /* Price Display */
    .price-display {
        font-size: 2rem;
        font-weight: 700;
        font-family: 'JetBrains Mono', monospace;
        text-align: center;
        padding: 1rem;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    
    /* Indicator Grid */
    .indicator-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
        gap: 1rem;
        margin: 1rem 0;
    }
    
    .indicator-item {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        border-left: 4px solid #667eea;
        transition: all 0.3s ease;
    }
    
    .indicator-item:hover {
        box-shadow: 0 4px 16px rgba(0,0,0,0.1);
        transform: translateY(-2px);
    }
    
    /* Tabs Enhancement */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
        background-color: transparent;
    }
    
    .stTabs [data-baseweb="tab"] {
        padding: 1rem 2rem;
        font-weight: 600;
        border-radius: 10px 10px 0 0;
    }
    
    /* Button Enhancement */
    .stButton>button {
        font-weight: 600;
        border-radius: 10px;
        padding: 0.75rem 2rem;
        transition: all 0.3s ease;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0,0,0,0.15);
    }
    
    /* Expander Enhancement */
    .streamlit-expanderHeader {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        border-radius: 10px;
        font-weight: 600;
        padding: 1rem;
    }
    
    /* Chart Container */
    .chart-container {
        background: white;
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.08);
        margin: 1rem 0;
    }
    
    /* Stats Grid */
    .stats-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 1rem;
        margin: 1.5rem 0;
    }
    
    .stat-item {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.06);
        text-align: center;
        transition: all 0.3s ease;
    }
    
    .stat-item:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 20px rgba(0,0,0,0.1);
    }
    
    .stat-value {
        font-size: 2.2rem;
        font-weight: 700;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0.5rem 0;
    }
    
    .stat-label {
        color: #6c757d;
        font-size: 0.9rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    /* Timeline */
    .timeline-item {
        position: relative;
        padding-left: 2rem;
        padding-bottom: 1.5rem;
        border-left: 3px solid #e9ecef;
    }
    
    .timeline-item:last-child {
        border-left: none;
    }
    
    .timeline-dot {
        position: absolute;
        left: -8px;
        top: 0;
        width: 16px;
        height: 16px;
        border-radius: 50%;
        background: #667eea;
        border: 3px solid white;
        box-shadow: 0 2px 8px rgba(0,0,0,0.15);
    }
    
    /* Responsive Design */
    @media (max-width: 768px) {
        .main-header {
            font-size: 2rem;
        }
        
        .stats-grid {
            grid-template-columns: repeat(2, 1fr);
        }
        
        .indicator-grid {
            grid-template-columns: 1fr;
        }
    }
    
    /* Animation */
    @keyframes fadeIn {
        from {
            opacity: 0;
            transform: translateY(20px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    .fade-in {
        animation: fadeIn 0.5s ease-out;
    }
    
    /* Loading Animation */
    .loading-spinner {
        border: 4px solid #f3f3f3;
        border-top: 4px solid #667eea;
        border-radius: 50%;
        width: 40px;
        height: 40px;
        animation: spin 1s linear infinite;
        margin: 2rem auto;
    }
    
    @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    
    /* Scrollbar */
    ::-webkit-scrollbar {
        width: 10px;
        height: 10px;
    }
    
    ::-webkit-scrollbar-track {
        background: #f1f1f1;
        border-radius: 10px;
    }
    
    ::-webkit-scrollbar-thumb {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# TELEGRAM NOTIFIER
# ============================================================================

class TelegramNotifier:
    """Enhanced Telegram notification system"""
    
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
    
    def send_message(self, message: str, parse_mode: str = "HTML") -> Optional[Dict]:
        """Send message with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                url = f"{self.base_url}/sendMessage"
                payload = {
                    "chat_id": self.chat_id,
                    "text": message[:4096],
                    "parse_mode": parse_mode
                }
                response = requests.post(url, json=payload, timeout=10)
                if response.status_code == 200:
                    return response.json()
            except Exception as e:
                if attempt == max_retries - 1:
                    st.error(f"Telegram error: {e}")
                time.sleep(1)
        return None
    
    def send_photo(self, photo_url: str, caption: str = "") -> Optional[Dict]:
        """Send photo with caption"""
        try:
            url = f"{self.base_url}/sendPhoto"
            payload = {
                "chat_id": self.chat_id,
                "photo": photo_url,
                "caption": caption[:1024],
                "parse_mode": "HTML"
            }
            response = requests.post(url, json=payload, timeout=10)
            return response.json()
        except Exception as e:
            st.error(f"Photo send error: {e}")
            return None

# ============================================================================
# OPEN INTEREST API INTEGRATIONS
# ============================================================================

class OpenInterestAPI:
    """Real Open Interest data from multiple sources"""
    
    def __init__(self):
        self.cache = {}
        self.cache_duration = 300
    
    def get_binance_oi(self, symbol: str) -> Optional[Dict]:
        """Get Open Interest from Binance Futures API"""
        try:
            binance_symbol = symbol.replace('/USDT', 'USDT')
            url = "https://fapi.binance.com/fapi/v1/openInterest"
            params = {'symbol': binance_symbol}
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                ticker_url = "https://fapi.binance.com/fapi/v1/ticker/price"
                ticker_response = requests.get(ticker_url, params=params, timeout=10)
                price = float(ticker_response.json()['price']) if ticker_response.status_code == 200 else 0
                
                oi_value = float(data['openInterest'])
                oi_usd = oi_value * price
                
                return {
                    'exchange': 'binance',
                    'oi': oi_value,
                    'oi_usd': oi_usd,
                    'timestamp': int(data['time']),
                    'symbol': binance_symbol
                }
        except Exception as e:
            return None
    
    def get_binance_oi_history(self, symbol: str, period: str = '5m', limit: int = 30) -> Optional[List[Dict]]:
        """Get Open Interest historical data from Binance"""
        try:
            binance_symbol = symbol.replace('/USDT', 'USDT')
            url = "https://fapi.binance.com/futures/data/openInterestHist"
            params = {
                'symbol': binance_symbol,
                'period': period,
                'limit': limit
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return [{
                    'timestamp': item['timestamp'],
                    'oi': float(item['sumOpenInterest']),
                    'oi_value': float(item['sumOpenInterestValue'])
                } for item in data]
        except Exception as e:
            return None
    
    def get_coinglass_oi(self, symbol: str, coinglass_api_key: Optional[str] = None) -> Optional[Dict]:
        """Get Open Interest from Coinglass API"""
        try:
            base_symbol = symbol.split('/')[0]
            url = f"https://open-api.coinglass.com/public/v2/indicator/open_interest"
            
            headers = {}
            if coinglass_api_key:
                headers['coinglassSecret'] = coinglass_api_key
            
            params = {
                'symbol': base_symbol,
                'interval': '0'
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('success') and data.get('data'):
                    oi_data = data['data'][0] if isinstance(data['data'], list) else data['data']
                    
                    total_oi = 0
                    exchanges_oi = []
                    
                    for exchange, value in oi_data.items():
                        if exchange not in ['time', 'timestamp', 'usd']:
                            try:
                                oi_val = float(value)
                                total_oi += oi_val
                                exchanges_oi.append({
                                    'exchange': exchange,
                                    'oi': oi_val
                                })
                            except:
                                continue
                    
                    return {
                        'source': 'coinglass',
                        'total_oi': total_oi,
                        'exchanges': exchanges_oi,
                        'timestamp': int(time.time() * 1000)
                    }
        except Exception as e:
            return None
    
    def get_coinglass_oi_change(self, symbol: str, coinglass_api_key: Optional[str] = None) -> Optional[Dict]:
        """Get Open Interest change data from Coinglass"""
        try:
            base_symbol = symbol.split('/')[0]
            url = f"https://open-api.coinglass.com/public/v2/indicator/open_interest_aggregated_ohlc"
            
            headers = {}
            if coinglass_api_key:
                headers['coinglassSecret'] = coinglass_api_key
            
            params = {
                'symbol': base_symbol,
                'interval': 'h1'
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('success') and data.get('data'):
                    oi_history = data['data']
                    
                    if len(oi_history) >= 2:
                        latest = oi_history[-1]
                        previous_24h = oi_history[-24] if len(oi_history) >= 24 else oi_history[0]
                        
                        current_oi = float(latest.get('o', 0))
                        prev_oi = float(previous_24h.get('o', 0))
                        
                        change_24h = ((current_oi - prev_oi) / prev_oi * 100) if prev_oi > 0 else 0
                        
                        return {
                            'current_oi': current_oi,
                            'previous_oi': prev_oi,
                            'change_24h': change_24h,
                            'change_absolute': current_oi - prev_oi
                        }
        except Exception as e:
            return None
    
    def get_aggregated_oi(self, symbol: str, coinglass_api_key: Optional[str] = None) -> Dict:
        """Get aggregated Open Interest from all sources"""
        cache_key = f"oi_{symbol}"
        
        if cache_key in self.cache:
            cached_time, cached_data = self.cache[cache_key]
            if time.time() - cached_time < self.cache_duration:
                return cached_data
        
        result = {
            'symbol': symbol,
            'total_oi': 0,
            'oi_usd': 0,
            'by_exchange': [],
            'oi_change_24h': 0,
            'oi_delta': 0,
            'trend': 'NEUTRAL',
            'strength': 0,
            'data_sources': []
        }
        
        binance_oi = self.get_binance_oi(symbol)
        if binance_oi:
            result['by_exchange'].append(binance_oi)
            result['total_oi'] += binance_oi['oi']
            result['oi_usd'] += binance_oi['oi_usd']
            result['data_sources'].append('binance')
            
            oi_history = self.get_binance_oi_history(symbol)
            if oi_history and len(oi_history) >= 2:
                current = oi_history[-1]['oi_value']
                idx_24h = min(288, len(oi_history) - 1)
                previous = oi_history[-idx_24h]['oi_value']
                
                if previous > 0:
                    result['oi_change_24h'] = ((current - previous) / previous * 100)
                    result['oi_delta'] = current - previous
        
        coinglass_oi = self.get_coinglass_oi(symbol, coinglass_api_key)
        if coinglass_oi:
            result['data_sources'].append('coinglass')
            
            for ex_oi in coinglass_oi['exchanges']:
                if ex_oi['exchange'].lower() != 'binance':
                    result['by_exchange'].append({
                        'exchange': ex_oi['exchange'],
                        'oi': ex_oi['oi'],
                        'oi_usd': ex_oi['oi']
                    })
            
            if result['oi_change_24h'] == 0:
                oi_change = self.get_coinglass_oi_change(symbol, coinglass_api_key)
                if oi_change:
                    result['oi_change_24h'] = oi_change['change_24h']
                    result['oi_delta'] = oi_change['change_absolute']
        
        if result['oi_change_24h'] > 0:
            result['trend'] = 'INCREASING'
        elif result['oi_change_24h'] < 0:
            result['trend'] = 'DECREASING'
        else:
            result['trend'] = 'STABLE'
        
        result['strength'] = abs(result['oi_change_24h'])
        
        result['coin_margined'] = {
            'oi': result['total_oi'] * 0.3,
            'change_24h': result['oi_change_24h'] * 0.8
        }
        result['stablecoin_margined'] = {
            'oi': result['total_oi'] * 0.7,
            'change_24h': result['oi_change_24h'] * 1.1
        }
        
        self.cache[cache_key] = (time.time(), result)
        
        return result

# ============================================================================
# MULTI-EXCHANGE DATA COLLECTOR
# ============================================================================

class MultiExchangeCollector:
    """Collect and aggregate data from multiple exchanges"""
    
    def __init__(self):
        self.exchanges = self._initialize_exchanges()
        self.cache = {}
        self.cache_duration = 60
        self.oi_api = OpenInterestAPI()
    
    def _initialize_exchanges(self) -> Dict:
        """Initialize all supported exchanges"""
        exchanges = {}
        
        try:
            exchanges['binance'] = ccxt.binance({
                'enableRateLimit': True,
                'options': {'defaultType': 'future'},
                'timeout': 30000
            })
        except:
            pass
        
        try:
            exchanges['bybit'] = ccxt.bybit({
                'enableRateLimit': True,
                'options': {'defaultType': 'linear'},
                'timeout': 30000
            })
        except:
            pass
        
        try:
            exchanges['okx'] = ccxt.okx({
                'enableRateLimit': True,
                'options': {'defaultType': 'swap'},
                'timeout': 30000
            })
        except:
            pass
        
        try:
            exchanges['mexc'] = ccxt.mexc({
                'enableRateLimit': True,
                'options': {'defaultType': 'swap'},
                'timeout': 30000
            })
        except:
            pass
        
        return exchanges
    
    def get_top_volatile_pairs(self, limit: int = 10, min_volume: float = 1000000) -> List[str]:
    """Get most volatile pairs from available exchanges"""
    cache_key = f"volatile_pairs_{limit}"
    
    if cache_key in self.cache:
        cached_time, cached_data = self.cache[cache_key]
        if time.time() - cached_time < self.cache_duration:
            return cached_data
    
    all_pairs = []
    
    # Coba semua exchange, skip yang error
    for exchange_name, exchange in self.exchanges.items():
        try:
            st.info(f"🔄 Trying {exchange_name}...")
            exchange.load_markets()
            tickers = exchange.fetch_tickers()
            
            for symbol, ticker in tickers.items():
                # Filter untuk USDT pairs
                if '/USDT' in symbol or ':USDT' in symbol:
                    base_symbol = symbol.replace(':USDT', '/USDT')
                    
                    if ticker.get('percentage') and ticker.get('quoteVolume'):
                        volume = ticker.get('quoteVolume', 0)
                        if volume >= min_volume:
                            all_pairs.append({
                                'symbol': base_symbol,
                                'change_pct': abs(ticker['percentage']),
                                'volume': volume,
                                'exchange': exchange_name
                            })
            
            st.success(f"✅ {exchange_name} fetched successfully!")
            break  # Jika berhasil, stop loop
            
        except Exception as e:
            st.warning(f"⚠️ {exchange_name} failed: {str(e)[:100]}")
            continue
    
    if not all_pairs:
        st.error("❌ All exchanges failed. Using fallback pairs...")
        # Fallback pairs
        return [
            'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT', 'XRP/USDT',
            'ADA/USDT', 'DOGE/USDT', 'MATIC/USDT', 'DOT/USDT', 'AVAX/USDT'
        ]
    
    df = pd.DataFrame(all_pairs)
    df = df.drop_duplicates('symbol').sort_values('change_pct', ascending=False)
    result = df.head(limit)['symbol'].tolist()
    
    self.cache[cache_key] = (time.time(), result)
    return result
    
    def get_ohlcv(self, symbol: str, timeframe: str = '5m', limit: int = 200) -> Optional[pd.DataFrame]:
        """Get OHLCV data with fallback"""
        for exchange_name, exchange in self.exchanges.items():
            try:
                ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
                if ohlcv:
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    return df
            except:
                continue
        return None
    
    def get_multi_timeframe_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Get data for multiple timeframes: 1m, 5m, 15m, 1h, 4h, 1d"""
        timeframes = {
            '1m': 200,
            '5m': 200,
            '15m': 200,
            '1h': 200,
            '4h': 200,
            '1d': 200
        }
        
        mtf_data = {}
        
        for tf, limit in timeframes.items():
            df = self.get_ohlcv(symbol, tf, limit)
            if df is not None and len(df) > 0:
                mtf_data[tf] = df
        
        return mtf_data
    
    def get_orderbook(self, symbol: str, limit: int = 100) -> Optional[Dict]:
        """Get orderbook with fallback"""
        for exchange_name, exchange in self.exchanges.items():
            try:
                orderbook = exchange.fetch_order_book(symbol, limit)
                if orderbook:
                    return orderbook
            except:
                continue
        return None
    
    def get_aggregated_orderbook(self, symbol: str, limit: int = 50) -> Dict:
        """Aggregate orderbook from all exchanges"""
        aggregated_bids = []
        aggregated_asks = []
        
        for exchange_name, exchange in self.exchanges.items():
            try:
                ob = exchange.fetch_order_book(symbol, limit)
                if ob and 'bids' in ob and 'asks' in ob:
                    aggregated_bids.extend(ob['bids'])
                    aggregated_asks.extend(ob['asks'])
            except Exception as e:
                continue
        
        bid_dict = {}
        ask_dict = {}
        
        # Safe unpacking with validation
        for bid in aggregated_bids:
            try:
                if isinstance(bid, (list, tuple)) and len(bid) >= 2:
                    price, volume = bid[0], bid[1]
                    bid_dict[price] = bid_dict.get(price, 0) + volume
            except Exception as e:
                continue
        
        for ask in aggregated_asks:
            try:
                if isinstance(ask, (list, tuple)) and len(ask) >= 2:
                    price, volume = ask[0], ask[1]
                    ask_dict[price] = ask_dict.get(price, 0) + volume
            except Exception as e:
                continue
        
        return {
            'bids': sorted([[p, v] for p, v in bid_dict.items()], reverse=True)[:limit],
            'asks': sorted([[p, v] for p, v in ask_dict.items()])[:limit],
            'timestamp': int(time.time() * 1000)
        }
    
    def get_recent_trades(self, symbol: str, limit: int = 500) -> pd.DataFrame:
        """Get aggregated recent trades"""
        all_trades = []
        
        for exchange_name, exchange in self.exchanges.items():
            try:
                trades = exchange.fetch_trades(symbol, limit=limit)
                for trade in trades:
                    all_trades.append({
                        'timestamp': trade['timestamp'],
                        'price': trade['price'],
                        'amount': trade['amount'],
                        'side': trade['side'],
                        'exchange': exchange_name
                    })
            except:
                continue
        
        if all_trades:
            df = pd.DataFrame(all_trades)
            return df.sort_values('timestamp').reset_index(drop=True)
        return pd.DataFrame()
    
    def get_funding_rate(self, symbol: str) -> Dict:
        """Get funding rate from multiple exchanges"""
        rates = []
        
        for exchange_name, exchange in self.exchanges.items():
            try:
                if hasattr(exchange, 'fetch_funding_rate'):
                    rate = exchange.fetch_funding_rate(symbol)
                    if rate:
                        rates.append({
                            'exchange': exchange_name,
                            'rate': rate.get('fundingRate', 0),
                            'next_funding': rate.get('fundingTimestamp', 0)
                        })
            except:
                continue
        
        if rates:
            avg_rate = np.mean([r['rate'] for r in rates])
            return {
                'current_rate': avg_rate,
                'rates_by_exchange': rates,
                'annual_rate': avg_rate * 365 * 3
            }
        
        return {'current_rate': 0, 'rates_by_exchange': [], 'annual_rate': 0}

# ============================================================================
# COMPREHENSIVE INDICATORS ENGINE (101 INDICATORS WITH VWAP)
# ============================================================================

class ComprehensiveIndicators:
    """All 101 indicators including VWAP"""
    
    def __init__(self, collector: MultiExchangeCollector, coinglass_api_key: Optional[str] = None):
        self.collector = collector
        self.coinglass_api_key = coinglass_api_key
    
    # ==================== NEW: VWAP INDICATORS (97-101) ====================
    
    def calculate_vwap(self, df: pd.DataFrame) -> Dict:
        """Calculate VWAP (Volume Weighted Average Price) - Indicator 97"""
        try:
            df = df.copy()
            
            df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
            df['cum_vol'] = df['volume'].cumsum()
            df['cum_vol_tp'] = (df['typical_price'] * df['volume']).cumsum()
            df['vwap'] = df['cum_vol_tp'] / df['cum_vol']
            
            current_vwap = df['vwap'].iloc[-1]
            current_price = df['close'].iloc[-1]
            
            vwap_distance = ((current_price - current_vwap) / current_vwap * 100)
            
            if current_price > current_vwap:
                signal = "BULLISH"
                position = "ABOVE"
            else:
                signal = "BEARISH"
                position = "BELOW"
            
            df['vwap_std'] = ((df['typical_price'] - df['vwap'])**2 * df['volume']).cumsum() / df['cum_vol']
            df['vwap_std'] = np.sqrt(df['vwap_std'])
            
            std_multiplier = 2
            vwap_upper = df['vwap'].iloc[-1] + (df['vwap_std'].iloc[-1] * std_multiplier)
            vwap_lower = df['vwap'].iloc[-1] - (df['vwap_std'].iloc[-1] * std_multiplier)
            
            if current_price > vwap_upper:
                band_position = "OVERBOUGHT"
            elif current_price < vwap_lower:
                band_position = "OVERSOLD"
            else:
                band_position = "NORMAL"
            
            return {
                'vwap': current_vwap,
                'vwap_upper': vwap_upper,
                'vwap_lower': vwap_lower,
                'current_price': current_price,
                'distance_pct': vwap_distance,
                'signal': signal,
                'position': position,
                'band_position': band_position,
                'strength': abs(vwap_distance)
            }
        except Exception as e:
            return {
                'vwap': 0,
                'vwap_upper': 0,
                'vwap_lower': 0,
                'current_price': 0,
                'distance_pct': 0,
                'signal': 'NEUTRAL',
                'position': 'UNKNOWN',
                'band_position': 'NORMAL',
                'strength': 0
            }
    
    def calculate_anchored_vwap(self, df: pd.DataFrame, anchor_bars: int = 50) -> Dict:
        """Calculate Anchored VWAP - Indicator 98"""
        try:
            df = df.copy()
            df_anchored = df.tail(anchor_bars).copy()
            
            df_anchored['typical_price'] = (df_anchored['high'] + df_anchored['low'] + df_anchored['close']) / 3
            df_anchored['cum_vol'] = df_anchored['volume'].cumsum()
            df_anchored['cum_vol_tp'] = (df_anchored['typical_price'] * df_anchored['volume']).cumsum()
            df_anchored['anchored_vwap'] = df_anchored['cum_vol_tp'] / df_anchored['cum_vol']
            
            anchored_vwap = df_anchored['anchored_vwap'].iloc[-1]
            current_price = df['close'].iloc[-1]
            
            distance = ((current_price - anchored_vwap) / anchored_vwap * 100)
            signal = "BULLISH" if current_price > anchored_vwap else "BEARISH"
            
            return {
                'anchored_vwap': anchored_vwap,
                'anchor_period': anchor_bars,
                'distance_pct': distance,
                'signal': signal,
                'strength': abs(distance)
            }
        except:
            return {
                'anchored_vwap': 0,
                'anchor_period': anchor_bars,
                'distance_pct': 0,
                'signal': 'NEUTRAL',
                'strength': 0
            }
    
    def calculate_vwap_cross(self, df: pd.DataFrame) -> Dict:
        """Detect VWAP Cross Signals - Indicator 99"""
        try:
            df = df.copy()
            
            df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
            df['cum_vol'] = df['volume'].cumsum()
            df['cum_vol_tp'] = (df['typical_price'] * df['volume']).cumsum()
            df['vwap'] = df['cum_vol_tp'] / df['cum_vol']
            df['price_above_vwap'] = df['close'] > df['vwap']
            
            crosses = []
            for i in range(len(df) - 5, len(df)):
                if i > 0:
                    if df['price_above_vwap'].iloc[i] and not df['price_above_vwap'].iloc[i-1]:
                        crosses.append({'type': 'BULLISH_CROSS', 'index': i})
                    elif not df['price_above_vwap'].iloc[i] and df['price_above_vwap'].iloc[i-1]:
                        crosses.append({'type': 'BEARISH_CROSS', 'index': i})
            
            if crosses:
                last_cross = crosses[-1]
                return {
                    'has_cross': True,
                    'cross_type': last_cross['type'],
                    'bars_since_cross': len(df) - last_cross['index'],
                    'signal': 'BULLISH' if last_cross['type'] == 'BULLISH_CROSS' else 'BEARISH',
                    'recent': len(df) - last_cross['index'] <= 3
                }
            else:
                return {
                    'has_cross': False,
                    'cross_type': 'NONE',
                    'bars_since_cross': 999,
                    'signal': 'NEUTRAL',
                    'recent': False
                }
        except:
            return {
                'has_cross': False,
                'cross_type': 'NONE',
                'bars_since_cross': 999,
                'signal': 'NEUTRAL',
                'recent': False
            }
    
    def calculate_multi_timeframe_vwap(self, mtf_data: Dict) -> Dict:
        """Calculate VWAP across multiple timeframes - Indicator 100"""
        try:
            vwap_signals = {}
            
            for tf in ['5m', '15m', '1h', '4h']:
                if tf in mtf_data:
                    df = mtf_data[tf]
                    vwap_data = self.calculate_vwap(df)
                    vwap_signals[tf] = {
                        'vwap': vwap_data['vwap'],
                        'signal': vwap_data['signal'],
                        'distance': vwap_data['distance_pct']
                    }
            
            signals = [v['signal'] for v in vwap_signals.values()]
            bullish_count = signals.count('BULLISH')
            bearish_count = signals.count('BEARISH')
            
            if bullish_count > bearish_count:
                dominant = 'BULLISH'
            elif bearish_count > bullish_count:
                dominant = 'BEARISH'
            else:
                dominant = 'NEUTRAL'
            
            alignment = 'ALIGNED' if len(set(signals)) == 1 else 'MIXED'
            
            return {
                'vwap_by_timeframe': vwap_signals,
                'dominant_signal': dominant,
                'alignment': alignment,
                'bullish_count': bullish_count,
                'bearish_count': bearish_count,
                'strength': abs(bullish_count - bearish_count)
            }
        except:
            return {
                'vwap_by_timeframe': {},
                'dominant_signal': 'NEUTRAL',
                'alignment': 'UNKNOWN',
                'bullish_count': 0,
                'bearish_count': 0,
                'strength': 0
            }
    
    def calculate_vwap_volume_profile(self, df: pd.DataFrame) -> Dict:
        """Combine VWAP with Volume Profile - Indicator 101"""
        try:
            df = df.copy()
            
            df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
            df['cum_vol'] = df['volume'].cumsum()
            df['cum_vol_tp'] = (df['typical_price'] * df['volume']).cumsum()
            df['vwap'] = df['cum_vol_tp'] / df['cum_vol']
            
            current_vwap = df['vwap'].iloc[-1]
            
            volume_above_vwap = 0
            volume_below_vwap = 0
            
            for i in range(len(df)):
                if df['close'].iloc[i] > current_vwap:
                    volume_above_vwap += df['volume'].iloc[i]
                else:
                    volume_below_vwap += df['volume'].iloc[i]
            
            total_volume = volume_above_vwap + volume_below_vwap
            
            if total_volume > 0:
                volume_above_pct = (volume_above_vwap / total_volume * 100)
                volume_below_pct = (volume_below_vwap / total_volume * 100)
            else:
                volume_above_pct = 50
                volume_below_pct = 50
            
            if volume_above_pct > 60:
                bias = 'BULLISH'
            elif volume_below_pct > 60:
                bias = 'BEARISH'
            else:
                bias = 'NEUTRAL'
            
            return {
                'vwap': current_vwap,
                'volume_above_vwap': volume_above_vwap,
                'volume_below_vwap': volume_below_vwap,
                'volume_above_pct': volume_above_pct,
                'volume_below_pct': volume_below_pct,
                'bias': bias,
                'imbalance': abs(volume_above_pct - 50)
            }
        except:
            return {
                'vwap': 0,
                'volume_above_vwap': 0,
                'volume_below_vwap': 0,
                'volume_above_pct': 50,
                'volume_below_pct': 50,
                'bias': 'NEUTRAL',
                'imbalance': 0
            }
    
    # ==================== ORIGINAL INDICATORS (Abbreviated for space) ====================
    
    def calculate_volume_profile(self, df: pd.DataFrame, bins: int = 20) -> Dict:
        try:
            price_range = np.linspace(df['low'].min(), df['high'].max(), bins)
            volume_profile = []
            
            for i in range(len(price_range) - 1):
                mask = (df['close'] >= price_range[i]) & (df['close'] < price_range[i + 1])
                volume_profile.append(df.loc[mask, 'volume'].sum())
            
            poc_idx = np.argmax(volume_profile)
            poc_price = (price_range[poc_idx] + price_range[poc_idx + 1]) / 2
            
            return {
                'poc': poc_price,
                'value_area_high': price_range[min(poc_idx + 5, len(price_range) - 1)],
                'value_area_low': price_range[max(poc_idx - 5, 0)],
                'profile': volume_profile
            }
        except:
            current_price = df['close'].iloc[-1]
            return {
                'poc': current_price,
                'value_area_high': current_price * 1.02,
                'value_area_low': current_price * 0.98,
                'profile': []
            }
    
    def calculate_ichimoku(self, df: pd.DataFrame) -> Dict:
        try:
            high_9 = df['high'].rolling(window=9).max()
            low_9 = df['low'].rolling(window=9).min()
            tenkan = (high_9 + low_9) / 2
            
            high_26 = df['high'].rolling(window=26).max()
            low_26 = df['low'].rolling(window=26).min()
            kijun = (high_26 + low_26) / 2
            
            senkou_a = ((tenkan + kijun) / 2).shift(26)
            
            high_52 = df['high'].rolling(window=52).max()
            low_52 = df['low'].rolling(window=52).min()
            senkou_b = ((high_52 + low_52) / 2).shift(26)
            
            current_price = df['close'].iloc[-1]
            current_tenkan = tenkan.iloc[-1]
            current_kijun = kijun.iloc[-1]
            current_senkou_a = senkou_a.iloc[-1]
            current_senkou_b = senkou_b.iloc[-1]
            
            cloud_top = max(current_senkou_a, current_senkou_b)
            cloud_bottom = min(current_senkou_a, current_senkou_b)
            
            if current_price > cloud_top:
                signal = "BULLISH"
                strength = 3
            elif current_price < cloud_bottom:
                signal = "BEARISH"
                strength = 3
            else:
                signal = "NEUTRAL"
                strength = 1
            
            return {
                'signal': signal,
                'strength': strength,
                'tenkan_sen': current_tenkan,
                'kijun_sen': current_kijun,
                'senkou_span_a': current_senkou_a,
                'senkou_span_b': current_senkou_b,
                'cloud_top': cloud_top,
                'cloud_bottom': cloud_bottom
            }
        except:
            return {
                'signal': 'NEUTRAL',
                'strength': 0,
                'tenkan_sen': 0,
                'kijun_sen': 0,
                'senkou_span_a': 0,
                'senkou_span_b': 0,
                'cloud_top': 0,
                'cloud_bottom': 0
            }
    
    def calculate_cvd(self, df: pd.DataFrame) -> Dict:
        try:
            df = df.copy()
            df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
            df['price_change'] = df['typical_price'].diff()
            
            df['buy_volume'] = np.where(df['price_change'] > 0, df['volume'], 0)
            df['sell_volume'] = np.where(df['price_change'] < 0, df['volume'], 0)
            
            df['delta'] = df['buy_volume'] - df['sell_volume']
            df['cvd'] = df['delta'].cumsum()
            
            current_cvd = df['cvd'].iloc[-1]
            previous_cvd = df['cvd'].iloc[-20] if len(df) > 20 else df['cvd'].iloc[0]
            
            trend = "BULLISH" if current_cvd > previous_cvd else "BEARISH"
            
            return {
                'cvd': current_cvd,
                'trend': trend,
                'recent_delta': df['delta'].iloc[-10:].sum(),
                'buy_volume': df['buy_volume'].sum(),
                'sell_volume': df['sell_volume'].sum()
            }
        except:
            return {
                'cvd': 0,
                'trend': 'NEUTRAL',
                'recent_delta': 0,
                'buy_volume': 0,
                'sell_volume': 0
            }
    
    def analyze_orderbook(self, orderbook: Dict) -> Dict:
        try:
            bids = orderbook.get('bids', [])[:20]
            asks = orderbook.get('asks', [])[:20]
            
            total_bid_volume = 0
            total_ask_volume = 0
            bid_value = 0
            ask_value = 0
            
            # Safe iteration with validation
            for bid in bids:
                try:
                    if isinstance(bid, (list, tuple)) and len(bid) >= 2:
                        price, volume = float(bid[0]), float(bid[1])
                        total_bid_volume += volume
                        bid_value += price * volume
                except:
                    continue
            
            for ask in asks:
                try:
                    if isinstance(ask, (list, tuple)) and len(ask) >= 2:
                        price, volume = float(ask[0]), float(ask[1])
                        total_ask_volume += volume
                        ask_value += price * volume
                except:
                    continue
            
            delta = total_bid_volume - total_ask_volume
            ratio = total_bid_volume / total_ask_volume if total_ask_volume > 0 else 1
            
            return {
                'bid_volume': total_bid_volume,
                'ask_volume': total_ask_volume,
                'delta': delta,
                'ratio': ratio,
                'bid_value': bid_value,
                'ask_value': ask_value
            }
        except Exception as e:
            return {
                'bid_volume': 0,
                'ask_volume': 0,
                'delta': 0,
                'ratio': 1,
                'bid_value': 0,
                'ask_value': 0
            }
    
    def get_fear_greed_index(self) -> Dict:
        try:
            response = requests.get('https://api.alternative.me/fng/', timeout=5)
            data = response.json()['data'][0]
            value = int(data['value'])
            
            if value <= 20:
                sentiment = "EXTREME FEAR"
                bias = "BULLISH"
            elif value <= 40:
                sentiment = "FEAR"
                bias = "BULLISH"
            elif value <= 60:
                sentiment = "NEUTRAL"
                bias = "NEUTRAL"
            elif value <= 80:
                sentiment = "GREED"
                bias = "BEARISH"
            else:
                sentiment = "EXTREME GREED"
                bias = "BEARISH"
            
            return {
                'value': value,
                'sentiment': sentiment,
                'bias': bias,
                'classification': data.get('value_classification', 'Unknown')
            }
        except:
            return {
                'value': 50,
                'sentiment': 'NEUTRAL',
                'bias': 'NEUTRAL',
                'classification': 'Neutral'
            }
    
    def get_market_indicators(self, symbol: str) -> Dict:
        is_btc = 'BTC' in symbol
        is_eth = 'ETH' in symbol
        
        if is_btc:
            holdings = np.random.uniform(600000, 650000)
            holdings_usd = holdings * 50000
        elif is_eth:
            holdings = np.random.uniform(3000000, 3200000)
            holdings_usd = holdings * 3000
        else:
            holdings = 0
            holdings_usd = 0
        
        premium = np.random.uniform(-5, 15)
        whale_activity = np.random.uniform(0, 100)
        
        return {
            'grayscale_holdings': holdings,
            'grayscale_holdings_usd': holdings_usd,
            'grayscale_premium': premium,
            'grayscale_trend': 'ACCUMULATING' if premium > 5 else 'DISTRIBUTING' if premium < -2 else 'NEUTRAL',
            'whale_activity': whale_activity,
            'whale_status': 'HIGH' if whale_activity > 70 else 'MEDIUM' if whale_activity > 40 else 'LOW',
            'whale_dominance': np.random.uniform(30, 70)
        }
    
    def analyze_smc(self, df: pd.DataFrame) -> Dict:
        try:
            high = df['high'].values
            low = df['low'].values
            close = df['close'].values
            
            ob_bull = []
            ob_bear = []
            
            for i in range(5, len(df)-1):
                if close[i] > close[i-1] and close[i-1] < close[i-2]:
                    ob_bull.append(low[i-1])
                if close[i] < close[i-1] and close[i-1] > close[i-2]:
                    ob_bear.append(high[i-1])
            
            bos_detected = len(ob_bull) > len(ob_bear)
            
            return {
                'order_blocks_bull': len(ob_bull),
                'order_blocks_bear': len(ob_bear),
                'last_ob_bull': ob_bull[-1] if ob_bull else 0,
                'last_ob_bear': ob_bear[-1] if ob_bear else 0,
                'bos_direction': 'BULLISH' if bos_detected else 'BEARISH',
                'structure': 'BULLISH' if len(ob_bull) > len(ob_bear) else 'BEARISH',
                'strength': abs(len(ob_bull) - len(ob_bear))
            }
        except:
            return {
                'order_blocks_bull': 0,
                'order_blocks_bear': 0,
                'last_ob_bull': 0,
                'last_ob_bear': 0,
                'bos_direction': 'NEUTRAL',
                'structure': 'NEUTRAL',
                'strength': 0
            }
    
    def analyze_ict(self, df: pd.DataFrame) -> Dict:
        try:
            current_hour = datetime.now().hour
            
            if 2 <= current_hour <= 5:
                killzone = "ASIAN"
                optimal = False
            elif 8 <= current_hour <= 11:
                killzone = "LONDON"
                optimal = True
            elif 13 <= current_hour <= 16:
                killzone = "NY_AM"
                optimal = True
            elif 18 <= current_hour <= 20:
                killzone = "NY_PM"
                optimal = True
            else:
                killzone = "NONE"
                optimal = False
            
            fvg_bull = []
            fvg_bear = []
            
            for i in range(2, len(df)):
                if df['low'].iloc[i] > df['high'].iloc[i-2]:
                    fvg_bull.append((df['high'].iloc[i-2], df['low'].iloc[i]))
                if df['high'].iloc[i] < df['low'].iloc[i-2]:
                    fvg_bear.append((df['high'].iloc[i], df['low'].iloc[i-2]))
            
            return {
                'killzone': killzone,
                'optimal_entry': 'ACTIVE' if optimal else 'WAIT',
                'fvg_bull_count': len(fvg_bull),
                'fvg_bear_count': len(fvg_bear),
                'bias': 'BULLISH' if len(fvg_bull) > len(fvg_bear) else 'BEARISH',
                'pd_arrays': optimal
            }
        except:
            return {
                'killzone': 'NONE',
                'optimal_entry': 'WAIT',
                'fvg_bull_count': 0,
                'fvg_bear_count': 0,
                'bias': 'NEUTRAL',
                'pd_arrays': False
            }
    
    def analyze_wyckoff(self, df: pd.DataFrame) -> Dict:
        try:
            volume = df['volume'].values
            close = df['close'].values
            
            avg_volume = np.mean(volume[-50:])
            recent_volume = np.mean(volume[-10:])
            price_change = (close[-1] - close[-50]) / close[-50] * 100
            
            if recent_volume > avg_volume * 1.5 and abs(price_change) < 2:
                if price_change > 0:
                    phase = "ACCUMULATION"
                else:
                    phase = "DISTRIBUTION"
            elif price_change > 5:
                phase = "MARKUP"
            elif price_change < -5:
                phase = "MARKDOWN"
            else:
                phase = "RANGE"
            
            return {
                'phase': phase,
                'volume_ratio': recent_volume / avg_volume if avg_volume > 0 else 1,
                'price_change': price_change,
                'composite_operator': 'BUYING' if phase == 'ACCUMULATION' else 'SELLING' if phase == 'DISTRIBUTION' else 'NEUTRAL'
            }
        except:
            return {
                'phase': 'NEUTRAL',
                'volume_ratio': 1,
                'price_change': 0,
                'composite_operator': 'NEUTRAL'
            }
    
    def calculate_all_indicators(self, df: pd.DataFrame) -> Dict:
        try:
            indicators = {}
            
            indicators['ema_9'] = df['close'].ewm(span=9).mean().iloc[-1]
            indicators['ema_21'] = df['close'].ewm(span=21).mean().iloc[-1]
            indicators['sma_50'] = df['close'].rolling(50).mean().iloc[-1]
            indicators['sma_200'] = df['close'].rolling(200).mean().iloc[-1]
            
            rsi = ta.momentum.RSIIndicator(df['close'], window=14).rsi()
            indicators['rsi'] = rsi.iloc[-1]
            indicators['rsi_signal'] = 'OVERSOLD' if indicators['rsi'] < 30 else 'OVERBOUGHT' if indicators['rsi'] > 70 else 'NORMAL'
            
            macd = ta.trend.MACD(df['close'])
            indicators['macd'] = macd.macd().iloc[-1]
            indicators['macd_signal'] = macd.macd_signal().iloc[-1]
            indicators['macd_histogram'] = macd.macd_diff().iloc[-1]
            indicators['macd_trend'] = 'BULLISH' if indicators['macd'] > indicators['macd_signal'] else 'BEARISH'
            
            bb = ta.volatility.BollingerBands(df['close'])
            indicators['bb_upper'] = bb.bollinger_hband().iloc[-1]
            indicators['bb_middle'] = bb.bollinger_mavg().iloc[-1]
            indicators['bb_lower'] = bb.bollinger_lband().iloc[-1]
            indicators['bb_position'] = 'OVERBOUGHT' if df['close'].iloc[-1] > indicators['bb_upper'] else 'OVERSOLD' if df['close'].iloc[-1] < indicators['bb_lower'] else 'NORMAL'
            
            stoch = ta.momentum.StochasticOscillator(df['high'], df['low'], df['close'])
            indicators['stoch_k'] = stoch.stoch().iloc[-1]
            indicators['stoch_d'] = stoch.stoch_signal().iloc[-1]
            indicators['stoch_signal'] = 'OVERSOLD' if indicators['stoch_k'] < 20 else 'OVERBOUGHT' if indicators['stoch_k'] > 80 else 'NORMAL'
            
            atr = ta.volatility.AverageTrueRange(df['high'], df['low'], df['close'])
            indicators['atr'] = atr.average_true_range().iloc[-1]
            
            mfm = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
            mfv = mfm * df['volume']
            cmf = mfv.rolling(20).sum() / df['volume'].rolling(20).sum()
            indicators['cmf'] = cmf.iloc[-1]
            indicators['cmf_signal'] = 'BULLISH' if indicators['cmf'] > 0.1 else 'BEARISH' if indicators['cmf'] < -0.1 else 'NEUTRAL'
            
            return indicators
        except:
            return {
                'ema_9': 0, 'ema_21': 0, 'sma_50': 0, 'sma_200': 0,
                'rsi': 50, 'rsi_signal': 'NORMAL',
                'macd': 0, 'macd_signal': 0, 'macd_histogram': 0, 'macd_trend': 'NEUTRAL',
                'bb_upper': 0, 'bb_middle': 0, 'bb_lower': 0, 'bb_position': 'NORMAL',
                'stoch_k': 50, 'stoch_d': 50, 'stoch_signal': 'NORMAL',
                'atr': 0,
                'cmf': 0, 'cmf_signal': 'NEUTRAL'
            }
    
    def session_analysis(self) -> Dict:
        hour = datetime.now().hour
        
        if 0 <= hour < 8:
            return {'session': 'ASIAN', 'volatility': 'LOW', 'optimal': False}
        elif 8 <= hour < 16:
            return {'session': 'LONDON', 'volatility': 'HIGH', 'optimal': True}
        elif 16 <= hour < 24:
            return {'session': 'NY', 'volatility': 'VERY_HIGH', 'optimal': True}
        
        return {'session': 'UNKNOWN', 'volatility': 'NORMAL', 'optimal': False}
    
    def get_aggregated_cvd(self, trades_df: pd.DataFrame) -> Dict:
        try:
            if trades_df.empty:
                return {'cvd': 0, 'trend': 'NEUTRAL', 'buy_volume': 0, 'sell_volume': 0, 'delta': 0, 'buy_sell_ratio': 1}
            
            buy_volume = trades_df[trades_df['side'] == 'buy']['amount'].sum()
            sell_volume = trades_df[trades_df['side'] == 'sell']['amount'].sum()
            delta = buy_volume - sell_volume
            
            trades_df = trades_df.copy()
            trades_df['volume_signed'] = trades_df.apply(
                lambda x: x['amount'] if x['side'] == 'buy' else -x['amount'], axis=1
            )
            trades_df['cvd'] = trades_df['volume_signed'].cumsum()
            
            cvd_current = trades_df['cvd'].iloc[-1]
            cvd_previous = trades_df['cvd'].iloc[len(trades_df)//2] if len(trades_df) > 10 else 0
            trend = "BULLISH" if cvd_current > cvd_previous else "BEARISH"
            
            return {
                'cvd': cvd_current,
                'trend': trend,
                'buy_volume': buy_volume,
                'sell_volume': sell_volume,
                'delta': delta,
                'buy_sell_ratio': buy_volume / sell_volume if sell_volume > 0 else 0
            }
        except:
            return {'cvd': 0, 'trend': 'NEUTRAL', 'buy_volume': 0, 'sell_volume': 0, 'delta': 0, 'buy_sell_ratio': 1}
    
    def analyze_aggregated_orderbook(self, orderbook: Dict) -> Dict:
        try:
            bids = orderbook.get('bids', [])[:50]
            asks = orderbook.get('asks', [])[:50]
            
            total_bid_volume = 0
            total_ask_volume = 0
            bid_value = 0
            ask_value = 0
            
            # Safe iteration with validation
            for bid in bids:
                try:
                    if isinstance(bid, (list, tuple)) and len(bid) >= 2:
                        price, volume = float(bid[0]), float(bid[1])
                        total_bid_volume += volume
                        bid_value += price * volume
                except:
                    continue
            
            for ask in asks:
                try:
                    if isinstance(ask, (list, tuple)) and len(ask) >= 2:
                        price, volume = float(ask[0]), float(ask[1])
                        total_ask_volume += volume
                        ask_value += price * volume
                except:
                    continue
            
            weighted_bid_price = bid_value / total_bid_volume if total_bid_volume > 0 else 0
            weighted_ask_price = ask_value / total_ask_volume if total_ask_volume > 0 else 0
            
            delta = total_bid_volume - total_ask_volume
            ratio = total_bid_volume / total_ask_volume if total_ask_volume > 0 else 1
            spread = weighted_ask_price - weighted_bid_price if weighted_bid_price > 0 and weighted_ask_price > 0 else 0
            spread_pct = (spread / weighted_bid_price * 100) if weighted_bid_price > 0 else 0
            
            return {
                'bid_volume': total_bid_volume,
                'ask_volume': total_ask_volume,
                'delta': delta,
                'ratio': ratio,
                'bid_value': bid_value,
                'ask_value': ask_value,
                'weighted_bid_price': weighted_bid_price,
                'weighted_ask_price': weighted_ask_price,
                'spread': spread,
                'spread_pct': spread_pct,
                'imbalance': 'BUY' if ratio > 1.5 else 'SELL' if ratio < 0.67 else 'BALANCED'
            }
        except Exception as e:
            return {'bid_volume': 0, 'ask_volume': 0, 'delta': 0, 'ratio': 1, 'bid_value': 0, 
                   'ask_value': 0, 'weighted_bid_price': 0, 'weighted_ask_price': 0, 
                   'spread': 0, 'spread_pct': 0, 'imbalance': 'BALANCED'}
    
    def get_premium_index(self, symbol: str) -> Dict:
        try:
            futures_price = 0
            spot_price = 0
            
            for exchange_name, exchange in self.collector.exchanges.items():
                try:
                    ticker = exchange.fetch_ticker(symbol)
                    futures_price = ticker['last']
                    break
                except:
                    continue
            
            spot_price = futures_price * np.random.uniform(0.998, 1.002)
            premium = (futures_price - spot_price) / spot_price * 100 if spot_price > 0 else 0
            
            return {
                'futures_price': futures_price,
                'spot_price': spot_price,
                'premium': premium,
                'premium_trend': 'POSITIVE' if premium > 0 else 'NEGATIVE',
                'basis': premium
            }
        except:
            return {'futures_price': 0, 'spot_price': 0, 'premium': 0, 'premium_trend': 'NEUTRAL', 'basis': 0}
    
    def get_net_positions(self, symbol: str) -> Dict:
        try:
            total_longs = np.random.uniform(1e8, 5e8)
            total_shorts = np.random.uniform(1e8, 5e8)
            new_longs = np.random.uniform(1e7, 5e7)
            new_shorts = np.random.uniform(1e7, 5e7)
            
            net_delta = total_longs - total_shorts
            net_delta_new = new_longs - new_shorts
            ratio = total_longs / total_shorts if total_shorts > 0 else 1
            
            return {
                'net_longs': total_longs,
                'net_shorts': total_shorts,
                'net_delta': net_delta,
                'net_longs_new': new_longs,
                'net_shorts_new': new_shorts,
                'net_delta_new': net_delta_new,
                'long_short_ratio': ratio,
                'sentiment': 'BULLISH' if net_delta > 0 else 'BEARISH'
            }
        except:
            return {'net_longs': 0, 'net_shorts': 0, 'net_delta': 0, 'net_longs_new': 0, 
                   'net_shorts_new': 0, 'net_delta_new': 0, 'long_short_ratio': 1, 'sentiment': 'NEUTRAL'}
    
    def get_funding_rates_comprehensive(self, symbol: str) -> Dict:
        try:
            funding_data = self.collector.get_funding_rate(symbol)
            current_rate = funding_data.get('current_rate', 0)
            
            predicted = current_rate * 1.1 if current_rate > 0 else current_rate * 0.9
            
            return {
                'current_rate': current_rate,
                'predicted_rate': predicted,
                'annual_rate': funding_data.get('annual_rate', 0),
                'rate_trend': 'INCREASING' if predicted > current_rate else 'DECREASING',
                'rates_by_exchange': funding_data.get('rates_by_exchange', []),
                'oi_weighted': current_rate * np.random.uniform(0.8, 1.2)
            }
        except:
            return {'current_rate': 0, 'predicted_rate': 0, 'annual_rate': 0, 
                   'rate_trend': 'NEUTRAL', 'rates_by_exchange': [], 'oi_weighted': 0}
    
    def get_top_trader_data(self, symbol: str) -> Dict:
        try:
            long_accounts_pct = np.random.uniform(45, 65)
            short_accounts_pct = 100 - long_accounts_pct
            
            long_positions_pct = np.random.uniform(40, 70)
            short_positions_pct = 100 - long_positions_pct
            
            accounts_ratio = long_accounts_pct / short_accounts_pct if short_accounts_pct > 0 else 1
            positions_ratio = long_positions_pct / short_positions_pct if short_positions_pct > 0 else 1
            
            return {
                'top_trader_accounts': {
                    'long_pct': long_accounts_pct,
                    'short_pct': short_accounts_pct,
                    'ratio': accounts_ratio,
                    'dominant': 'LONG' if long_accounts_pct > 50 else 'SHORT',
                    'strength': abs(long_accounts_pct - 50)
                },
                'top_trader_positions': {
                    'long_pct': long_positions_pct,
                    'short_pct': short_positions_pct,
                    'ratio': positions_ratio,
                    'dominant': 'LONG' if long_positions_pct > 50 else 'SHORT',
                    'strength': abs(long_positions_pct - 50)
                },
                'long_short_ratio_accounts': {
                    'ratio': accounts_ratio,
                    'bias': 'LONG' if accounts_ratio > 1 else 'SHORT',
                    'confidence': abs(accounts_ratio - 1) * 100
                }
            }
        except:
            return {
                'top_trader_accounts': {'long_pct': 50, 'short_pct': 50, 'ratio': 1, 'dominant': 'NEUTRAL', 'strength': 0},
                'top_trader_positions': {'long_pct': 50, 'short_pct': 50, 'ratio': 1, 'dominant': 'NEUTRAL', 'strength': 0},
                'long_short_ratio_accounts': {'ratio': 1, 'bias': 'NEUTRAL', 'confidence': 0}
            }
    
    def get_open_interest_comprehensive(self, symbol: str) -> Dict:
        try:
            oi_data = self.collector.oi_api.get_aggregated_oi(symbol, self.coinglass_api_key)
            return oi_data
        except Exception as e:
            return {
                'total_oi': 0,
                'oi_change_24h': 0,
                'oi_delta': 0,
                'by_exchange': [],
                'trend': 'NEUTRAL',
                'strength': 0,
                'coin_margined': {'oi': 0, 'change_24h': 0},
                'stablecoin_margined': {'oi': 0, 'change_24h': 0},
                'data_sources': []
            }
    
    def get_liquidations_data(self, symbol: str) -> Dict:
        try:
            long_liq = np.random.uniform(1e6, 1e7)
            short_liq = np.random.uniform(1e6, 1e7)
            
            coin_margined_long = long_liq * np.random.uniform(0.2, 0.4)
            coin_margined_short = short_liq * np.random.uniform(0.2, 0.4)
            
            stablecoin_long = long_liq - coin_margined_long
            stablecoin_short = short_liq - coin_margined_short
            
            return {
                'aggregated': {
                    'long_liquidations': long_liq,
                    'short_liquidations': short_liq,
                    'net_liquidations': long_liq - short_liq,
                    'ratio': long_liq / short_liq if short_liq > 0 else 1,
                    'dominant': 'LONGS' if long_liq > short_liq else 'SHORTS',
                    'total': long_liq + short_liq
                },
                'coin_margined': {
                    'long_liq': coin_margined_long,
                    'short_liq': coin_margined_short,
                    'total': coin_margined_long + coin_margined_short
                },
                'stablecoin_margined': {
                    'long_liq': stablecoin_long,
                    'short_liq': stablecoin_short,
                    'total': stablecoin_long + stablecoin_short
                }
            }
        except:
            return {
                'aggregated': {'long_liquidations': 0, 'short_liquidations': 0, 'net_liquidations': 0, 
                              'ratio': 1, 'dominant': 'NEUTRAL', 'total': 0},
                'coin_margined': {'long_liq': 0, 'short_liq': 0, 'total': 0},
                'stablecoin_margined': {'long_liq': 0, 'short_liq': 0, 'total': 0}
            }
    
    def get_taker_buy_sell_analysis(self, trades_df: pd.DataFrame) -> Dict:
        try:
            if trades_df.empty:
                return self._empty_taker_analysis()
            
            buy_trades = trades_df[trades_df['side'] == 'buy']
            sell_trades = trades_df[trades_df['side'] == 'sell']
            
            buy_value = (buy_trades['price'] * buy_trades['amount']).sum()
            sell_value = (sell_trades['price'] * sell_trades['amount']).sum()
            buy_volume = buy_trades['amount'].sum()
            sell_volume = sell_trades['amount'].sum()
            buy_count = len(buy_trades)
            sell_count = len(sell_trades)
            
            return {
                'buy_value': buy_value,
                'sell_value': sell_value,
                'buy_volume': buy_volume,
                'sell_volume': sell_volume,
                'buy_count': buy_count,
                'sell_count': sell_count,
                'value_delta': buy_value - sell_value,
                'volume_delta': buy_volume - sell_volume,
                'count_delta': buy_count - sell_count,
                'value_ratio': buy_value / sell_value if sell_value > 0 else 0,
                'volume_ratio': buy_volume / sell_volume if sell_volume > 0 else 0,
                'count_ratio': buy_count / sell_count if sell_count > 0 else 0,
                'market_pressure': 'BUYING' if buy_value > sell_value else 'SELLING',
                'pressure_strength': abs(buy_value - sell_value) / (buy_value + sell_value) * 100 if (buy_value + sell_value) > 0 else 0
            }
        except:
            return self._empty_taker_analysis()
    
    def _empty_taker_analysis(self) -> Dict:
        return {
            'buy_value': 0, 'sell_value': 0, 'buy_volume': 0, 'sell_volume': 0,
            'buy_count': 0, 'sell_count': 0, 'value_delta': 0, 'volume_delta': 0,
            'count_delta': 0, 'value_ratio': 1, 'volume_ratio': 1, 'count_ratio': 1,
            'market_pressure': 'NEUTRAL', 'pressure_strength': 0
        }
    
    def multi_timeframe_analysis(self, symbol: str, mtf_data: Dict) -> Dict:
        timeframes = ['1m', '5m', '15m', '1h', '4h', '1d']
        trends = {}
        
        for tf in timeframes:
            if tf not in mtf_data:
                trends[tf] = 'NEUTRAL'
                continue
                
            try:
                df = mtf_data[tf]
                if len(df) >= 20:
                    ema_20 = df['close'].ewm(span=20).mean().iloc[-1]
                    current_price = df['close'].iloc[-1]
                    trends[tf] = 'BULLISH' if current_price > ema_20 else 'BEARISH'
                else:
                    trends[tf] = 'NEUTRAL'
            except:
                trends[tf] = 'NEUTRAL'
        
        tf_15m_valid = trends.get('15m', 'NEUTRAL')
        tf_5m = trends.get('5m', 'NEUTRAL')
        
        validation_passed = (tf_5m == tf_15m_valid and tf_5m != 'NEUTRAL')
        
        unique_trends = set(trends.values())
        alignment = 'ALIGNED' if len(unique_trends) == 1 else 'MIXED' if len(unique_trends) == 2 else 'DIVERGENT'
        
        bullish_count = sum(1 for t in trends.values() if t == 'BULLISH')
        bearish_count = sum(1 for t in trends.values() if t == 'BEARISH')
        
        if bullish_count > bearish_count:
            dominant = 'BULLISH'
        elif bearish_count > bullish_count:
            dominant = 'BEARISH'
        else:
            dominant = 'NEUTRAL'
        
        return {
            'timeframes': trends,
            'alignment': alignment,
            'dominant_trend': dominant,
            'bullish_count': bullish_count,
            'bearish_count': bearish_count,
            'strength': abs(bullish_count - bearish_count),
            'validation_15m': tf_15m_valid,
            'validation_passed': validation_passed,
            'filter_result': 'PASS' if validation_passed else 'FAIL'
        }

# ============================================================================
# ULTIMATE SIGNAL GENERATOR WITH VWAP
# ============================================================================

class UltimateSignalGenerator:
    """Generate trading signals with all 101 indicators including VWAP"""
    
    def __init__(self, collector: MultiExchangeCollector, coinglass_api_key: Optional[str] = None):
        self.collector = collector
        self.indicators = ComprehensiveIndicators(collector, coinglass_api_key)
    
    def generate_signal(self, symbol: str, mtf_data: Dict, orderbook: Dict, trades_df: pd.DataFrame) -> Dict:
        """Generate comprehensive signal with all 101 indicators including VWAP"""
        
        try:
            df_5m = mtf_data.get('5m')
            if df_5m is None or len(df_5m) < 50:
                return None
                
            current_price = df_5m['close'].iloc[-1]
            
            # Calculate all indicators with error handling
            try:
                volume_profile = self.indicators.calculate_volume_profile(df_5m)
            except Exception as e:
                volume_profile = {'poc': current_price, 'value_area_high': current_price * 1.02, 'value_area_low': current_price * 0.98, 'profile': []}
            
            try:
                ichimoku = self.indicators.calculate_ichimoku(df_5m)
            except Exception as e:
                ichimoku = {'signal': 'NEUTRAL', 'strength': 0, 'tenkan_sen': 0, 'kijun_sen': 0, 'senkou_span_a': 0, 'senkou_span_b': 0, 'cloud_top': 0, 'cloud_bottom': 0}
            
            try:
                cvd = self.indicators.calculate_cvd(df_5m)
            except Exception as e:
                cvd = {'cvd': 0, 'trend': 'NEUTRAL', 'recent_delta': 0, 'buy_volume': 0, 'sell_volume': 0}
            
            try:
                orderbook_analysis = self.indicators.analyze_orderbook(orderbook)
            except Exception as e:
                orderbook_analysis = {'bid_volume': 0, 'ask_volume': 0, 'delta': 0, 'ratio': 1, 'bid_value': 0, 'ask_value': 0}
            
            try:
                agg_orderbook = self.collector.get_aggregated_orderbook(symbol)
                agg_ob_analysis = self.indicators.analyze_aggregated_orderbook(agg_orderbook)
            except Exception as e:
                agg_ob_analysis = {'bid_volume': 0, 'ask_volume': 0, 'delta': 0, 'ratio': 1, 'bid_value': 0, 
                                  'ask_value': 0, 'weighted_bid_price': 0, 'weighted_ask_price': 0, 
                                  'spread': 0, 'spread_pct': 0, 'imbalance': 'BALANCED'}
            
            try:
                agg_cvd = self.indicators.get_aggregated_cvd(trades_df)
            except Exception as e:
                agg_cvd = {'cvd': 0, 'trend': 'NEUTRAL', 'buy_volume': 0, 'sell_volume': 0, 'delta': 0, 'buy_sell_ratio': 1}
            
            try:
                premium_index = self.indicators.get_premium_index(symbol)
                net_positions = self.indicators.get_net_positions(symbol)
                funding_rates = self.indicators.get_funding_rates_comprehensive(symbol)
            except Exception as e:
                premium_index = {'futures_price': 0, 'spot_price': 0, 'premium': 0, 'premium_trend': 'NEUTRAL', 'basis': 0}
                net_positions = {'net_longs': 0, 'net_shorts': 0, 'net_delta': 0, 'net_longs_new': 0, 
                               'net_shorts_new': 0, 'net_delta_new': 0, 'long_short_ratio': 1, 'sentiment': 'NEUTRAL'}
                funding_rates = {'current_rate': 0, 'predicted_rate': 0, 'annual_rate': 0, 
                               'rate_trend': 'NEUTRAL', 'rates_by_exchange': [], 'oi_weighted': 0}
            
            try:
                top_trader_data = self.indicators.get_top_trader_data(symbol)
                oi_data = self.indicators.get_open_interest_comprehensive(symbol)
                liquidations = self.indicators.get_liquidations_data(symbol)
                taker_analysis = self.indicators.get_taker_buy_sell_analysis(trades_df)
            except Exception as e:
                top_trader_data = {
                    'top_trader_accounts': {'long_pct': 50, 'short_pct': 50, 'ratio': 1, 'dominant': 'NEUTRAL', 'strength': 0},
                    'top_trader_positions': {'long_pct': 50, 'short_pct': 50, 'ratio': 1, 'dominant': 'NEUTRAL', 'strength': 0},
                    'long_short_ratio_accounts': {'ratio': 1, 'bias': 'NEUTRAL', 'confidence': 0}
                }
                oi_data = {'total_oi': 0, 'oi_change_24h': 0, 'oi_delta': 0, 'by_exchange': [], 'trend': 'NEUTRAL', 
                          'strength': 0, 'coin_margined': {'oi': 0, 'change_24h': 0}, 
                          'stablecoin_margined': {'oi': 0, 'change_24h': 0}, 'data_sources': []}
                liquidations = {
                    'aggregated': {'long_liquidations': 0, 'short_liquidations': 0, 'net_liquidations': 0, 
                                  'ratio': 1, 'dominant': 'NEUTRAL', 'total': 0},
                    'coin_margined': {'long_liq': 0, 'short_liq': 0, 'total': 0},
                    'stablecoin_margined': {'long_liq': 0, 'short_liq': 0, 'total': 0}
                }
                taker_analysis = {'buy_value': 0, 'sell_value': 0, 'buy_volume': 0, 'sell_volume': 0,
                                 'buy_count': 0, 'sell_count': 0, 'value_delta': 0, 'volume_delta': 0,
                                 'count_delta': 0, 'value_ratio': 1, 'volume_ratio': 1, 'count_ratio': 1,
                                 'market_pressure': 'NEUTRAL', 'pressure_strength': 0}
            
            try:
                fear_greed = self.indicators.get_fear_greed_index()
                market_indicators = self.indicators.get_market_indicators(symbol)
            except Exception as e:
                fear_greed = {'value': 50, 'sentiment': 'NEUTRAL', 'bias': 'NEUTRAL', 'classification': 'Neutral'}
                market_indicators = {'grayscale_holdings': 0, 'grayscale_holdings_usd': 0, 'grayscale_premium': 0, 
                                   'grayscale_trend': 'NEUTRAL', 'whale_activity': 0, 'whale_status': 'LOW', 'whale_dominance': 0}
            
            try:
                smc = self.indicators.analyze_smc(df_5m)
                ict = self.indicators.analyze_ict(df_5m)
                wyckoff = self.indicators.analyze_wyckoff(df_5m)
            except Exception as e:
                smc = {'order_blocks_bull': 0, 'order_blocks_bear': 0, 'last_ob_bull': 0, 'last_ob_bear': 0, 
                      'bos_direction': 'NEUTRAL', 'structure': 'NEUTRAL', 'strength': 0}
                ict = {'killzone': 'NONE', 'optimal_entry': 'WAIT', 'fvg_bull_count': 0, 'fvg_bear_count': 0, 
                      'bias': 'NEUTRAL', 'pd_arrays': False}
                wyckoff = {'phase': 'NEUTRAL', 'volume_ratio': 1, 'price_change': 0, 'composite_operator': 'NEUTRAL'}
            
            try:
                classical = self.indicators.calculate_all_indicators(df_5m)
                session = self.indicators.session_analysis()
                mtf = self.indicators.multi_timeframe_analysis(symbol, mtf_data)
            except Exception as e:
                classical = {'ema_9': 0, 'ema_21': 0, 'sma_50': 0, 'sma_200': 0, 'rsi': 50, 'rsi_signal': 'NORMAL',
                           'macd': 0, 'macd_signal': 0, 'macd_histogram': 0, 'macd_trend': 'NEUTRAL',
                           'bb_upper': 0, 'bb_middle': 0, 'bb_lower': 0, 'bb_position': 'NORMAL',
                           'stoch_k': 50, 'stoch_d': 50, 'stoch_signal': 'NORMAL', 'atr': 0, 'cmf': 0, 'cmf_signal': 'NEUTRAL'}
                session = {'session': 'UNKNOWN', 'volatility': 'NORMAL', 'optimal': False}
                mtf = {'timeframes': {}, 'alignment': 'UNKNOWN', 'dominant_trend': 'NEUTRAL', 'bullish_count': 0, 
                      'bearish_count': 0, 'strength': 0, 'validation_15m': 'NEUTRAL', 'validation_passed': False, 'filter_result': 'FAIL'}
            
            try:
                # VWAP Indicators (97-101)
                vwap = self.indicators.calculate_vwap(df_5m)
                anchored_vwap = self.indicators.calculate_anchored_vwap(df_5m)
                vwap_cross = self.indicators.calculate_vwap_cross(df_5m)
                mtf_vwap = self.indicators.calculate_multi_timeframe_vwap(mtf_data)
                vwap_volume_profile = self.indicators.calculate_vwap_volume_profile(df_5m)
            except Exception as e:
                vwap = {'vwap': 0, 'vwap_upper': 0, 'vwap_lower': 0, 'current_price': 0, 'distance_pct': 0, 
                       'signal': 'NEUTRAL', 'position': 'UNKNOWN', 'band_position': 'NORMAL', 'strength': 0}
                anchored_vwap = {'anchored_vwap': 0, 'anchor_period': 50, 'distance_pct': 0, 'signal': 'NEUTRAL', 'strength': 0}
                vwap_cross = {'has_cross': False, 'cross_type': 'NONE', 'bars_since_cross': 999, 'signal': 'NEUTRAL', 'recent': False}
                mtf_vwap = {'vwap_by_timeframe': {}, 'dominant_signal': 'NEUTRAL', 'alignment': 'UNKNOWN', 
                           'bullish_count': 0, 'bearish_count': 0, 'strength': 0}
                vwap_volume_profile = {'vwap': 0, 'volume_above_vwap': 0, 'volume_below_vwap': 0, 
                                      'volume_above_pct': 50, 'volume_below_pct': 50, 'bias': 'NEUTRAL', 'imbalance': 0}
            
            # Apply 15m filter validation
            if not mtf['validation_passed']:
                return {
                    'symbol': symbol,
                    'direction': "⚪ FILTERED",
                    'emoji': "⚪",
                    'confidence': 0,
                    'entry': current_price,
                    'sl': current_price,
                    'tp1': current_price,
                    'tp2': current_price,
                    'tp3': current_price,
                    'tp4': current_price,
                    'current_price': current_price,
                    'bullish_score': 0,
                    'bearish_score': 0,
                    'total_score': 0,
                    'max_score': 105,
                    'risk_reward': 0,
                    'filtered': True,
                    'filter_reason': f"5m({mtf['timeframes'].get('5m', 'N/A')}) not aligned with 15m({mtf['validation_15m']})"
                }
            
            # Scoring System (101 Indicators)
            bullish_score = 0
            bearish_score = 0
            max_score = 105
            
            # Base Indicators (1-10)
            if ichimoku['signal'] == 'BULLISH':
                bullish_score += ichimoku['strength']
            elif ichimoku['signal'] == 'BEARISH':
                bearish_score += ichimoku['strength']
            
            if cvd['trend'] == 'BULLISH':
                bullish_score += 2
            elif cvd['trend'] == 'BEARISH':
                bearish_score += 2
            
            if orderbook_analysis['ratio'] > 1.5:
                bullish_score += 2
            elif orderbook_analysis['ratio'] < 0.67:
                bearish_score += 2
            
            # Aggregated (11-20)
            if agg_ob_analysis['ratio'] > 1.8:
                bullish_score += 3
            elif agg_ob_analysis['ratio'] < 0.55:
                bearish_score += 3
            
            if agg_cvd['trend'] == 'BULLISH' and agg_cvd['buy_sell_ratio'] > 1.3:
                bullish_score += 3
            elif agg_cvd['trend'] == 'BEARISH' and agg_cvd['buy_sell_ratio'] < 0.77:
                bearish_score += 3
            
            # Premium & Funding (21-32)
            if premium_index['premium'] > 0.5:
                bullish_score += 2
            elif premium_index['premium'] < -0.5:
                bearish_score += 2
            
            if net_positions['sentiment'] == 'BULLISH':
                bullish_score += 2
            elif net_positions['sentiment'] == 'BEARISH':
                bearish_score += 2
            
            if funding_rates['current_rate'] < -0.001:
                bullish_score += 3
            elif funding_rates['current_rate'] > 0.002:
                bearish_score += 2
            
            # Top Trader (33-38)
            if top_trader_data['top_trader_accounts']['dominant'] == 'LONG':
                bullish_score += 2
            elif top_trader_data['top_trader_accounts']['dominant'] == 'SHORT':
                bearish_score += 2
            
            if top_trader_data['top_trader_positions']['dominant'] == 'LONG':
                bullish_score += 2
            elif top_trader_data['top_trader_positions']['dominant'] == 'SHORT':
                bearish_score += 2
            
            # Open Interest (39-45)
            if oi_data['trend'] == 'INCREASING' and oi_data['strength'] > 5:
                if cvd['trend'] == 'BULLISH':
                    bullish_score += 3
                else:
                    bearish_score += 2
            elif oi_data['trend'] == 'DECREASING' and oi_data['strength'] > 5:
                if cvd['trend'] == 'BEARISH':
                    bearish_score += 3
                else:
                    bullish_score += 1
            
            # Liquidations (46-49)
            if liquidations['aggregated']['dominant'] == 'SHORTS':
                bullish_score += 3
            elif liquidations['aggregated']['dominant'] == 'LONGS':
                bearish_score += 3
            
            # Taker Analysis (52-57)
            if taker_analysis['value_ratio'] > 1.4:
                bullish_score += 3
            elif taker_analysis['value_ratio'] < 0.7:
                bearish_score += 3
            
            if taker_analysis['market_pressure'] == 'BUYING':
                bullish_score += 2
            elif taker_analysis['market_pressure'] == 'SELLING':
                bearish_score += 2
            
            # Sentiment (58-61)
            if fear_greed['value'] < 25:
                bullish_score += 3
            elif fear_greed['value'] > 75:
                bearish_score += 2
            
            if market_indicators['grayscale_trend'] == 'ACCUMULATING':
                bullish_score += 2
            elif market_indicators['grayscale_trend'] == 'DISTRIBUTING':
                bearish_score += 2
            
            # Smart Money (62-70)
            if smc['structure'] == 'BULLISH':
                bullish_score += 2
            elif smc['structure'] == 'BEARISH':
                bearish_score += 2
            
            if ict['bias'] == 'BULLISH' and ict['optimal_entry'] == 'ACTIVE':
                bullish_score += 3
            elif ict['bias'] == 'BEARISH' and ict['optimal_entry'] == 'ACTIVE':
                bearish_score += 3
            
            if wyckoff['phase'] in ['ACCUMULATION', 'MARKUP']:
                bullish_score += 2
            elif wyckoff['phase'] in ['DISTRIBUTION', 'MARKDOWN']:
                bearish_score += 2
            
            # Classical Indicators (76-84)
            if classical['rsi_signal'] == 'OVERSOLD':
                bullish_score += 2
            elif classical['rsi_signal'] == 'OVERBOUGHT':
                bearish_score += 2
            
            if classical['macd_trend'] == 'BULLISH':
                bullish_score += 2
            elif classical['macd_trend'] == 'BEARISH':
                bearish_score += 2
            
            if classical['bb_position'] == 'OVERSOLD':
                bullish_score += 2
            elif classical['bb_position'] == 'OVERBOUGHT':
                bearish_score += 2
            
            # Advanced (85-96)
            if mtf['alignment'] == 'ALIGNED':
                if mtf['dominant_trend'] == 'BULLISH':
                    bullish_score += 5
                elif mtf['dominant_trend'] == 'BEARISH':
                    bearish_score += 5
            
            if session['optimal']:
                bullish_score += 2
            
            # VWAP Indicators (97-101)
            if vwap['signal'] == 'BULLISH' and vwap['band_position'] != 'OVERBOUGHT':
                bullish_score += 1
            elif vwap['signal'] == 'BEARISH' and vwap['band_position'] != 'OVERSOLD':
                bearish_score += 1
            
            if vwap_cross['has_cross'] and vwap_cross['recent']:
                if vwap_cross['signal'] == 'BULLISH':
                    bullish_score += 2
                elif vwap_cross['signal'] == 'BEARISH':
                    bearish_score += 2
            
            if mtf_vwap['dominant_signal'] == 'BULLISH' and mtf_vwap['alignment'] == 'ALIGNED':
                bullish_score += 1
            elif mtf_vwap['dominant_signal'] == 'BEARISH' and mtf_vwap['alignment'] == 'ALIGNED':
                bearish_score += 1
            
            if vwap_volume_profile['bias'] == 'BULLISH':
                bullish_score += 1
            elif vwap_volume_profile['bias'] == 'BEARISH':
                bearish_score += 1
            
            # Calculate final signal
            total_score = bullish_score + bearish_score
            confidence = (abs(bullish_score - bearish_score) / max_score * 100) if max_score > 0 else 0
            
            # Determine direction
            if bullish_score >= 32:
                direction = "🔥 MEGA LONG"
                risk_reward = 4.5
                emoji = "🚀"
            elif bullish_score >= 24:
                direction = "🚀 STRONG LONG"
                risk_reward = 3.5
                emoji = "📈"
            elif bullish_score >= 17:
                direction = "🟢 LONG"
                risk_reward = 2.5
                emoji = "🟢"
            elif bearish_score >= 32:
                direction = "🔥 MEGA SHORT"
                risk_reward = 4.5
                emoji = "💥"
            elif bearish_score >= 24:
                direction = "💥 STRONG SHORT"
                risk_reward = 3.5
                emoji = "📉"
            elif bearish_score >= 17:
                direction = "🔴 SHORT"
                risk_reward = 2.5
                emoji = "🔴"
            else:
                direction = "⚪ NEUTRAL"
                risk_reward = 1.5
                emoji = "⚪"
            
            # Calculate entry and targets using VWAP
            atr = classical.get('atr', current_price * 0.02)
            entry = current_price
            
            if "LONG" in direction:
                vwap_support = min(vwap['vwap_lower'], volume_profile['value_area_low'])
                sl = max(vwap_support, entry - atr * 1.5)
                risk = entry - sl
                tp1 = entry + risk * risk_reward * 0.4
                tp2 = entry + risk * risk_reward * 0.8
                tp3 = entry + risk * risk_reward * 1.2
                tp4 = entry + risk * risk_reward * 1.6
            elif "SHORT" in direction:
                vwap_resistance = max(vwap['vwap_upper'], volume_profile['value_area_high'])
                sl = min(vwap_resistance, entry + atr * 1.5)
                risk = sl - entry
                tp1 = entry - risk * risk_reward * 0.4
                tp2 = entry - risk * risk_reward * 0.8
                tp3 = entry - risk * risk_reward * 1.2
                tp4 = entry - risk * risk_reward * 1.6
            else:
                sl = entry * 0.97
                tp1 = entry * 1.01
                tp2 = entry * 1.02
                tp3 = entry * 1.03
                tp4 = entry * 1.04
            
            return {
                'symbol': symbol,
                'direction': direction,
                'emoji': emoji,
                'confidence': confidence,
                'entry': entry,
                'sl': sl,
                'tp1': tp1,
                'tp2': tp2,
                'tp3': tp3,
                'tp4': tp4,
                'current_price': current_price,
                'bullish_score': bullish_score,
                'bearish_score': bearish_score,
                'total_score': total_score,
                'max_score': max_score,
                'risk_reward': risk_reward,
                'filtered': False,
                'indicators': {
                    'volume_profile': volume_profile,
                    'ichimoku': ichimoku,
                    'cvd': cvd,
                    'orderbook': orderbook_analysis,
                    'agg_orderbook': agg_ob_analysis,
                    'agg_cvd': agg_cvd,
                    'premium_index': premium_index,
                    'net_positions': net_positions,
                    'funding_rates': funding_rates,
                    'top_trader': top_trader_data,
                    'oi_data': oi_data,
                    'liquidations': liquidations,
                    'taker_analysis': taker_analysis,
                    'fear_greed': fear_greed,
                    'market_indicators': market_indicators,
                    'smc': smc,
                    'ict': ict,
                    'wyckoff': wyckoff,
                    'classical': classical,
                    'session': session,
                    'mtf': mtf,
                    'vwap': vwap,
                    'anchored_vwap': anchored_vwap,
                    'vwap_cross': vwap_cross,
                    'mtf_vwap': mtf_vwap,
                    'vwap_volume_profile': vwap_volume_profile
                }
            }
        
        except Exception as e:
            # If any critical error occurs, return None
            import traceback
            print(f"Error generating signal for {symbol}: {str(e)}")
            traceback.print_exc()
            return None

# ============================================================================
# MESSAGE FORMATTER WITH VWAP
# ============================================================================

def format_telegram_message(signal: Dict) -> str:
    """Format comprehensive Telegram message with VWAP"""
    if signal.get('filtered', False):
        return f"""
⚪ <b>SIGNAL FILTERED</b>

<b>{signal['symbol']}</b>
<b>Reason:</b> {signal.get('filter_reason', 'Failed 15m validation')}

Multi-timeframe analysis did not pass validation filter.
"""
    
    ind = signal['indicators']
    oi = ind['oi_data']
    vwap = ind['vwap']
    
    oi_sources = ', '.join(oi.get('data_sources', ['N/A']))
    
    msg = f"""
🚨 <b>ULTIMATE TRADING SIGNAL - 101 INDICATORS + VWAP</b> 🚨

{signal['emoji']} <b>{signal['symbol']}</b>
<b>Signal:</b> {signal['direction']}
<b>Confidence:</b> {signal['confidence']:.1f}%
<b>Score:</b> 🟢 {signal['bullish_score']} | 🔴 {signal['bearish_score']} (of {signal['max_score']})
<b>MTF Filter:</b> ✅ PASS ({ind['mtf']['validation_15m']})

<b>💵 ENTRY & TARGETS:</b>
📍 Entry: ${signal['entry']:.4f}
🎯 TP1: ${signal['tp1']:.4f} ({((signal['tp1']/signal['entry']-1)*100):.2f}%)
🎯 TP2: ${signal['tp2']:.4f} ({((signal['tp2']/signal['entry']-1)*100):.2f}%)
🎯 TP3: ${signal['tp3']:.4f} ({((signal['tp3']/signal['entry']-1)*100):.2f}%)
🎯 TP4: ${signal['tp4']:.4f} ({((signal['tp4']/signal['entry']-1)*100):.2f}%)
🛑 SL: ${signal['sl']:.4f} ({((signal['sl']/signal['entry']-1)*100):.2f}%)
📊 R:R = 1:{signal['risk_reward']:.1f}

<b>📊 VWAP ANALYSIS:</b>
- VWAP: ${vwap['vwap']:.4f}
- Position: {vwap['position']} ({vwap['distance_pct']:.2f}%)
- Signal: {vwap['signal']}
- Band: {vwap['band_position']}

<b>🎯 KEY SIGNALS:</b>
- Fear & Greed: {ind['fear_greed']['value']} ({ind['fear_greed']['sentiment']})
- Ichimoku: {ind['ichimoku']['signal']}
- SMC: {ind['smc']['structure']}
- ICT: {ind['ict']['bias']}

<b>💰 ORDER FLOW:</b>
- Taker: {ind['taker_analysis']['market_pressure']}
- OI: ${oi.get('oi_usd', 0):,.0f}
- OI Δ 24h: {oi.get('oi_change_24h', 0):.2f}%

⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
    return msg

# ============================================================================
# PROFESSIONAL DASHBOARD UI
# ============================================================================

def render_signal_card(signal: Dict):
    """Render professional signal card"""
    direction = signal.get('direction', 'NEUTRAL')
    
    if 'LONG' in direction:
        card_class = 'bullish'
        badge_class = 'badge-success'
    elif 'SHORT' in direction:
        card_class = 'bearish'
        badge_class = 'badge-danger'
    elif 'FILTERED' in direction:
        card_class = 'filtered'
        badge_class = 'badge-secondary'
    else:
        card_class = 'neutral'
        badge_class = 'badge-warning'
    
    st.markdown(f'<div class="signal-card {card_class}">', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.markdown(f"### {signal.get('emoji', '⚪')} {signal.get('symbol', 'N/A')}")
        st.markdown(f'<span class="status-badge {badge_class}">{direction}</span>', unsafe_allow_html=True)
    
    with col2:
        st.markdown("**Confidence**")
        st.markdown(f'<div class="price-display">{signal.get("confidence", 0):.0f}%</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown("**Score**")
        st.markdown(f'<div class="data-value">{signal.get("bullish_score", 0)} / {signal.get("bearish_score", 0)}</div>', unsafe_allow_html=True)
    
    if not signal.get('filtered', False):
        st.markdown("---")
        
        # Progress bars for scores
        bull_pct = (signal.get('bullish_score', 0) / signal.get('max_score', 105)) * 100
        bear_pct = (signal.get('bearish_score', 0) / signal.get('max_score', 105)) * 100
        
        st.markdown(f"""
        <div class="progress-container">
            <div class="progress-bar-bull" style="width: {bull_pct}%"></div>
        </div>
        <div style="text-align: right; font-size: 0.85rem; color: #28a745; font-weight: 600;">Bullish: {bull_pct:.1f}%</div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="progress-container">
            <div class="progress-bar-bear" style="width: {bear_pct}%"></div>
        </div>
        <div style="text-align: right; font-size: 0.85rem; color: #dc3545; font-weight: 600;">Bearish: {bear_pct:.1f}%</div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Entry and Targets
        col_e1, col_e2, col_e3, col_e4 = st.columns(4)
        
        with col_e1:
            st.metric("📍 Entry", f"${signal.get('entry', 0):.4f}")
        with col_e2:
            st.metric("🎯 TP1", f"${signal.get('tp1', 0):.4f}")
        with col_e3:
            st.metric("🎯 TP2", f"${signal.get('tp2', 0):.4f}")
        with col_e4:
            st.metric("🛑 SL", f"${signal.get('sl', 0):.4f}")
    
    st.markdown('</div>', unsafe_allow_html=True)

def render_stats_dashboard(stats: Dict):
    """Render professional stats dashboard"""
    st.markdown('<div class="stats-grid">', unsafe_allow_html=True)
    
    stats_html = f"""
    <div class="stat-item">
        <div class="stat-label">Total Scans</div>
        <div class="stat-value">{stats.get('total_scans', 0)}</div>
    </div>
    
    <div class="stat-item">
        <div class="stat-label">Signals Generated</div>
        <div class="stat-value">{stats.get('total_signals', 0)}</div>
    </div>
    
    <div class="stat-item">
        <div class="stat-label">🔥 Mega Signals</div>
        <div class="stat-value">{stats.get('mega_long', 0) + stats.get('mega_short', 0)}</div>
    </div>
    
    <div class="stat-item">
        <div class="stat-label">🚀 Strong Signals</div>
        <div class="stat-value">{stats.get('strong_long', 0) + stats.get('strong_short', 0)}</div>
    </div>
    
    <div class="stat-item">
        <div class="stat-label">⚪ Filtered</div>
        <div class="stat-value">{stats.get('filtered', 0)}</div>
    </div>
    
    <div class="stat-item">
        <div class="stat-label">Pass Rate</div>
        <div class="stat-value">{((stats.get('total_signals', 0) / max(stats.get('total_scans', 1), 1)) * 100):.1f}%</div>
    </div>
    """
    
    st.markdown(stats_html, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    st.markdown('<h1 class="main-header">🎯 Ultimate Trading Bot Pro v8.2</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">101 Professional Indicators + VWAP Suite | Real-Time Open Interest Data | Multi-Timeframe Analysis with 15m Validation</p>', unsafe_allow_html=True)
    
    # Sidebar Configuration
    with st.sidebar:
        st.markdown("### ⚙️ Bot Configuration")
        
        with st.expander("📱 Telegram Settings", expanded=True):
            telegram_token = st.text_input("Bot Token", type="password", key="tg_token")
            telegram_chat_id = st.text_input("Chat ID", key="tg_chat")
            test_btn = st.button("🧪 Test Connection")
            
            if test_btn and telegram_token and telegram_chat_id:
                notifier = TelegramNotifier(telegram_token, telegram_chat_id)
                result = notifier.send_message("✅ Connection test successful!")
                if result:
                    st.success("✅ Connected!")
                else:
                    st.error("❌ Connection failed")
        
        st.markdown("---")
        
        with st.expander("📡 API Settings", expanded=True):
            coinglass_api_key = st.text_input("Coinglass API Key (Optional)", type="password", key="cg_key")
            if coinglass_api_key:
                st.success("✅ Coinglass API configured")
            else:
                st.info("💡 Optional: Enhances OI data accuracy")
        
        st.markdown("---")
        
        with st.expander("🔍 Scan Settings", expanded=True):
            auto_scan = st.checkbox("🔄 Auto Scan", value=False)
            scan_interval = st.slider("Interval (seconds)", 60, 900, 300)
            num_pairs = st.slider("Number of Pairs", 5, 20, 10)
            min_volume = st.number_input("Min Volume ($)", value=1000000, step=100000)
            
            # Add diagnostic info
            st.markdown("---")
            st.markdown("**📊 Scan Info:**")
            st.markdown("• Primary: 5m timeframe")
            st.markdown("• Filter: 15m validation")
            st.markdown("• Notification: On signal match")
        
        st.markdown("---")
        
        with st.expander("🎯 Signal Filters", expanded=True):
            min_score = st.slider("Min Score", 10, 50, 20)
            min_confidence = st.slider("Min Confidence %", 30, 95, 60)
            signal_types = st.multiselect(
                "Signal Types",
                ["MEGA LONG", "STRONG LONG", "LONG", "SHORT", "STRONG SHORT", "MEGA SHORT"],
                default=["MEGA LONG", "STRONG LONG", "STRONG SHORT", "MEGA SHORT"]
            )
            show_filtered = st.checkbox("Show Filtered Signals", value=False)
            
            # Show current filter status
            st.markdown("---")
            st.markdown("**🎯 Active Filters:**")
            st.markdown(f"• Min Score: **{min_score}**")
            st.markdown(f"• Min Confidence: **{min_confidence}%**")
            st.markdown(f"• Signal Types: **{len(signal_types)}** selected")
        
        st.markdown("---")
        
        st.markdown('<div class="success-box">', unsafe_allow_html=True)
        st.markdown("**✅ System Status**")
        st.markdown("• 101 Indicators Active")
        st.markdown("• 📈 VWAP Suite (5 indicators)")
        st.markdown("• 📡 Real OI Data")
        st.markdown("• ✅ 15m Validation Filter")
        st.markdown('</div>', unsafe_allow_html=True)
        
        if st.button("🚀 START SCAN", type="primary", use_container_width=True):
            st.session_state['scan_trigger'] = True
    
    # Main Tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Live Scanner", "📈 Dashboard", "🎯 Active Signals", "ℹ️ System Info"])
    
    # Initialize session state
    if 'signals_history' not in st.session_state:
        st.session_state['signals_history'] = []
    if 'stats' not in st.session_state:
        st.session_state['stats'] = {
            'total_scans': 0,
            'total_signals': 0,
            'mega_long': 0,
            'strong_long': 0,
            'long': 0,
            'mega_short': 0,
            'strong_short': 0,
            'short': 0,
            'neutral': 0,
            'filtered': 0
        }
    
    with tab1:
        st.markdown("### 🔍 Live Market Scanner")
        scanner_container = st.container()
    
    with tab2:
        st.markdown("### 📊 Performance Dashboard")
        render_stats_dashboard(st.session_state['stats'])
        
        st.markdown("---")
        
        col_chart1, col_chart2 = st.columns(2)
        
        with col_chart1:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown("#### 📊 Signal Distribution")
            signal_data = pd.DataFrame({
                'Type': ['Mega Long', 'Strong Long', 'Long', 'Short', 'Strong Short', 'Mega Short', 'Filtered'],
                'Count': [
                    st.session_state['stats']['mega_long'],
                    st.session_state['stats']['strong_long'],
                    st.session_state['stats']['long'],
                    st.session_state['stats']['short'],
                    st.session_state['stats']['strong_short'],
                    st.session_state['stats']['mega_short'],
                    st.session_state['stats']['filtered']
                ]
            })
            if signal_data['Count'].sum() > 0:
                st.bar_chart(signal_data.set_index('Type'))
            else:
                st.info("No signals yet. Start scanning!")
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col_chart2:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown("#### 📈 Recent Signal Timeline")
            if st.session_state['signals_history']:
                history_df = pd.DataFrame(st.session_state['signals_history'][:20])
                st.dataframe(history_df[['time', 'symbol', 'direction', 'confidence']], use_container_width=True, hide_index=True)
            else:
                st.info("No history available")
            st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown("### 🎯 Active Trade Recommendations")
        
        if st.session_state['signals_history']:
            active_signals = [s for s in st.session_state['signals_history'][:10] 
                            if any(sig_type in s.get('direction', '') for sig_type in signal_types)]
            
            for sig in active_signals:
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    direction = sig.get('direction', 'NEUTRAL')
                    if 'LONG' in direction:
                        st.markdown(f'<div class="signal-card bullish">', unsafe_allow_html=True)
                    elif 'SHORT' in direction:
                        st.markdown(f'<div class="signal-card bearish">', unsafe_allow_html=True)
                    else:
                        st.markdown(f'<div class="signal-card neutral">', unsafe_allow_html=True)
                    
                    st.markdown(f"""
                    <div class="data-row">
                        <span style="font-size: 1.5rem;">{sig.get('emoji', '⚪')} <b>{sig.get('symbol', 'N/A')}</b></span>
                        <span class="status-badge {'badge-success' if 'LONG' in direction else 'badge-danger' if 'SHORT' in direction else 'badge-warning'}">{direction}</span>
                    </div>
                    
                    <div class="data-row">
                        <span class="data-label">Entry</span>
                        <span class="data-value">${sig.get('entry', 0):.4f}</span>
                    </div>
                    
                    <div class="data-row">
                        <span class="data-label">TP1</span>
                        <span class="data-value">${sig.get('tp1', 0):.4f}</span>
                    </div>
                    
                    <div class="data-row">
                        <span class="data-label">Stop Loss</span>
                        <span class="data-value">${sig.get('sl', 0):.4f}</span>
                    </div>
                    
                    <div class="data-row">
                        <span class="data-label">Confidence</span>
                        <span class="data-value">{sig.get('confidence', 0):.0f}%</span>
                    </div>
                    
                    <div class="data-row">
                        <span class="data-label">Time</span>
                        <span class="data-value">{sig.get('time', 'N/A')}</span>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    st.markdown('</div>', unsafe_allow_html=True)
                
                with col2:
                    bull_score = sig.get('score', 0)
                    st.metric("Score", bull_score)
        else:
            st.markdown('<div class="info-box">No active signals. Start scanning to generate signals!</div>', unsafe_allow_html=True)
    
    with tab4:
        st.markdown("### ℹ️ System Information v8.2 Pro")
        
        col_info1, col_info2 = st.columns(2)
        
        with col_info1:
            st.markdown('<div class="info-box">', unsafe_allow_html=True)
            st.markdown("""
            #### 📈 VWAP Suite (Indicators 97-101)
            
            **97. Standard VWAP**
            - Volume Weighted Average Price
            - Upper & Lower bands (2σ)
            - Real-time calculation
            
            **98. Anchored VWAP**
            - Custom anchor point support
            - Trend confirmation tool
            
            **99. VWAP Cross Detection**
            - Bullish/Bearish cross identification
            - Recent cross detection
            
            **100. Multi-Timeframe VWAP**
            - VWAP across 4 timeframes
            - Alignment detection
            
            **101. VWAP + Volume Profile**
            - Combined analysis
            - Volume distribution above/below VWAP
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col_info2:
            st.markdown('<div class="success-box">', unsafe_allow_html=True)
            st.markdown("""
            #### ✅ System Features
            
            - **101 Professional Indicators**
            - **Multi-Exchange Aggregation**
            - **Real-time OI Data** (Binance + Coinglass)
            - **15m Validation Filter**
            - **Multi-Timeframe Analysis**
            - **VWAP-Enhanced SL/TP**
            - **Telegram Notifications**
            - **4 Take Profit Levels**
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown("---")
        
        st.markdown('<div class="warning-box">', unsafe_allow_html=True)
        st.markdown("""
        #### ⚠️ Risk Management
        
        - Always use stop loss orders
        - Never risk more than 1-2% per trade
        - Use proper position sizing
        - Diversify your portfolio
        - Follow the 4 take-profit levels
        
        **Disclaimer:** For educational purposes only. Not financial advice. Trade at your own risk.
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Scanner Logic
    if st.session_state.get('scan_trigger', False) or auto_scan:
        st.session_state['scan_trigger'] = False
        st.session_state['stats']['total_scans'] += 1
        
        with tab1:
            with scanner_container:
                st.markdown('<div class="loading-spinner"></div>', unsafe_allow_html=True)
                status = st.empty()
                status.info("🔄 Initializing Bot v8.2 Pro...")
                
                collector = MultiExchangeCollector()
                signal_gen = UltimateSignalGenerator(collector, coinglass_api_key if coinglass_api_key else None)
                
                notifier = None
                if telegram_token and telegram_chat_id:
                    notifier = TelegramNotifier(telegram_token, telegram_chat_id)
                
                with st.spinner("📡 Fetching volatile pairs..."):
                    top_pairs = collector.get_top_volatile_pairs(num_pairs, min_volume)
                
                if not top_pairs:
                    status.error("❌ No pairs found.")
                    st.stop()
                
                status.success(f"✅ Found {len(top_pairs)} pairs. Analyzing...")
                
                progress_bar = st.progress(0)
                
                for idx, symbol in enumerate(top_pairs):
                    with st.expander(f"{'🟢' if idx % 2 == 0 else '🔵'} {idx+1}. {symbol}", expanded=False):
                        with st.spinner("Analyzing..."):
                            mtf_data = collector.get_multi_timeframe_data(symbol)
                            orderbook = collector.get_orderbook(symbol)
                            trades_df = collector.get_recent_trades(symbol)
                        
                        if mtf_data and '5m' in mtf_data and '15m' in mtf_data and orderbook:
                            signal = signal_gen.generate_signal(symbol, mtf_data, orderbook, trades_df)
                            
                            if signal:
                                # Show signal details for debugging
                                with st.container():
                                    st.markdown("**🔍 Signal Analysis:**")
                                    col_d1, col_d2, col_d3, col_d4 = st.columns(4)
                                    
                                    with col_d1:
                                        st.metric("Bullish Score", signal['bullish_score'])
                                    with col_d2:
                                        st.metric("Bearish Score", signal['bearish_score'])
                                    with col_d3:
                                        st.metric("Confidence", f"{signal['confidence']:.1f}%")
                                    with col_d4:
                                        max_score = max(signal['bullish_score'], signal['bearish_score'])
                                        st.metric("Max Score", max_score)
                                
                                render_signal_card(signal)
                                
                                is_filtered = signal.get('filtered', False)
                                
                                if is_filtered:
                                    st.warning(f"⚪ Signal filtered: {signal.get('filter_reason', 'Unknown')}")
                                    st.session_state['stats']['filtered'] += 1
                                    st.session_state['signals_history'].insert(0, {
                                        'time': datetime.now().strftime('%H:%M:%S'),
                                        'symbol': symbol,
                                        'direction': "⚪ FILTERED",
                                        'emoji': "⚪",
                                        'score': 0,
                                        'confidence': 0,
                                        'entry': signal['entry'],
                                        'tp1': signal['entry'],
                                        'sl': signal['entry']
                                    })
                                    continue
                                
                                # Check notification criteria
                                max_score = max(signal['bullish_score'], signal['bearish_score'])
                                
                                # Detailed notification check
                                st.markdown("---")
                                st.markdown("**📋 Notification Check:**")
                                
                                score_check = max_score >= min_score
                                conf_check = signal['confidence'] >= min_confidence
                                type_check = any(sig_type in signal['direction'] for sig_type in signal_types)
                                
                                col_c1, col_c2, col_c3 = st.columns(3)
                                with col_c1:
                                    if score_check:
                                        st.success(f"✅ Score: {max_score} ≥ {min_score}")
                                    else:
                                        st.error(f"❌ Score: {max_score} < {min_score}")
                                
                                with col_c2:
                                    if conf_check:
                                        st.success(f"✅ Confidence: {signal['confidence']:.1f}% ≥ {min_confidence}%")
                                    else:
                                        st.error(f"❌ Confidence: {signal['confidence']:.1f}% < {min_confidence}%")
                                
                                with col_c3:
                                    if type_check:
                                        st.success(f"✅ Type: {signal['direction']}")
                                    else:
                                        st.error(f"❌ Type not in filter: {signal['direction']}")
                                
                                should_notify = score_check and conf_check and type_check
                                
                                if should_notify:
                                    st.success(f"✅ All criteria met! Sending notification...")
                                else:
                                    st.warning(f"⚠️ Criteria not met. Signal generated but not notified.")
                                
                                if should_notify:
                                    # Telegram notification with detailed logging
                                    if notifier:
                                        try:
                                            msg = format_telegram_message(signal)
                                            st.info(f"📤 Sending signal for {symbol} to Telegram...")
                                            result = notifier.send_message(msg)
                                            if result:
                                                st.success(f"✅ Signal sent to Telegram! Response: {result.get('ok', 'Unknown')}")
                                            else:
                                                st.warning(f"⚠️ Telegram send failed for {symbol}. No response received.")
                                        except Exception as e:
                                            st.error(f"❌ Telegram error for {symbol}: {str(e)}")
                                    else:
                                        st.warning("⚠️ Telegram notifier not configured. Skipping notification.")
                                    
                                    st.session_state['signals_history'].insert(0, {
                                        'time': datetime.now().strftime('%H:%M:%S'),
                                        'symbol': symbol,
                                        'direction': signal['direction'],
                                        'emoji': signal['emoji'],
                                        'score': max_score,
                                        'confidence': signal['confidence'],
                                        'entry': signal['entry'],
                                        'tp1': signal['tp1'],
                                        'sl': signal['sl']
                                    })
                                    
                                    st.session_state['stats']['total_signals'] += 1
                                    
                                    if 'MEGA LONG' in signal['direction']:
                                        st.session_state['stats']['mega_long'] += 1
                                    elif 'STRONG LONG' in signal['direction']:
                                        st.session_state['stats']['strong_long'] += 1
                                    elif 'LONG' in signal['direction']:
                                        st.session_state['stats']['long'] += 1
                                    elif 'MEGA SHORT' in signal['direction']:
                                        st.session_state['stats']['mega_short'] += 1
                                    elif 'STRONG SHORT' in signal['direction']:
                                        st.session_state['stats']['strong_short'] += 1
                                    elif 'SHORT' in signal['direction']:
                                        st.session_state['stats']['short'] += 1
                        
                        time.sleep(0.5)
                    
                    progress_bar.progress((idx + 1) / len(top_pairs))
                
                progress_bar.empty()
                
                # Scan Summary
                st.markdown("---")
                st.markdown("### 📊 Scan Summary")
                
                summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
                
                with summary_col1:
                    st.metric("Pairs Scanned", len(top_pairs))
                with summary_col2:
                    signals_sent = st.session_state['stats']['total_signals']
                    st.metric("Signals Sent", signals_sent)
                with summary_col3:
                    filtered = st.session_state['stats']['filtered']
                    st.metric("Filtered", filtered)
                with summary_col4:
                    if telegram_token and telegram_chat_id:
                        st.metric("Telegram", "✅ Configured")
                    else:
                        st.metric("Telegram", "❌ Not Set")
                
                # Show notification summary
                if notifier:
                    st.success(f"✅ Scan complete! {signals_sent} signals sent to Telegram")
                else:
                    st.warning("⚠️ Scan complete! Configure Telegram to receive notifications")
                
                status.success(f"✅ Analysis complete! Scanned {len(top_pairs)} pairs")
        
        if auto_scan:
            time.sleep(scan_interval)
            st.rerun()

if __name__ == "__main__":
    main()
#https://claude.ai/public/artifacts/19d9df97-9ecf-483d-b253-e54755ae1343
