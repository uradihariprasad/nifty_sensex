"""
=============================================================
  NIFTY & SENSEX Trading Intelligence System - Backend
  Real-time market analysis using Upstox API v2
=============================================================

SETUP INSTRUCTIONS:
  1. pip install flask flask-cors flask-socketio requests aiohttp numpy
  2. python app.py
  3. Open http://localhost:5000 in your browser
  4. Enter your Upstox Access Token in the UI

REQUIRED PIP INSTALLS:
  pip install flask flask-cors flask-socketio requests numpy

NOTE: Access token is entered via the web UI - no hardcoding needed.
=============================================================
"""

import json
import time
import math
import threading
import requests
import numpy as np
from datetime import datetime, timedelta
from flask import Flask, render_template, send_from_directory, jsonify, request
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from collections import deque

# ============================================================
# Flask App Setup
# ============================================================
app = Flask(__name__, static_folder='.', template_folder='.')
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# ============================================================
# Global State
# ============================================================
ACCESS_TOKEN = None
DATA_THREAD = None
THREAD_RUNNING = False

# Instrument keys
INSTRUMENTS = {
    'NIFTY': {
        'index_key': 'NSE_INDEX|Nifty 50',
        'option_prefix': 'NSE_FO',
        'name': 'NIFTY',
        'strike_gap': 50,
        'lot_size': 50
    },
    'SENSEX': {
        'index_key': 'BSE_INDEX|SENSEX',
        'option_prefix': 'BSE_FO',
        'name': 'SENSEX',
        'strike_gap': 100,
        'lot_size': 10
    }
}

# Data stores
market_data = {
    'NIFTY': {
        'ltp': 0, 'open': 0, 'high': 0, 'low': 0, 'close': 0,
        'prev_close': 0, 'prev_high': 0, 'prev_low': 0,
        'volume': 0, 'change': 0, 'change_pct': 0,
        'candles_3m': [], 'candles_5m': [],
        'vwap': 0, 'vwap_data': {'cum_vol': 0, 'cum_tp_vol': 0},
        'option_chain': [], 'supports': [], 'resistances': [],
        'alerts': [], 'trade_suggestion': {},
        'smc_zones': [], 'market_breadth': {},
        'oi_analysis': {}, 'vix': 0,
        'institutional': {}, 'dashboard': {}
    },
    'SENSEX': {
        'ltp': 0, 'open': 0, 'high': 0, 'low': 0, 'close': 0,
        'prev_close': 0, 'prev_high': 0, 'prev_low': 0,
        'volume': 0, 'change': 0, 'change_pct': 0,
        'candles_3m': [], 'candles_5m': [],
        'vwap': 0, 'vwap_data': {'cum_vol': 0, 'cum_tp_vol': 0},
        'option_chain': [], 'supports': [], 'resistances': [],
        'alerts': [], 'trade_suggestion': {},
        'smc_zones': [], 'market_breadth': {},
        'oi_analysis': {}, 'vix': 0,
        'institutional': {}, 'dashboard': {}
    }
}

alert_history = {'NIFTY': deque(maxlen=50), 'SENSEX': deque(maxlen=50)}


# ============================================================
# Upstox API Helper Functions
# ============================================================
def upstox_headers():
    """Get headers for Upstox API calls."""
    return {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }


def fetch_market_quote(instrument_key):
    """Fetch full market quote for an instrument."""
    try:
        url = f'https://api.upstox.com/v2/market-quote/quotes?instrument_key={instrument_key}'
        resp = requests.get(url, headers=upstox_headers(), timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'success' and data.get('data'):
                return data['data']
        return None
    except Exception as e:
        print(f"[ERROR] fetch_market_quote: {e}")
        return None


def fetch_ohlc_quote(instrument_key):
    """Fetch OHLC quote."""
    try:
        url = f'https://api.upstox.com/v2/market-quote/ohlc?instrument_key={instrument_key}&interval=1d'
        resp = requests.get(url, headers=upstox_headers(), timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'success':
                return data.get('data')
        return None
    except Exception as e:
        print(f"[ERROR] fetch_ohlc_quote: {e}")
        return None


def fetch_historical_candles(instrument_key, interval='5minute', days_back=5):
    """Fetch historical candle data."""
    try:
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        url = f'https://api.upstox.com/v2/historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}'
        resp = requests.get(url, headers={'Accept': 'application/json'}, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'success' and data.get('data', {}).get('candles'):
                return data['data']['candles']
        return []
    except Exception as e:
        print(f"[ERROR] fetch_historical_candles: {e}")
        return []


def fetch_intraday_candles(instrument_key, interval='1minute'):
    """Fetch intraday candle data."""
    try:
        url = f'https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/{interval}'
        resp = requests.get(url, headers={'Accept': 'application/json'}, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'success' and data.get('data', {}).get('candles'):
                return data['data']['candles']
        return []
    except Exception as e:
        print(f"[ERROR] fetch_intraday_candles: {e}")
        return []


def fetch_option_chain(instrument_key, expiry_date=None):
    """Fetch option chain data."""
    try:
        url = 'https://api.upstox.com/v2/option/chain'
        params = {'instrument_key': instrument_key}
        if expiry_date:
            params['expiry_date'] = expiry_date
        resp = requests.get(url, params=params, headers=upstox_headers(), timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'success':
                return data.get('data', [])
        return []
    except Exception as e:
        print(f"[ERROR] fetch_option_chain: {e}")
        return []


def fetch_option_expiries(instrument_key):
    """Get nearest expiry date for option chain."""
    try:
        url = f'https://api.upstox.com/v2/option/contract?instrument_key={instrument_key}'
        resp = requests.get(url, headers=upstox_headers(), timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'success' and data.get('data'):
                expiries = set()
                for item in data['data']:
                    if item.get('expiry'):
                        expiries.add(item['expiry'])
                sorted_expiries = sorted(expiries)
                # Return nearest expiry
                today = datetime.now().strftime('%Y-%m-%d')
                for exp in sorted_expiries:
                    if exp >= today:
                        return exp
                return sorted_expiries[0] if sorted_expiries else None
        return None
    except Exception as e:
        print(f"[ERROR] fetch_option_expiries: {e}")
        return None


def fetch_india_vix():
    """Fetch India VIX data."""
    try:
        url = 'https://api.upstox.com/v2/market-quote/quotes?instrument_key=NSE_INDEX|India VIX'
        resp = requests.get(url, headers=upstox_headers(), timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'success' and data.get('data'):
                for key, val in data['data'].items():
                    if val and val.get('last_price'):
                        return val['last_price']
        return 0
    except Exception as e:
        print(f"[ERROR] fetch_india_vix: {e}")
        return 0


# ============================================================
# Analysis Engine Functions
# ============================================================

def aggregate_candles(minute_candles, period_minutes):
    """Aggregate 1-minute candles into N-minute candles."""
    if not minute_candles:
        return []
    
    # Candle format: [timestamp, open, high, low, close, volume, oi]
    # Sort by time ascending
    sorted_candles = sorted(minute_candles, key=lambda x: x[0])
    
    aggregated = []
    i = 0
    while i < len(sorted_candles):
        batch = sorted_candles[i:i + period_minutes]
        if not batch:
            break
        ts = batch[0][0]
        o = batch[0][1]
        h = max(c[2] for c in batch)
        l = min(c[3] for c in batch)
        c_val = batch[-1][4]
        v = sum(c[5] for c in batch)
        oi = batch[-1][6] if len(batch[-1]) > 6 else 0
        aggregated.append([ts, o, h, l, c_val, v, oi])
        i += period_minutes
    
    return aggregated


def calculate_vwap(candles):
    """Calculate VWAP from candle data."""
    cum_vol = 0
    cum_tp_vol = 0
    vwap_series = []
    
    for c in candles:
        tp = (c[2] + c[3] + c[4]) / 3  # (high + low + close) / 3
        vol = c[5]
        cum_vol += vol
        cum_tp_vol += tp * vol
        vwap = cum_tp_vol / cum_vol if cum_vol > 0 else tp
        vwap_series.append(vwap)
    
    return vwap_series[-1] if vwap_series else 0, vwap_series


def detect_support_resistance(candles, ltp, num_levels=2):
    """Detect support and resistance levels from price action."""
    if not candles or len(candles) < 5:
        return [], []
    
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    volumes = [c[5] for c in candles]
    
    # Pivot-based S/R detection
    pivot_highs = []
    pivot_lows = []
    
    for i in range(2, len(candles) - 2):
        # Pivot high
        if highs[i] > highs[i-1] and highs[i] > highs[i-2] and \
           highs[i] > highs[i+1] and highs[i] > highs[i+2]:
            pivot_highs.append({
                'price': highs[i],
                'volume': volumes[i],
                'idx': i,
                'tests': 1,
                'strength': 'strong'
            })
        
        # Pivot low
        if lows[i] < lows[i-1] and lows[i] < lows[i-2] and \
           lows[i] < lows[i+1] and lows[i] < lows[i+2]:
            pivot_lows.append({
                'price': lows[i],
                'volume': volumes[i],
                'idx': i,
                'tests': 1,
                'strength': 'strong'
            })
    
    # Cluster nearby levels
    def cluster_levels(levels, threshold_pct=0.002):
        if not levels:
            return []
        sorted_levels = sorted(levels, key=lambda x: x['price'])
        clusters = []
        current_cluster = [sorted_levels[0]]
        
        for i in range(1, len(sorted_levels)):
            if abs(sorted_levels[i]['price'] - current_cluster[-1]['price']) / current_cluster[-1]['price'] < threshold_pct:
                current_cluster.append(sorted_levels[i])
            else:
                avg_price = sum(l['price'] for l in current_cluster) / len(current_cluster)
                total_vol = sum(l['volume'] for l in current_cluster)
                tests = len(current_cluster)
                strength = 'strong' if tests >= 3 else ('moderate' if tests >= 2 else 'weak')
                clusters.append({
                    'price': round(avg_price, 2),
                    'volume': total_vol,
                    'tests': tests,
                    'strength': strength
                })
                current_cluster = [sorted_levels[i]]
        
        if current_cluster:
            avg_price = sum(l['price'] for l in current_cluster) / len(current_cluster)
            total_vol = sum(l['volume'] for l in current_cluster)
            tests = len(current_cluster)
            strength = 'strong' if tests >= 3 else ('moderate' if tests >= 2 else 'weak')
            clusters.append({
                'price': round(avg_price, 2),
                'volume': total_vol,
                'tests': tests,
                'strength': strength
            })
        
        return clusters
    
    resistance_clusters = cluster_levels(pivot_highs)
    support_clusters = cluster_levels(pivot_lows)
    
    # Filter: resistance above LTP, support below LTP
    resistances = sorted([r for r in resistance_clusters if r['price'] > ltp], key=lambda x: x['price'])
    supports = sorted([s for s in support_clusters if s['price'] < ltp], key=lambda x: x['price'], reverse=True)
    
    # Always have at least 2 levels using simple calculation
    if len(resistances) < 2:
        last_high = max(highs[-20:]) if len(highs) >= 20 else max(highs)
        gap = abs(ltp * 0.003)
        while len(resistances) < 2:
            next_r = (resistances[-1]['price'] + gap) if resistances else (ltp + gap)
            resistances.append({'price': round(next_r, 2), 'volume': 0, 'tests': 0, 'strength': 'weak'})
            gap += abs(ltp * 0.002)
    
    if len(supports) < 2:
        gap = abs(ltp * 0.003)
        while len(supports) < 2:
            next_s = (supports[-1]['price'] - gap) if supports else (ltp - gap)
            supports.append({'price': round(next_s, 2), 'volume': 0, 'tests': 0, 'strength': 'weak'})
            gap += abs(ltp * 0.002)
    
    return supports[:num_levels], resistances[:num_levels]


def detect_smc_patterns(candles, ltp):
    """Detect Smart Money Concepts: BOS, CHOCH, Liquidity Sweeps, Order Blocks."""
    zones = []
    if not candles or len(candles) < 10:
        return zones
    
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    opens = [c[1] for c in candles]
    
    # Track swing highs and lows for BOS/CHOCH
    swing_highs = []
    swing_lows = []
    
    for i in range(2, len(candles) - 2):
        if highs[i] > highs[i-1] and highs[i] > highs[i+1]:
            swing_highs.append({'price': highs[i], 'idx': i})
        if lows[i] < lows[i-1] and lows[i] < lows[i+1]:
            swing_lows.append({'price': lows[i], 'idx': i})
    
    # Detect Break of Structure (BOS)
    for i in range(1, len(swing_highs)):
        if swing_highs[i]['price'] > swing_highs[i-1]['price']:
            zones.append({
                'type': 'BOS_BULLISH',
                'price': swing_highs[i]['price'],
                'label': f"BOS ↑ {swing_highs[i]['price']:.0f}",
                'color': '#00ff88'
            })
    
    for i in range(1, len(swing_lows)):
        if swing_lows[i]['price'] < swing_lows[i-1]['price']:
            zones.append({
                'type': 'BOS_BEARISH',
                'price': swing_lows[i]['price'],
                'label': f"BOS ↓ {swing_lows[i]['price']:.0f}",
                'color': '#ff4444'
            })
    
    # Detect Change of Character (CHOCH)
    if len(swing_highs) >= 2 and len(swing_lows) >= 2:
        last_sh = swing_highs[-1]
        prev_sh = swing_highs[-2]
        last_sl = swing_lows[-1]
        prev_sl = swing_lows[-2]
        
        # Bullish CHOCH: lower lows then higher high
        if prev_sl['price'] > last_sl['price'] and ltp > prev_sh['price']:
            zones.append({
                'type': 'CHOCH_BULLISH',
                'price': prev_sh['price'],
                'label': f"CHOCH ↑ {prev_sh['price']:.0f}",
                'color': '#00ffcc'
            })
        
        # Bearish CHOCH: higher highs then lower low
        if prev_sh['price'] < last_sh['price'] and ltp < prev_sl['price']:
            zones.append({
                'type': 'CHOCH_BEARISH',
                'price': prev_sl['price'],
                'label': f"CHOCH ↓ {prev_sl['price']:.0f}",
                'color': '#ff6666'
            })
    
    # Detect Order Blocks
    for i in range(1, len(candles) - 1):
        # Bullish OB: last bearish candle before strong bullish move
        if closes[i] < opens[i] and i + 1 < len(closes):
            if closes[i+1] > opens[i+1] and (closes[i+1] - opens[i+1]) > abs(closes[i] - opens[i]) * 1.5:
                zones.append({
                    'type': 'ORDER_BLOCK_BULL',
                    'price': lows[i],
                    'price_high': highs[i],
                    'label': f"OB+ {lows[i]:.0f}",
                    'color': '#0088ff44'
                })
        
        # Bearish OB: last bullish candle before strong bearish move
        if closes[i] > opens[i] and i + 1 < len(closes):
            if closes[i+1] < opens[i+1] and abs(closes[i+1] - opens[i+1]) > (closes[i] - opens[i]) * 1.5:
                zones.append({
                    'type': 'ORDER_BLOCK_BEAR',
                    'price': highs[i],
                    'price_low': lows[i],
                    'label': f"OB- {highs[i]:.0f}",
                    'color': '#ff444444'
                })
    
    # Detect Liquidity Sweeps
    if len(swing_lows) >= 2:
        recent_low = swing_lows[-1]
        prev_low = swing_lows[-2]
        if recent_low['price'] < prev_low['price'] and ltp > prev_low['price']:
            zones.append({
                'type': 'LIQUIDITY_SWEEP_BULL',
                'price': recent_low['price'],
                'label': f"Liq Sweep ↑ {recent_low['price']:.0f}",
                'color': '#ffaa00'
            })
    
    if len(swing_highs) >= 2:
        recent_high = swing_highs[-1]
        prev_high = swing_highs[-2]
        if recent_high['price'] > prev_high['price'] and ltp < prev_high['price']:
            zones.append({
                'type': 'LIQUIDITY_SWEEP_BEAR',
                'price': recent_high['price'],
                'label': f"Liq Sweep ↓ {recent_high['price']:.0f}",
                'color': '#ff8800'
            })
    
    # Keep only recent zones near current price
    relevant_zones = []
    for z in zones:
        if abs(z['price'] - ltp) / ltp < 0.03:  # Within 3% of LTP
            relevant_zones.append(z)
    
    return relevant_zones[-10:]  # Last 10 zones


def analyze_option_chain_data(oc_data, ltp, strike_gap, num_strikes=7):
    """Analyze option chain for ATM ± num_strikes."""
    if not oc_data:
        return {
            'chain': [],
            'analysis': {
                'strongest_support': 0,
                'strongest_resistance': 0,
                'pcr': 0,
                'total_ce_oi': 0,
                'total_pe_oi': 0,
                'max_ce_oi_strike': 0,
                'max_pe_oi_strike': 0,
                'signals': []
            }
        }
    
    # Find ATM strike
    atm_strike = round(ltp / strike_gap) * strike_gap
    min_strike = atm_strike - (num_strikes * strike_gap)
    max_strike = atm_strike + (num_strikes * strike_gap)
    
    filtered_chain = []
    total_ce_oi = 0
    total_pe_oi = 0
    max_ce_oi = 0
    max_pe_oi = 0
    max_ce_oi_strike = 0
    max_pe_oi_strike = 0
    ce_oi_change_total = 0
    pe_oi_change_total = 0
    
    for item in oc_data:
        strike = item.get('strike_price', 0)
        if min_strike <= strike <= max_strike:
            call_data = item.get('call_options', {})
            put_data = item.get('put_options', {})
            
            ce_market = call_data.get('market_data', {})
            pe_market = put_data.get('market_data', {})
            ce_greeks = call_data.get('option_greeks', {})
            pe_greeks = put_data.get('option_greeks', {})
            
            ce_oi = ce_market.get('oi', 0)
            pe_oi = pe_market.get('oi', 0)
            ce_prev_oi = ce_market.get('prev_oi', 0) if ce_market.get('prev_oi') else 0
            pe_prev_oi = pe_market.get('prev_oi', 0) if pe_market.get('prev_oi') else 0
            ce_oi_chg = ce_oi - ce_prev_oi
            pe_oi_chg = pe_oi - pe_prev_oi
            
            total_ce_oi += ce_oi
            total_pe_oi += pe_oi
            ce_oi_change_total += ce_oi_chg
            pe_oi_change_total += pe_oi_chg
            
            if ce_oi > max_ce_oi:
                max_ce_oi = ce_oi
                max_ce_oi_strike = strike
            if pe_oi > max_pe_oi:
                max_pe_oi = pe_oi
                max_pe_oi_strike = strike
            
            filtered_chain.append({
                'strike': strike,
                'is_atm': strike == atm_strike,
                'ce_ltp': ce_market.get('ltp', 0),
                'ce_oi': ce_oi,
                'ce_oi_chg': ce_oi_chg,
                'ce_vol': ce_market.get('volume', 0),
                'ce_iv': ce_greeks.get('iv', 0),
                'ce_delta': ce_greeks.get('delta', 0),
                'pe_ltp': pe_market.get('ltp', 0),
                'pe_oi': pe_oi,
                'pe_oi_chg': pe_oi_chg,
                'pe_vol': pe_market.get('volume', 0),
                'pe_iv': pe_greeks.get('iv', 0),
                'pe_delta': pe_greeks.get('delta', 0),
            })
    
    # Sort by strike
    filtered_chain.sort(key=lambda x: x['strike'])
    
    pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 1
    
    # Generate OI signals
    signals = []
    
    if ce_oi_change_total > 0 and pe_oi_change_total > 0:
        if pe_oi_change_total > ce_oi_change_total * 1.5:
            signals.append({'signal': 'Strong PE Writing', 'bias': 'bullish', 'icon': '🟢'})
        elif ce_oi_change_total > pe_oi_change_total * 1.5:
            signals.append({'signal': 'Strong CE Writing', 'bias': 'bearish', 'icon': '🔴'})
    
    if ce_oi_change_total < 0:
        signals.append({'signal': 'CE Unwinding', 'bias': 'bullish', 'icon': '🟡'})
    if pe_oi_change_total < 0:
        signals.append({'signal': 'PE Unwinding', 'bias': 'bearish', 'icon': '🟡'})
    
    if pcr > 1.3:
        signals.append({'signal': f'PCR Bullish ({pcr:.2f})', 'bias': 'bullish', 'icon': '🟢'})
    elif pcr < 0.7:
        signals.append({'signal': f'PCR Bearish ({pcr:.2f})', 'bias': 'bearish', 'icon': '🔴'})
    
    return {
        'chain': filtered_chain,
        'analysis': {
            'strongest_support': max_pe_oi_strike,
            'strongest_resistance': max_ce_oi_strike,
            'pcr': round(pcr, 2),
            'total_ce_oi': total_ce_oi,
            'total_pe_oi': total_pe_oi,
            'max_ce_oi_strike': max_ce_oi_strike,
            'max_pe_oi_strike': max_pe_oi_strike,
            'ce_oi_change': ce_oi_change_total,
            'pe_oi_change': pe_oi_change_total,
            'signals': signals
        }
    }


def analyze_vwap_price_action(candles, vwap, ltp):
    """Analyze VWAP relationship and price action patterns."""
    if not candles or len(candles) < 3:
        return {'status': 'neutral', 'signals': []}
    
    signals = []
    closes = [c[4] for c in candles]
    opens = [c[1] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    
    # VWAP analysis
    if ltp > vwap:
        if closes[-2] < vwap and closes[-1] > vwap:
            signals.append({'signal': 'VWAP Reclaim', 'bias': 'bullish', 'strength': 'strong'})
        else:
            signals.append({'signal': 'Above VWAP', 'bias': 'bullish', 'strength': 'moderate'})
    else:
        if closes[-2] > vwap and closes[-1] < vwap:
            signals.append({'signal': 'VWAP Rejection', 'bias': 'bearish', 'strength': 'strong'})
        else:
            signals.append({'signal': 'Below VWAP', 'bias': 'bearish', 'strength': 'moderate'})
    
    # Momentum analysis
    recent_bodies = [abs(closes[i] - opens[i]) for i in range(-3, 0)]
    avg_body = sum(recent_bodies) / len(recent_bodies) if recent_bodies else 0
    
    last_body = abs(closes[-1] - opens[-1])
    
    if last_body > avg_body * 1.5:
        if closes[-1] > opens[-1]:
            signals.append({'signal': 'Strong Bullish Candle', 'bias': 'bullish', 'strength': 'strong'})
        else:
            signals.append({'signal': 'Strong Bearish Candle', 'bias': 'bearish', 'strength': 'strong'})
    
    # Exhaustion detection
    if len(closes) >= 5:
        last_5_trend = closes[-1] - closes[-5]
        last_2_trend = closes[-1] - closes[-2]
        
        if last_5_trend > 0 and last_2_trend < 0:
            signals.append({'signal': 'Bullish Exhaustion', 'bias': 'bearish', 'strength': 'moderate'})
        elif last_5_trend < 0 and last_2_trend > 0:
            signals.append({'signal': 'Bearish Exhaustion', 'bias': 'bullish', 'strength': 'moderate'})
    
    # Continuation pattern
    if len(closes) >= 3:
        if closes[-1] > closes[-2] > closes[-3]:
            signals.append({'signal': 'Bullish Continuation', 'bias': 'bullish', 'strength': 'moderate'})
        elif closes[-1] < closes[-2] < closes[-3]:
            signals.append({'signal': 'Bearish Continuation', 'bias': 'bearish', 'strength': 'moderate'})
    
    # Determine overall status
    bullish_count = sum(1 for s in signals if s['bias'] == 'bullish')
    bearish_count = sum(1 for s in signals if s['bias'] == 'bearish')
    
    if bullish_count > bearish_count:
        status = 'bullish'
    elif bearish_count > bullish_count:
        status = 'bearish'
    else:
        status = 'neutral'
    
    return {'status': status, 'signals': signals}


def generate_alerts(index_name, data, oi_analysis, vwap_analysis, smc_zones, supports, resistances):
    """Generate smart alerts based on analysis."""
    alerts = []
    ltp = data['ltp']
    ts = datetime.now().strftime('%H:%M:%S')
    
    # Support/Resistance proximity alerts
    for s in supports:
        dist = abs(ltp - s['price']) / ltp
        if dist < 0.001:
            if s['strength'] == 'weak':
                alerts.append({
                    'time': ts, 'type': 'warning',
                    'message': f"Support weakening at {s['price']:.0f} — downside risk increasing"
                })
            else:
                alerts.append({
                    'time': ts, 'type': 'info',
                    'message': f"Testing support {s['price']:.0f} ({s['strength']})"
                })
    
    for r in resistances:
        dist = abs(ltp - r['price']) / ltp
        if dist < 0.001:
            if r['strength'] == 'weak':
                alerts.append({
                    'time': ts, 'type': 'bullish',
                    'message': f"Resistance weakening at {r['price']:.0f} — breakout probability increasing"
                })
            else:
                alerts.append({
                    'time': ts, 'type': 'info',
                    'message': f"Testing resistance {r['price']:.0f} ({r['strength']})"
                })
    
    # OI-based alerts
    for sig in oi_analysis.get('signals', []):
        if sig['bias'] == 'bullish':
            alerts.append({'time': ts, 'type': 'bullish', 'message': sig['signal']})
        elif sig['bias'] == 'bearish':
            alerts.append({'time': ts, 'type': 'bearish', 'message': sig['signal']})
    
    # VWAP alerts
    for sig in vwap_analysis.get('signals', []):
        atype = 'bullish' if sig['bias'] == 'bullish' else ('bearish' if sig['bias'] == 'bearish' else 'info')
        alerts.append({'time': ts, 'type': atype, 'message': sig['signal']})
    
    # SMC alerts
    for zone in smc_zones:
        if 'BOS' in zone['type']:
            alerts.append({'time': ts, 'type': 'info', 'message': zone['label']})
        elif 'CHOCH' in zone['type']:
            alerts.append({'time': ts, 'type': 'warning', 'message': zone['label']})
        elif 'LIQUIDITY' in zone['type']:
            alerts.append({'time': ts, 'type': 'warning', 'message': zone['label']})
    
    return alerts[-15:]  # Keep last 15


def generate_trade_suggestion(index_name, data, oi_analysis, vwap_analysis, smc_zones, supports, resistances, vix):
    """Generate dynamic trade suggestion based on all analysis."""
    ltp = data['ltp']
    if ltp == 0:
        return {}
    
    bullish_score = 0
    bearish_score = 0
    reasons = []
    
    # 1. VWAP analysis (weight: 2)
    if vwap_analysis.get('status') == 'bullish':
        bullish_score += 2
        reasons.append('VWAP reclaim/above VWAP')
    elif vwap_analysis.get('status') == 'bearish':
        bearish_score += 2
        reasons.append('VWAP rejection/below VWAP')
    
    # 2. Option chain signals (weight: 3)
    for sig in oi_analysis.get('signals', []):
        if sig['bias'] == 'bullish':
            bullish_score += 3
            reasons.append(sig['signal'])
        elif sig['bias'] == 'bearish':
            bearish_score += 3
            reasons.append(sig['signal'])
    
    # 3. PCR analysis (weight: 2)
    pcr = oi_analysis.get('pcr', 1)
    if pcr > 1.2:
        bullish_score += 2
        reasons.append(f'PCR bullish ({pcr:.2f})')
    elif pcr < 0.8:
        bearish_score += 2
        reasons.append(f'PCR bearish ({pcr:.2f})')
    
    # 4. SMC zones (weight: 2)
    for zone in smc_zones:
        if 'BULL' in zone['type']:
            bullish_score += 1
        elif 'BEAR' in zone['type']:
            bearish_score += 1
    
    # 5. Support/Resistance strength (weight: 2)
    if supports and supports[0]['strength'] == 'strong':
        bullish_score += 2
        reasons.append('Strong support nearby')
    if resistances and resistances[0]['strength'] == 'weak':
        bullish_score += 1
        reasons.append('Resistance weakening')
    if resistances and resistances[0]['strength'] == 'strong':
        bearish_score += 1
    if supports and supports[0]['strength'] == 'weak':
        bearish_score += 1
        reasons.append('Support weakening')
    
    # 6. VIX filter (weight: 1)
    if vix > 20:
        reasons.append(f'High VIX ({vix:.1f}) — volatile conditions')
    elif vix < 13:
        bullish_score += 1
        reasons.append(f'Low VIX ({vix:.1f}) — trending conditions')
    
    # 7. Price action signals
    for sig in vwap_analysis.get('signals', []):
        if sig.get('strength') == 'strong':
            if sig['bias'] == 'bullish':
                bullish_score += 2
            elif sig['bias'] == 'bearish':
                bearish_score += 2
    
    # Determine bias
    total_score = bullish_score + bearish_score
    if total_score == 0:
        total_score = 1
    
    strike_gap = INSTRUMENTS[index_name]['strike_gap']
    
    if bullish_score > bearish_score:
        confidence_pct = min(95, (bullish_score / total_score) * 100)
        confidence = 'High' if confidence_pct > 70 else ('Medium' if confidence_pct > 50 else 'Low')
        
        entry = round(ltp + ltp * 0.001)
        sl = round(ltp - ltp * 0.003)
        t1 = round(ltp + ltp * 0.003)
        t2 = round(ltp + ltp * 0.005)
        
        return {
            'action': 'BUY CE',
            'bias': 'BULLISH',
            'entry': f"Above {entry:,.0f}",
            'entry_price': entry,
            'stoploss': f"{sl:,.0f}",
            'sl_price': sl,
            'target1': f"{t1:,.0f}",
            'target2': f"{t2:,.0f}",
            'confidence': confidence,
            'confidence_pct': round(confidence_pct),
            'reasons': reasons[:5],
            'timestamp': datetime.now().strftime('%H:%M:%S')
        }
    elif bearish_score > bullish_score:
        confidence_pct = min(95, (bearish_score / total_score) * 100)
        confidence = 'High' if confidence_pct > 70 else ('Medium' if confidence_pct > 50 else 'Low')
        
        entry = round(ltp - ltp * 0.001)
        sl = round(ltp + ltp * 0.003)
        t1 = round(ltp - ltp * 0.003)
        t2 = round(ltp - ltp * 0.005)
        
        return {
            'action': 'BUY PE',
            'bias': 'BEARISH',
            'entry': f"Below {entry:,.0f}",
            'entry_price': entry,
            'stoploss': f"{sl:,.0f}",
            'sl_price': sl,
            'target1': f"{t1:,.0f}",
            'target2': f"{t2:,.0f}",
            'confidence': confidence,
            'confidence_pct': round(confidence_pct),
            'reasons': reasons[:5],
            'timestamp': datetime.now().strftime('%H:%M:%S')
        }
    else:
        return {
            'action': 'WAIT',
            'bias': 'NEUTRAL',
            'entry': 'No clear setup',
            'entry_price': 0,
            'stoploss': '-',
            'sl_price': 0,
            'target1': '-',
            'target2': '-',
            'confidence': 'Low',
            'confidence_pct': 30,
            'reasons': ['No clear directional bias', 'Wait for confirmation'],
            'timestamp': datetime.now().strftime('%H:%M:%S')
        }


def calculate_trend_strength(candles):
    """Calculate trend strength from recent candles."""
    if not candles or len(candles) < 5:
        return 50
    
    closes = [c[4] for c in candles[-20:]]
    if len(closes) < 2:
        return 50
    
    # Simple momentum score
    changes = [(closes[i] - closes[i-1]) / closes[i-1] * 100 for i in range(1, len(closes))]
    positive = sum(1 for c in changes if c > 0)
    total = len(changes)
    
    return int((positive / total) * 100) if total > 0 else 50


def generate_dashboard(index_name, data, supports, resistances, oi_analysis, vwap_analysis, trade_suggestion, vix, candles):
    """Generate dashboard summary."""
    ltp = data['ltp']
    
    # Market bias
    bullish_signals = sum(1 for s in vwap_analysis.get('signals', []) if s.get('bias') == 'bullish')
    bearish_signals = sum(1 for s in vwap_analysis.get('signals', []) if s.get('bias') == 'bearish')
    
    if bullish_signals > bearish_signals:
        market_bias = 'BULLISH'
    elif bearish_signals > bullish_signals:
        market_bias = 'BEARISH'
    else:
        market_bias = 'NEUTRAL'
    
    trend_strength = calculate_trend_strength(candles)
    
    # Breakout probability
    r1_dist = abs(resistances[0]['price'] - ltp) / ltp * 100 if resistances else 5
    breakout_prob = max(10, min(90, 100 - r1_dist * 20))
    if resistances and resistances[0]['strength'] == 'weak':
        breakout_prob = min(90, breakout_prob + 20)
    
    # Momentum strength
    if candles and len(candles) >= 3:
        recent_momentum = abs(candles[-1][4] - candles[-3][4]) / candles[-3][4] * 100
        momentum_str = 'Strong' if recent_momentum > 0.3 else ('Moderate' if recent_momentum > 0.1 else 'Weak')
    else:
        momentum_str = 'N/A'
    
    return {
        'market_bias': market_bias,
        'trend_strength': trend_strength,
        'strongest_support': supports[0]['price'] if supports else 0,
        'strongest_resistance': resistances[0]['price'] if resistances else 0,
        'breakout_probability': round(breakout_prob),
        'momentum': momentum_str,
        'best_trade': trade_suggestion.get('action', 'WAIT'),
        'confidence': trade_suggestion.get('confidence', 'Low'),
        'vix': round(vix, 2),
        'pcr': oi_analysis.get('pcr', 0),
        'vwap_status': vwap_analysis.get('status', 'neutral')
    }


# ============================================================
# Main Data Fetching Loop
# ============================================================

def data_fetch_loop():
    """Main loop that fetches data and pushes to frontend via SocketIO."""
    global THREAD_RUNNING, market_data
    
    print("[INFO] Data fetch loop started")
    
    # Track expiry dates
    expiry_cache = {}
    last_expiry_fetch = 0
    iteration = 0
    
    while THREAD_RUNNING:
        try:
            iteration += 1
            
            # Fetch expiry dates every 30 minutes
            if time.time() - last_expiry_fetch > 1800 or not expiry_cache:
                for idx_name, idx_info in INSTRUMENTS.items():
                    exp = fetch_option_expiries(idx_info['index_key'])
                    if exp:
                        expiry_cache[idx_name] = exp
                        print(f"[INFO] {idx_name} nearest expiry: {exp}")
                last_expiry_fetch = time.time()
            
            # Fetch India VIX
            vix = fetch_india_vix()
            
            for idx_name, idx_info in INSTRUMENTS.items():
                instrument_key = idx_info['index_key']
                
                # 1. Fetch market quote
                quote_data = fetch_market_quote(instrument_key)
                if quote_data:
                    for key, val in quote_data.items():
                        if val:
                            market_data[idx_name]['ltp'] = val.get('last_price', 0)
                            ohlc = val.get('ohlc', {})
                            market_data[idx_name]['open'] = ohlc.get('open', 0)
                            market_data[idx_name]['high'] = ohlc.get('high', 0)
                            market_data[idx_name]['low'] = ohlc.get('low', 0)
                            market_data[idx_name]['close'] = ohlc.get('close', 0)
                            market_data[idx_name]['prev_close'] = val.get('previous_day_close', ohlc.get('close', 0))
                            market_data[idx_name]['volume'] = val.get('volume', 0) if val.get('volume') else 0
                            
                            net_change = val.get('net_change', 0)
                            market_data[idx_name]['change'] = net_change if net_change else 0
                            pct_change = val.get('percent_change', 0) if val.get('percent_change') else 0
                            market_data[idx_name]['change_pct'] = pct_change
                            break
                
                ltp = market_data[idx_name]['ltp']
                if ltp == 0:
                    continue
                
                # 2. Fetch intraday candles
                minute_candles = fetch_intraday_candles(instrument_key, '1minute')
                
                if minute_candles:
                    # Sort ascending by time
                    minute_candles.sort(key=lambda x: x[0])
                    
                    # Aggregate to 3m and 5m
                    candles_3m = aggregate_candles(minute_candles, 3)
                    candles_5m = aggregate_candles(minute_candles, 5)
                    
                    market_data[idx_name]['candles_3m'] = candles_3m
                    market_data[idx_name]['candles_5m'] = candles_5m
                    
                    # Calculate VWAP
                    vwap_val, _ = calculate_vwap(minute_candles)
                    market_data[idx_name]['vwap'] = round(vwap_val, 2)
                    
                    # Get prev day high/low from historical
                    if iteration == 1 or iteration % 60 == 0:
                        hist_candles = fetch_historical_candles(instrument_key, 'day', 5)
                        if hist_candles and len(hist_candles) >= 2:
                            prev_day = hist_candles[-2] if hist_candles[-1][0][:10] == datetime.now().strftime('%Y-%m-%d') else hist_candles[-1]
                            market_data[idx_name]['prev_high'] = prev_day[2]
                            market_data[idx_name]['prev_low'] = prev_day[3]
                    
                    # Detect S/R
                    all_candles = minute_candles
                    if len(all_candles) > 5:
                        supports, resistances = detect_support_resistance(all_candles, ltp)
                        
                        # Add prev day high/low as levels
                        prev_h = market_data[idx_name].get('prev_high', 0)
                        prev_l = market_data[idx_name].get('prev_low', 0)
                        if prev_h and prev_h > ltp:
                            resistances.append({'price': prev_h, 'volume': 0, 'tests': 0, 'strength': 'prev_day_high'})
                        if prev_l and prev_l < ltp:
                            supports.append({'price': prev_l, 'volume': 0, 'tests': 0, 'strength': 'prev_day_low'})
                        
                        market_data[idx_name]['supports'] = supports
                        market_data[idx_name]['resistances'] = resistances
                    
                    # SMC detection
                    smc_zones = detect_smc_patterns(candles_5m if candles_5m else minute_candles, ltp)
                    market_data[idx_name]['smc_zones'] = smc_zones
                    
                    # VWAP price action analysis
                    vwap_analysis = analyze_vwap_price_action(
                        candles_5m if candles_5m else minute_candles,
                        market_data[idx_name]['vwap'],
                        ltp
                    )
                else:
                    supports = market_data[idx_name]['supports']
                    resistances = market_data[idx_name]['resistances']
                    smc_zones = market_data[idx_name]['smc_zones']
                    vwap_analysis = {'status': 'neutral', 'signals': []}
                    candles_5m = market_data[idx_name]['candles_5m']
                
                # 3. Fetch option chain
                expiry = expiry_cache.get(idx_name)
                if expiry:
                    oc_raw = fetch_option_chain(instrument_key, expiry)
                    oc_result = analyze_option_chain_data(oc_raw, ltp, idx_info['strike_gap'])
                    market_data[idx_name]['option_chain'] = oc_result['chain']
                    oi_analysis = oc_result['analysis']
                    market_data[idx_name]['oi_analysis'] = oi_analysis
                else:
                    oi_analysis = market_data[idx_name].get('oi_analysis', {})
                
                # 4. Store VIX
                market_data[idx_name]['vix'] = vix
                
                # 5. Generate alerts
                alerts = generate_alerts(
                    idx_name, market_data[idx_name],
                    oi_analysis, vwap_analysis,
                    smc_zones, supports, resistances
                )
                # Only add new unique alerts
                existing = set(a['message'] for a in alert_history[idx_name])
                for alert in alerts:
                    if alert['message'] not in existing:
                        alert_history[idx_name].append(alert)
                
                market_data[idx_name]['alerts'] = list(alert_history[idx_name])
                
                # 6. Generate trade suggestion
                trade_suggestion = generate_trade_suggestion(
                    idx_name, market_data[idx_name],
                    oi_analysis, vwap_analysis,
                    smc_zones, supports, resistances, vix
                )
                market_data[idx_name]['trade_suggestion'] = trade_suggestion
                
                # 7. Generate dashboard
                dashboard = generate_dashboard(
                    idx_name, market_data[idx_name],
                    supports, resistances,
                    oi_analysis, vwap_analysis,
                    trade_suggestion, vix,
                    candles_5m
                )
                market_data[idx_name]['dashboard'] = dashboard
                
                # 8. Market breadth (approximate)
                market_data[idx_name]['market_breadth'] = {
                    'advance_decline': 'Positive' if market_data[idx_name]['change'] > 0 else 'Negative',
                    'bank_nifty_correlation': 'Aligned' if (idx_name == 'NIFTY' and market_data['NIFTY']['change'] > 0) else 'Diverging'
                }
                
                # 9. Institutional positioning (from OI data)
                market_data[idx_name]['institutional'] = {
                    'fii_bias': 'Long' if oi_analysis.get('pcr', 1) > 1.1 else ('Short' if oi_analysis.get('pcr', 1) < 0.9 else 'Neutral'),
                    'futures_buildup': 'Long buildup' if oi_analysis.get('pe_oi_change', 0) > 0 else 'Short buildup',
                    'directional_bias': 'Bullish' if oi_analysis.get('pcr', 1) > 1.0 else 'Bearish'
                }
            
            # Emit data to all connected clients
            payload = {
                'NIFTY': serialize_market_data('NIFTY'),
                'SENSEX': serialize_market_data('SENSEX'),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            socketio.emit('market_update', payload)
            
            # Wait before next fetch (Upstox rate limits: ~1 req/sec per endpoint)
            time.sleep(5)
            
        except Exception as e:
            print(f"[ERROR] Data loop error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)
    
    print("[INFO] Data fetch loop stopped")


def serialize_market_data(idx_name):
    """Serialize market data for JSON transport."""
    d = market_data[idx_name]
    
    # Convert candles - limit to last 100
    def format_candles(candles):
        formatted = []
        for c in candles[-100:]:
            formatted.append({
                'time': c[0] if isinstance(c[0], str) else str(c[0]),
                'open': c[1],
                'high': c[2],
                'low': c[3],
                'close': c[4],
                'volume': c[5] if len(c) > 5 else 0
            })
        return formatted
    
    return {
        'ltp': d['ltp'],
        'open': d['open'],
        'high': d['high'],
        'low': d['low'],
        'close': d['close'],
        'prev_close': d['prev_close'],
        'prev_high': d.get('prev_high', 0),
        'prev_low': d.get('prev_low', 0),
        'volume': d['volume'],
        'change': d['change'],
        'change_pct': d['change_pct'],
        'vwap': d['vwap'],
        'vix': d['vix'],
        'candles_3m': format_candles(d['candles_3m']),
        'candles_5m': format_candles(d['candles_5m']),
        'option_chain': d['option_chain'],
        'oi_analysis': d['oi_analysis'],
        'supports': d['supports'],
        'resistances': d['resistances'],
        'smc_zones': d['smc_zones'],
        'alerts': d['alerts'],
        'trade_suggestion': d['trade_suggestion'],
        'dashboard': d['dashboard'],
        'market_breadth': d['market_breadth'],
        'institutional': d['institutional']
    }


# ============================================================
# Flask Routes
# ============================================================

@app.route('/')
def index():
    """Serve the frontend."""
    return send_from_directory('.', 'index.html')


@app.route('/api/set_token', methods=['POST'])
def set_token():
    """Set the Upstox access token."""
    global ACCESS_TOKEN, DATA_THREAD, THREAD_RUNNING
    
    data = request.json
    token = data.get('access_token', '').strip()
    
    if not token:
        return jsonify({'status': 'error', 'message': 'Access token is required'}), 400
    
    ACCESS_TOKEN = token
    
    # Validate token by making a test call
    try:
        url = 'https://api.upstox.com/v2/market-quote/ltp?instrument_key=NSE_INDEX|Nifty 50'
        resp = requests.get(url, headers=upstox_headers(), timeout=10)
        if resp.status_code != 200:
            return jsonify({'status': 'error', 'message': f'Invalid token. API returned: {resp.status_code}'}), 401
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Connection error: {str(e)}'}), 500
    
    # Start data thread if not running
    if not THREAD_RUNNING:
        THREAD_RUNNING = True
        DATA_THREAD = threading.Thread(target=data_fetch_loop, daemon=True)
        DATA_THREAD.start()
        print("[INFO] Data thread started")
    
    return jsonify({'status': 'success', 'message': 'Token set successfully. Live data starting...'})


@app.route('/api/status')
def api_status():
    """Check API connection status."""
    return jsonify({
        'connected': ACCESS_TOKEN is not None,
        'thread_running': THREAD_RUNNING,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })


# ============================================================
# SocketIO Events
# ============================================================

@socketio.on('connect')
def handle_connect():
    """Handle client connection."""
    print(f"[INFO] Client connected")
    if ACCESS_TOKEN and THREAD_RUNNING:
        # Send initial data
        payload = {
            'NIFTY': serialize_market_data('NIFTY'),
            'SENSEX': serialize_market_data('SENSEX'),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        emit('market_update', payload)


@socketio.on('disconnect')
def handle_disconnect():
    print(f"[INFO] Client disconnected")


@socketio.on('request_data')
def handle_request_data():
    """Handle manual data request from client."""
    if ACCESS_TOKEN:
        payload = {
            'NIFTY': serialize_market_data('NIFTY'),
            'SENSEX': serialize_market_data('SENSEX'),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        emit('market_update', payload)


# ============================================================
# Run the App
# ============================================================

if __name__ == '__main__':
    print("=" * 60)
    print("  NIFTY & SENSEX Trading Intelligence System")
    print("  Open http://localhost:5000 in your browser")
    print("  Enter your Upstox Access Token in the UI")
    print("=" * 60)
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
