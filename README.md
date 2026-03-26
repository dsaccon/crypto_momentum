# Crypto Momentum

Automated cryptocurrency momentum trading system supporting backtesting and live trading on Binance futures markets. Uses technical indicators (Williams %R, Bollinger Bands, EMA) to generate trading signals across multiple assets.

## Features

- **Backtesting** — Run historical simulations with configurable strategies, capital, and timeframes
- **Live Trading** — Execute real trades on Binance USDT-margined futures with position management
- **Multi-Asset** — Trade BTC, ETH, BCH, LTC, LINK, XLM, ONT simultaneously
- **Multiple Strategies** — Williams %R + Bollinger Bands, Williams %R + EMA, Heikin-Ashi, EMA + Linear Regression
- **Monitoring** — PnL logging to InfluxDB, trade logs to S3, SNS alerts
- **Containerized** — Docker deployment with per-asset containers

## Project Structure

```
├── backtester.py          # Backtesting engine
├── live_trader.py         # Live trading execution
├── config.json            # Strategy & pair configurations
├── strategies/            # Trading strategy implementations
│   ├── base.py            #   Base strategy class
│   ├── willr_bband.py     #   Williams %R + Bollinger Bands
│   ├── willr_bband_evo.py #   WillR+BBand evolution variant
│   ├── willr_ema.py       #   Williams %R + EMA
│   ├── ha.py              #   Heikin-Ashi
│   └── ema_lrc.py         #   EMA + Linear Regression Curve
├── exchanges/             # Exchange API integrations
│   ├── binance.py         #   Binance futures/spot
│   ├── ib.py              #   Interactive Brokers
│   └── okex.py            #   OKEX
├── utils/                 # Utilities (InfluxDB, S3, SNS, analytics)
├── docker-compose.yml
├── Dockerfile
└── live_trader_cli.sh     # CLI helper for Docker management
```

## Prerequisites

- Python 3.9
- [TA-Lib](https://github.com/mrjbq7/ta-lib) C library
- Pipenv
- Docker & Docker Compose (for containerized deployment)

## Installation

```bash
# Linux only (installs ta-lib, Python 3.9, pipenv, Docker)
./install.sh

# Or manually install dependencies
pipenv install
```

## Configuration

Create a `.env` file in the project root:

```env
PYTHONPATH=${PWD}
BINANCE_KEY=<your-binance-api-key>
BINANCE_SECRET=<your-binance-api-secret>
S3_BUCKET_NAME=atg2-ta-strat
AWS_S3_KEY=<optional>
AWS_S3_SECRET=<optional>
AWS_SNS_KEY=<optional>
AWS_SNS_SECRET=<optional>
INFLUXDB_ADDR=<optional>
INFLUXDB_USER=<optional>
INFLUXDB_PW=<optional>
```

Trading pairs and strategy parameters are defined in `config.json`. Each entry specifies the strategy, candle intervals, capital, leverage, position sizing, and entry/exit thresholds.

## Usage

### Backtesting

```bash
python backtester.py -n WillRBband_BTC_3m_60m
python backtester.py -n WillRBband_BTC_3m_60m --symbol ETHUSDT --period 3m 60m
```

### Live Trading

```bash
# Start trading a config entry
python live_trader.py -n WillRBband_BTC_3m_60m

# Check open positions
python live_trader.py --live-status BTC ETH LTC

# Close positions
python live_trader.py --close-position BTC ETH
```

### Docker Management

```bash
./live_trader_cli.sh --start <token>         # Start a token trader
./live_trader_cli.sh --stop-all              # Stop all traders
./live_trader_cli.sh --show-live-status      # Show P&L of all positions
```

## Strategies

| Strategy | Description |
|----------|-------------|
| **WillRBband** | Williams %R oscillator combined with Bollinger Bands for mean-reversion momentum entries |
| **WillRBbandEvo** | Evolution variant with refined entry/exit logic |
| **WillREma** | Williams %R with EMA trend filter |
| **HeikinAshi** | Heikin-Ashi candle pattern recognition |
| **EmaLrc** | EMA crossover with Linear Regression Curve confirmation |
