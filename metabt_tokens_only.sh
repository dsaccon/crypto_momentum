for token in
{BTCUSDT,ETHUSDT,BCHUSDT,XRPUSDT,EOSUSDT,LTCUSDT,TRXUSDT,ETCUSDT,LINKUSDT,XLMUSDT,ADAUSDT,XMRUSDT,DASHUSDT,ZECUSDT,XTZUSDT,BNBUSDT}

do
  date +%Y%m%d%H%M%S
  python backtester.py -n WillRBband_BTC_3m_60m --symbol $token
  python ta2_stats_reportonly.py
done
