for token in {BTCUSDT,ETHUSDT,BCHUSDT,XRPUSDT,EOSUSDT,LTCUSDT,TRXUSDT,ETCUSDT,LINKUSDT,XLMUSDT,ADAUSDT,XMRUSDT,DASHUSDT,ZECUSDT,XTZUSDT,BNBUSDT}

do
  python backtester.py -n WillRBband_BTC_3m_60m --symbol $token --start 2021 3 5 --end 2021 4 7
  python ta2_stats_reportonly.py

  python backtester.py -n WillRBband_BTC_3m_60m --symbol $token --start 2021 2 10 --end 2021 3 5
  python ta2_stats_reportonly.py

  python backtester.py -n WillRBband_BTC_3m_60m --symbol $token --start 2020 12 26 --end 2021 2 10
  python ta2_stats_reportonly.py

  python backtester.py -n WillRBband_BTC_3m_60m --symbol $token --start 2020 12 1 --end 2020 12 21
  python ta2_stats_reportonly.py

  python backtester.py -n WillRBband_BTC_3m_60m --symbol $token --start 2020 6 29 --end 2020 11 29
  python ta2_stats_reportonly.py

  python backtester.py -n WillRBband_BTC_3m_60m --symbol $token --start 2020 4 26 --end 2020 6 27
  python ta2_stats_reportonly.py

  python backtester.py -n WillRBband_BTC_3m_60m --symbol $token --start 2020 3 6 --end 2020 4 23
  python ta2_stats_reportonly.py

  python backtester.py -n WillRBband_BTC_3m_60m --symbol $token --start 2020 2 20 --end 2020 3 4
  python ta2_stats_reportonly.py

  python backtester.py -n WillRBband_BTC_3m_60m --symbol $token --start 2020 2 10 --end 2020 2 19
  python ta2_stats_reportonly.py

done

