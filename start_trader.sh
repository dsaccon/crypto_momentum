#!/bin/bash

#sudo BINANCE_KEY=$BINANCE_KEY BINANCE_SECRET=$BINANCE_SECRET docker run --name ta_trader_LTC -d --env BINANCE_KEY --env BINANCE_SECRET ta_trader_img WillRBband_LTC_3m_60m

ctr_name=ta_trader_$1
sudo AWS_SNS_KEY=$AWS_SNS_KEY AWS_SNS_SECRET=$AWS_SNS_SECRET BINANCE_KEY=$BINANCE_KEY BINANCE_SECRET=$BINANCE_SECRET docker-compose up -d $ctr_name
