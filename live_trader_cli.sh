#!/bin/bash

#sudo BINANCE_KEY=$BINANCE_KEY BINANCE_SECRET=$BINANCE_SECRET docker run --name ta_trader_LTC -d --env BINANCE_KEY --env BINANCE_SECRET ta_trader_img WillRBband_LTC_3m_60m

if [ "$1" == --start ]; then
	ctr_name=ta_trader_$2
    if [ "$3" == --build ]; then
        sudo AWS_SNS_KEY=$AWS_SNS_KEY AWS_SNS_SECRET=$AWS_SNS_SECRET \
            BINANCE_KEY=$BINANCE_KEY BINANCE_SECRET=$BINANCE_SECRET \
            docker-compose up -d --build $ctr_name
    else
        sudo docker-compose start $ctr_name
    fi
elif [ "$1" == --stop ]; then
	ctr_name=ta_trader_$2
	sudo docker-compose stop $ctr_name
elif [ "$1" == --show ]; then
	sudo docker ps --filter "name=ta_trader_"
else
    echo 'Manage per-token trader instances. Options:'
    echo '    --start <token> '
    echo '    --start <token> --build '
    echo '    --stop <token> '
    echo '    --show '
fi
