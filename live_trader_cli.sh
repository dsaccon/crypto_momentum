#!/bin/bash

#sudo BINANCE_KEY=$BINANCE_KEY BINANCE_SECRET=$BINANCE_SECRET docker run --name ta_trader_LTC -d --env BINANCE_KEY --env BINANCE_SECRET ta_trader_img WillRBband_LTC_3m_60m

if [ "$1" == --start ]; then
	ctr_name=ta_trader_$2
    str=$(sudo docker ps --filter "name=$ctr_name" | grep -v CREATED)
    if [[ "$str" == *"$ctr_name"* ]]; then
        # Stop container if its already running
        sudo docker stop $ctr_name
    fi
    str=$(sudo docker ps -a --filter "name=$ctr_name" | grep -v CREATED)
    if [[ "$str" == *"$ctr_name"* ]]; then
        # Remove container if it already exists
        sudo docker rm $ctr_name
    fi
    if [ "$3" == --build ]; then
        suffix='--build '$ctr_name
    else
        suffix=$ctr_name
    fi
    sudo INFLUXDB_ADDR=$INFLUXDB_ADDR INFLUXDB_PW=$INFLUXDB_PW INFLUXDB_USER=$INFLUXDB_USER \
        AWS_SNS_KEY=$AWS_SNS_KEY AWS_SNS_SECRET=$AWS_SNS_SECRET \
        BINANCE_KEY=$BINANCE_KEY BINANCE_SECRET=$BINANCE_SECRET \
        docker-compose up -d $suffix
elif [ "$1" == --stop ]; then
	ctr_name=ta_trader_$2
	sudo docker-compose stop $ctr_name
elif [ "$1" == --stop-all ]; then
	sudo docker-compose down
elif [ "$1" == --show-running ]; then
	sudo docker ps --filter "name=ta_trader_"
elif [ "$1" == --show-live-status ]; then
    symbols=''
	str=$(sudo docker ps -a --filter "name=ta_trader_" | grep ta_trader_)
    for s in $str
    do
        s_filtered=$(echo $s | grep -v ta_trader_img)
        if [[ "$s_filtered" == *"ta_trader_"* ]]; then
            len_s=${#s_filtered}
            tkn=${s:10:$len_s-1}
            symbols="$symbols $tkn"
        fi
    done
    python live_trader.py --live-status $symbols
elif [ "$1" == --close-position ]; then
    args=$@
    len_args=${#args}
    _args=${args:17:$len_args}
    python live_trader.py --close-position $_args
elif [ "$1" == --show-log ]; then
    tail -f logs/docker/$2/live_trader.log
else
    echo 'Manage per-token trader instances. Options:'
    echo '    --start <token>                       (Start token, will restart if already running)'
    echo '    --start <token> --build               (Start token, build first (i.e. include any recent changes)'
    echo '    --stop <token>                        (Stop token)'
    echo '    --stop-all                            (Stop all running tokens)'
    echo '    --show-running                        (Show all running tokens)'
    echo '    --show-live-status                    (Show uPNL of any open positions, and desk NL'
    echo '    --close-position <token> ... <token>  (Close token position, if open)'
    echo '    --show-log <token>                    (Show live trader log)'
fi
