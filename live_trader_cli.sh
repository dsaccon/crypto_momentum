#!/bin/bash

#sudo BINANCE_KEY=$BINANCE_KEY BINANCE_SECRET=$BINANCE_SECRET docker run --name ta_trader_LTC -d --env BINANCE_KEY --env BINANCE_SECRET ta_trader_img WillRBband_LTC_3m_60m

if [ "$1" == --start ]; then
	ctr_name=ta_trader_$2
    if [ "$3" == --build ]; then
        sudo docker stop $ctr_name
        sudo docker rm $ctr_name
        sudo AWS_SNS_KEY=$AWS_SNS_KEY AWS_SNS_SECRET=$AWS_SNS_SECRET \
            BINANCE_KEY=$BINANCE_KEY BINANCE_SECRET=$BINANCE_SECRET \
            docker-compose up -d --build $ctr_name
    else
	    str=$(sudo docker ps -a --filter "name=$ctr_name" | grep -v CREATED)
        if [[ "$str" == *"$ctr_name"* ]]; then
            # Container already exists. Just start it up
            sudo docker-compose start $ctr_name
        else
            # Container does not exist already. Bring a new one up
            sudo AWS_SNS_KEY=$AWS_SNS_KEY AWS_SNS_SECRET=$AWS_SNS_SECRET \
                BINANCE_KEY=$BINANCE_KEY BINANCE_SECRET=$BINANCE_SECRET \
                docker-compose up -d $ctr_name
        fi
    fi
elif [ "$1" == --stop ]; then
	ctr_name=ta_trader_$2
	sudo docker-compose stop $ctr_name
elif [ "$1" == --show-running ]; then
	sudo docker ps --filter "name=ta_trader_"
elif [ "$1" == --show-log ]; then
    tail -f logs/docker/$2/live_trader.log
else
    echo 'Manage per-token trader instances. Options:'
    echo '    --start <token> '
    echo '    --start <token> --build '
    echo '    --stop <token> '
    echo '    --show-running '
    echo '    --show-log <token> '
fi
