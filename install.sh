#!/bin/bash

OS=`uname`

install_talib() {
    # Install ta-lib dependency
    wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
    tar xvzf ta-lib-0.4.0-src.tar.gz
    cd ta-lib
    ./configure --prefix=/usr
    make
    $1 make install
    cd ..
    rm ta-lib-0.4.0-src.tar.gz
    rm -r ta-lib
}

if [ "$OS" == 'Darwin' ]; then
    echo 'Mac not supported. Can only run on Linux'
    exit 0
elif [ "$OS" == 'Linux' ]; then
    USER=`whoami`
    if [ "$USER" == 'root' ]; then
        echo 'Installing as root'
        apt update
        install_talib ''
    else
        sudo apt update
        sudo apt install -y pipenv
        sudo apt install -y software-properties-common
        sudo add-apt-repository ppa:deadsnakes/ppa
        sudo apt install -y python3.9
        sudo apt install python3.9-dev libpq-dev
        install_talib 'sudo'
    fi
fi

if [ "$1" == --no-pipenv-install ]; then
    : # do nothing
else
    pipenv install
fi
