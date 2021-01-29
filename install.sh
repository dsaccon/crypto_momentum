#!/bin/bash

OS=`uname`

if [ "$OS" == 'Darwin' ]; then
    brew update
    brew cask install ta-lib
elif [ "$OS" == 'Linux' ]; then
    sudo apt update
    sudo apt install -y pipenv
    wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
    tar xvzf ta-lib-0.4.0-src.tar.gz
    cd ta-lib
    ./configure --prefix=/usr
    make
    sudo make install
    cd ..
    rm ta-lib-0.4.0-src.tar.gz
		rm -r ta-lib
fi

pipenv install
