#!/bin/bash

OS=`uname`

if [ "$OS" == 'Darwin' ]; then
    brew update
    brew cask install ta-lib
elif [ "$OS" == 'Linux' ]; then
    USER=`whoami`
    if [ "$USER" == 'root' ]; then
        PREFIX=''
    else
        PREFIX='sudo '
    fi
    $PREFIX apt update
    $PREFIX apt install -y pipenv
    wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
    tar xvzf ta-lib-0.4.0-src.tar.gz
    cd ta-lib
    ./configure --prefix=/usr
    make
    $PREFIX make install
    cd ..
    rm ta-lib-0.4.0-src.tar.gz
    rm -r ta-lib
fi

if [ "$1" == --no-pipenv-install ]; then
    : # do nothing
else
    pipenv install
fi
