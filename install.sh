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

        sudo apt install python3-pip -y
        pip3 install --user pipenv
        _DIR=`python3 -m site --user-base`
        export PATH=$_DIR/bin:$PATH

        sudo apt install -y software-properties-common
        sudo add-apt-repository ppa:deadsnakes/ppa
        sudo apt install -y python3.9
        sudo apt install python3.9-dev libpq-dev
        install_talib 'sudo'

        # Docker
        sudo apt-get install -y curl apt-transport-https ca-certificates software-properties-common

        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
        echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

        sudo apt-get update
        sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose
    fi
fi

if [ "$1" == --no-pipenv-install ]; then
    : # do nothing
else
    pipenv install
fi
