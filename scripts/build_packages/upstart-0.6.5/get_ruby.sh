#!/bin/bash

set -e
set -x

MAJOR_VERSION="1.9"
MINOR_VERSION="${MAJOR_VERSION}.3-p194"

cd /tmp 
yum install -y wget
wget http://ftp.ruby-lang.org/pub/ruby/${MAJOR_VERSION}/ruby-${MINOR_VERSION}.tar.gz
tar -xvzf $BASE_PATH/scripts/build_packages/upstart-0.6.5/ruby-${MINOR_VERSION}.tar.gz
cd /tmp/ruby-${MINOR_VERSION}
./configure
make
make install
