#!/bin/bash
set -eu

# DEBUG
set -x

git clone --branch=0.9.0 https://github.com/pmem/rpma.git
mkdir -p rpma/build
cd rpma/build
cmake .. \ 
	-DCMAKE_BUILD_TYPE=Release \
	-DCMAKE_INSTALL_PREFIX=/usr \
	-DBUILD_DOC=OFF \
	-DBUILD_EXAMPLES=OFF \
	-DBUILD_TESTS=OFF
make -j2
sudo make -j2 install
cd ../..
rm -r rpma
