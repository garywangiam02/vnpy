#!/bin/bash
echo "begin build trade-lite-py"
docker build -t friky/trade-lite-py38:1.0.0 -f Dockerfile.base .
echo "begin push trade-lite-py"
docker push friky/trade-lite-py38:1.0.0
