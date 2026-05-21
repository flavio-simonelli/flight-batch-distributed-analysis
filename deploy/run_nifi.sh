#!/bin/bash

curl -v -X POST http://localhost:8085/experiment \
     -H "Content-Type: application/json" \
     -d @nifi.json