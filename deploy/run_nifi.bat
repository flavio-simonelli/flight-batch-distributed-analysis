@echo off
curl -v -X POST http://nifi-node.flight-analysis.remote:8085/experiment ^
     -H "Content-Type: application/json" ^
     -d @nifi.json