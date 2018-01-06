#!/bin/bash
cd "$(dirname "$0")"

cp data.js ~/Library/Application\ Support/com.dmitrynikolaev.numi/extensions/
cp __units.js ~/Library/Application\ Support/com.dmitrynikolaev.numi/extensions/
python3.5 updater.py
#$SHELL
