#!/bin/bash
cd "$(dirname "$0")"

cp Main.js ~/Library/Application\ Support/com.dmitrynikolaev.numi/extensions/
cp Default.js ~/Library/Application\ Support/com.dmitrynikolaev.numi/extensions/
python3.5 main.py
$SHELL
