#!/bin/bash

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Job started."

hadoop fs -rm -r /lab1/output

yarn jar ./target/lab1-1.0.jar ru.mephi.bsbda.MainCounter /lab1/input1 /lab1/input2 /lab1/output


echo " RESULTS: "
echo ""

hadoop fs -text /lab1/output/part-r-*

echo ""
echo " END."
