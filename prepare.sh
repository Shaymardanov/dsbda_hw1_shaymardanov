#!/bin/bash

hadoop fs -mkdir /hw1
hadoop fs -mkdir /hw1/input1
hadoop fs -mkdir /hw1/input2

hadoop fs -rm /hw1/input1/*
hadoop fs -rm /hw1/input2/*

hadoop fs -put ./storage/log1.txt /hw1/input1
hadoop fs -put ./storage/ip.txt   /hw1/input2
