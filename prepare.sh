#!/bin/bash

hadoop fs -mkdir /lab1
hadoop fs -mkdir /lab1/input1
hadoop fs -mkdir /lab1/input2

hadoop fs -rm /lab1/input1/*
hadoop fs -rm /lab1/input2/*

hadoop fs -put ./storage/log1.txt /lab1/input1
hadoop fs -put ./storage/ip.txt   /lab1/input2
