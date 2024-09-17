#!/bin/bash


spark-submit --driver-java-options=-Dlog4j.configuration=file:./log4j.properties 01-anime-explore.py
