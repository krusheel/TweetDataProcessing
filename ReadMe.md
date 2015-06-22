Introduction

This project extracts youtube video urls in tweet data and get its statistics.
It consists of the following 
1. A thread which can read tweet data from a json file and writes the text data in the tweet to RabbitMQ.
2. A worker which read text data from RabbitMQ, extract youtube urls from tweet data and fetch its statistics
 
