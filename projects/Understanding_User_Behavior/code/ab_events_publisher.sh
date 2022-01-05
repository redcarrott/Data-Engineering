#!/bin/bash

function get_events(){
NUM=$(($RANDOM % 4 + 1))
event=''
if [ $NUM -eq 1 ]; then
event='http://localhost:5000/join_a_restaurant'
elif [ $NUM -eq 2 ]; then
event='http://localhost:5000/purchase_an_ingredient'
elif [ $NUM -eq 3 ]; then
event='http://localhost:5000/enter_a_contest'
else
event='http://localhost:5000/'
fi
docker-compose exec mids ab -n 1 -H "Host: ${1}" $event
}

function post_events(){
NUM=$(($RANDOM % 3 + 1))
event=''
if [ $NUM -eq 1 ]; then
event='http://localhost:5000/join_a_restaurant'
ROW=$(( $RANDOM % $(cat restaurant_events.txt | wc -l) + 1))
awk "NR==$ROW" restaurant_events.txt > ~/w205/event.txt
docker-compose exec mids ab -n 1 -p event.txt -T application/json -H "Host: ${1}" $event
elif [ $NUM -eq 2 ]; then
event='http://localhost:5000/purchase_a_ingredient'
ROW=$(( $RANDOM % $(cat ingredient_events.txt | wc -l) + 1))
awk "NR==$ROW" ingredient_events.txt > ~/w205/event.txt
docker-compose exec mids ab -n 1 -p event.txt -T application/json -H "Host: ${1}" $event
else
event='http://localhost:5000/enter_a_contest'
ROW=$(( $RANDOM % $(cat bcontest_events.txt | wc -l) + 1))
awk "NR==$ROW" contest_events.txt > ~/w205/event.txt
docker-compose exec mids ab -n 1 -p event.txt -T application/json -H "Host: ${1}" $event
fi
}

function hosts(){
user=$(($RANDOM % 50 + 1))
NUM=$(($RANDOM % 5 + 1))
if [ $NUM -eq 1 ]; then
local hosts="user$user.att.com"
echo "$hosts"
elif [ $NUM -eq 2 ]; then
local hosts="user$user.comcast.com"
echo "$hosts"
elif [ $NUM -eq 3 ]; then
local hosts="user$user.verizon.com"
echo "$hosts"
elif [ $NUM -eq 4 ]; then
local hosts="user$user.sprint.com"
echo "$hosts"
else
local hosts="localhost"
echo "$hosts"
fi
}

while true; do

HOST=$(hosts)
RAND=$(($RANDOM % 15 + 1))
if [ $RAND -gt 5 ]; then
post_events $HOST
else
get_events $HOST
fi

sleep 2
done