#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')

def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())

@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"

@app.route("/purchase_an_ingredient", methods = ['GET', 'POST'])
def purchase_an_ingredient():
    if request.method == 'GET':
        purchase_ingredient_event = {'event_type': 'purchase_ingredient'}
        log_to_kafka('events', purchase_ingredient_event)
        return "Ingredient Purchased!\n"
    else:
        if request.headers['Content-Type'] == 'application/json':
            purchase_ingredient_event = {'event_type': 'purchase_ingredient', 'attributes': json.dumps(request.json)}
            log_to_kafka('events', purchase_ingredient_event)
            return "Ingredient Purchased!" + json.dumps(request.json) + "\n"

@app.route("/join_a_restaurant", methods = ['GET', 'POST'])
def join_a_restaurant():
    if request.method == 'GET':
        join_restaurant_event = {'event_type': 'join_restaurant'}
        log_to_kafka('events', join_restaurant_event)
        return "Joined a Restaurant!\n"
    else:
        if request.headers['Content-Type'] == 'application/json':
            join_restaurant_event = {'event_type': 'join_restaurant', 'attributes': json.dumps(request.json)}
            log_to_kafka('events', join_restaurant_event)
            return "Joined a Restaurant!" + json.dumps(request.json) + "\n"

@app.route("/enter_a_contest", methods = ['GET','POST'])
def enter_contest():
    if request.method == 'GET':
        enter_contest_event = {'event_type': 'enter_contest'}
        log_to_kafka('events', enter_contest_event)
        return "Entered a Contest!\n"
    else:
        if request.headers['Content-Type'] == 'application/json':
            enter_contest_event = {'event_type': 'enter_contest', 'attributes': json.dumps(request.json)}
            log_to_kafka('events', enter_contest_event)
            return "Entered a Contest!" + json.dumps(request.json) + "\n"