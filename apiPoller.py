import aiohttp
import asyncio
import json
import os, sys
import threading
import pika
import json
import pymongo
from urllib3.exceptions import HTTPError

from aiohttp import ClientSession

ENDPOINT1_URL = "https://randomuser.me/api/"
ENDPOINT2_URL = "https://www.thecocktaildb.com/api/json/v1/1/random.php"

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["TechStax"]

def save():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='data')

    def callback(ch, method, properties, body):
        responses = json.loads(body)
        for resp in responses:
            if "error" in resp:
                print("error")
            elif "drinks" in resp:
                mycol = mydb['drinks']
                if mycol.count() == 0:
                    mycol.insert_one(resp)
                else:
                    mycol.replace_one({"_id":mycol.find()[0]["_id"]},resp,)
            elif "results" in resp:
                mycol = mydb["user"]
                for user in resp["results"]:
                    if mycol.count() == 0:
                        mycol.insert_one(user)
                    else:
                        query = {"name" : user["name"]}
                        mycol.replace_one(query,user, upsert=True)
        print(" [x] Received")

    channel.basic_consume(queue='data', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

def push(responses):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='data')
    channel.basic_publish(exchange='',
                        routing_key='data',
                        body=json.dumps(responses))
    print(" [x] Sent 'Hello World!'")
    connection.close()

async def make_request(url, session):
    try:
        response = await session.request(method='GET', url=url)
    except HTTPError as http_err:
        return {"error":f"{http_err}"}
    except Exception as err:
        return {"error":f"{err}"}
    response_json = await response.json()
    return response_json

async def get_data():
    async with aiohttp.ClientSession() as session:
        tasks = []
        tasks.append(make_request(ENDPOINT1_URL,session))
        tasks.append(make_request(ENDPOINT2_URL,session))
        responses = await asyncio.gather(*tasks,return_exceptions=True)
        push(responses)

def main():
    # loop = asyncio.get_event_loop()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    responses = loop.run_until_complete(get_data())
    loop.close()
    try:
        threading.Timer(5.0, main).start()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

if __name__ == '__main__':
    try:
        t = threading.Thread(target = save) 
        t.daemon =True
        t.start()  
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)



