# https://www.thethingsnetwork.org/forum/t/a-python-program-to-listen-to-your-devices-with-mqtt/9036/6
# Get data from MQTT server
# Run this with python 3, install paho.mqtt prior to use

import paho.mqtt.client as mqtt
import json
import mysql.connector


#Database Creation
mydb = mysql.connector.connect(
    host = "localhost",
    user="root",
    password="0077",
    database="CarBayAnalyzer"
)
mycursor = mydb.cursor()

#mycursor.execute("CREATE TABLE readings (number INTEGER(10), date VARCHAR(255), time VARCHAR(255), status VARCHAR(255))")






import base64

APPEUI = "70B3D57ED00229D8"
APPID  = "smartparkuwa"
PSW    = 'ttn-account-v2.ybyEYGJ3uY3O7J6NIVW5HMmVfcarcGxV20P7C6gztlA'

# gives connection message
def on_connect(mqttc, mosq, obj,rc):
    print("Connected with result code:"+str(rc))
    # subscribe for all devices of user
    mqttc.subscribe('smartparkuwa/devices/+/up')

# gives message from device
def on_message(mqttc,obj,msg):
    try:
        # print(msg.payload)
        x = json.loads(msg.payload.decode('utf-8'))
        device = x["dev_id"]
        counter = x["counter"]
        payload_raw = x["payload_raw"]
        payload_fields = x["payload_fields"]
        datetime = x["metadata"]["time"]
        gateways = x["metadata"]["gateways"]
        # print for every gateway that has received the message and extract RSSI
        for gw in gateways:
            gateway_id = gw["gtw_id"]
            rssi = gw["rssi"]
           # print(datetime + ", " + device + ", " + str(counter) + ", "+ gateway_id + ", "+ str(rssi) + ", " + str(payload_fields))
            sqlFormula = "INSERT INTO readings (number, date, time, status) VALUES (%s, %s, %s, %s)"
            counter_c = str(counter)
            datetime_c= str(datetime)
            date = datetime_c[0:9]
            time = datetime_c[11:18]
            payload_fields_c = str(payload_fields)
            status = payload_fields_c[-2]
            readings = [(counter_c, date, time, status)]
            mycursor.executemany(sqlFormula, readings)
            mydb.commit()
        #
        #
        #
        #
        #
        #
        #
    except Exception as e:
        print(e)
        pass

def on_publish(mosq, obj, mid):
    print("mid: " + str(mid))

def on_subscribe(mosq, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

def on_log(mqttc,obj,level,buf):
    print("message:" + str(buf))
    print("userdata:" + str(obj))

mqttc= mqtt.Client()
# Assign event callbacks
mqttc.on_connect=on_connect
mqttc.on_subscribe=on_subscribe
mqttc.on_message=on_message

mqttc.username_pw_set(APPID, PSW)
mqttc.connect("thethings.meshed.com.au",1883,60)

# and listen to server
run = True
while run:
    mqttc.loop()

