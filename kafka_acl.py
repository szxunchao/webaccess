from kafka import KafkaProducer
from kafka.errors import KafkaError,KafkaTimeoutError

import argparse
import json
import re
import time
import schedule
import os
import logging
import atexit
import requests

topic_name='log-analyzer'
filepath='/home/xchen/cloudacl/cloudacl_log_10.txt'

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('acl')
logger.setLevel(logging.DEBUG)

def ipdecoder(ip):
    freegeoip = 'http://freegeoip.net/json/'
    url=freegeoip+ip
    response=requests.get(url)
    return response.json()

def shutdown_hook(producer):
    """
    a shutdown hook to be called before the shutdown
    :param producer: instance of a kafka producer
    :return: None
    """
    try:
        logger.info('Flushing pending messages to kafka, timeout is set to 10s')
        producer.flush(10)
        logger.info('Finish flushing pending messages to kafka')
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
    finally:
        try:
            logger.info('Closing kafka connection')
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection, caused by: %s', e.message)

"""
def shut_down(producer):
    logger.debug('exiting program')
    producer.flush(10)
    producer.close()
    logger.debug('kafka producer closed, exiting')
"""
if __name__ =='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('topic_name',help='the kafka topic push to')
    parser.add_argument('kafka_broker',help='the location of the kafka broker')

    args=parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    producer = KafkaProducer(bootstrap_servers=kafka_broker)
    with open(filepath,'r') as f:
        for line in f:
            tmp=line.encode('utf-8').strip()
            m=re.match(r'(?P<ip>.*?) .*?\[(?P<date>.*?)\].*[&\?]ur[li]=(?P<url>.*?)&cat=(?P<category>.*?)[ &].*', tmp)
            if m != None:
                ip,date,url,cat=m.groups()
                location=ipdecoder(ip)
                city=location['city']
                country=location['country_name']
                state=location['region_code']
            #data = ('country:{},state:{},date:{},url:{},cat:{}'.format(country,state,date,url,cat))
            data = json.dumps(
                {
                'country': country,
                'state':state,
                'date':date,
                'url':url,
                'cat':cat
                }
                )
            producer.send(topic=topic_name,value=data,timestamp_ms=time.time())
    logger.debug('Send log data into %s',topic_name)
#    atexit.register(shutdown_hook, producer)
