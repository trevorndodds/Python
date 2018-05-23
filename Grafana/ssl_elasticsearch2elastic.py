#!/usr/bin/env python
import datetime
import time
import json
import os
import sys
import requests

# Username and password for authentication against searchguard
AuthUser = "admin"
AuthPassword = "admin"
VerifyCert = False

# ElasticSearch Cluster to Monitor
elasticServer = os.environ.get('ES_METRICS_CLUSTER_URL', 'https://server1:9200')
interval = int(os.environ.get('ES_METRICS_INTERVAL', '60'))

# ElasticSearch Cluster to Send Metrics
elasticIndex = os.environ.get('ES_METRICS_INDEX_NAME', 'elasticsearch_metrics')
elasticMonitoringCluster = os.environ.get('ES_METRICS_MONITORING_CLUSTER_URL', 'https://server2:9200')

def fetch_clusterhealth():
    try:
        utc_datetime = datetime.datetime.utcnow()
        endpoint = "/_cluster/health"
        urlData = elasticServer + endpoint
        response = requests.get(urlData, verify=VerifyCert, auth=requests.auth.HTTPBasicAuth(AuthUser, AuthPassword))
        jsonData = response.json()
        clusterName = jsonData['cluster_name']
        jsonData['@timestamp'] = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
        if jsonData['status'] == 'green':
            jsonData['status_code'] = 0
        elif jsonData['status'] == 'yellow':
            jsonData['status_code'] = 1
        elif jsonData['status'] == 'red':
            jsonData['status_code'] = 2
        post_data(jsonData)
        return clusterName
    except IOError as err:
        print "IOError: Maybe can't connect to elasticsearch: {0}".format(sys.exc_info()[1])
        clusterName = "unknown"
        return clusterName

def fetch_clusterstats():
    utc_datetime = datetime.datetime.utcnow()
    endpoint = "/_cluster/stats"
    urlData = elasticServer + endpoint
    response = requests.get(urlData, verify=VerifyCert, auth=requests.auth.HTTPBasicAuth(AuthUser, AuthPassword))
    jsonData = response.json()
    jsonData['@timestamp'] = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
    post_data(jsonData)

def fetch_nodestats(clusterName):
    utc_datetime = datetime.datetime.utcnow()
    endpoint = "/_cat/nodes?v&h=n"
    urlData = elasticServer + endpoint
    response = requests.get(urlData, verify=VerifyCert, auth=requests.auth.HTTPBasicAuth(AuthUser, AuthPassword))
    nodes = response.text[1:-1].strip().split('\n')
    for node in nodes:
        endpoint = "/_nodes/%s/stats" % node.rstrip()
        urlData = elasticServer + endpoint
        response = requests.get(urlData, verify=VerifyCert, auth=requests.auth.HTTPBasicAuth(AuthUser, AuthPassword))
        jsonData = response.json()
        nodeID = jsonData['nodes'].keys()
        try:
            jsonData['nodes'][nodeID[0]]['@timestamp'] = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
            jsonData['nodes'][nodeID[0]]['cluster_name'] = clusterName
            newJsonData = jsonData['nodes'][nodeID[0]]
            post_data(newJsonData)
        except:
            continue

def fetch_indexstats(clusterName):
    utc_datetime = datetime.datetime.utcnow()
    endpoint = "/_stats"
    urlData = elasticServer + endpoint
    response = requests.get(urlData, verify=VerifyCert, auth=requests.auth.HTTPBasicAuth(AuthUser, AuthPassword))
    jsonData = response.json()
    jsonData['_all']['@timestamp'] = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
    jsonData['_all']['cluster_name'] = clusterName
    post_data(jsonData['_all'])

def post_data(data):
    utc_datetime = datetime.datetime.utcnow()
    url_parameters = {'cluster': elasticMonitoringCluster, 'index': elasticIndex,
        'index_period': utc_datetime.strftime("%Y.%m.%d"), }
    url = "%(cluster)s/%(index)s-%(index_period)s/message" % url_parameters
    headers = {'content-type': 'application/json'}
    try:
        req = requests.post(url, verify=VerifyCert, headers=headers, data=json.dumps(data), auth=requests.auth.HTTPBasicAuth(AuthUser, AuthPassword))
    except Exception as e:
        print "Error: {}".format(str(e))

def main():
    clusterName = fetch_clusterhealth()
    if clusterName != "unknown":
        fetch_clusterstats()
        fetch_nodestats(clusterName)
        fetch_indexstats(clusterName)

if __name__ == '__main__':
    try:
        nextRun = 0
        while True:
            if time.time() >= nextRun:
                nextRun = time.time() + interval
                now = time.time()
                main()
                elapsed = time.time() - now
                print "Total Elapsed Time: %s" % elapsed
                timeDiff = nextRun - time.time()
 
                # Check timediff , if timediff >=0 sleep, if < 0 send metrics to es
                if timeDiff >= 0:
                    time.sleep(timeDiff)

    except KeyboardInterrupt:
        print 'Interrupted'
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
