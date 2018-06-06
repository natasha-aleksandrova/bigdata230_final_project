import requests
from elasticsearch import Elasticsearch
import json
import os


es = Elasticsearch()

mappings = {
	'mappings': {
		"seat_geek": {
			"properties": {
				"location": {
					"type": "geo_point"
				}
			}
		}
	}
}


try:
    es.indices.create(index='sg2', body=mappings)
except Exception as e:
    print(e)


def ingest_events(page, per_page=1000):
    response = requests.get('https://api.seatgeek.com/2/events?per_page=%s&page=%s' % (per_page, page), auth=(os.environ["GS_CLIENT_ID"], os.environ["GS_CLIENT_SECRET"]))
    response_json = response.json()
    
    print(response_json['meta'])
    print(len(response_json["events"]))
    
    if not response_json["events"]:
        raise StopIteration
        
    for event in response_json["events"]:
        event["location"] = event["venue"]["location"]
        es.index(index='sg2', doc_type='seat_geek', id=event["id"], body=event)


page = 1
while True:
    try:
        ingest_events(page)
    except StopIteration:
        break
    page += 1


print("data ingest complete")
