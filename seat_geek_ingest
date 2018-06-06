import requests
from elasticsearch import Elasticsearch
import json
import os


es = Elasticsearch()

# TODO: implement pagination like use total and keep looking til you get total records
# TODO: setup cronjob

mappings = {
    "seat_geek": {
        "properties": {
            "geo": {
                 "properties": {
                     "location": {
                         "type": "geo_point"
                     }
                 }
             }
        }
    }
}
mappings ={    'mappings': {
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

# ...
# es_entries['geo'] = { 'location': str(data['_longitude_'])+","+str(data['_latitude_'])}
# ...
# es.index(index="geodata", doc_type="doc", body=es_entries)




def ingest_events(page, per_page=1000):
    # api call
    response = requests.get('https://api.seatgeek.com/2/events?per_page=%s&page=%s' % (per_page, page), auth=(os.environ["GS_CLIENT_ID"], os.environ["GS_CLIENT_SECRET"]))
    # write to elastic search
    response_json = response.json()
    print(response_json['meta'])
    print(len(response_json["events"]))
    if not response_json["events"]:
        raise StopIteration
    for event in response_json["events"]:
        location = event["venue"]["location"]
        l = {"location": location}
        event.update(l)
        es.index(index='sg2', doc_type='seat_geek', id=event["id"], body=event)


page = 1
while True:
    try:
        ingest_events(page)
    except StopIteration:
        break
    page += 1


print("data ingest complete")
