#!/usr/bin/env python2.7

""" This procedure get wazes object from web service  
must be crontabd from system, 20 minutes is enough """

import requests,json
from couchpy.client import Client as cc
from couchpy.doc import Document as cd

req=requests.get('https://www.waze.com/row-rtserver/web/TGeoRSS?ma=600&mj=100&mu=100&left=13.264274597167969&right=13.462028503417969&bottom=38.0825953313728&top=38.165241102984645&_=1489857001718')

dbcon=cc()
db=dbcon['infotraffic']
olddoc=db.Document('waze_infotraffic_doc')
doc=json.loads(req.text)
doc['_id']='waze_infotraffic_doc'
try:
    olddoc.fetch()
    print olddoc['_rev']
    doc['_rev']=olddoc['_rev']
    cdoc=db.Document(doc)
    cdoc.update()
    cdoc.put()
except:
    print 'not fetch'
    cdoc=db.Document(doc)
    cdoc.post()

