#!/usr/bin/env python2.7

""" Update a catalog RDF object, serialize and put as downloadable document """

import json,threading
import requests as requ
from datetime import datetime as dt

from rdflib import Namespace,Graph, Literal, BNode, RDF, plugin
from rdflib.namespace import FOAF, DC, XSD
from rdflib.extras.describer import Describer
from rdflib.serializer import Serializer

# compile temporal and distribution attribute for mediatype json, and return dataset template for all data
# Catalog
#   Datasets
#     Distribution


# my application intepret json response 
headers={"Accept":"application/json"}
host="192.107.94.227:5984"
date={}
store=Graph()
tns="resource"
trafficresource="_trafficResource"
tweetsresource="_tweetsResource"

DCATAPIT=Namespace("http://dati.gov.it/onto/dcatapit#")
DCAT=Namespace("http://www.w3.org/ns/dcat#")
DCT=Namespace("http://purl.org/dc/terms/")
TNS=Namespace("http://neuron4web.palermo.enea.it/opendata/{0}".format(tns))
#store.bind("dcat",DCAT)
#store.bind("dc",DC)
#store.bind("dct",DCT)
#store.bind("dcatapit",DCATAPIT)
#store.bind("xsd",XSD)
#store.bind("tns",TNS)

def newstore():
    global store
    store=Graph()

    store.bind("dcat",DCAT)
    store.bind("dc",DC)
    store.bind("dct",DCT)
    store.bind("dcatapit",DCATAPIT)
    store.bind("xsd",XSD)
    store.bind("tns",TNS)


lang='en'
def generateCatalog():
    Catalog=Describer(graph=store,about="#catalog",base=TNS)
    Catalog.rdftype(DCAT.Catalog)
    Catalog.rdftype(DCATAPIT.Catalog)
    #Catalog.value(DCT.modified,Literal(millistoiso(dt),datatype=XSD.dateTime))
    #Catalog.value(DCT.issued,Literal(millistoiso(dt),datatype=XSD.dateTime))
    Catalog.rel(p=DCT.publisher,o="http://github.com/imuttley")
    Catalog.rel(p=DCAT.themeTaxonomy,o="http://publications.europa.eu/resource/authority/data-theme")
    Catalog.value(DCT.title,Literal("infotraffic Catalog",lang=lang))
    Catalog.value(DCT.description,Literal("Catalog for datasets captured from Twitter and Waze applications",lang=lang))
    Catalog.rel(p=DCT.language,o="http://publications.europa.eu/resource/authority/language/ENG")
    #Catalog.rel(p=FOAF.homepage,o="http://192.107.94.227/infotraffic/index.html")
    #Catalog.rel(p=DCAT.dataset,o=trafficdataset)
    #Catalog.rel(p=DCAT.dataset,o=tweetsdataset)
    

def generateTrafficDataset(doc,destResource):
    Dataset=Describer(graph=store,about="#"+doc['_rev'],base=TNS)
    Dataset.rdftype(DCAT.Dataset)
    Dataset.value(DCT.title,Literal("waze_infotraffic_doc#"+doc['_rev'],lang=lang))
    Dataset.value(DCT.description,Literal("json data from waze information",lang=lang))
    Dataset.rel(p=DCT.identifier,o="http://dbpedia.org/resource/Traffic")
    
    Distribution=Describer(graph=store)
    Distribution.rdftype(DCAT.Distribution)
    Distribution.rel(p=DCT['format'],o="http://publications.europa.eu/resource/authority/file-type/JSON")
    Distribution.rel(p=DCAT.accessURL,o="http://neuron4web.palermo.enea.it/opendata/{0}/{1}".format(destResource,doc['_rev']))
    Distribution.value(DCAT.byteSize,doc['Content-Length'],datatype=XSD['integer'])
    
    POT=Describer(graph=store)
    POT.rdftype(DCT.PeriodOfTime)
    POT.value(DCATAPIT.startDate,Literal(millistoiso(doc['startTimeMillis']),datatype=XSD.dateTime))
    POT.value(DCATAPIT.endDate,Literal(millistoiso(doc['endTimeMillis']),datatype=XSD.dateTime))
    
    #Frq=Describer(graph=store)
    #Frq.rdftype(DCT.Frequency)
    
    Dataset.rel(p=DCAT.temporal,o=POT._current())
    Dataset.rel(p=DCAT.distribution,o=Distribution._current())
    Dataset.rel(p=DCT.accrualPeriodicity,o="http://publications.europa.eu/resource/authority/frequency/CONT")
    Dataset.rel(p=DCAT.theme,o="http://publications.europa.eu/resource/authority/data-theme/TRAN")
    Dataset.value(DCT.modified,Literal(millistoiso(doc['endTimeMillis']),datatype=XSD.dateTime))
    
    #Dataset.rel(DCAT.distribution,)
    #store.add((Dataset,DCT.temporal,Literal("dataset from waze information",lang='en')))
    #store.add((Dataset,DCT.title,Literal("waze_infotraffic_doc#"+doc['_rev'],lang='en')))

def generateTweetsDataset(doc,destResource):
    Dataset=Describer(graph=store,about="#"+doc['_rev'],base=TNS)
    Dataset.rdftype(DCAT.Dataset)
    Dataset.value(DCT.title,Literal("pm_tweets_doc#"+doc['_rev'],lang=lang))
    Dataset.value(DCT.description,Literal("json data from twitter pm information",lang=lang))
    Dataset.rel(p=DCT.identifier,o="http://dbpedia.org/resource/Twitter") 
    
    Distribution=Describer(graph=store)
    Distribution.rdftype(DCAT.Distribution)
    Distribution.rel(p=DCT['format'],o="http://publications.europa.eu/resource/authority/file-type/JSON")
    Distribution.rel(p=DCAT.accessURL,o="http://neuron4web.palermo.enea.it/opendata/{0}/{1}".format(destResource,doc['_rev']))
    Distribution.value(DCAT.byteSize,doc['Content-Length'],datatype=XSD['integer'])
    
    POT=Describer(graph=store)
    POT.rdftype(DCT.PeriodOfTime)
    POT.value(DCATAPIT.startDate,Literal(millistoiso(int(doc['timestamp_ms'])),datatype=XSD.dateTime))
    #POT.value(DCATAPIT.endDate,Literal(millistoiso(doc['endTimeMillis']),datatype=XSD.dateTime))
    
    #Frq=Describer(graph=store)
    #Frq.rdftype(DCT.Frequency)
    
    Dataset.rel(p=DCAT.temporal,o=POT._current())
    Dataset.rel(p=DCAT.distribution,o=Distribution._current())
    Dataset.rel(p=DCT.accrualPeriodicity,o="http://publications.europa.eu/resource/authority/frequency/CONT")
    Dataset.rel(p=DCAT.theme,o="http://publications.europa.eu/resource/authority/data-theme/TRAN")
    Dataset.value(DCT.modified,Literal(millistoiso(int(doc['timestamp_ms'])),datatype=XSD.dateTime))
            

def millistoiso(millis):
    return dt.utcfromtimestamp(millis/1000.0).isoformat()

def getResource(resource,funct=None):
    if funct is not None: 
        requ.get("http://neuron4web.palermo.enea.it/opendata/{0}".format(resource),headers=headers,hooks={"response":funct})
    else:
        res=requ.get("http://http://neuron4web.palermo.enea.it/opendata/{0}".format(resource),headers=headers)
        return res.content

def getDate(r,*arg,**kwarg):
    doc=json.loads(r.content)
    doc.update({'Content-Length':r.headers['Content-Length']})
    # first response is 303 see also that not contains document at all
    if doc.has_key('startTimeMillis'):
        #print 'getdate for {0}'.format(r.url)
        #date.update({doc['_rev']:{'start':doc['startTimeMillis'],'end':doc['endTimeMillis']}})
        generateTrafficDataset(doc,trafficresource)
        #print millistoiso(doc['startTimeMillis']),millistoiso(doc['endTimeMillis'])
    elif doc.has_key('timestamp_ms'):
        generateTweetsDataset(doc,tweetsresource)    

def getRev(r,*arg,**kwarg):
    if r.content:
	resp=json.loads(r.content)
    	url=r.url
    	if resp:
           if 'error' in resp: print '{0} error: {1}'.format(r.url,resp['error'])
           else:
            	revs=[r['rev'] for r in resp['revs']]
            	# traffic resources
   	    	if trafficresource in url: [getResource("{0}/{1}".format(trafficresource,r),getDate) for r in revs ]
            	# tweets resources
            	elif tweetsresource in url: [getResource("{0}/{1}".format(tweetsresource,r),getDate) for r in revs ]
     
def saveCatalog(gr):
    res=requ.get("http://neuron4web.palermo.enea.it/opendata/resource")
    resj=json.loads(res.content)
    data=gr.serialize(format='json-ld')
    headers={"Content-Type":"application/ld+json"}
    #test="resourceCatalog"
    #data={"_id":test,"items":jsongr}
    if resj.has_key('_rev'): headers.update({"If-Match":resj['_rev']})
    res=requ.put("http://neuron4web.palermo.enea.it/opendata/resource/catalog.jsonld".format(host),headers=headers,data=data)
    resj=json.loads(res.content)
    headers.update({"If-Match":resj['rev']})
    res.close()
    data=gr.serialize(format='pretty-xml')
    headers.update({"Content-Type":"application/rdf+xml"})
    res=requ.put("http://neuron4web.palermo.enea.it/opendata/resource/catalog.rdf".format(host),headers=headers,data=data)
    resj=json.loads(res.content)
    headers.update({"If-Match":resj['rev']})
    res.close()
    data=gr.serialize(format='turtle')
    headers.update({"Content-Type":"text/plain"})
    res=requ.put("http://neuron4web.palermo.enea.it/opendata/resource/catalog.ttl".format(host),headers=headers,data=data)
    resj=json.loads(res.content)
    headers.update({"If-Match":resj['rev']})
    res.close()

# infinite loop
def waitForChanges():
    requ.post('http://127.0.0.1:5984/infotraffic/_changes?filter=_doc_ids&feed=continuous&since=now',stream=True,hooks={'response':scanResources},json={'doc_ids':['pm_tweets_doc','waze_infotraffic_doc']})

def scanResources(r,*argv,**kwargv):    
    if 'changes' in r.content:    
	newstore()
    	getResource(trafficresource,getRev)
    	getResource(tweetsresource,getRev)
    	saveCatalog(store)
def main():
    while (True): 
		requ.post('http://127.0.0.1:5984/infotraffic/_changes?filter=_doc_ids&feed=continuous&since=now',stream=True,hooks={'response':scanResources},json={'doc_ids':['pm_tweets_doc','waze_infotraffic_doc']})
	# end of loop


# main entry point
if __name__=="__main__":
    main()

