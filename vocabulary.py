#!/usr/bin/env python2.7

""" get requests for vocabulary and map as internal request """

import sys,json,thread,time
import requests as requ

from datetime import datetime as dt

from rdflib import Namespace,Graph, Literal, BNode, RDF
from rdflib.namespace import FOAF, DC, XSD
from rdflib.extras.describer import Describer

#{
#       'body': 'undefined',
#       'cookie': {
#            '__utma': '96992031.3087658685658095000.1224404084.1226129950.1226169567.5',
#            '__utmz': '96992031.1224404084.1.1.utmcsr'
#        },
#       'form': {},
#       'info': {
#           'compact_running': False,
#           'db_name': 'couchbox',
#           'disk_size': 50559251,
#           'doc_count': 9706,
#           'doc_del_count': 0,
#           'purge_seq': 0,
#           'update_seq': 9706},
#       'path': [],
#       'query': {},
#   'method': 'GET'
#   'headers' : request headers 'Accept','Host',etc..
# }


def getowl(host,owl):
    res=requ.get("http://neuron4web.palermo.enea.it/opendata/vocabulary/{1}.xml".format(host,owl))
    respond(body=res.content,headers={'Content-Location':'{0}.rdf'.format(owl),'Content-Type':'application/rdf+xml'})

def getdocrev(doc,rev):
    if rev is not None: res=requ.get("http://localhost:5984/infotraffic/waze_infotraffic_doc?rev={0}".format(rev))
    else: res=requ.get("http://localhost:5984/infotraffic/waze_infotraffic_doc?revs_info=true")
    respond(data={"document":json.loads(res.content)})

def requests():
    # 'for line in sys.stdin' won't work here
    line = sys.stdin.readline()
    while line:
        yield json.loads(line)
        line = sys.stdin.readline()

def respond(code=200, data={}, headers={},body={}):
    sys.stdout.write("%s\n" % json.dumps({"code": code, "body": body, "headers": headers}))
    sys.stdout.flush()

def main():
    for req in requests():
	host=req["headers"]["Host"]
	accept=req["headers"]["Accept"]
	# type application subtype json 'seealso' to requested resource 
	if len(req['path'])>1: getowl(host,req['path'][2])

if __name__ == "__main__":
    main()

