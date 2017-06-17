#!/usr/bin/env python2.7

""" a sparlq endpoint service """

import sys,json,time
import requests as requ
import threading,urlparse

from datetime import datetime as dt
from HTTP4Store import HTTP4Store

#from rdflib import Namespace,Graph, Literal, BNode, RDF
#from rdflib.namespace import FOAF, DC, XSD
#from rdflib.extras.describer import Describer

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

# 4store backends assignments
backends={'catalog':'http:\/\/localhost:9991','Twitter':'http:\/\/localhost:9992','Traffic':'http:\/\/localhost:9993'}

class th_cataloglisten(threading.Thread): 
    from rdflib import Graph, URIRef    

    def __init__(self,id):
        super(th_cataloglisten,self).__init__()
        self.id=id
        self.mime='application/x-turtle'
        self.graph=self.Graph('Sleepycat', identifier=self.id)
        self.graph.open(id.split('/').pop(), create = True)
	#self.graph=HTTP4Store(http_endpoint=backends[id.split('/').pop()])

    def parseuri(self,r,*k,**v):
        self.graph.parse(data=r.text,format=self.mime)
    	#self.graph.add_graph(r.url,r.content)
    def newdoc(self,r,*arg,**kwarg):
        if 'changes' in r.content:
		resp=json.loads(r.text)
        	if resp.has_key('seq'): self.loadgraph()
        
    def loadgraph(self):
	# is 4store backend and http running ?
        #catalog=HTTP4Store(http_endpoint=backends['catalog'])
        #catalog.add_from_uri('http://127.0.0.1:5984/infotraffic/resource/catalog.rdf')

        #resources=catalog.sparql("PREFIX dbp:<http://dbpedia.org/resource/> PREFIX dcat:<http://www.w3.org/ns/dcat#> SELECT ?url WHERE { ?s ?p <"+self.id+">;dcat:distribution ?dist. ?dist dcat:accessURL ?url } ",accept='application/json',headers={'Accept':'application/json'})
	# else use rdflib graph
	catalog=Graph()
	catalog.parse("http://127.0.0.1:5984/infotraffic/resource/catalog.rdf")
	resources=catalog.query("PREFIX dbp:<http://dbpedia.org/resource/> PREFIX dcat:<http://www.w3.org/ns/dcat#> SELECT ?url WHERE { ?s ?p <"+self.id+">;dcat:distribution ?dist. ?dist dcat:accessURL ?url } ")
	#log ('resources {0}'.format(resources.serialize(format='json')))
        try:
		bindings=(json.loads(resources.serialize(format='json')))['results']['bindings']
        	uris=[res['url']['value'] for res in bindings]
        	#uris=[self.URIRef.decode(r['url']) for r in resources]
		#log ('uris {0}'.format(uris))
		[requ.get(r,headers={'Accept':self.mime},hooks={'response':self.parseuri}) for r in uris if (self.URIRef(r),None,None) not in self.graph]
        except Exception as e:
		log('loadgraph {0}'.format(e)) 
		print "exception in requests uri"
	    
    def query(self,q):
        return self.graph.query(q)
    def waitfor(self):
	try:
		while(True): requ.post('http://127.0.0.1:5984/infotraffic/_changes?filter=_doc_ids&feed=continuous&since=now',stream=True,hooks={'response':self.newdoc},json={'doc_ids':['resource']})
	except Exception as e:
		log('waitfor {0}'.format(e))
	#finally:
	#	self.waitfor()

    def run(self):
        try:
		self.loadgraph()
        except Exception as e:
		log('run {0}'.format(e))
        finally:
		self.waitfor()
	        #print "exception from run {0}: {1}".format(self.name,e)



def requests():
    # 'for line in sys.stdin' won't work here
    line = sys.stdin.readline()
    while line:
        yield json.loads(line)
        line = sys.stdin.readline()

def respond(code=200, data={}, headers={},body={}):
    if body: sys.stdout.write("%s\n" % json.dumps({"code": code, "body": body, "headers": headers}))
    else: sys.stdout.write("%s\n" % json.dumps({"code": code, "json": data, "headers": headers}))
    sys.stdout.flush()

def log (msg):
    with open('sparql{0}.log'.format(sys.argv[1].split('/').pop()),'w') as f: f.write('exception {0}'.format(msg))
def main():
    myth=th_cataloglisten(sys.argv[1])
    #myth.daemon=True
    myth.start()
    for req in requests():
	host=req["headers"]["Host"]
	accept=req["headers"]["Accept"]
	# type application subtype json 'seealso' to requested resource 
	#contenttype=req["headers"]["Content-type"]
	params=req["query"]
	if not params.has_key('query'): 
		#respond(body=myth.graph.serialize(format='pretty-xml'),headers={'Content-Type':'application/rdf+xml'})
		res=requ.get('http://127.0.0.1:5984/infotraffic/resource/sparql.html')
		respond(body=res.content)
	elif req["method"]=='GET':
		if params.has_key('default-graph-uri'): defaultgraph=params['default-graph-uri']
		if params.has_key('using-named-graph-uri'): usinggraph=params['using-named-graph-uri']
		if params.has_key('format'): frm=params['format']
		if params.has_key('timeout'): timeout=params['timeout']
		try:
			res=myth.query(params['query'])
			if frm: respond(body=res.serialize(format=frm),headers={'Content-Type':'text/plain'})
			else: respond(body=res.serialize(),headers={'Content-Type':'application/sparql-result+xml'})
		except Exception as e:
			log('request {0}\n{1}'.format(params['query'],e))
			respond(data={'sparql':sys.argv[1]})
	elif req["method"]=='POST':
		contenttype=req["headers"]["Content-Type"]
		respond(data=contenttype)		

if __name__ == "__main__":
    main()

