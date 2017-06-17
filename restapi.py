#!/usr/bin/env python2.7

""" mapping URI request to internal request 
convert from json to RDF object 
implements seeAlso 303 """

import re,pickle,sys,json,thread,time,urlparse,traceback

import requests as requ

from datetime import datetime as dt
from difflib import SequenceMatcher 
from urllib import urlencode

from rdflib import Namespace, Graph, Literal, BNode, RDF
from rdflib.namespace import DC,DCTERMS,RDF,RDFS,FOAF,XSD,OWL
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

store=Graph()

#MYO=Namespace("http://192.107.94.227:5984/infotraffic/_vocabulary/myowl#")
MYO=Namespace("http://neuron4web.palermo.enea.it/opendata/_vocabulary/myowl#")
GEO=Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
TNS=Namespace("http://127.0.0.1")

#with open('/usr/local/src/stradario_palermo.csv','r') as f: streets=f.read()
#scod=[csv.split(';')[0].lower() for csv in streets.splitlines()]
#skind=[csv.split(';')[1].lower() for csv in streets.splitlines()]
#sname=[csv.split(';')[2].lower() for csv in streets.splitlines()]
#streets=zip(scod,skind,sname)

with open('/usr/local/src/stradario_palermo_esteso.json','r') as f:
    jsonstreets=json.load(f,encoding='utf-8')

#stopwords=[]
#with open('/usr/local/src/stopword.txt','r') as f:
#   for line in f: 
#        stopwords.append(re.match('\w+',line).group()) 

def isuri(tok):
    return (urlparse.urlparse(tok)).scheme!=''


#def line2myo(point,seq,index,pi):
def line2myo(prevobj,objs,index,deepindex):
    global TNS
    point=objs.pop()
    pointobj=Describer(graph=store,about=TNS['#line{0:03}_point{1:03}'.format(index,deepindex)])
    pointobj.value(GEO['lat'],point['y'])
    pointobj.value(GEO['long'],point['x'])
    pointobj.rdftype(GEO.Point)
    if deepindex==0: pr=MYO.hasPoint
    else: pr=MYO['next']
    pointobj.rev(p=pr,s=prevobj)
    if len(objs)>0: line2myo(pointobj._current(),objs,index,deepindex+1)



def jam2myo(jam,index):
    global TNS
    jamobj=Describer(graph=store,about=TNS['#line{0:03}'.format(index)])
    jamobj.rdftype(MYO.WazeItem)
    jamobj.value(MYO.speed,jam['speed'])
    jamobj.value(MYO.time,Literal(millistoiso(int(jam['updateMillis'])),datatype=XSD.dateTime))
    jamobj.value(MYO.length,jam['length'])
    
    #seq=Describer(graph=store,about=TNS['#line{0:03}_seq'.format(index)])
    #seq.rdftype(RDF.Seq)
    #jamobj.rel(p=MYO.hasPoint,o=seq._current())
    line2myo(jamobj._current(),jam['line'],index,0)
    #[line2myo(point,seq,index,pi) for point,pi in zip(jam['line'],range(len(jam['line'])))]
    return jamobj
 
def convertwazedoc(r,resuri):
    global store,TNS
    TNS=Namespace("{0}".format(resuri))
    store=Graph()
    store.bind("dc",DC)
    store.bind("dct",DCTERMS)
    store.bind("rdfs",RDFS)
    store.bind("rdf",RDF)
    store.bind("myo",MYO)
    store.bind("geo",GEO)
    store.bind("tns",TNS)

    resp=json.loads(r.text)
    
    j=resp['jams']
    if j:
    	obj=[jam2myo(jam,index) for jam,index in zip(j,range(len(j)))]
    	resourceobj=Describer(graph=store,about=resuri)
    	resourceobj.rdftype(MYO.Resource)
    	resourceobj.value(DCTERMS.issued,Literal(millistoiso(int(resp['startTimeMillis'])),datatype=XSD.dateTime))
    	[resourceobj.rel(p=MYO.hasItem,o=ob._current()) for ob in obj]

def pointfor(a, li):
    litoken=li.split()
    vect=[0*i for i in range(len(litoken))]
    atoken=a.split()
    for token,deep in zip(atoken,range(1,len(atoken)+1)):
        test=[deep*similarity(token,s,0.8) for s in li.split()]
        vect=[test[i]+vect[i] for i in range(len(litoken))]
    res=[v for v in vect if v>0]
    ret=0
    if (res==sorted(res) and res==range(1,len(atoken)+1)): ret=10
    if (res==sorted(res)): ret=1
    if (res==range(1,len(atoken)+1)): ret=5
    return ret*len(atoken)

def thereissimilar(a, li,ratio):
    litoken=[term for term in li.split() if term not in stopwords]
    vect=[0*i for i in range(len(litoken))]
    atoken=[term for term in a.split() if term not in stopwords]
    print litoken,atoken
    for token,deep in zip(atoken,range(1,len(atoken)+1)):
        test=[deep*similarity(token,s,rt=ratio) for s in litoken]
        vect=[test[i]+vect[i] for i in range(len(litoken))]
    res=[v for v in vect if v!=0]
    real=[r for r in res]
    res.sort()
    print real,res
    p=[1 for r,i in zip(res,range(len(res))) if real[i]==res[i]]
    if len(real)>0:
    	return (float(sum(p))/float(len(real)))
    else: 
	return 0.0

def similarity(a, b,rt=0.9):
    return SequenceMatcher(None,a, b).ratio()>rt

def geojsontopoint(obj,prev,pi):
    global TNS
    if obj==None: return prev,pi
    if not hasattr(obj[0],"__iter__"):
      	#for lon,lat in obj['osm']['geojson']['coordinates']:
	pointobj=Describer(graph=store,about=TNS['#point{0:03}'.format(pi)])
       	pointobj.rdftype(GEO.Point)
       	pointobj.value(GEO['lat'],obj[1])
      	pointobj.value(GEO['long'],obj[0])
       	if pi==0: pr=MYO.hasPoint
       	else: pr=MYO['next']
       	pointobj.rev(p=pr,s=prev)
       	prev=pointobj._current()
       	pi=pi+1
       	return prev,pi
    if hasattr(obj,"__iter__"):
	for li in obj: prev,pi=geojsontopoint(li,prev,pi)

def returnannotate(text):
    res=requ.get("http://api.dbpedia-spotlight.org/annotate?text="+text+"&confidence=0.2",headers={'Content-Type':'application/x-www-form-urlencoded','Accept':'application/json'})
    #with open('twitter_annontation.log','w') as f: f.write(res.content)    
    resj=json.loads(res.content)
    if resj.has_key('Resources'): 
	try:
	   return [ann['@URI'] for ann in resj['Resources']]
	except:
	   pass
    return []

def converttweetdoc(r,resuri):
    global jsonstreets,store,TNS
    TNS=Namespace("{0}".format(resuri))
    store=Graph()
    store.bind("dc",DC)
    store.bind("dct",DCTERMS)
    store.bind("rdfs",RDFS)
    store.bind("rdf",RDF)
    store.bind("myo",MYO)
    store.bind("geo",GEO)
    store.bind("tns",TNS)
    store.bind("owl",OWL)   
    if r.status_code!=200: return 
    resp=json.loads(r.text)
    
    twitype=['rallentamenti','fermi','incidente','lavori','manifestazione','interruzione','blocco'] 
    resourceobj=Describer(graph=store,about=resuri)
    resourceobj.rdftype(MYO.Resource)
    itemobj=Describer(graph=store,about=TNS['#tweet'])
    itemobj.rdftype(MYO.TwitterItem)
    itemobj.rev(p=MYO.hasItem,s=resourceobj._current())
    if resp.has_key('user'):
    	if resp['user']['name']=='Polizia MunicipalePA':
		resourceobj.value(DCTERMS.issued,Literal(millistoiso(int(resp['timestamp_ms'])),datatype=XSD.dateTime))
    		text=(resp['text']).lower()
    		#[resourceobj.rel(p=MYO.hasAnnotation,o=token) for token in text.split() if isuri(token)]
		#types=[token.title() for token in twitype if thereissimilar(token,text,0.9)]	
		#if types!=[]:
		#	[resourceobj.rel(p=OWL.equivalentClass,o=MYO[typ]) for typ in types]
    		#else:
		#resourceobj.rel(p=MYO.hasAnnotation,o=MYO.Unclassified)
		rest=' '.join([t for t in text.split() if not isuri(t)]) 
		urltext='%20'.join([t for t in text.split() if not isuri(t)])
		[resourceobj.rel(p=MYO.hasAnnotation,o=ret) for ret in returnannotate(urltext) if ret!=None] 
		#and t not in twitype])
    		itemobj.value(MYO.twittertext,Literal(rest,lang=resp['user']['lang']))
		# search on palermo csv opendata street information	
		#result=[(obj,len('{0} {1}'.format(obj['kind'],obj['name']))) for obj in jsonstreets if thereissimilar('{0} {1}'.format(obj['kind'],obj['name']),rest)]
		result=[(obj,pointfor('{0} {1}'.format(obj['kind'],obj['name']),rest)) for obj in jsonstreets]
		bigmatch=(None,0)
		for robj,l in result: 
			if l>bigmatch[1]: 
				bigmatch=(robj,l)
		if bigmatch[1]>6:
			bobj=bigmatch[0]
			itemobj.value(MYO['match'],Literal(bigmatch[1]))
			itemobj.value(MYO['code'],Literal(bobj['cod']))
			itemobj.value(MYO['onStreet'],Literal('{0} {1}'.format(bobj['kind'],bobj['name']),lang='it'))
			# search on webservices nomination http://nominatim.openstreetmap.org/search.php for lat and long
			polygontype=bobj['osm']['geojson']['type']
			polygonobj=Describer(graph=store,about=TNS['#osmItem'])
			polygonobj.rdftype(MYO.Osm)
			polygonobj.rel(p=MYO['geojsonType'],o=MYO[polygontype])
			polygonobj.rev(p=MYO.geometry,s=itemobj._current())
               		[polygonobj.rel(p=DCTERMS.license,o=token) for token in (bobj['osm']['licence']).split() if isuri(token)]
			prev=polygonobj._current()
			#LineString:[[]], Point:[], Polygon:[[[]]]
			objosm=bobj['osm']
			geoj=objosm['geojson']
			coordobj=geoj['coordinates']
			try:
    				geojsontopoint(coordobj,prev,0)
			except Exception as e:
				pass
		try:
			with open('/usr/local/src/rss_feed.pickle','r') as f: rssobj=pickle.load(f)
			result=[(obj,pointfor(obj['text'].lower(),rest)) for obj in rssobj]
      			bigmatch=(None,0)
        		for robj,l in result:
        	       		if l>bigmatch[1]:
          				bigmatch=(robj,l)
        		if bigmatch[1]>6:
				rss=bigmatch[0]
				resourceobj.rel(p=MYO.hasAnnotation,o='https://mobilitasostenibile.comune.palermo.it/news.php?func=1&id='+rss['id'])
		except Exception as e:
			with open('restlog.txt','w') as f:
        			f.write('exception {0}'.format(e))

def serializedoc(host,db,res,rev,form):
    #desturi="http://{0}/{1}/{2}/{3}".format(host,db,res,rev)
    desturi="http://neuron4web.palermo.enea.it/opendata/{0}/{1}".format(res,rev)
    #r=requ.get("http://{0}/{1}/{2}?rev={3}".format(host,db,sys.argv[1],rev))
    r=requ.get("http://neuron4web.palermo.enea.it/opendata/{0}?rev={1}".format(sys.argv[1],rev))
    if "waze" in sys.argv[1]: convertwazedoc(r,desturi)
    else: converttweetdoc(r,desturi)
    #with open('mylog.txt','w') as f:
	#f.write(store.serialize(format=form))
    respond(code=200,body=store.serialize(format=form))

def millistoiso(millis):
    return dt.utcfromtimestamp(millis/1000.0).isoformat()

def getdocrevs(host,db):
    #res=requ.get("http://{0}/infotraffic/{1}?revs_info=true".format(host,db))
    res=requ.get("http://neuron4web.palermo.enea.it/opendata/{0}?revs_info=true".format(db))
    respond(data={"revs":(json.loads(res.content))['_revs_info']})

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
    if body:
	  sys.stdout.write("%s\n" % json.dumps({"code": code, "body": body, "headers": headers}))
    else:
	  sys.stdout.write("%s\n" % json.dumps({"code": code, "json": data, "headers": headers}))
    sys.stdout.flush()

def main():
    for req in requests():
	host=req["headers"]["Host"]
	accept=req["headers"]["Accept"]
	# type application subtype json 'seealso' to requested resource 
	if accept=="application/json": 
		if len(req["path"])>2: 
			header={"Content-Location":"http://neuron4web.palermo.enea.it/opendata/{0}?rev={1}".format(sys.argv[1],req["path"][2])}
			header.update({"Location":header["Content-Location"]})
                	respond(code=303,headers=header)
		else: getdocrevs(host,sys.argv[1])
		#header={"Content-Location":"http://{0}/infotraffic/{1}?revs_info=true".format(req["headers"]["Host"],sys.argv[1])}
		#header.update({"Location":header["Content-Location"]})
		#respond(code=303,headers=header)
	elif accept=="application/x-line-jams":
		if len(req["path"])>2:
			doc=requ.get("http://neuron4web.palermo.enea.it/opendata/{1}?rev={2}".format(host,sys.argv[1],req["path"][2]))
                        d=json.loads(doc.content)
			lines=[]	
			#for j in d['jams']:	
			respond(code=200,data=d)

	# all others
	elif accept=="application/rdf+xml":
		if len(req["path"])>2: serializedoc(host,req["path"][0],req["path"][1],req["path"][2],"pretty-xml")
	
	elif accept=="text/plain":
		if len(req["path"])>2: serializedoc(host,req["path"][0],req["path"][1],req["path"][2],"nt")	
	elif accept=="application/x-turtle":
		if len(req["path"])>2: serializedoc(host,req["path"][0],req["path"][1],req["path"][2],"turtle")		

	else: respond(data={"respondto":req["headers"],"path":req["path"],"querystring": req["query"],"body":req["body"]})

if __name__ == "__main__":
    main()

