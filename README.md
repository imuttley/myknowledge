# myknowledge

* library requirements for python: rdflib, couchpy, difflib, tweepy, urllib, requests, json, threading, pickle
* system requirements couchdb confiuration :

## [external]
### restinfotraffic = /path_to/restapi.py waze_infotraffic_doc
### resttweet = /path_to/restapi.py pm_tweets_doc
### sparqltweet = /path_to/sparqlendpoint.py http://dbpedia.org/resource/Twitter
### sparqltraffic = /path_to/sparqlendpoint.py http://dbpedia.org/resource/Traffic
### vocabulary = /path_to/vocabulary.py

## [httpd_db_handlers]
### _trafficResource = {couch_httpd_external, handle_external_req, <<"restinfotraffic">>}
### _tweetsResource = {couch_httpd_external, handle_external_req, <<"resttweet">>}
### _trafficendpoint = {couch_httpd_external, handle_external_req, <<"sparqltraffic">>}
### _twitterendpoint = {couch_httpd_external, handle_external_req, <<"sparqltweet">>}
### _vocabulary = {couch_httpd_external, handle_external_req, <<"vocabulary">>}

## [os_daemons]
### updateCatalog = /path_to/updateCatalog.py
