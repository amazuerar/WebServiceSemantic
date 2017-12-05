import tornado.ioloop
import tornado.web
from geopy.geocoders import Nominatim
from json import dumps
from pymongo import MongoClient
from bson.json_util import dumps
import matplotlib
matplotlib.use('Agg')
import re
import sys
reload(sys)
from SPARQLWrapper import SPARQLWrapper, JSON
sys.setdefaultencoding('utf8')

# Configuracion de las caracteristicas de los servicios
class BaseHandler( tornado.web.RequestHandler ):
    def set_default_headers( self ):
        self.set_header( 'Access-Control-Allow-Origin', '*' )
        self.set_header( 'Access-Control-Allow-Headers', 'origin, x-requested-with, content-type' )
        self.set_header( 'Access-Control-Allow-Methods', 'POST, GET, PUT, DELETE, OPTIONS' )

# Deficion de la aplicacion REST
class Application( tornado.web.Application ):
    def __init__( self ):
        handlers = [
			(r'/', BaseHandler ),
            (r"/getQuestions", getQuestions),
            (r"/getEntitiesByID/(.*)", getEntitiesByID),
            (r"/getAnswersByID/(.*)", getAnswers),
            (r"/getLocationsByID/(.*)", getLocationsByID),
            (r"/getTracksByID/(.*)", getTracksByID),
            (r"/getTweetsByID/(.*)", getTweetsByID),
            (r"/getSimilarQuestionsByID/(.*)", getSimilarQuestionsByID),
            (r"/getQuestionsByQuery/(.*)", getQuestionsByQuery),
            (r"/getSparqlQuery/(.*)", getSparqlQuery)
		]
        tornado.web.Application.__init__( self, handlers )


class getQuestions(BaseHandler):
    def get(self):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['w4_musicfans']

        self.write(dumps(mine.find( {
            "$and" :[
                        {"type":"question"},
                            {"$or":
                                [
                                    {"entities_persons": { "$exists": True, "$ne": []}},
                                    {"entities_organizations": { "$exists": True, "$ne": []}},
                                    {"entities_locations": { "$exists": True, "$ne": []}}
                                ]
                            }
                     ]
        })))

class getEntitiesByID(BaseHandler):
    def get(self, id):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['w4_musicfans']

        self.write(dumps(mine.find({"_id":id},{'entities_persons':1,'entities_organizations':1,'entities_locations':1, 'categories':1 })))

class getAnswers(BaseHandler):
    def get(self, id):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['w4_musicfans']

        self.write(dumps(mine.find( {
            "$and" :[
                        {"type":"answer"},
                        {"in_reply_to": int(id)},
                            {"$or":
                                [
                                    {"entities_persons": { "$exists": True, "$ne": []}},
                                    {"entities_organizations": { "$exists": True, "$ne": []}},
                                    {"entities_locations": { "$exists": True, "$ne": []}}
                                ]
                            }
                     ]
        })))

class getLocationsByID(BaseHandler):
    def get(self, id):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['w4_musicfans']

        locations = []

        json = mine.find({"$or": [{"_id": id}, {"in_reply_to": int(id)}]},
                         {'_id': 0, 'entities_locations': 1})

        for item in json:
            if len(item['entities_locations'])>0:
                for loc in item['entities_locations']:
                    locations.append(loc)

        geolocator = Nominatim()

        response = []

        for loc in locations:
            locationData = {}
            try:
                location = geolocator.geocode(loc)
                locationData['name'] = loc
                locationData['latitude'] = location.latitude
                locationData['longitude'] = location.longitude
                locationAddress = geolocator.reverse(str(location.latitude)+", "+str(location.longitude))
                locationData['description'] = locationAddress.address
                response.append(locationData)
            except Exception:
                continue

        self.write(dumps(response))

class getTracksByID(BaseHandler):
    def get(self, id):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['w4_musicfans']
        tracks = db['w4_tracks']

        persons = []

        json = mine.find({"$or": [{"_id": id}, {"in_reply_to": int(id)}]},
                         {'_id': 0, 'entities_persons': 1})

        for item in json:
            if len(item['entities_persons'])>0:
                for per in item['entities_persons']:
                    persons.append(per)

        nduppersons = []
        for i in persons:
            if i not in nduppersons:
                nduppersons.append(i)

        response = []

        for person in nduppersons:
            tracksData = {}
            tracksData['artist'] = person
            tracksAll = tracks.find({"artist_found_name" : person})
            tracksData["tracks"] = tracksAll
            if tracksAll.count() > 0:
                response.append(tracksData)

        self.write(dumps(response))

class getTweetsByID(BaseHandler):
    def get(self, id):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['w4_musicfans']

        # TRAE A LAS PERSONAS QUE ESTAN RELACIONADAS A UNA PREGUNTA DEL RSS
        persons = []

        json = mine.find({"$or": [{"_id": id}, {"in_reply_to": int(id)}]},
                         {'_id': 0, 'entities_persons': 1})

        for item in json:
            if len(item['entities_persons'])>0:
                for per in item['entities_persons']:
                    persons.append(per)

        # TRAE LOS SCREEN NAMES DE LAS PERSONAS SEGUN UN NOMBRE
        screenNames = []

        for per in persons:
            regx = re.compile("^"+per, re.IGNORECASE)

            a = db['w4_users'].distinct('screen_name', {'name' : {'$regex': regx}})

            if len(a) > 0:
                screenNames.append(a[0])

        tweets = []

        # TRAE TWEETS QUE MENCIONEN AL SCREEN NAME EN LAS ENTITIES

        for screen in screenNames:
            tweet = {}
            tweetdb = db['w4_tweets'].find({'entities_mentions': screen})

            tweet['tweets'] = []

            if tweetdb.count() > 0:
                tweet["descripction"] = "@"+screen
                for t in tweetdb:
                    t['tid'] = str(t['_id'])
                    tweet['tweets'].append(t)
            if len(tweet['tweets']) > 0:
                tweets.append(tweet)

        # TRAE TWEET QUE MENCIONEN A LA PERSON EN EL TEXT DEL TWEET
        for per in persons:
            tweet = {}
            regx = re.compile("^"+per, re.IGNORECASE)
            tweetdb = db['w4_tweets'].find({'entities_persons': {'$regex': regx}})


            tweet['tweets'] = []

            if tweetdb.count() > 0:
                tweet["descripction"] = per
                for t in tweetdb:
                    t['tid'] = str(t['_id'])
                    tweet['tweets'].append(t)

            if len(tweet['tweets']) >0:
                tweets.append(tweet)

        self.write(dumps(tweets))

class getSimilarQuestionsByID(BaseHandler):
    def get(self, id):
            client = MongoClient('bigdata-mongodb-01', 27017)
            db = client['Grupo10']
            mine = db['w4_musicfans']

            concat = ""
            for txt in mine.find({"_id": id}, {'title': 1, 'description': 1, 'categories': 1, "entities_locations":1, "entities_persons":1, "entities_organizations": 1}):
                concat+= txt['title'] + txt['description']
                for cat in txt['categories']:
                    concat+= " "+cat
                for cat in txt['entities_locations']:
                    concat+= " "+cat
                for cat in txt['entities_persons']:
                    concat+= " "+cat
                for cat in txt['entities_organizations']:
                    concat+= " "+cat

            response = mine.find( { "$text": { "$search": concat, "$language": "en"}}, {"score": { "$meta": "textScore"}} )
            response.sort([('score', {'$meta': 'textScore'})])
            self.write(dumps(response))

class getQuestionsByQuery(BaseHandler):
        def get(self, query):
            client = MongoClient('bigdata-mongodb-01', 27017)
            db = client['Grupo10']
            mine = db['w4_musicfans']

            response = mine.find({"$text": {"$search": query, "$language": "en"}}, {"score": {"$meta": "textScore"}})
            response.sort([('score', {'$meta': 'textScore'})])
            self.write(dumps(response))


class getSparqlQuery(BaseHandler):
                def get(self, id):

                    client = MongoClient('bigdata-mongodb-01', 27017)
                    db = client['Grupo10']
                    mine = db['w4_musicfans']

                    persons = []

                    json = mine.find({"$or": [{"_id": id}, {"in_reply_to": int(id)}]},
                                     {'_id': 0, 'entities_persons': 1})

                    for item in json:
                        if len(item['entities_persons']) > 0:
                            for per in item['entities_persons']:
                                persons.append(per)

                    nduppersons = []
                    for i in persons:
                        if i not in nduppersons:
                            nduppersons.append(i)

                    fresults = []
                    if len(nduppersons)>0:
                        for per in nduppersons:

                            try:
                                sparql = SPARQLWrapper("http://dbpedia.org/sparql")
                                sparql.setQuery("""
                                PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                                PREFIX dbo: <http://dbpedia.org/ontology/>
                                SELECT DISTINCT ?singer ?name ?birthPlace ?birthDate ?genre ?residence ?partner ?thumbnail ?abstract
                                WHERE { 
                                ?x dbo:musicalArtist ?singer.
                                OPTIONAL{ ?singer foaf:name ?name }
                                OPTIONAL{ ?singer dbo:birthPlace ?birthPlace }
                                OPTIONAL{ ?singer dbo:birthDate ?birthDate }
                                OPTIONAL{ ?x dbo:genre ?genre }
                                OPTIONAL{ ?singer dbo:residence ?residence }
                                OPTIONAL{ ?singer dbo:partner ?partner }
                                OPTIONAL{ ?singer dbo:thumbnail ?thumbnail }
                                OPTIONAL{ ?singer dbo:abstract ?abstract }
                                FILTER langMatches(lang(?abstract),'en')
                                FILTER (regex(?name,'^""" + per + """'))
                                }
                                GROUP BY ?genre LIMIT 1
                                """)
                                sparql.setReturnFormat(JSON)
                                results = sparql.query().convert()

                                if len(results["results"]["bindings"])>0:
                                    fresults.append(results["results"])

                            except Exception:
                                continue

                    self.write(dumps(fresults))


#Metodo main
if __name__ == "__main__":
    app = Application()
    app.listen(8083)
    tornado.ioloop.IOLoop.current().start()