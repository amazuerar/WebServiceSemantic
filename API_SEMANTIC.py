import tornado.ioloop
import tornado.web
from geopy.geocoders import Nominatim
from json import dumps
from pymongo import MongoClient
from bson.json_util import dumps
from wordcloud import WordCloud
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from bson.code import Code
import re
import json
from os import path
import sys
reload(sys)
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
            (r"/getLastSentimentReplayByTweetID/(.*)", getLastSentimentReplayByTweetID),
            (r"/images/(.*)", tornado.web.StaticFileHandler, {'path': "/images/"}),
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

        response = []

        for person in persons:
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
            tweetdb = db['w4_tweets'].find( "id", {'entities_mentions': screen})

            tweet['tweets'] = []

            if tweetdb.count() > 0:
                tweet["descripction"] = screen
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









class getInfoGeneralDos(BaseHandler):
    def get(self):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']

        json = []
        response_json = {}
        response_json["name"] = "Tweets"
        series = mine.aggregate([{'$group':{'_id':{'year':{'$year':'$tweet_date'},'month':{'$month':'$tweet_date'},'day':{'$dayOfMonth':'$tweet_date'}},'value':{'$sum':1},'name':{'$first':"$tweet_date"}}},{'$project':{'name':{'$dateToString':{'format':"%Y-%m-%d",'date':"$name"}},'value':1,'_id': 0}},
        {   '$sort':
            {
                'name': 1
            }
        }])
        response_json["series"] = series

        json.append(response_json)
        self.write(dumps(json))

class getInfoGeneralTres(BaseHandler):
    def get(self):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']
        personajes = ['CGurisattiNTN24', 'DanielSamperO', 'ELTIEMPO', 'elespectador', 'NoticiasCaracol', 'NoticiasRCN',
                      'CaracolRadio', 'BluRadioCo', 'JuanManSantos', 'ClaudiaLopez', 'German_Vargas', 'AlvaroUribeVel',
                      'AndresPastrana_', 'TimoFARC', 'OIZuluaga', 'A_OrdonezM', 'JSantrich_FARC', 'IvanDuque',
                      'mluciaramirez', 'petrogustavo', 'DeLaCalleHum', 'FARC_EPaz'];

        json = []


        for persona in personajes:
            response_json = {}
            response_json["name"] = persona
            series = mine.aggregate([
                {"$match": {"screen_name":persona}},
                {"$group": {"_id": "$sentiment", "value": {"$sum": 1}}},
                {"$project": {
                    "name": "$_id",
                    "value": 1,
                    "_id":0
                }}])

            response_json["series"] = series
            json.append(response_json)

        self.write(dumps(json))

class getInfoGeneralCuatro(BaseHandler):
    def get(self):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']
        personajes = ['CGurisattiNTN24', 'DanielSamperO', 'ELTIEMPO', 'elespectador', 'NoticiasCaracol', 'NoticiasRCN',
                      'CaracolRadio', 'BluRadioCo', 'JuanManSantos', 'ClaudiaLopez', 'German_Vargas', 'AlvaroUribeVel',
                      'AndresPastrana_', 'TimoFARC', 'OIZuluaga', 'A_OrdonezM', 'JSantrich_FARC', 'IvanDuque',
                      'mluciaramirez', 'petrogustavo', 'DeLaCalleHum', 'FARC_EPaz'];

        json = []


        for persona in personajes:
            response_json = {}
            response_json["name"] = persona
            series = mine.aggregate([
                {"$match": {"entities_mentions":persona}},
                {"$group": {"_id": "$sentiment", "value": {"$sum": 1}}},
                {"$project": {
                    "name": "$_id",
                    "value": 1,
                    "_id":0
                }}])

            response_json["series"] = series
            json.append(response_json)

        self.write(dumps(json))

class getGeoSentiment(BaseHandler):
    def get(self):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']

        self.write(dumps(mine.find({"tweet_location_lati" : {'$ne' : None}, }, {'screen_name':1, 'entities_mentions':1, 'entities_hashtags':1, 'text':1,'sentiment':1,'tweet_location_lati': 1,'tweet_location_long': 1, '_id':0})))

class getFollowers(BaseHandler):
    def get(self, name):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['users']

        json = []
        response_json = {}
        response_json["name"] = "Followers"

        series = mine.aggregate(
            [
                {'$match': {'screen_name': name}},
                {'$project':
                    {
                        '_id': 0, 'name': "$downloaded_date", 'value': "$followers_number"}
                }
            ])

        response_json["series"] = series

        json.append(response_json)
        self.write(dumps(json))

class getFollowersAll(BaseHandler):
    def get(self):
                personajes =['CGurisattiNTN24','DanielSamperO','ELTIEMPO','elespectador','NoticiasCaracol','NoticiasRCN','CaracolRadio','BluRadioCo','JuanManSantos','ClaudiaLopez','German_Vargas','AlvaroUribeVel','AndresPastrana_','TimoFARC','OIZuluaga','A_OrdonezM','JSantrich_FARC','IvanDuque','mluciaramirez','petrogustavo','DeLaCalleHum','FARC_EPaz'];

                client = MongoClient('bigdata-mongodb-01', 27017)
                db = client['Grupo10']
                mine = db['users']

                json = []


                for person in personajes:
                    response_json = {}
                    response_json["name"] = "Seguidores de " + person

                    series = mine.aggregate(
                    [
                        {'$match': {'screen_name': person}},
                        {'$project':
                            {
                                '_id': 0, 'name': "$downloaded_date", 'value': "$followers_number"}
                        }
                    ])

                    response_json["series"] = series

                    json.append(response_json)

                self.write(dumps(json))

class getLastTweetsByAccount(BaseHandler):
    def get(self, account):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']

        self.write(dumps(mine.aggregate([{ '$match': {'screen_name': account } }, {'$lookup': { 'from': 'tweets', 'localField': '_id', 'foreignField': 'in_reply_to_status_id', 'as': 'grp' }},{'$sort': { 'tweet_date': -1 }},{ '$project': { '_id': 1, 'text': 1 }},{ '$limit': 20 }])))

class getLastReplayByTweetID(BaseHandler):
    def get(self, id):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']
        self.write(dumps(mine.find({ 'in_reply_to_status_id': long(float(str(id))) }, { '_id': 0, 'screen_name': 1, 'text': 1, 'sentiment': 1 })))

class getLastSentimentReplayByTweetID(BaseHandler):
    def get(self, id):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']
        self.write(dumps(mine.aggregate([{'$match': { 'in_reply_to_status_id': long(float(str(id))) } }, { '$group': { '_id': '$sentiment', 'count': { '$sum': 1 }}},{ '$project' : { '_id': 0, 'name': '$_id', 'value': '$count' } }])))

#EN DESARROLLO
class getTopics(BaseHandler):
    def get(self):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['trends']
        self.write(dumps(mine.find().limit(2)))

class doCloudUser(BaseHandler):
    def get(self, name):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']

        map = Code( """function()
        {
        var
        text = this.text;
        text = text.replace('.',' '); text = text.replace(',',' '); text = text.replace('(',' '); text = text.replace(')',' '); text = text.replace(':',' ');
        var
        wordArr = text.toLowerCase().split(' ');
        var
        stoppedwords = 'el, la, de, es, a, un, una, que, de, por, para, como, al, ?, !, +, y, no, los, las, en, se, lo, con, o, del, q, su, //t, https, si, mas, le, cuando, ellos, este, son, tan, esa, eso, ha, sus, e, pero, porque, tienen, d';
        var
        stoppedwordsobj = [];
        var
        uncommonArr = [];
        stoppedwords = stoppedwords.split(',');
        for (i = 0; i < stoppedwords.length; i++ ) {stoppedwordsobj[stoppedwords[i].trim()] = true;}
        for ( i = 0; i < wordArr.length; i++ ) {word = wordArr[i].trim().toLowerCase(); if ( !stoppedwordsobj[word] ) {uncommonArr.push(word);}}
        for (var i = uncommonArr.length - 1; i >= 0; i--) {if (uncommonArr[i]) {if (uncommonArr[i].startsWith("#")) {emit(uncommonArr[i], 1);}}}}""")


        reduce = Code("""function( key, values ) {
        var count = 0;
        values.forEach(function(v) {
            count +=v;
        });
        return count;
        }""")

        result = mine.map_reduce(map, reduce, "myresults", query={'entities_mentions': name})

        json_result = []
        for doc in result.find():
            json_result.append(doc)

        d = path.dirname(__file__)
        #text = "y es es es es es es es es es es es es forcing the closing of the figure window in my giant loop, so I do"
        #text = open(path.join(d, 'images/red.txt')).read()

        jsonCloud = json.loads(dumps(json_result))
        text = ""

        for item in jsonCloud:
            for x in xrange(1, int(item['value']*2)):
                text += " "+item['_id']

        wordcloud = WordCloud(width=1000, height=800, max_font_size=1000).generate(text)
        #wordcloud = WordCloud(mask=col_mask, max_font_size=1000).generate(text)
        #fig = plt.figure(figsize=(4.2,6.2))
        fig = plt.figure(figsize=(20,10))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        fig.savefig('images/foo.png', facecolor='k', bbox_inches='tight')
        self.write(dumps(json_result))

class doCloudTopic(BaseHandler):
    def get(self, topic):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']

        map = Code( """function()
        {var
        text = this.text;
        text = text.replace('.',' '); text = text.replace(',',' '); text = text.replace('(',' '); text = text.replace(')',' '); text = text.replace(':',' ');
        var
        wordArr = text.toLowerCase().split(' ');
        var
        stoppedwords = 'el, la, de, es, a, un, una, que, de, por, para, como, al, ?, !, +, y, no, los, las, en, se, lo, con, o, del, q, su, //t, https, si, mas, le, cuando, ellos, este, son, tan, esa, eso, ha, sus, e, pero, porque, tienen, d';
        var
        stoppedwordsobj = [];
        var
        uncommonArr = [];
        stoppedwords = stoppedwords.split(',');
        for (i = 0; i < stoppedwords.length; i++ ) {stoppedwordsobj[stoppedwords[i].trim()] = true;}
        for ( i = 0; i < wordArr.length; i++ ) {word = wordArr[i].trim().toLowerCase(); if ( !stoppedwordsobj[word] ) {uncommonArr.push(word);}}
        for (var i = uncommonArr.length - 1; i >= 0; i--) {if (uncommonArr[i]) {if (uncommonArr[i].startsWith("#")) {emit(uncommonArr[i], 1);}}}}""")


        reduce = Code("""function( key, values ) {
        var count = 0;
        values.forEach(function(v) {
            count +=v;
        });
        return count;
        }""")

        regx = re.compile(topic, re.IGNORECASE)
        result = mine.map_reduce(map, reduce, "myresults", query={"text": regx})

        json_result = []
        for doc in result.find():
            json_result.append(doc)

        d = path.dirname(__file__)
        #text = "y es es es es es es es es es es es es forcing the closing of the figure window in my giant loop, so I do"
        #text = open(path.join(d, 'images/red.txt')).read()

        jsonCloud = json.loads(dumps(json_result))
        text = ""

        for item in jsonCloud:
            for x in xrange(1, int(item['value']*2)):
                text += " "+item['_id']

        wordcloud = WordCloud(width=1000, height=800, max_font_size=1000).generate(text)
        #wordcloud = WordCloud(mask=col_mask, max_font_size=1000).generate(text)
        #fig = plt.figure(figsize=(4.2,6.2))
        fig = plt.figure(figsize=(20,10))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        fig.savefig('images/foo.png', facecolor='k', bbox_inches='tight')
        self.write(dumps(json_result))

class getTweetsByHastag(BaseHandler):
    def get(self, hash):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']

        regx = re.compile(hash, re.IGNORECASE)
        val = mine.find({"entities_hashtags": regx})
        self.write(dumps(val))

class getMostFrequentWordsByUser(BaseHandler):
    def get(self):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']

        map = Code( "function() {"  
                    "var text = this.text;"
                    "if (text) { "
                    "text = text.toLowerCase().split(' ');"
                    "for (var i = text.length - 1; i >= 0; i--) {"
                    "if (text[i]) {"
                    "emit(text[i], 1);"
                    "}}}}")

        reduce = Code("function( key, values ) {"    
                      "var count = 0; "  
                      "values.forEach(function(v) {"          
                      "count +=v;"    
                      "});"
                      "return count;}")


        result = mine.map_reduce(map, reduce, "myresults", query={ 'screen_name': "elespectador" })
        json_result = []
        for doc in result.find():
            doc['name'] = doc['_id']
            json_result.append(doc)




        self.write(dumps(json_result))

class getFrequencyByTopic(BaseHandler):
    def get(self):
            temas = ['Corrup', 'jep', 'farc', 'presidencial', 'paz', 'candida', 'coca', 'eln', 'narco', 'mermelada', 'justicia'];

            client = MongoClient('bigdata-mongodb-01', 27017)
            db = client['Grupo10']
            mine = db['tweets']

            json = []

            for tema in temas:
                regx = re.compile(tema, re.IGNORECASE)
                val = mine.find({"text" : regx}).count()
                response_json = {}
                response_json["name"] = tema
                response_json["value"] = val

                json.append(response_json)

            self.write(dumps(json))

class getFrequencyByTopicByUsername(BaseHandler):
        def get(self, name):
            temas = ['Corrup', 'jep', 'farc', 'presidencial', 'paz', 'candida', 'coca', 'eln', 'narco', 'mermelada',
                     'justicia'];

            client = MongoClient('bigdata-mongodb-01', 27017)
            db = client['Grupo10']
            mine = db['tweets']

            json = []

            for tema in temas:
                regx = re.compile(tema, re.IGNORECASE)
                val = mine.find({"$and":[ {"text" :regx}, {"entities_mentions":name} ]}).count()
                response_json = {}
                response_json["name"] = tema
                response_json["value"] = val

                json.append(response_json)

            self.write(dumps(json))

class getFrequencyByTopicUsedByUser(BaseHandler):
            def get(self, name):
                temas = ['Corrup', 'jep', 'farc', 'presidencial', 'paz', 'candida', 'coca', 'eln', 'narco', 'mermelada',
                         'justicia'];

                client = MongoClient('bigdata-mongodb-01', 27017)
                db = client['Grupo10']
                mine = db['tweets']

                json = []
                otrosJ = {};


                contador = 0;

                for tema in temas:
                    response_json = {}
                    regx = re.compile(tema, re.IGNORECASE)
                    val = mine.find({"$and": [{"text": regx}, {"screen_name": name}]}).count()
                    contador += val


                    if val > 0:
                        response_json["name"] = tema
                        response_json["value"] = val

                        json.append(response_json)

                total = mine.find({"screen_name": name}).count()
                otros = total - contador
                otrosJ["name"] = 'Otros temas'
                otrosJ["value"] = otros

                json.append(otrosJ)

                self.write(dumps(json))

#EN DESARROLLO
class getFrequencyByTopicByUsernameGetHashtags(BaseHandler):
            def get(self, name):
                temas = ['Corrup', 'jep', 'farc', 'presidencial', 'paz', 'candida', 'coca', 'eln', 'narco', 'mermelada',
                         'justicia'];

                client = MongoClient('bigdata-mongodb-01', 27017)
                db = client['Grupo10']
                mine = db['tweets']

                for tema in temas:
                    regx = re.compile(tema, re.IGNORECASE)
                    val = mine.find({"$and": [{"text": regx}, {"entities_mentions": name}]}).count()
                    response_json = {}
                    response_json["name"] = tema
                    response_json["value"] = val

                    #json.append(response_json)

                self.write(dumps(mine.find({"$and": [{"text": regx}, {"entities_mentions": name}]})))

class getUsersByCityandByTopic(BaseHandler):
    def get(self, city, topic):
        client = MongoClient('bigdata-mongodb-01', 27017)
        db = client['Grupo10']
        mine = db['tweets']

        regxCiudad = re.compile(city, re.IGNORECASE)
        regxTopic = re.compile(topic, re.IGNORECASE)

        a = mine.aggregate([
                {'$match': { 'text' : regxTopic }},
                {'$lookup': { 'from': 'users', 'localField': 'user_id', 'foreignField': 'user_id', 'as': "users"}},
                {'$unwind' : "$users"},
                {'$match' : {"users.location": regxCiudad}},
                {'$sort': {"tweet_date": -1}},
                {'$project' : {
                     'screen_name' : 1,
                     'text': 1,
                     'entities_mentions': 1,
                     'sentiment': 1
                        }},
                {'$limit': 50}
                    ])
        self.write(dumps(a))

class getUserNature(BaseHandler):
        def get(self, user):
            client = MongoClient('bigdata-mongodb-01', 27017)
            db = client['Grupo10']
            mine = db['users']

            a = mine.find({'screen_name':'AlvaroUribeVel'},{'user_category':1}).limit(1)

            self.write(dumps(a))

class getEnglishTweets(BaseHandler):
            def get(self):
                client = MongoClient('bigdata-mongodb-01', 27017)
                db = client['Grupo10']
                mine = db['tweets_provided']

                a = mine.find({},{'content':1, 'rating_1':1, 'rating_2':1, 'rating_3':1, 'rating_9':1, '_id':0}).limit(100)

                self.write(dumps(a))

#Metodo main
if __name__ == "__main__":
    app = Application()
    app.listen(8083)
    tornado.ioloop.IOLoop.current().start()