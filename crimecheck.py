import logging

from spyne import Application, srpc, ServiceBase, Iterable, UnsignedInteger, Float, String, rpc
from spyne.protocol.json import JsonDocument
from spyne.protocol.http import HttpRpc
from spyne.server.wsgi import WsgiApplication
import urllib, json
from pprint import pprint
from collections import defaultdict
import datetime, re, operator
from flask import jsonify


class CheckCrimeService(ServiceBase):
    @rpc(Float, Float, Float, _returns=Iterable(String))
    def  checkcrime(ctx, lat, lon, radius):
        url = "https://api.spotcrime.com/crimes.json?lat="+str(lat)+"&lon="+str(lon)+"&radius="+str(radius)+"&key=."
        response = urllib.urlopen(url)
        data = json.loads(response.read())      
        data_crime = data["crimes"]
        #return data["crimes"]
        total_crime=len(data["crimes"])
        print "total crime:"
        print total_crime

        crimetypedict={}
        crimetimedict = { '12:01am-3am': 0, '3:01am-6am': 0, '6:01am-9am': 0, '9:01am-12noon': 0, '12:01pm-3pm': 0,
                    '3:01pm-6pm': 0, '6:01pm-9pm': 0, '9:01pm-12midnight': 0 }
        pattern_search = r'([\d]*\sblock\s)*([\S\s]+\w)'
        street_names={}

        for crime in data_crime:
            crime_date = datetime.datetime.strptime(crime['date'], "%m/%d/%y %I:%M %p")
            if crime_date.replace(hour=0, minute=1) < crime_date <= crime_date.replace(hour=3, minute=0):
                crimetimedict['12:01am-3am'] += 1
            elif crime_date.replace(hour=3, minute=1) < crime_date <= crime_date.replace(hour=6, minute=0):
                crimetimedict['3:01am-6am'] += 1
            elif crime_date.replace(hour=6, minute=1) < crime_date <= crime_date.replace(hour=9, minute=0):
                crimetimedict['6:01am-9am'] += 1
            elif crime_date.replace(hour=9, minute=1) < crime_date <= crime_date.replace(hour=12, minute=0):
                crimetimedict['9:01am-12noon'] += 1
            elif crime_date.replace(hour=12, minute=1) < crime_date <= crime_date.replace(hour=15, minute=0):
                crimetimedict['12:01pm-3pm'] += 1
            elif crime_date.replace(hour=15, minute=1) < crime_date <= crime_date.replace(hour=18, minute=0):
                crimetimedict['3:01pm-6pm'] += 1
            elif crime_date.replace(hour=18, minute=1) < crime_date <= crime_date.replace(hour=21, minute=0):
                crimetimedict['6:01pm-9pm'] += 1
            else:
                crimetimedict['9:01pm-12midnight'] += 1

            if crime['type'] not in crimetypedict :
                crimetypedict[crime['type']]=1
            else:
                crimetypedict[crime['type']]+=1
            
            text = crime['address'].replace('BLOCK BLOCK', 'BLOCK OF').replace('BLOCK OF', 'BLOCK').\
                    replace(' AND ', ' & ')
            address = re.search(pattern_search, text, re.I)
            if address:
                text, " >>> ", address.group(1), " >>> ", address.group(2)
                if address.group(2) not in street_names:
                    street_names[address.group(2)] = 1
                else:
                    street_names[address.group(2)] += 1

        street_names = sorted(street_names.items(), key=operator.itemgetter(1), reverse=True)
        dangerous_streets = [street_names[x][0] for x in range(min(3, len(street_names)))]
        

        
            
        #yield {"total_crime": total_crimes, "crime_type_count": datadict, "event_time_count": crimetimedict}

        finalresult = {'total_crime': total_crime, 'the_most_dangerous_streets':dangerous_streets ,'crime_type_count': crimetypedict, "event_time_count": crimetimedict, }
        
        yield finalresult

if __name__ == '__main__':
    # Python daemon boilerplate
    from wsgiref.simple_server import make_server

    logging.basicConfig(level=logging.DEBUG)

    # Instantiate the application by giving it:
    #   * The list of services it should wrap,
    #   * A namespace string.
    #   * An input protocol.
    #   * An output protocol.
    application = Application([CheckCrimeService], 'spyne.model.complex.checkcrime',
        # The input protocol is set as HttpRpc to make our service easy to
        # call. Input validation via the 'soft' engine is enabled. (which is
        # actually the the only validation method for HttpRpc.)
        in_protocol=HttpRpc(validator='soft'),

        # The ignore_wrappers parameter to JsonDocument simplifies the reponse
        # dict by skipping outer response structures that are redundant when
        # the client knows what object to expect.
        out_protocol=JsonDocument(ignore_wrappers=True),
    )

    # Now that we have our application, we must wrap it inside a transport.
    # In this case, we use Spyne's standard Wsgi wrapper. Spyne supports
    # popular Http wrappers like Twisted, Django, Pyramid, etc. as well as
    # a ZeroMQ (REQ/REP) wrapper.
    wsgi_application = WsgiApplication(application)

    # More daemon boilerplate
    server = make_server('0.0.0.0', 8000, wsgi_application)

    logging.info("listening to http://0.0.0.0:8000")
    logging.info("wsdl is at: http://localhost:8000/?wsdl")

    server.serve_forever()
