#
# Assignment5 Interface
# Name:Saicharan Papani
#

from pymongo.collation import Collation, CollationStrength
import math


def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    filter= {
        'city': cityToSearch
    }
    project = {
        'name': 1, 
        'full_address': 1, 
        'city': 1, 
        'state': 1
    }
    
    result = collection.find(
      filter=filter,
      projection=project
    ).collation(
    Collation(locale='en', strength=2))
    with open(saveLocation1, 'w') as f:
        for i in result:
            print(i)
            line = "$".join([i['name'].upper(), i['full_address'].upper(), i['city'].upper(), i['state'].upper()])
            f.write(line)
            f.write('\n')

# function is already given
def DistanceFunction(lat2, lon2, lat1, lon1):
    R = 3959
    φ1 = math.radians(lat1)
    φ2 = math.radians(lat2)
    Δφ = math.radians((lat2-lat1))
    Δλ = math.radians((lon2-lon1))
    a = math.sin(Δφ/2) * math.sin(Δφ/2) + math.cos(φ1) * math.cos(φ2) * math.sin(Δλ/2) * math.sin(Δλ/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c
    return d

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    results = collection.find({"categories": {"$in": categoriesToSearch}})
    with open(saveLocation2, 'w') as f:
        for doc in results:
            distance = DistanceFunction(float(doc['latitude']), float(doc['longitude']), float(myLocation[0]), float(myLocation[1]))
            if distance < maxDistance:
                f.write(doc['name'].upper())
                f.write('\n')
    
    
