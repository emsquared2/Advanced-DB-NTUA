import geopy.distance

# calculate the distance between two points [ lat1 , long1 ] , [ lat2 , long2 ] in km
def get_distance(lat1 , long1 , lat2 , long2):
    return geopy.distance.geodesic((lat1 , long1), (lat2 , long2)).km