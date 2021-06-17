#
# Assignment5 Interface
# Name: 
#


from pymongo import MongoClient
import os
import sys
import json
import math

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    # regex : to find the pattern matching with cityToSearch
    # option i : Both lower and upper capitals
    reads = collection.find({'city':{'$regex':cityToSearch, '$options':"$i"}})
    res = open(saveLocation1,'w')
    for rd in reads:
        name = rd['name']
        full_address = rd['full_address'].replace("\n",", ")
        city = rd['city']
        state = rd['state']
        res.write(name.upper()+"$"+full_address.upper()+"$"+city.upper()+"$"+state.upper())
        res.write("\n")

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    readLoc = collection.find({"categories":{'$in':categoriesToSearch}},{'name':1,'latitude':1,'longitude':1,'categories':1})
    lat = float(myLocation[0])
    lon = float(myLocation[1])
    res = open(saveLocation2,'w')
    for row in readLoc:
        name = row['name']
        latitude = float(row['latitude'])
        longitude = float(row['longitude'])
        if maxDistance >= DFunction(float(lat),float(lon),float(latitude),float(longitude)):
            res.write(name.upper()+"\n")

def DFunction(lat, lon, latitude, longitude):
    R = 3959 # miles
    phi1 = math.radians(lat)
    phi2 = math.radians(latitude)
    delphi = math.radians(latitude-lat)
    dellam = math.radians(longitude-lon)
    a = math.sin(delphi/2)*math.sin(delphi/2)+math.cos(phi1)*math.cos(phi2)*math.sin(dellam/2)*math.sin(dellam/2)
    c = 2*math.atan2(math.sqrt(a),math.sqrt(1-a))
    d = R*c
    return d
