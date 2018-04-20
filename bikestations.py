# -*- coding: utf-8 -*-
"""
Created on Tue Apr 07 15:11:49 2014
@author: Maurizio Napolitano <napo@fbk.eu>
The MIT License (MIT)
Copyright (c) 2016 Fondazione Bruno Kessler http://fbk.eu
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""
#from __future__ import unicode_literals
#from builtins import str
from pysqlite2 import dbapi2 as sqlite3
import requests, datetime

class Bikestations():
    url = "https://os.smartcommunitylab.it/core.mobility/bikesharing/"
    db = "bikestations_trentino.sqlite"
    cities = [
        "trento",
        "rovereto",
        "pergine_valsugana",
        "lavis",
        "mezzocorona",
        "mezzolombardo",
        "sanmichelealladige"
    ]
    defaultCity = cities[0]
    wgs84 = 4326

    def __init__(self):
        self.con = sqlite3.connect(self.db)
        self.con.enable_load_extension(True)
        self.cur = self.con.cursor()
        print 'load mod_spatialite...'
        self.cur.execute('SELECT load_extension("mod_spatialite")')

        check = self.cur.execute('''
            SELECT COUNT(name) 
            FROM sqlite_master 
            WHERE type = 'table' AND name = 'geometry_columns';
        ''')
        if (check.fetchall()[0][0] == 0):
            print 'init spatialite...'
            self.cur.execute('SELECT InitSpatialMetadata();')

        create_stations = '''
            CREATE TABLE IF NOT EXISTS stations (
                id TEXT NOT NULL,
                city TEXT,
                name TEXT,
                address TEXT,
                latitude REAL,
                longitude REAL,
                slots INTEGER
            );
        '''
        addGeometry_stations = "SELECT AddGeometryColumn('stations', 'geom', %s, 'POINT', 'XY');" % self.wgs84
        create_bikesuse= '''
            CREATE TABLE IF NOT EXISTS bikeuse (
                id INTEGER PRIMARY KEY ASC,
                idstation TEXT,
                bikes INTEGER,
                slots INTEGER,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        '''
        create_bikesusegeo= '''
            CREATE VIEW IF NOT EXISTS bikeusegeo AS 
            SELECT stations.id, stations.geom AS the_geom, bikeuse.timestamp, bikeuse.bikes, bikeuse.slots 
            FROM bikeuse, stations 
            WHERE bikeuse.idstation = stations.id;
        '''
        addGeometry_bikeusegeo = "SELECT AddGeometryColumn('bikeusegeo', 'geom', %s, 'POINT', 'XY');" % self.wgs84
        
        self.cur.execute(create_stations);
        check = self.cur.execute('''
            SELECT COUNT(name) 
            FROM sqlite_master 
            WHERE type = 'trigger' AND name = 'ggi_stations_geom';
        ''')
        if (check.fetchall()[0][0] == 0):
            print 'create table stations...'
            self.cur.execute(addGeometry_stations)

        self.cur.execute(create_bikesusegeo)

        # check = self.cur.execute('''
        #     SELECT COUNT(name) 
        #     FROM sqlite_master 
        #     WHERE type='trigger' AND name = 'ggi_bikeusegeo_geom';
        # ''')
        # if (check.fetchall()[0][0] == 0):
        #     print 'create table bikeusegeo...'
        self.cur.execute(addGeometry_bikeusegeo)

        print 'create table bikeuse...'
        self.cur.execute(create_bikesuse)
        
        cname = self.cur.execute("SELECT COUNT(name) FROM stations").fetchone()[0]
        if cname == 0:
            for city in self.cities:
                print 'insert stations for city '+ city
                urlc = self.url + city
                print urlc
                r = requests.get(urlc)
                rows =  r.json()
                for row in rows:
                    idstation = row['id']
                    address = row['address']
                    slots = row['slots']
                    name = row['name']
                    latitude = row['position'][1]
                    longitude = row['position'][0]
                    #geomfromtext = "GeomFromText('POINT(%s %s)', %s)" % (latitude, longitude, self.wgs84)
                    insertsql = "INSERT INTO stations VALUES (?,?,?,?,?,?,?, GoempFromText('POINT(%f %f)', %u) );" % (latitude, longitude, self.wgs84)
                    insertvals = (idstation, city, name, address, latitude, longitude, slots)
                    self.cur.execute(insertsql, insertvals)
            self.con.commit()

    def addbikes(self, city = None):
        city = city if city is not None else self.defaultCity
        urlc = self.url + city
        r = requests.get(urlc)
        rows =  r.json()
        for row in rows:
            idstation = row['id']
            bikes = row['bikes']
            slots = row['slots']
            insertsql = '''
                INSERT INTO bikeuse (idstation, bikes, slots) 
                    VALUES ('%s', %s, %s)
            ''' % (idstation, bikes, slots)
            self.cur.execute(insertsql)
        self.con.commit()
