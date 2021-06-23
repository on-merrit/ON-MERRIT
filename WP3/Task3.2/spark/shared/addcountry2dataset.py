import csv
from os import listdir
from os.path import isfile, join

from osgeo import ogr
from multiprocessing import Pool

driver = ogr.GetDriverByName('GeoJSON')
countryFile = driver.Open("../data/external/countries.json")
layer = countryFile.GetLayer()


class Point(object):
    """ Wrapper for ogr point """

    def __init__(self, lat, lng):
        """ Coordinates are in degrees """
        self.point = ogr.Geometry(ogr.wkbPoint)
        self.point.AddPoint(lng, lat)

    def getOgr(self):
        return self.point

    ogr = property(getOgr)


class Country(object):
    """ Wrapper for ogr country shape. Not meant to be instantiated directly. """

    def __init__(self, shape):
        self.shape = shape

    def getIso(self):
        return self.shape.GetField('ISO_A3')

    iso = property(getIso)

    def __str__(self):
        return self.shape.GetField('ADMIN')

    def contains(self, point):
        return self.shape.geometry().Contains(point.ogr)


def getCountry(lat, lng):
    """
    Checks given gps-incoming coordinates for country.
    Output is either country shape index or None
    """
    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint(lng, lat)
    for i in range(layer.GetFeatureCount()):
        country = layer.GetFeature(i)
        if country.geometry().Contains(point):
            return Country(country).iso

    # nothing found
    return None



def process_chunk(file):
    with open(file, 'r') as read_obj, open(f"{file}_done.csv", 'w') as write_obj:
        # pass the file object to reader() to get the reader object
        csv_reader = csv.reader(read_obj)
        csv_writer = csv.writer(write_obj)
        # Iterate over each row in the csv using reader object
        count=0
        for row in csv_reader:
            # row variable is a list that represents a row in csv

            if row[2] and row[3]:
                country = getCountry(float(row[2]), float(row[3]))
                row.append(country)
                csv_writer.writerow(row)
            count+=1
            if count%100==0:
                print(f"File {file} progress: {count}/100000")
        print(f"Processing {file} terminated")


allfiles = [join("q1a_latlon_split", f) for f in listdir("q1a_latlon_split") if isfile(join("q1a_latlon_split", f))]

with Pool(32) as p:
    p.map(process_chunk, allfiles)
