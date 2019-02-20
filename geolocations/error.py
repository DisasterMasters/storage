import math

# Constants
cos_phi = math.sqrt(math.pi / 2)
sec_phi = math.sqrt(2 / math.pi)
R_earth = 6371.0

# Map latitude and longitude to points on a Smyth cylindrical projection.
#
# This needs to be done because an equirectangular projection (i.e. using
# longitude and latitude as horizontal and vertical points directly) is not
# equal area, and points closer to the poles are disproportionately large
# compared to points near the equator.
def smyth_map(lat, lon):
    return (math.radians(lon) * cos_phi * R_earth, math.sin(math.radians(lat)) * sec_phi * R_earth)

# Functions to get lists of the points by the GeoJSON object's type
def __point(coordinates):
    return [smyth_map(coordinates[1], coordinates[0])]

def __multipoint(coordinates):
    return [smyth_map(lat, lon) for [lon, lat] in coordinates]

def __linestring(coordinates):
    return [smyth_map(lat, lon) for [lon, lat] in coordinates]

def __multilinestring(coordinates):
    return [smyth_map(lat, lon) for [lon, lat] in line for line in coordinates]

def __polygon(coordinates):
    return [smyth_map(lat, lon) for [lon, lat] in coordinates[0]]

def __multipolygon(coordinates):
    return [smyth_map(lat, lon) for [lon, lat] in poly[0] for poly in coordinates]

def geojson_error(clat, clon, geojson):
    points = {
        "Point": __point,
        "MultiPoint": __multipoint,
        "LineString": __linestring,
        "MultiLineString": __multilinestring,
        "Polygon": __polygon,
        "MultiPolygon": __multipolygon
    }.get(geojson["type"], lambda _: None)(geojson["coordinates"])

    if points is None:
        return None

    cx, cy = smyth_map(clat, clon)
    return max(math.hypot(x - cx, y - cy) for x, y in points)
