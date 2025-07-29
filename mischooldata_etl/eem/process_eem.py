from transform import transform_eem
from load import load_eem, load_school_geocode
from geocode import geocode_schools


transform_eem()
geocode_schools()
load_eem()
load_school_geocode()
