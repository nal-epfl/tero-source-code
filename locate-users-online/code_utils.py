import subprocess
import unicodedata
import os

from config import osm_data_path, geonames_data_path


def get_osm_entries(country_code, city_name):
    if not os.path.isdir('{}/{}'.format(osm_data_path, country_code)):
        return None

    result = subprocess.run(['grep', '-hri', city_name, '{}/{}'.format(osm_data_path, country_code)], stdout=subprocess.PIPE)
    return result.stdout


def get_geonames_entries(base_name):
    result = subprocess.run(['grep', '-i', base_name, '{}/alternateNamesV2.txt'.format(geonames_data_path)], stdout=subprocess.PIPE)
    return result.stdout


def get_from_all_countries_entries(base_name):
    result = subprocess.run(['grep', '-i', base_name, '{}/allCountries.txt'.format(geonames_data_path)], stdout=subprocess.PIPE)
    return result.stdout


def get_region_alternative_entries(geoname_id):
    result = subprocess.run(['grep', '-i', geoname_id, '{}/alternateNamesV2.txt'.format(geonames_data_path)], stdout=subprocess.PIPE)
    return result.stdout


def get_ascii(text):
    return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode("ascii")
