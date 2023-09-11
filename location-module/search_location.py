import json
import redis
import re
import sys
import geopy.distance
import pymongo
import hmac
import hashlib

import pycountry
from datetime import datetime
from config import secret_key
from time import sleep
from logger import get_logger
from geopy.geocoders import Nominatim, GeoNames
from geopy.exc import GeocoderTimedOut
from config import redis_host, redis_port, redis_password, mongo_host, mongo_port, sleep_on_success, sleep_on_failure, mongo_user, mongo_password, storage, geonames_usename, nominatim_user_agent
from pymongo import MongoClient
from db.online_storage import OnlineStorage

from twitch_descriptions.cliff_locator import CLIFFLocator
from twitch_descriptions.xponents_locator import XponentsLocator
from twitch_descriptions.mordecai_locator import MordecaiLocator

from nlp_utils import compare_nlp_locations, compare_cliff_mordecai, compare_cliff_xponents


nominatim_valid_types = ['bay', 'land_area', 'administrative', 'obsolete_administrative', 'traditional', 'ceremonial', 'state', 'region', 'county', 'city', 'village', 'town', 'hamlet',
                   'census', 'census-designated', 'locality', 'quarter', 'neighbourhood', 'suburb', 'island', 'archipelago', 'historic', 'province', 'political', 'postcode']

geonames_valid_types = ["A", "P", "L", "T"]

known_problems_for_geonames = {
    "tschechische republik": "czechia"
}

class LocationParser:
    def __init__(self) -> None:
        self.storage_controller = OnlineStorage()
        self.logger = get_logger("search_location")
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)  
        self.cache = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
        self.geolocator = Nominatim(user_agent=nominatim_user_agent)
        self.geonames = GeoNames(username=geonames_usename)

    @staticmethod
    def remove_emoji(string):
        emoji_pattern = re.compile("["
                                u"\U0001F600-\U0001F64F"  # emoticons
                                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                u"\U00002500-\U00002BEF"  # chinese char
                                u"\U00002702-\U000027B0"
                                u"\U00002702-\U000027B0"
                                u"\U000024C2-\U0001F251"
                                u"\U0001f926-\U0001f937"
                                u"\U00010000-\U0010ffff"
                                u"\u2640-\u2642"
                                u"\u2600-\u2B55"
                                u"\u200d"
                                u"\u23cf"
                                u"\u23e9"
                                u"\u231a"
                                u"\ufe0f"  # dingbats
                                u"\u3030"
                                "]+", flags=re.UNICODE)
        return emoji_pattern.sub(r' ', string)


    @staticmethod
    def remove_special_char(string):
        characters_to_remove = re.compile("[-|!?;:.+()0123456789]+")
        clean_location = characters_to_remove.sub(r'', string)

        without_hashtags = []
        tokens = clean_location.split()
        for t in tokens:
            if t[0] != '#':
                without_hashtags.append(t.strip())

        return ' '.join(without_hashtags)

    @staticmethod
    def format_geo_info(geodata):
        geolocation_address = geodata.get('address', None)

        if geolocation_address:
            state = None

            if "state" in geolocation_address:
                state = geolocation_address['state']
            elif "region" in geolocation_address:
                state = geolocation_address['region']
            elif "archipelago" in geolocation_address:
                state = geolocation_address['archipelago']

            county = None

            if "county" in geolocation_address:
                county = geolocation_address['county']
            elif "state_district" in geolocation_address:
                county = geolocation_address['state_district']
            elif "boundary" in geolocation_address:
                county = geolocation_address['boundary']

            city = None

            if "village" in geolocation_address:
                city = geolocation_address['village']
            elif "town" in geolocation_address:
                city = geolocation_address['town']
            elif "city" in geolocation_address:
                city = geolocation_address['city']
            elif "municipality" in geolocation_address:
                city = geolocation_address['municipality']
            elif "place" in geolocation_address:
                city = geolocation_address['place']
            elif "natural" in geolocation_address:
                city = geolocation_address['natural']

            subdivision = None
            
            if "subdivision" in geolocation_address:
                subdivision = geolocation_address['subdivision']
            elif "suburb" in geolocation_address:
                subdivision = geolocation_address['suburb']
            elif "borough" in geolocation_address:
                subdivision = geolocation_address['borough']
            elif "district" in geolocation_address:
                subdivision = geolocation_address['district']
            elif "city_district" in geolocation_address:
                subdivision = geolocation_address['city_district']
            elif "place" in geolocation_address:
                subdivision = geolocation_address['place']

            to_return = {
                    "country": geolocation_address.get('country', None),
                    "country_code": geolocation_address.get('country_code', None)
            }

            if state:
                to_return["region"] = state
            if county:
                to_return["county"] = county
            if city:
                to_return["city"] = city
            if subdivision:
                to_return["subdivision"] = subdivision

            to_return["lat"] = geodata.get("lat", None)
            to_return["long"] = geodata.get("lon", None)
            
            return to_return, geodata.get("osm_id", geodata["place_id"])


    def query_geolocation(self, location_str):
        try:
            location = self.geolocator.geocode(location_str, addressdetails=True)
            sleep(sleep_on_success)
        except GeocoderTimedOut as e:
            location = None
            sleep(sleep_on_failure)
        except Exception as e:
            location = None
            sleep(sleep_on_failure)

        return location
    

    @staticmethod
    def compare_locations(loc1, loc2):
        for key, value in loc1.items():
            if key in ["lat", "long"]:
                continue

            if key not in loc2:
                return 1
            elif loc2[key] != value:            
                return -1

        if len(loc2.keys()) > len(loc1.keys()):
            return 2

        return 0


    def merge_georesults(self, georesults):
        # If you found more than one match for a given string
        all_the_same = True
        smallest = georesults[0]
        for l in georesults[1:]:
            result = self.compare_locations(georesults[0], l)
            
            if result == 2:
                smallest = l

            all_the_same = all_the_same and result >= 0

        if all_the_same and smallest.get("country", None):
            return smallest
        
        # If the different results are contradictory you will need to skip this user
        return None


    @staticmethod
    def parse_geonames(geonames_data):
        if "fcl" not in geonames_data or geonames_data["fcl"] not in geonames_valid_types or "countryCode" not in geonames_data:
            return

        parsed = {
            "country": geonames_data["countryName"],
            "country_code": geonames_data["countryCode"].lower()
        }

        if geonames_data["fcl"] == "P":
            parsed["region"] = geonames_data["adminName1"]
            parsed["city"] = geonames_data["name"]

        if geonames_data["fcl"] == "L" or geonames_data["fcl"] == "T":
            parsed["region"] = geonames_data["adminName1"]
        
        parsed["lat"] = geonames_data["lat"]
        parsed["long"] = geonames_data["lng"]

        return parsed


    @staticmethod
    def get_query_dict(location):
        query_dict = {}
        for key, value in location.items():
            if key in ["country", "lat", "long"]:
                continue

            query_dict["location.{}".format(key)] = value

        all_keys = ["region", "county", "city", "subdivision"]
        for key in all_keys:
            if key not in location.keys():
                query_dict["location.{}".format(key)] = {"$exists": False}

        return query_dict


    def merge_results(self, results):
        keys = ["country", "country_code", "region", "county", "city", "subdivision"]
        to_return = {}
        skip = False

        for k in keys:
            if k not in results[0]:
                skip = True
                break
            
            if skip:
                break

            value = results[0][k]

            for r in results[1:]:
                if k not in r or r[k] != value:
                    skip = True
                    break
            
            if skip:
                break

            to_return[k] = value

        to_query = {**self.get_query_dict(to_return), "location.lat": {"$exists": True}}
        
        location = self.mongo_client.location.users.find_one(to_query)
        if location:
            return location["location"]
        else:
            country_name_query = self.mongo_client.location.cache.find({"data.data.country_code": to_return["country_code"].lower()})
            country = None
            for r in country_name_query:
                country = r["data"]["data"]["country"]
                break
            
            raw_locations = []

            if "city" in to_return and "region" not in to_return:
                raw_locations.append("{}, {}".format(to_return["city"], country))
            elif "city" in to_return and "region" in to_return:
                raw_locations.append("{}, {}, {}".format(to_return["city"], to_return["region"], country))
                raw_locations.append("{}, {}".format(to_return["city"], country))
            elif "region" in to_return:
                raw_locations.append("{}, {}".format(to_return["region"], country))
            else:
                raw_locations.append(country)

            for rl in raw_locations:
                try:
                    location = self.geolocator.geocode(rl, addressdetails=True)
                    sleep(sleep_on_success)
                except GeocoderTimedOut as e:
                    location = None
                    sleep(sleep_on_failure)

                if location: 
                    return self.format_geo_info(location.raw)[0]           


    def search_geodata_fuzzy(self, location_str):
        results = [x for x in self.mongo_client.location.cache.find({"$text": {"$search": "\"{}\"".format(location_str)}}, projection=({"query": 1, "data": 1, "score":{"$meta": "textScore"}})).limit(5).sort("score", direction=pymongo.DESCENDING)]
        
        if results:
            merged = self.merge_results([x["data"]["data"] for x in results])
            if merged:
                return merged


    @staticmethod
    def get_to_store(user):
        to_store = {"user_id": hmac.new(secret_key.encode("utf-8"), user["twitch_id"].encode("utf-8"), hashlib.sha1).hexdigest(), "raw_location": user["raw_location"]}

        if "twitter_id" in user:
            to_store["external_id"] = hmac.new(secret_key.encode("utf-8"), user["twitter_id"].encode("utf-8"), hashlib.sha1).hexdigest()

        if "steam_id" in user:
            to_store["external_id"] = hmac.new(secret_key.encode("utf-8"), user["steam_id"].encode("utf-8"), hashlib.sha1).hexdigest()

        if "youtube_id" in user:
            to_store["external_id"] = hmac.new(secret_key.encode("utf-8"), user["youtube_id"].encode("utf-8"), hashlib.sha1).hexdigest()

        return to_store


    def check_cache(self, possible_versions):
        found = []
        for pl in possible_versions:            
            # Check if it's in your cache
            matches_found  = [x for x in self.mongo_client.location.cache.find({"query": pl})]
            if matches_found:
                if "data" in matches_found[0]["data"]:
                    location_info = matches_found[0]["data"]["data"]
                else:
                    location_info = matches_found[0]["data"]

                found.append(location_info)              

        return found


    def format_dict_queries(self, user):
        if not user["location"]["country_code"]:
            return []

        country = None
        country_data = pycountry.countries.get(alpha_2=user["location"]["country_code"])
        if country_data:
            try:
                country = country_data.common_name
            except AttributeError:
                country = country_data.name

        if not country:
            country_name_query = self.mongo_client.location.cache.find({"data.data.country_code": user["location"]["country_code"].lower()})
            country = None
            for r in country_name_query:
                country = r["data"]["data"]["country"]
                break
                
        if not country:
            return []

        raw_locations = []
        if "city" in user["location"] and "region" not in user["location"]:
            raw_locations.append("{}, {}".format(user["location"]["city"], country))
        if "city" in user["location"] and "region" in user["location"]:
            raw_locations.append("{}, {}, {}".format(user["location"]["city"], user["location"]["region"], country))
            raw_locations.append("{}, {}".format(user["location"]["city"], country))
        if "region" in user["location"]:
            raw_locations.append("{}, {}".format(user["location"]["region"], country))
        
        raw_locations.append(country)

        if user["location"]["country_code"] == "uk" and "region" in user["location"]:
            raw_locations.append("{}, United Kingdom".format(user["location"]["region"]))

        return raw_locations      


    def parse_user_location(self, user):
        if "location" not in user:
            return None, False
                    
        self.logger.info("Trying to parse: {}".format(user["location"]))

        raw_locations = []
        if isinstance(user["location"], str):
            raw_locations.append(user["location"])
        elif isinstance(user["location"], dict):
            raw_locations.extend(self.format_dict_queries(user))

        possible_versions = []

        for raw_location in raw_locations:
            if "(great britain)" in raw_location.lower():
                possible_versions.append(raw_location.lower().replace("(great britain)",'').strip())

            possible_versions.extend([
                raw_location.lower().strip(),
                self.remove_emoji(raw_location.lower().strip()).strip(),
                self.remove_special_char(raw_location.lower().strip()).strip()
            ])    

        found = self.check_cache(possible_versions)

        if found:
            parsed_location = found[0]
            if len(found) > 1:
                parsed_location = self.merge_georesults(found)

            if parsed_location:
                return {
                    **user,
                    "location": parsed_location,
                    "raw_location": user["location"]
                }, True
        else:            
            first_found = None
            nominatim_result = None
            nominatim_id = None

            for final_query in possible_versions:
                if nominatim_result:
                    continue

                location = self.query_geolocation(final_query)

                if location:
                    if location.raw['type'] in nominatim_valid_types or location.raw["class"] in nominatim_valid_types or \
                    location.raw['type'] == 'residential' and location.raw['class'] == 'landuse':
                        
                        parsed, location_id = self.format_geo_info(location.raw)
                        nominatim_result = parsed
                        nominatim_id = location_id
                        first_found = final_query
                        
            parsed_location = None

            if nominatim_result:
                # Geonames time: if you managed to find a match you need to confirm it's correct 
                raw_geonames_result = None
                
                self.logger.info("Found nominatim result: {}. Querying GeoNames for: {}".format({"query": first_found.lower(), "data": {"id": nominatim_id, "data": nominatim_result}}, self.remove_emoji(first_found)))

                try:
                    to_query = self.remove_emoji(first_found).lower()

                    for problem, solution in known_problems_for_geonames.items():
                        if problem in to_query:
                            to_query.replace(problem, solution)

                    raw_geonames_result = self.geonames.geocode(to_query)
                    self.logger.info("Found geonames result: {}.".format(raw_geonames_result))
                except (geopy.exc.GeocoderTimedOut, geopy.exc.GeocoderUnavailable):
                    self.logger.info("GeoNames failed with Timeout")
                    pass
            
                if raw_geonames_result:
                    geonames_result = self.parse_geonames(raw_geonames_result.raw)

                    self.logger.info("GeoNames after parsing: {}".format(geonames_result))

                    if nominatim_result and geonames_result:
                        distance = geopy.distance.distance((nominatim_result["lat"], nominatim_result["long"]), (geonames_result['lat'], geonames_result['long'])).km
                    
                        self.logger.info("Comparing locations. Distance: {}".format(distance))

                        if distance < 500:
                            # Save the new information in the geo_data cache
                            parsed_location = nominatim_result
                            self.logger.info("Wrote new result in the geocache.")

                            self.storage_controller.insert_in_cache(first_found, nominatim_id, nominatim_result)
                            
            if parsed_location:
                return {
                    **user,
                    "location": parsed_location,
                    "raw_location": user["location"]
                }, True
            else:
                self.logger.info("Couldn't find a match.")
                # Users without a useful location should also be stored, but on a different pile: yes, a "not_parsed" pile
                to_return = []
                user["original_location"] = user["location"]
                
                if isinstance(user["location"], str):
                    user["description"] = user["location"]
                    
                    to_return.append(user)
                else:
                    str_versions = self.format_dict_queries(user)

                    for x in str_versions:
                        to_return.append({**user, "description": x})
                
                return to_return, False

        return None, False


    def run(self):
        batch_size = 10000
    
        self.logger.info("Starting processing")     
        
        raw_users_to_locate = self.cache.spop("to_locate", count=batch_size)
        
        if raw_users_to_locate:
            with open("{}/users-to-locate/{}-to_locate.json".format(storage, datetime.now().strftime('%Y-%m-%d-%H-%M-%S')), "a+") as f:
                for raw_user in raw_users_to_locate:
                    f.write("{}\n".format(json.dumps(json.loads(raw_user.decode("utf-8")))))

        if not raw_users_to_locate:
            sys.exit(0)    
        
        self.logger.info("Found a list of {} users with location to process".format(len(raw_users_to_locate)))
        
        location_not_parsed = []
        tuple_to_dict = {}
        users_to_locate = {}
                
        for raw_user in raw_users_to_locate:
            x = json.loads(raw_user.decode("utf-8"))
                    
            if isinstance(x["location"], str):
                location = x["location"].lower()
            elif isinstance(x["location"], dict):
                location = tuple(x["location"].values())
                tuple_to_dict[tuple(x["location"].values())] = x["location"]

            if location not in users_to_locate:        
                users_to_locate[location] = []
            
            users_to_locate[location].append(x)
        
        total_processed = 0

        for location, users in users_to_locate.items():
            if location in tuple_to_dict:
                representative = {"location": tuple_to_dict[location]}
            else:
                representative = {"location": location}

            parsed_user, parsed = self.parse_user_location(representative)
            if parsed:
                for user in users:
                    user["location"] = parsed_user["location"]
                    user["raw_location"] = representative["location"]
                    user_to_store = self.get_to_store(user)
                    user_to_store["location"] = user["location"]

                    self.storage_controller.store_parsed(user_to_store, user["twitch_id"])
            elif parsed_user:
                for user in users:
                    for p in parsed_user:
                        user = {**user, **p}
                        location_not_parsed.append(user)    
            
            total_processed += len(users)
            self.logger.info("Finished processing {}/{}".format(total_processed, len(raw_users_to_locate)))

        if location_not_parsed:
            not_parsed = []

            for data in location_not_parsed:
                data["id"] = data["twitch_id"]
                
                if isinstance(data["location"], dict):
                    raw_locations = self.format_dict_queries(data)
                    for r in raw_locations:
                        not_parsed.append({**data, "description": r})
                else:
                    not_parsed.append(data)

            npl_to_parse = []

            cliff_locator = CLIFFLocator()
            direct_insert_cliff, to_compare_cliff = cliff_locator.run([{"data": not_parsed}], keep_description=True)

            xponents_locator = XponentsLocator()
            direct_insert_xponents, to_compare_xponents = xponents_locator.run([{"data": not_parsed}], keep_description=True)

            direct_by_user = {}
            for l in [*direct_insert_cliff, *direct_insert_xponents]:
                if l["twitch_id"] not in direct_by_user:
                    direct_by_user[l["twitch_id"]] = []

                direct_by_user[l["twitch_id"]].append(l)
   
            for l in direct_by_user.values():
                to_insert = None
                if len(l) > 1:
                    comparison = compare_nlp_locations(l[0]["location"], l[1]["location"])
                    if comparison == 0 or comparison == 1:
                        to_insert = l[0]
                    elif comparison == 2:
                        to_insert = l[1]
                elif len(l) == 1:
                    to_insert = l[0]

                if to_insert:            
                    npl_to_parse.append(to_insert)
           
            to_insert, to_check_location = compare_cliff_xponents(to_compare_cliff, to_compare_xponents)                
            for l in to_insert:
                npl_to_parse.append(l)
        
            final_to_check = self.check_locations(to_check_location)
            for l in final_to_check:
                npl_to_parse.append(l)
           
            mordecai_locator = MordecaiLocator()
            to_compare_mordecai = mordecai_locator.run([{"data": not_parsed}])

            to_insert = compare_cliff_mordecai(to_compare_cliff, to_compare_mordecai)
                              
            for l in to_insert:
                npl_to_parse.append(l)
                    
            to_parse_by_loc = {}
            tuple_to_dict = {}
            for x in npl_to_parse:       
                if isinstance(x["location"], str):
                    location = x["location"].lower()
                elif isinstance(x["location"], dict):
                    location = tuple(x["location"].values())
                    tuple_to_dict[tuple(x["location"].values())] = x["location"]

                if location not in to_parse_by_loc:        
                    to_parse_by_loc[location] = []
                
                to_parse_by_loc[location].append(x)

            found_ids = set()
            for location, users in to_parse_by_loc.items():
                if location in tuple_to_dict:
                    representative = {"location": tuple_to_dict[location]}
                else:
                    representative = {"location": location}

                parsed_user, parsed = self.parse_user_location(representative)

                if parsed:
                    for user in users:
                        original = user.pop("description")
                        user["location"] = parsed_user["location"]
                        user["raw_location"] = original

                        user_to_store = self.get_to_store(user)
                        user_to_store["location"] = user["location"]

                        self.storage_controller.store_parsed(user_to_store, user["twitch_id"])

                        if "user_id" in user:
                            found_ids.add(user["user_id"])
                        elif "twitch_id" in user:
                            found_ids.add(user["twitch_id"])

            for x in not_parsed:
                if x["id"] not in found_ids:
                    x_id = x.pop("id")
                    x.pop("description")
                    if "original_location" in x:
                        x.pop("original_location")
                    
                    user = {
                        "twitch_id": x_id,
                        **x
                    }

                    self.storage_controller.insert_not_parsed(user)

        self.logger.info("Found {} users with useful locations".format(self.storage_controller.useful_stored))


    def check_locations(self, to_confirm):        
        final_to_check = []

        for tc in to_confirm:
            to_store = {}
            
            for key in ["xponents", "cliff"]:
                raw_locations = self.format_dict_queries(tc[key])

                possible_versions = []

                for raw_location in raw_locations:
                    if "(great britain)" in raw_location.lower():
                        possible_versions.append(raw_location.lower().replace("(great britain)",'').strip())

                    possible_versions.extend([
                        raw_location.lower().strip(),
                        self.remove_emoji(raw_location.lower().strip()).strip(),
                        self.remove_special_char(raw_location.lower().strip()).strip()
                    ])  
                
                found = self.check_cache(possible_versions)

                if found:
                    parsed_location = found[0]
                    if len(found) > 1:
                        parsed_location = self.merge_georesults(found)

                    if parsed_location:
                        to_store[key] = {"data": tc[key], "location": parsed_location}
                else:
                    location = self.query_geolocation(raw_location)
                    if location:
                        parsed_location, _ = self.format_geo_info(location.raw)

                        to_store[key] = {"data": tc[key], "location": parsed_location}
            
            if "xponents" in to_store and "cliff" in to_store:
                final_to_check.append(to_store)

        to_insert = []
        for b in final_to_check:
            loc_cliff = b["cliff"]["location"]
            loc_xponents = b["xponents"]["location"]

            location_comparison = compare_nlp_locations(loc_xponents, loc_cliff)

            if location_comparison == 0:
                to_insert.append(b["xponents"]["data"])
            elif location_comparison == 1:
                to_insert.append(b["cliff"]["data"])
            elif location_comparison == 2:
                to_insert.append(b["xponents"]["data"])

        return to_insert



if __name__ == '__main__':
    location_parser = LocationParser()
    location_parser.run()   
    