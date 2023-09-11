import re
import pycountry
import gettext
import requests

from langdetect import detect, lang_detect_exception
from time import sleep
from unidecode import unidecode
from cliff.api import Cliff 
from nltk.corpus import stopwords
from config import cliff_host, cliff_port


class CLIFFLocator:
    def __init__(self):
        self.cliff_api = Cliff('http://{}:{}'.format(cliff_host, cliff_port))
        self.alternative_map = {
            "it": ["italy"],
            "pt": ["portugal"],
            "gb": ["uk"],
            "us": ["us", "usa", "america"],
            "gr": ["greece"],
            "ru": ["russia"],
            "ro": ["romania"],
            "nz": ["nz"],
            "ar": ["argentina"],
            "cz": ["czech"],
            "ae": ["uae"],
            "nl": ["holland"],
            "br": ["brasil"],
            "de": ["deutschland"],
            "es": ["espana"],
            "at": ["osterreich"]
        }

        self.sw = [*stopwords.words('english'), *stopwords.words('german'), *stopwords.words('spanish')]
        self.characters_to_remove = re.compile("[-|!?;:,.+()0123456789]+")

        self.translations = {
            "EN": gettext.translation('iso3166-2', pycountry.LOCALES_DIR, languages=['en']),
            "DE": gettext.translation('iso3166', pycountry.LOCALES_DIR, languages=['de']),
            "ES": gettext.translation('iso3166', pycountry.LOCALES_DIR, languages=['es'])
        }

        self.valid_codes = ["ADM1", "ADM1H", "ADM2", "ADM2H", "ADM3", "ADM3H", "ADM4", "ADM4H", "ADM5", "ADM5H", "PPL", "PPLA", "PPLA2", "PPLA3", 
                "PPLA4", "PPLA5", "PPLC", "PPLCH", "PCLI", "PCLD", "PCLIX"]

     
    def get_location_from_geodata(self, data, description):
        mentions = data["results"]["places"]["mentions"]

        countries = set()
        regions = set()

        for m in mentions:
            if m["featureCode"] in self.valid_codes:
                countries.add(m["countryCode"])
                if m["stateGeoNameId"]:
                    regions.add(m["stateGeoNameId"])

        if len(countries) > 1 or len(regions) > 1:
            return

        places = data["results"]["places"]["focus"]

        countries = []
        for x in places["countries"]:
            if x["countryCode"] in ["TD", "JO", "IL"]:
                continue

            if x["countryCode"] in ["AF", "IQ"] and ("army" in description.lower() or "veteran" in description.lower() or "military" in description.lower()):
                continue 
            
            if x["countryCode"] != "US" and "the bay" in description.lower():
                continue

            if x["countryCode"] == "JP" and ("anime" in description.lower() or "weeb" in description.lower() or "otaku" in description.lower()):
                continue

            countries.append(x)

        if len(countries) == 1:
            country = countries[0]      

            location = {
                "country": country["name"],
                "country_code": country["countryCode"].lower()
            }
            
            states = []
            for state in places["states"]:
                if state["countryCode"] == country:
                    states.append(state["name"])
            
            if len(states) == 1:
                location["region"] = states[0]
            
            cities = []
            for city in places["cities"]:
                if city["name"].lower() in ["aloha"]:
                    return  
                
                if city["countryCode"] == country:
                    cities.append(city["name"])
                
            if len(cities) == 1:
                location["city"] = cities[0]

            return location



    def sanity_check(self, data, language):
        country_data = pycountry.countries.get(alpha_2=data["location"]["country_code"])
        if country_data:
            try:
                country_name_english = pycountry.countries.get(alpha_2=data["location"]["country_code"]).common_name
            except AttributeError:
                country_name_english = pycountry.countries.get(alpha_2=data["location"]["country_code"]).name

            self.translations[language].install()            
            country = "{} {}".format(unidecode(_(country_name_english)), country_name_english)
        else:
            country = data["location"]["country"]

        if "region" not in data["location"] and "city" not in data["location"]:
            country_tokens = country.split()

            found = False
            for t in country_tokens:
                if t in self.sw:
                    continue

                found = found or t.lower() in data["description"].lower()

            if data["location"]["country_code"] in self.alternative_map:
                alternatives = self.alternative_map[data["location"]["country_code"]]

                for alternative in alternatives:
                    found = found or alternative in [self.characters_to_remove.sub(r'', x) for x in data["description"].lower().split()]

            return found

        if "region" in data["location"]:
            country_tokens = country.split()

            found_country = False
            for t in country_tokens:
                if t in self.sw:
                    continue

                found_country = found_country or t.lower() in data["description"].lower()

            if data["location"]["country_code"] in self.alternative_map:
                alternatives = self.alternative_map[data["location"]["country_code"]]

                for alternative in alternatives:
                    found_country = found_country or alternative in [self.characters_to_remove.sub(r'', x) for x in data["description"].lower().split()]

            region_tokens = data["location"]["region"].split()

            found_region = False
            for t in region_tokens:
                if t in self.sw:
                    continue

                found_region = found_region or t.lower() in data["description"].lower()

            return found_country or found_region
        

    def geo_parse(self, description, lang):
        retries = 5

        while retries > 0:
            try:
                return self.cliff_api.parse_text(description, language=lang)
            except requests.exceptions.ConnectionError:
                retries -= 1
                sleep(5)


    def run(self, raw_data, keep_description=False):
        counter = 0

        direct_insert = []
        to_compare = []
        for result in raw_data:
            for user in result['data']:
                plain_description = self.remove_special_char(unidecode(user["description"]))

                if not plain_description.strip():
                    continue

                try:
                    description_language = detect(plain_description)
                except lang_detect_exception.LangDetectException:
                    continue            

                if description_language.upper() in self.translations:
                    language = description_language.upper()
                else:
                    language = "EN"
                
                parsed = self.geo_parse(plain_description, language)

                if parsed:
                    if parsed["results"]["places"]["mentions"]:
                        location = self.get_location_from_geodata(parsed, plain_description)
                        
                        if location:
                            candidate_data = {"twitch_id": user["id"], "location": location, "description": user["description"]}
                            if self.sanity_check(candidate_data, language): 
                                if not keep_description:
                                    candidate_data.pop("description")
                                direct_insert.append(candidate_data)
                            else:
                                to_compare.append(candidate_data)

                    counter += 1

                    if counter % 50 == 0:
                        sleep(1)     

        return direct_insert, to_compare


    def remove_special_char(self, string):
        return string.replace("\\", " ").replace("/", " ").replace("#", " "). replace("@", " ").replace(".", " ").replace("-", " ")
