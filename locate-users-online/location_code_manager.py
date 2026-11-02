import json
from pymongo import MongoClient

from config import mongo_host, mongo_port, mongo_user, mongo_password, geonames_data_path
from code_utils import get_geonames_entries, get_from_all_countries_entries, get_region_alternative_entries, get_ascii, get_osm_entries

region_special_cases = {
    "fr": {
        "Centre-Loire Valley": "Centre-Val de Loire",
        "Alvèrnia-Roine-Alps": "Auvergne-Rhône-Alpes",
        "Saint-Martin (France)": "Saint-Martin"
    },
    "et": {"ትግራይ": "Tigray"},
    "it": {
        "Friuli-Venezia Giulia": "Friuli Venezia Giulia"
    },
    "mn": {"စစ်ကိုင်းတိုင်း (Sagaing)": "Sagaing"},
    "sa": {"منطقة جازان": "Jazan"},
    "kw": {"الاحمدي": "Al Ahmadi"},
    "ca": {"ᓄᓇᕗᑦ Nunavut": "Nunavut"},
    "np": {"Province #2": "Madhesh Pradesh"},
    "so": {
        "Jubbada Dhexe جوبا الوسطى": "Jubbada Dhexe",
        "Bakool باكول": "Bakool"
        },
    "az": {
    "Quba-Xaçmaz": "Guba-Khachmaz"
    },
    "cn": {
        "\u9999\u6e2f Hong Kong": "Hong Kong",
        "\u6fb3\u9580 Macau": "Macau"
    }
}

city_special_cases = {
    "co": {
        'Bogotá, Distrito Capital': "Bogota"
    },
    "ca": {
        "Vancouver Island": "Vancouver"
    }
}


class LocationCodeManager:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)

    @staticmethod
    def format_region(to_search, country):
        country = country.upper()
        
        if not to_search:
            return []

        if country == "CR":
            to_search = to_search.replace("Provincia","").strip()

        if country == "IE":
            to_search = to_search.replace("The Municipal District of","").strip()

        if country == "NP":
            to_search = to_search.replace("Pradesh","").strip()

        if country == "FI":
            to_search = to_search.replace("aluehallintovirasto","").strip()
        
        if country == "SI":
            to_search = to_search.replace("Upravna Enota","").strip()
             
        if country in ["CN", "DZ", "AF"]:
            list_to_search = [x.strip() for x in to_search.split()]
        elif "/" in to_search:
            list_to_search = [to_search, *[x.strip() for x in to_search.split("/")]]
        elif " -" in to_search or "- " in to_search:
            list_to_search = [to_search, *[x.strip() for x in to_search.split("-")]]
        else:
            list_to_search = [to_search]

        return list_to_search


    def get_region_candidates(self, to_search, country):
        candidates = set()

        list_to_search = self.format_region(to_search, country)
        
        for l in list_to_search:
            r = get_geonames_entries(l.strip())

            if r:
                lines = r.decode("utf-8").split("\n")

                for line in lines:
                    data = line.split("\t")

                    if len(data) < 2 or data[2] == "link":
                        continue
                
                    candidates.add(data[1])
            else:
                r = get_from_all_countries_entries(l.strip())

                lines = r.decode("utf-8").split("\n")

                for line in lines:
                    data = line.split("\t")

                    if len(data) < 2:
                        continue
                
                    candidates.add(data[0])

        return candidates


    def create_id_to_feature_code(self):
        id_to_feature_code = []
        
        with open("{}/allCountries.txt".format(geonames_data_path), "r") as f:
            for l in f:
                data = l.strip().split("\t")
        
                if data[6] == "A" and ("ADM" in data[7] or data[7] == "PCLD" or data[7] == "PCLS"):
                    id_to_feature_code.append({"id": data[0], "name": data[1], "code": data[7], "country": data[8], "is_dependant": data[7] == "PCLD" or data[7] == "PCLS"})

                if data[6] == "A" and data[8] in ["US"] and data[7] == "TERR":
                    id_to_feature_code.append({"id": data[0], "name": data[1], "code": data[7], "country": data[8], "is_dependant": data[7] == "PCLD"})

                if data[6] == "L" and data[8] in ["TR", "AR", "PH", "LV", "BR", "TZ", "AZ"] and data[7] == "RGN":
                    id_to_feature_code.append({"id": data[0], "name": data[1], "code": "ADM1", "country": data[8], "is_dependant": data[7] == "PCLD"})

                if data[6] == "A" and data[8] == "SJ" and data[7] == "TERR":
                    id_to_feature_code.append({"id": data[0], "name": data[1], "code": "ADM1", "country": "NO", "is_dependant": data[7] == "PCLD"})

            self.mongo_client.location_data.id_to_feature_code.insert_many(id_to_feature_code)


    def get_new_region_mapping(self, country, region):
        if self.mongo_client.location_data.id_to_feature_code.count_documents({}, limit=1) == 0:
            self.create_id_to_feature_code()
        
        if country in region_special_cases and region in region_special_cases[country]:
            candidates = self.get_region_candidates(region_special_cases[country][region], country)
        else:
            candidates = self.get_region_candidates(region, country)
            
        chosen_codes = []
        for c in candidates:
            c = self.mongo_client.location_data.id_to_feature_code.find_one({"id": c}, projection={"_id": False})

            if c:
                if (c["country"].lower() == country or c["is_dependant"]):
                    chosen_codes.append({"geoname_id": c, "code": c["code"]})

        chosen_codes.sort(key=lambda x: x["code"])
            
        if chosen_codes:
            self.mongo_client.location_data.region_mappings.insert_one({"country_code": country, "original_name": region, **chosen_codes[0]})
            
            return chosen_codes[0]["geoname_id"]["id"]
        else:
            self.mongo_client.location_data.missing_region_mappings.insert_one({"country_code": country, "original_name": region})            


    def get_region_translations(self, country, region):
        # What to do with the missing translations?
        missing_translations = []
        
        alternatives = get_region_alternative_entries(region)
            
        if alternatives:
            lines = alternatives.decode("utf-8").split("\n")

            to_evaluate = []

            for line in lines:
                data = line.split("\t")

                if len(data) < 2 or data[2] == "link" or data[1] != region:
                    continue

                to_evaluate.append(data)
        
            if not to_evaluate:
                missing_translations.append(region)
                return

            candidates = []
        
            for a in to_evaluate:
                if a[1] == region and len(a) > 2:
                    candidates.append(a)

            candidates.sort(key=lambda x: (x[2] == "en", str(x[4]) == "1"), reverse=True)
                
            if candidates:
                official_name = get_ascii(candidates[0][3])
                
                if official_name:
                    if self.mongo_client.location_data.regions.count_documents({"region": region}, limit=1) == 0:
                        region_data = get_from_all_countries_entries(region)

                        lines = region_data.decode("utf-8").strip().split("\n")
                        filtered_lines = []
                        for l in lines:
                            data = l.strip().split("\t")
                            if data[0] == region:
                                filtered_lines.append(data)
                        
                        if len(filtered_lines) == 1:
                            data = filtered_lines[0]

                            self.mongo_client.location_data.regions.insert_one({"country_code": country, "region": region, "lat": data[4], "long": data[5], "official_name": official_name})
            
            translations = []
            for c in candidates:
                translations.append({"region_id": region, "name": c[3], "language": c[2], "country_code": country})
            
            if translations:
                for translation in translations:
                    if self.mongo_client.translations.regions.count_documents(translation, limit=1) == 0:
                        self.mongo_client.translations.regions.insert_one(translation)
            else:
                missing_translations.append(region)

    @staticmethod
    def get_city_translations(country, city_data):
        official_name = ""

        if city_data.get("other_names", {}):                
            if "name:en" in city_data["other_names"]:
                official_name = get_ascii(city_data["other_names"]["name:en"])

        if not official_name:
            if "name" in city_data:
                official_name = get_ascii(city_data["name"])
            elif "display_name" in city_data:
                official_name = get_ascii(city_data["display_name"])
        
        translations = []
        for language, name in city_data.get("other_names", {}).items():
            if "name:" in language:
                translations.append({"city_id": city_data["osm_id"], "name": name, "language": language.replace("name:", ""), "country_code": country})

        return translations, official_name

    @staticmethod
    def get_admin_subdivision(address_data):
        if address_data.get("state"):
            return address_data.get("state")
        elif address_data.get("region"):
            return address_data.get("region")
        elif address_data.get("county"):
            return address_data.get("county")
        elif address_data.get("archipelago"):
            return address_data.get("archipelago")

        return None

    @staticmethod
    def get_order(element):
        order = ["city", "town", "village", "hamlet"]

        try:
            return order.index(element.lower())
        except ValueError:
            return len(order)


    def get_city_id(self, country, region_id, city_data):
        city_name = city_data["city"]
        if country in city_special_cases and city_name in city_special_cases[country]:
            city_name = city_special_cases[country][city_name]

        city_entries = get_osm_entries(country, city_name)
            
        if not city_entries:
            return

        city_entries = [x.strip() for x in city_entries.decode("utf8").strip().split("\n")]

        city_candidates = []

        for l in city_entries:
            data = json.loads(l.strip())
        
            if ("name" in data and (data["name"].lower() in city_name.lower() or city_name.lower() in data["name"].lower())) or \
               ("display_name" in data and (data["display_name"].lower() in city_name.lower() or city_name.lower() in data["display_name"].lower())):
                city_candidates.append(data)

        # Last attempt: if no candidates were found, use translations
        if not city_candidates:
            for l in city_entries:
                data = json.loads(l.strip())

                city_translations = set([x["name"].lower() for x in self.mongo_client.translations.cities.find({"city_id": str(data["osm_id"])}, projection={"_id": False})])

                if city_name.lower() in city_translations:
                    city_candidates.append(data)

        city_candidates.sort(key=lambda x: self.get_order(x["type"]))
        chosen_candidate = None

        if len(city_candidates) == 1:
            chosen_candidate = city_candidates[0]
        else:
            if region_id:
                region_translations = set([x["name"] for x in self.mongo_client.translations.regions.find({"region_id": region_id}, projection={"_id": False})])
            
                for x in self.mongo_client.translations.regions.find({"geoname_id": region_id}, projection={"_id": False}):
                    region_translations.add(x["original_name"])
                    region_translations.add(x["name"])
                    region_translations.add(x["official_name"])
                                                                
                for c in city_candidates:
                    subdivision = self.format_region(self.get_admin_subdivision(c["address"]), country)

                    if (subdivision and len(set(subdivision).intersection(region_translations)) > 0) or ("name" in c and c["name"].lower() == city_name.lower()) or \
                        ("display_name" in c and c["display_name"].lower() == city_name.lower()):
                            chosen_candidate = c
                            break
            else:
                for c in city_candidates:
                    if ("name" in c and c["name"].lower() == city_name.lower()) or \
                       ("display_name" in c and c["display_name"].lower() == city_name.lower()):
                        chosen_candidate = c
                        break

        if chosen_candidate:
            translations, official_name = self.get_city_translations(country, chosen_candidate)                    

            if region_id:
                location_data_to_insert = {"country_code": country, "region": str(region_id), "city": str(chosen_candidate["osm_id"]), "lat": city_data["lat"], "long": city_data["long"], "official_name": official_name}
            else:
                location_data_to_insert = {"country_code": country, "city": str(chosen_candidate["osm_id"]), "lat": city_data["lat"], "long": city_data["long"], "official_name": official_name}
            
            if self.mongo_client.location_data.cities.count_documents({"city": str(chosen_candidate["osm_id"])}, limit=1) == 0:
                self.mongo_client.location_data.cities.insert_one(location_data_to_insert)

                if translations:
                    self.mongo_client.translations.cities.insert_many(translations)

            return str(chosen_candidate["osm_id"])
        
        return None
    
