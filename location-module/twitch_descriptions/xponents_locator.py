import pycountry
import gettext
import cld3

from langdetect import detect
from opensextant.xlayer import XlayerClient, PlaceCandidate
from config import xponents_host


# Requirements: https://hub.docker.com/r/mubaldino/opensextant
class XponentsLocator:
    def __init__(self):
        self.xponents_client = XlayerClient("{}:8787".format(xponents_host)) 
        self.countries_to_discard = ["TD", "JO", "IL", "DM", "GA", "MU", "ML"]

        self.discard_rules = [
            (["AF", "IQ"], ["army", "veteran", "military"]),
            (["JP"], ["anime", "manga", "fan", "weeb", "otaku", "samurai"]),
            (["IN"], ["indie"]),
            (["GR"], ["gris", "kris"]),
            (["SY"], ["aram"]),
            (["UZ"], ["soviet"]),
            (["US"], ["latin"]),
            (["IR"], ["persia"]),
            (["CH"], ["cheese", "knife"])
        ]

        self.translations = {
            "EN": gettext.translation('iso3166-2', pycountry.LOCALES_DIR, languages=['en']),
            "DE": gettext.translation('iso3166', pycountry.LOCALES_DIR, languages=['de']),
            "ES": gettext.translation('iso3166', pycountry.LOCALES_DIR, languages=['es']),
            "PT": gettext.translation('iso3166', pycountry.LOCALES_DIR, languages=['pt']),
            "FR": gettext.translation('iso3166', pycountry.LOCALES_DIR, languages=['fr']),
            "RU": gettext.translation('iso3166', pycountry.LOCALES_DIR, languages=['ru']),
            "JA": gettext.translation('iso3166', pycountry.LOCALES_DIR, languages=['ja']),
            "KO": gettext.translation('iso3166', pycountry.LOCALES_DIR, languages=['ko']),
            "ZH": gettext.translation('iso3166', pycountry.LOCALES_DIR, languages=['zh']),
            "IT": gettext.translation('iso3166', pycountry.LOCALES_DIR, languages=['it'])
        }

        self.direct_insert = []
        self.to_corroborate = []
        self.raw_to_compare = []
        

    def parse_xponents(self, tags):
        location = {}
        for tag in tags:
            if "country@" in tag["match-id"]:
                location["country_code"] = tag["cc"]
            elif "feat_class" in tag:
                location["country_code"] = tag["cc"]
                
                if tag["feat_class"] == "A":                      
                    location["region"] = tag.get("province-name", tag["name"])
                if tag["feat_class"] == "P":    
                    region = tag.get("province-name", "")
                    if region:
                        location["region"] = region
                    
                    location["city"] = tag["name"]

        return location


    def analyze_likely(self, user, base_tags, keep_description):
        tags = [x for x in base_tags if "country@" in x["match-id"] or "MajorPlace" in x["rules"]]

        if [x for x in tags if x["cc"] in self.countries_to_discard]:
            return 

        referenced_countries = set([x["cc"] for x in tags])

        if len(referenced_countries) > 1:
            return 

        if len([x for x in base_tags if "country@" in x["match-id"]]) == len(tags):
            self.to_corroborate.append({"twitch_id": user["id"], "description": user["description"], "tags": tags})
            return

        to_skip = False
        for discard in self.discard_rules:
            for x in tags:
                if x["cc"] in discard[0] and [y for y in discard[1] if y in user["description"].lower()]:
                    to_skip = True 

        if not to_skip:
            if keep_description:
                self.direct_insert.append({"twitch_id": user["id"], "location": self.parse_xponents(tags), "description": user["description"]})
            else:
                self.direct_insert.append({"twitch_id": user["id"], "location": self.parse_xponents(tags)})


    def run(self, raw_data, keep_description=False):        
        for result in raw_data:
            for user in result['data']:
                tags = self.xponents_client.process("test", user["description"], features=["countries", "places"])
                        
                to_store = []
                for t in tags:
                    conf = int(t.attrs.get("confidence", -1))
                    if isinstance(t, PlaceCandidate):
                        if conf >= 30:
                            to_store.append(t)

                if to_store:
                    has_country = []
                    places = []

                    for x in to_store:
                        if x.is_country:
                            has_country.append(x.attrs)
                        else:
                            rules = [y for y in x.rules if y in ["Location.InAdmin", "Location.InCountry"]]
                            if rules:
                                places.append(x.attrs)
                
                    if len(has_country) == 1 and not [x for x in places if x["cc"] != has_country[0]["cc"]]:
                        self.analyze_likely(user, [*has_country, *places], keep_description)
                    elif places:
                        self.raw_to_compare.append({"twitch_id": user["id"], "tags": places, "description": user["description"]})

        self.corraborate_language(keep_description)

        to_compare = []
        
        for u in self.raw_to_compare:
            tags = [x for x in u["tags"] if "country@" in x["match-id"] or "MajorPlace" in x["rules"]]

            if [x for x in tags if x["cc"] in self.countries_to_discard]:
                continue
            
            referenced_countries = set([x["cc"] for x in tags])

            if len(referenced_countries) > 1:
                continue

            to_skip = False
            for discard in self.discard_rules:
                for x in tags:
                    if x["cc"] in discard[0] and [y for y in discard[1] if y in u["description"].lower()]:
                       to_skip = True 

            if not to_skip:
                parsed = self.parse_xponents(tags)
                if parsed:
                    if keep_description:
                        to_compare.append({"twitch_id": u["twitch_id"], "location": parsed, "description": u["description"]})
                    else:
                        to_compare.append({"twitch_id": u["twitch_id"], "location": parsed})

        return self.direct_insert, to_compare


    def corraborate_language(self, keep_description):
        for tc in self.to_corroborate:    
            if not tc["description"]:
                continue
            
            try:
                description_language = detect(tc["description"])
                description_language2 = cld3.get_language(tc["description"])
            except Exception:
                continue

            if description_language == description_language2.language:
                if not description_language.upper() in self.translations:
                    try:
                        self.translations[description_language.upper()] = gettext.translation('iso3166', pycountry.LOCALES_DIR, languages=[description_language.lower()])
                    except Exception:
                        continue
                
                translation = self.translations.get(description_language.upper(), None)
                if translation:
                    country_data = pycountry.countries.get(alpha_2=tc["tags"][0]["cc"])
                    if not country_data:
                        continue

                    country_name_english = country_data.name
                    translation.install()
                    country_name_local = _(country_name_english)

                    if country_name_local.lower() == tc["tags"][0]["name"].lower():
                        if keep_description:
                            self.direct_insert.append({"twitch_id": tc["twitch_id"], "location": self.parse_xponents(tc["tags"]), "description": tc["description"]})
                        else:
                            self.direct_insert.append({"twitch_id": tc["twitch_id"], "location": self.parse_xponents(tc["tags"])})
                    else:
                        self.raw_to_compare.append({**tc, "description": tc["description"]})
            else:
                self.raw_to_compare.append({**tc, "description": tc["description"]})
