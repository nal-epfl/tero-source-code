import pycountry
import copy


def compare_nlp_locations(loc1, loc2):
    for key, value in loc1.items():
        if key in ["lat", "long", "country"]:
            continue

        if key not in loc2:
            return 1
        elif loc2[key].lower() != value.lower():            
            return -1

    keys1 = [x for x in loc1.keys() if x not in ["lat", "long", "country"]]
    keys2 = [x for x in loc2.keys() if x not in ["lat", "long", "country"]]

    if len(keys2) > len(keys1):
        return 2

    return 0



def compare_cliff_xponents(cliff_data, xponents_data):
    cliff_locations = {}
    for c in cliff_data:
        cliff_locations[c["twitch_id"]] = c
    
    xponents_locations = {}
    for x in xponents_data:
        xponents_locations[x["twitch_id"]] = x
    
    cliff_xponents_intersect = list(set(xponents_locations.keys()).intersection(set(cliff_locations.keys())))

    to_insert = []
    to_confirm_locations = []

    for user_id in cliff_xponents_intersect:
        loc_xponents = xponents_locations[user_id]["location"]
        loc_cliff = cliff_locations[user_id]["location"]

        location_comparison = compare_nlp_locations(loc_xponents, loc_cliff)

        if location_comparison < 0:
            if loc_cliff["country_code"].lower() == loc_xponents["country_code"].lower():
                to_confirm_locations.append({"xponents": xponents_locations[user_id], "cliff": cliff_locations[user_id]})
        elif location_comparison == 0:
            to_insert.append(xponents_locations[user_id])
        elif location_comparison == 1:
            to_insert.append(xponents_locations[user_id])
        elif location_comparison == 2:
            to_insert.append(cliff_locations[user_id])

    return to_insert, to_confirm_locations



def compare_cliff_mordecai(cliff_data, mordecai_data):
    cliff_locations = {}
    for c in cliff_data:
        cliff_locations[c["twitch_id"]] = c
    
    mordecai_locations = {}
    for x in mordecai_data:
        mordecai_locations[x["twitch_id"]] = x

    intersect_mordecai_cliff = list(set(mordecai_locations.keys()).intersection(set(cliff_locations.keys()))) 

    same_location = []

    for user_id in intersect_mordecai_cliff:
        loc_cliff = cliff_locations[user_id]["location"]

        for loc_mordecai in mordecai_locations[user_id]["location"]: 
            country_data = pycountry.countries.get(alpha_3=loc_mordecai["country_code3"])

            if not country_data:
                continue
            
            loc_mordecai["country_code"] = country_data.alpha_2
            country_cliff = loc_cliff["country_code"]

            if country_cliff.lower() == loc_mordecai["country_code"].lower():
                location_comparison = compare_nlp_locations(loc_mordecai, loc_cliff)

                if location_comparison == 0:
                    loc = copy.deepcopy(mordecai_locations[user_id])
                    loc["location"] = loc_mordecai
                    same_location.append(mordecai_locations[user_id])
                    continue
                elif location_comparison == 1:
                    loc = copy.deepcopy(mordecai_locations[user_id])
                    loc["location"] = loc_mordecai
                    same_location.append(mordecai_locations[user_id])
                    continue
                elif location_comparison == 2:
                    same_location.append(cliff_locations[user_id])
                    continue

    return same_location


def compare_xponents_mordecai(xponents_data, mordecai_data):
    xponents_locations = {}
    for c in xponents_data:
        xponents_locations[c["twitch_id"]] = c
    
    mordecai_locations = {}
    for x in mordecai_data:
        mordecai_locations[x["twitch_id"]] = x

    intersect_mordecai_xponents = list(set(mordecai_locations.keys()).intersection(set(xponents_locations.keys()))) 

    same_location = []

    for user_id in intersect_mordecai_xponents:
        loc_xponents = xponents_locations[user_id]["location"]

        for loc_mordecai in mordecai_locations[user_id]["location"]:       
            country_data = pycountry.countries.get(alpha_3=loc_mordecai["country_code3"])

            if not country_data:
                continue
            
            loc_mordecai["country_code"] = country_data.alpha_2
            country_xponents = loc_xponents["country_code"]

            if country_xponents.lower() == loc_mordecai["country_code"].lower():
                location_comparison = compare_nlp_locations(loc_mordecai, loc_xponents)

                if location_comparison == 0:
                    loc = copy.deepcopy(mordecai_locations[user_id])
                    loc["location"] = loc_mordecai
                    same_location.append(mordecai_locations[user_id])
                    continue
                elif location_comparison == 1:
                    loc = copy.deepcopy(mordecai_locations[user_id])
                    loc["location"] = loc_mordecai
                    same_location.append(mordecai_locations[user_id])
                    continue
                elif location_comparison == 2:
                    same_location.append(xponents_locations[user_id])
                    continue

    return same_location
