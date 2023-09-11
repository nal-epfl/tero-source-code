require 'steam_location'
require 'json'

def get_location(args)
    location = {"loccountrycode"=>args[0], "locstatecode"=>args[1], "loccityid"=>args[2].to_i}
    output = SteamLocation.find(location)
    puts "#{output.to_json}"
end

get_location(ARGV)