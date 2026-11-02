Docker containers:
CLIFF:
    1. docker pull rahulbot/cliff-clavin:2.6.1
    2. docker run --name cliff-clavin -p 8080:8080 -m 8G -d rahulbot/cliff-clavin:2.6.1

Xponents
    1. docker pull mubaldino/opensextant:xponents-3.5
    2. docker run -p 8787:8787 -e XLAYER_PORT=8787 --name xponents-api --rm  --detach mubaldino/opensextant:xponents-3.5

Elasticsearch/Mordecai
    1. docker pull elasticsearch:5.5.2
    2. wget https://andrewhalterman.com/files/geonames_index.tar.gz --output-file=wget_log.txt
    3. tar -xzf geonames_index.tar.gz
    4. docker run -d -p 127.0.0.1:9200:9200 -v $(pwd)/geonames_index/:/usr/share/elasticsearch/data elasticsearch:5.5.2

Special requirements:
    1. Install opensextant (not available in pip): https://github.com/OpenSextant/Xponents-Core/releases/tag/python-v1.6.2
    2. Install ruby: sudo apt install ruby