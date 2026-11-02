import json
import sys


def main(file_name, date):
    users_file = "users-{}.json".format(date)
    streams_file = "streams-{}.json".format(date)

    unique_streamers = {}

    with open(file_name, 'r') as f:
        with open(streams_file, 'w+') as output:
            for line in f:
                data = json.loads(line)

                data_to_save = []

                for elem in data['data']:
                    data_to_save.append({
                        "twitch_id": elem['id'],
                        "user_id": elem['user_id'],
                        "game_id": elem['game_id'],
                        "title": elem['title'],
                        "viewer_count": elem['viewer_count'],
                        "started_at": elem['started_at'],
                        "language": elem['language']
                    })

                    if elem['user_id'] not in unique_streamers:
                        unique_streamers[elem['user_id']] = {
                            "user_login": elem['user_login'],
                            "user_name": elem['user_name']
                        }

                output_data = {
                    "timestamp": data['timestamp'],
                    "data": data_to_save
                }

                output.write(json.dumps(output_data) + "\n")

    with open(users_file, 'w+') as f:
        for twitch_id, data in unique_streamers.items():
                f.writelines(json.dumps({"twitch_id": twitch_id, **data}) + "\n")


if __name__ == '__main__':       
    if len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    else:
        print("You forgot something! Usage: python compress-json.py <file-to-compress> <date>")