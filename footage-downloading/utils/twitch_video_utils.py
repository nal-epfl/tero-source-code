import pytz
import requests
import m3u8

from datetime import datetime, timedelta
from dateutil import parser

from config import CLIENT_ID

VIDEO_FIELDS = """
    id
    title
    publishedAt
    broadcastType
    lengthSeconds
    game {
        name
    }
    creator {
        login
        displayName
    }
"""


localtz = pytz.timezone('Europe/Zurich')

class InvalidUserException(Exception):
    def __init__(self, msg):
        self.msg = msg


class GQLError(Exception):
    def __init__(self, errors):
        super().__init__("GraphQL query failed")
        self.errors = errors


def get_date_diff(date1, date2):
    diff = date2 - date1 if date1 < date2 else date1 - date2
    return diff


def get_video_data(user_login, stream_starting_time):
    default_time = "2020-01-01T00:00:00Z"        
        
    try:
        video_info = get_channel_videos(user_login)
    except (GQLError, InvalidUserException):
        return None

    stream_time = datetime.fromtimestamp(stream_starting_time)
    stream_time = localtz.localize(stream_time)

    should_keep = True

    while should_keep:
        for video in video_info["edges"]:
            video_time = video.get("node", {}).get("publishedAt", default_time)

            video_start = parser.parse(video_time)
            time_diff = get_date_diff(video_start, stream_time)

            if time_diff < timedelta(minutes=1):
                return video

        should_keep = False
        if video_info["pageInfo"]["hasNextPage"]:
            should_keep = True
            try:
                video_info = get_channel_videos(user_login, after=video_info["edges"][-1]["cursor"])
            except (GQLError, InvalidUserException):
                return None

    return None


def authenticated_post(url, data=None, json=None, headers={}):
    headers['Client-ID'] = CLIENT_ID

    response = requests.post(url, data=data, json=json, headers=headers)
    if response.status_code == 400:
        data = response.json()
        raise Exception(data["message"])
    
    return response


def gql_query(query: str, headers={}):
    url = "https://gql.twitch.tv/gql"
    response = authenticated_post(url, json={"query": query}, headers=headers).json()

    if "errors" in response:
        raise GQLError(response["errors"])

    return response


def get_access_token(video_id, auth_token=None):
    query = """
    {{
        videoPlaybackAccessToken(
            id: "{video_id}",
            params: {{
                platform: "web",
                playerBackend: "mediaplayer",
                playerType: "site"
            }}
        ) {{
            signature
            value
        }}
    }}
    """

    query = query.format(video_id=video_id)

    headers = {}
    if auth_token is not None:
        headers['authorization'] = f'OAuth {auth_token}'

    try:
        response = gql_query(query, headers=headers)
        return response["data"]["videoPlaybackAccessToken"]
    except requests.HTTPError as error:
        # Provide a more useful error message when server returns HTTP 401
        # Unauthorized while using a user-provided auth token.
        if error.response.status_code == 401:
            raise Exception("Unauthorized. The provided auth token is not valid.")
            

def get_playlists(video_id, access_token):
    url = "https://usher.ttvnw.net/vod/{}".format(video_id)

    response = requests.get(url, params={
        "nauth": access_token['value'],
        "nauthsig": access_token['signature'],
        "allow_audio_only": "true",
        "allow_source": "true",
        "player": "twitchweb",
    })
    
    return response.content.decode('utf-8')


def parse_playlists(playlists_m3u8):
    playlists = m3u8.loads(playlists_m3u8)

    for p in sorted(playlists.playlists, key=lambda p: p.stream_info.resolution is None):
        if p.stream_info.resolution:
            name = p.media[0].name
            description = "x".join(str(r) for r in p.stream_info.resolution)
        else:
            name = p.media[0].group_id
            description = None

        yield name, description, p.uri


def get_correct_playlist(playlists, specify_quality=list()):
    if not playlists:
        return

    if specify_quality:
        base_qualities = specify_quality
    else:
        base_qualities = ["1080p30", "1080p60"]

    _, _, source_uri = playlists[0]

    for name, _, uri in playlists:
        if name in base_qualities:
            return uri

    # If you haven't found the correct quality, go back to source
    return source_uri


def get_files_from_playlist(playlist, start, end):
    files = []
    vod_start = 0
    for segment in playlist.segments:
        vod_end = vod_start + segment.duration

        # `vod_end > start` is used here becuase it's better to download a bit
        # more than a bit less, similar for the end condition
        start_condition = not start or vod_end > start
        end_condition = not end or vod_start < end

        if start_condition and end_condition and segment.uri not in files:
            files.append({"segment_uri": segment.uri, "ts_start": vod_start, "ts_end": vod_end})

        vod_start = vod_end
    
    return files


def get_channel_videos(channel_name, limit=10, sort="time", type="archive", game_ids=[], after=None):
    query = """
    {{
        user(login: "{channel_name}") {{
            videos(
                first: {limit},
                type: {type},
                sort: {sort},
                after: "{after}",
                options: {{
                    gameIDs: {game_ids}
                }}
            ) {{
                totalCount
                pageInfo {{
                    hasNextPage
                }}
                edges {{
                    cursor
                    node {{
                        {fields}
                    }}
                }}
            }}
        }}
    }}
    """

    query = query.format(
        channel_name=channel_name,
        game_ids=game_ids,
        after=after if after else "",
        limit=limit,
        sort=sort.upper(),
        type=type.upper(),
        fields=VIDEO_FIELDS
    )

    response = gql_query(query)

    if not response["data"]["user"]:
        raise InvalidUserException("Channel {} not found".format(channel_name))

    return response["data"]["user"]["videos"]
