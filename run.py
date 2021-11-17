import json

import googleapiclient.discovery
import googleapiclient.errors

# this is the meat, the bread and butter, the functionality
# this can be pipelined
# write the code that takes one channelId string identifier and outputs the data, note quota costs

# write parsers starting with a csv parser for the channelIds

# now we have a list of channelIds (which could be large, in the thousands)

#

# write outputs starting with a csv output


def main():
    # Note we don't need oauth for read only stuff (like the channels deal)
    # TODO@thyago Remember to properly install google-api-python-client before deploying
    #  anything

    # scopes = ["https://www.googleapis.com/auth/youtube.readonly"]
    #
    # # Disable OAuthlib's HTTPS verification when running locally.
    # # *DO NOT* leave this option enabled in production.
    # os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    # client_secrets_file = "YOUR_CLIENT_SECRET_FILE.json"
    #
    # # Get credentials and create an API client
    # flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
    #     client_secrets_file, scopes
    # )
    # credentials = flow.run_console()
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey="AIzaSyA28knBUSEtGQjlqFyL0iVNlIHA1xD_wdQ"
    )

    # request = youtube.channels().list(
    #     part="snippet,contentDetails,statistics,brandingSettings", id="UC_x5XG1OV2P6uZZ5FSM9Ttw"
    # )

    # request = youtube.playlists().list(
    #     part="snippet,contentDetails,status,id", id="UU_x5XG1OV2P6uZZ5FSM9Ttw"
    # )

    request = youtube.playlistItems().list(
        part="snippet,contentDetails,status,id",
        playlistId="UU_x5XG1OV2P6uZZ5FSM9Ttw",
        maxResults=10,
    )

    response = request.execute()

    print(json.dumps(response, indent=4))


if __name__ == "__main__":
    main()
