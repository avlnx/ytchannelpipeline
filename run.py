from __future__ import annotations

import asyncio
from pprint import pprint
from typing import List, TypeVar, Callable, Any
import argparse
from functools import wraps

import googleapiclient.discovery
import googleapiclient.errors

# See the very cool https://github.com/rustedpy/result
from result import Ok, Err


from my_types import (
    UploadsPlaylistIdResult,
    ChannelId,
    YoutubeClientGetter,
    YoutubeClientResult,
    Channel,
    ApiResponseResult,
    ApiResponse,
    ApiResult,
)


MAX_RESULTS = 1


def build_youtube_client_getter(developer_key: str) -> YoutubeClientGetter:
    """
    A closure that returns a youtube client getter. Can be called multiple times and returns the
    same instance. In preparation for async handling.
    :return: A function that returns a youtube client result
    """
    api_service_name = "youtube"
    api_version = "v3"
    client: YoutubeClientResult = Err("Youtube client has not been built yet")

    def make_client() -> YoutubeClientResult:
        nonlocal client
        if isinstance(client, Err):
            print(f"\n\n{client.value}")
            print("Now building youtube client...")
            try:
                c = googleapiclient.discovery.build(
                    api_service_name,
                    api_version,
                    developerKey=developer_key,
                )
                client = Ok(c)
            except googleapiclient.errors.Error as err:
                client = Err(f"[make_client] {str(err)}")
            except Exception as err:
                client = Err(f"[make_client] {str(err)}")
        return client

    return make_client


def check_youtube_client_getter(youtube: YoutubeClientGetter):
    client = youtube()
    if isinstance(client, Err):
        print("Failed!\n")
        raise ValueError(client.unwrap_err())


F = TypeVar("F", bound=Callable[..., Any])


# decorator that turns ApiResults into ApiResponseResults
def api_response(func: F) -> F:
    @wraps(func)
    async def as_api_response_result(*args, **kwargs) -> ApiResponseResult:
        result = await func(*args, **kwargs)
        if isinstance(result, Err):
            return result

        data = result.unwrap()
        return Ok(ApiResponse(kind=data["kind"], data=data))

    return as_api_response_result


@api_response
async def get_channel(youtube: YoutubeClientGetter, channel_id: ChannelId) -> ApiResult:
    client = youtube()
    if isinstance(client, Err):
        return Err(client.unwrap_err())

    request = (
        client.unwrap()
        .channels()
        .list(
            part="brandingSettings,contentDetails,contentOwnerDetails,id,localizations,"
            "snippet,statistics,status,topicDetails",
            id=channel_id,
        )
    )

    try:
        result = request.execute()
    except googleapiclient.errors.Error as err:
        return Err(f"[get_channel] {str(err)}")
    except Exception as err:
        return Err(f"[get_channel] {str(err)}")

    # Check the channel was actually found since technically it's not an Error :/
    if result["pageInfo"]["totalResults"] == 0:
        return Err(f"[get_channel] Channel not found {channel_id}")

    return Ok(result)


@api_response
async def get_playlist_items_with(
    youtube: YoutubeClientGetter, playlist_id: UploadsPlaylistIdResult
) -> ApiResult:
    if isinstance(playlist_id, Err):
        return Err(playlist_id.unwrap_err())

    client = youtube()
    if isinstance(client, Err):
        return Err(client.unwrap_err())

    request = (
        client.unwrap()
        .playlistItems()
        .list(
            part="snippet,contentDetails,status,id",
            playlistId=playlist_id.unwrap(),
            maxResults=MAX_RESULTS,
        )
    )

    try:
        result = request.execute()
    except googleapiclient.errors.Error as err:
        return Err(f"[get_playlist_items_with] {str(err)}")
    except Exception as err:
        return Err(f"[get_playlist_items_with] {str(err)}")

    return Ok(result)


# @api_response
# async def get_videos_from(
#     youtube: YoutubeClientGetter, playlist_items: PlaylistItemsResult
# ) -> ApiResult:
#     if isinstance(playlist_items, Err):
#         return Err(playlist_items.unwrap_err())
#
#     client = youtube()
#     if isinstance(client, Err):
#         return Err(client.unwrap_err())
#
#     try:
#         video_ids = ",".join(
#             [v["contentDetails"]["videoId"] for v in playlist_items.unwrap()["items"]]
#         )
#     except (KeyError, IndexError, ValueError) as err:
#         return Err(f"[get_videos_from] {str(err)}")
#
#     request = (
#         client.unwrap()
#         .videos()
#         .list(
#             part="snippet,contentDetails,status,id",
#             id=video_ids,
#             maxResults=MAX_RESULTS,
#         )
#     )
#
#     try:
#         result = request.execute()
#     except googleapiclient.errors.Error as err:
#         return Err(f"[get_videos_from] {str(err)}")
#     except Exception as err:
#         return Err(f"[get_videos_from] {str(err)}")
#
#     return Ok(result)


# @api_response
# async def get_videos_for_channel(
#     youtube: YoutubeClientGetter, channel: Channel
# ) -> ApiResult:
#     channel = await get_channel(youtube, channel.id_)
#     playlist_id = parse_uploads_playlist_from(channel)
#     playlist_items = await get_playlist_items_with(youtube, playlist_id)
#     videos = await get_videos_from(youtube, playlist_items)
#     return videos


async def process(youtube: YoutubeClientGetter, channel: Channel):

    get_channel_response: ApiResponseResult = await get_channel(youtube, channel.id_)

    # populate Channel fields that are parseable from the response above
    channel.ingest(get_channel_response)

    get_playlist_items_response: ApiResponseResult = await get_playlist_items_with(
        youtube, channel.uploads_playlist_id.result
    )

    # TODO: populate Channel fields that are parseable from the response above
    channel.ingest(get_playlist_items_response)

    pprint(channel)


async def main(developer_key: str):

    channels: List[Channel] = [
        Channel(id_=ChannelId("UC8butISFwT-Wl7EV0hUK0BQ")),  # Freecodecamp.org
        Channel(id_=ChannelId("UCsUalyRg43M8D60mtHe6YcA")),  # Honeypot IO
        Channel(id_=ChannelId("UC_x5XG1OV2P6uZZ5FSM9Ttw")),  # Google Developers
    ]

    youtube: YoutubeClientGetter = build_youtube_client_getter(developer_key)
    check_youtube_client_getter(youtube)

    await asyncio.gather(*(process(youtube, channel) for channel in channels))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get data for Youtube Channels")
    parser.add_argument("api_key", help="Your Youtube Data Api key")
    api_key = parser.parse_args().api_key

    asyncio.run(main(api_key))
