from __future__ import annotations

import asyncio
import logging
from pprint import pprint
from typing import List, TypeVar, Callable, Any
from functools import wraps

import googleapiclient.discovery
import googleapiclient.errors

# See the very cool https://github.com/rustedpy/result
from result import Ok, Err

import settings
from my_types import (
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


# Note the actual youtube client is not in global scope, just the getter of the client
# youtube: YoutubeClientGetter = build_youtube_client_getter(settings.DEVELOPER_KEY)


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
async def get(youtube: YoutubeClientGetter, channel: Channel) -> ApiResult:
    client = youtube()
    if isinstance(client, Err):
        return Err(client.unwrap_err())

    logging.info(f"Sleeping for 5 seconds before getting {channel.id_}")
    await asyncio.sleep(5)

    request = (
        client.unwrap()
        .channels()
        .list(
            part="brandingSettings,contentDetails,contentOwnerDetails,id,localizations,"
            "snippet,statistics,status,topicDetails",
            id=channel.id_,
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
        return Err(f"[get_channel] Channel not found {channel.id_}")

    return Ok(result)


@api_response
async def get_next_playlist_items_for(
    youtube: YoutubeClientGetter, channel: Channel
) -> ApiResult:
    uploads_playlist_id_result = channel.uploads_playlist_id.result
    if isinstance(uploads_playlist_id_result, Err):
        return Err(uploads_playlist_id_result.unwrap_err())

    client = youtube()
    if isinstance(client, Err):
        return Err(client.unwrap_err())

    logging.info(
        f"Sleeping for 5 seconds before getting playlist items for {channel.title.result.value}"
    )
    await asyncio.sleep(5)

    request = (
        client.unwrap()
        .playlistItems()
        .list(
            part="snippet,contentDetails,status,id",
            playlistId=uploads_playlist_id_result.unwrap(),
            maxResults=MAX_RESULTS,
            pageToken=channel.playlist_items_next_page_token.result.unwrap(),
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
#     channel = await get(youtube, channel.id_)
#     playlist_id = parse_uploads_playlist_from(channel)
#     playlist_items = await get_next_playlist_items_for(youtube, playlist_id)
#     videos = await get_videos_from(youtube, playlist_items)
#     return videos


async def process(channel: Channel):
    # TODO: they all can take youtube,channel right?

    get_channel_response: ApiResponseResult = await get(channel.id_)

    # populate Channel fields that are parseable from the response above
    channel.ingest(get_channel_response)

    # pprint(channel)

    get_playlist_items_response: ApiResponseResult = await get_next_playlist_items_for(
        channel
    )
    logging.info(f"Just got some playlist items for channel {channel.id_}...")

    # TODO: populate Channel fields that are parseable from the response above
    channel.ingest(get_playlist_items_response)

    # pprint(channel)

    # Now do it again
    get_playlist_items_response: ApiResponseResult = await get_next_playlist_items_for(
        channel
    )
    logging.info(f"Just got even more playlist items for channel {channel.id_}...")

    # TODO: populate Channel fields that are parseable from the response above
    channel.ingest(get_playlist_items_response)

    # Since the cost for a videos/ call is 1, we can call it twice for each channel hence getting up
    # to 100 videos. This solves all video calculation requests but:
    # - median of video views from videos released between 30 and 360 days ago
    #   (solves if the date range expires within the 100)
    # - top 2 videos by total views (solves if within latest 100)
    # pprint(channel)


async def channel_worker(worker_name: str, channel_queue: asyncio.Queue) -> None:
    while True:
        # One youtube client per worker
        youtube = build_youtube_client_getter(settings.DEVELOPER_KEY)

        # Get a ChannelId from the queue for processing
        channel = await channel_queue.get()
        logging.info(f"[{worker_name}] says: Now processing channel {channel.id_}...")

        # Process one Channel
        response: ApiResponseResult = await get(youtube, channel)

        # populate Channel fields that are parseable from the response above
        channel.ingest(response)

        # get playlist items for this channel
        playlist_items_response: ApiResponseResult = await get_next_playlist_items_for(
            youtube, channel
        )
        logging.info(
            f"[{worker_name}] says: Just got some playlist items "
            f"for [{channel.title.result.value}]..."
        )

        # Ingest the playlist_items data
        channel.ingest(playlist_items_response)

        # do it again to just check it's working async
        # get playlist items for this channel
        playlist_items_response: ApiResponseResult = await get_next_playlist_items_for(
            youtube, channel
        )
        logging.info(
            f"[{worker_name}] says: Wow! Just got even more playlist items "
            f"for [{channel.title.result.value}]..."
        )

        # Ingest the playlist_items data
        channel.ingest(playlist_items_response)

        logging.info(
            f"[{worker_name}] says: Finished processing [{channel.title.result.value}]:"
        )

        # pprint(channel)

        # Notify the queue this "work item" has been processed
        channel_queue.task_done()


async def main():
    def check_youtube_client_configuration():
        client = build_youtube_client_getter(settings.DEVELOPER_KEY)
        if isinstance(client, Err):
            print("Failed to load the youtube client!\n")
            print(
                "Please copy settings.sample.py into settings.py and update your DEVELOPER_KEY\n"
            )
            raise ValueError(client.unwrap_err())

    def set_logging_level():
        logging.basicConfig(level=settings.LOGGING_LEVEL)

    def initialize():
        check_youtube_client_configuration()
        set_logging_level()

    initialize()

    # Hardcoded list of channels for now, will get ingested possibly from a csv file
    channels: List[Channel] = [
        Channel(id_=ChannelId("UC8butISFwT-Wl7EV0hUK0BQ")),  # Freecodecamp.org
        Channel(id_=ChannelId("UCsUalyRg43M8D60mtHe6YcA")),  # Honeypot IO
        Channel(id_=ChannelId("UC_x5XG1OV2P6uZZ5FSM9Ttw")),  # Google Developers
    ]

    channel_queue = asyncio.Queue()
    for channel in channels:
        # Put all channel_ids in the queue for processing
        channel_queue.put_nowait(channel)

    # Create some worker tasks to process the queue concurrently
    tasks = []
    for i in range(settings.CHANNEL_WORKERS):
        task = asyncio.create_task(channel_worker(f"channel-worker-{i}", channel_queue))
        tasks.append(task)

    # Wait until the queue is fully processed, as in, all Channels have been processed
    await channel_queue.join()

    logging.info("Just finished processing all channels")

    # We're done, cancel the worker tasks so they unlock
    for task in tasks:
        task.cancel()

    # Wait until all worker tasks are cancelled
    await asyncio.gather(*tasks, return_exceptions=True)

    logging.info("All waiting tasks were successfully cancelled, good bye...")

    # youtube: YoutubeClientGetter = build_youtube_client_getter(developer_key)
    # check_youtube_client_getter(youtube)
    #
    # await asyncio.gather(*(process(youtube, channel) for channel in channels))


if __name__ == "__main__":
    asyncio.run(main())
