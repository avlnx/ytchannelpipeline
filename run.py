from __future__ import annotations

import asyncio
import logging
import math
import random
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


F = TypeVar("F", bound=Callable[..., Any])


def api_response(func: F) -> F:
    """
    Decorator used to standardize responses to an expected format
    :param func: Api getter function
    :return: an ApiResponse wrapped in an ApiResponseResult
    """

    @wraps(func)
    async def as_api_response_result(*args, **kwargs) -> ApiResponseResult:
        result = await func(*args, **kwargs)
        if isinstance(result, Err):
            return result

        data = result.unwrap()
        return Ok(ApiResponse(kind=data["kind"], data=data))

    return as_api_response_result


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


# Note the actual youtube client is not in global scope, just the getter of the client (a closure)
youtube: YoutubeClientGetter = build_youtube_client_getter(settings.DEVELOPER_KEY)


@api_response
async def get(channel: Channel) -> ApiResult:
    client = youtube()
    if isinstance(client, Err):
        return Err(client.unwrap_err())

    await nap_before(f"getting {channel.id_}")

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
        return Err(f"[get] {str(err)}")
    except Exception as err:
        return Err(f"[get] {str(err)}")

    # Check the channel was actually found since technically it's not an Error :/
    if result["pageInfo"]["totalResults"] == 0:
        return Err(f"[get] Channel not found {channel.id_}")

    return Ok(result)


@api_response
async def get_next_playlist_items_for(channel: Channel) -> ApiResult:
    uploads_playlist_id_result = channel.uploads_playlist_id.result
    if isinstance(uploads_playlist_id_result, Err):
        return Err(uploads_playlist_id_result.unwrap_err())

    client = youtube()
    if isinstance(client, Err):
        return Err(client.unwrap_err())

    await nap_before(f"getting playlist items for {channel.title.result.value}")

    request = (
        client.unwrap()
        .playlistItems()
        .list(
            part="snippet,contentDetails,status,id",
            playlistId=uploads_playlist_id_result.unwrap(),
            maxResults=settings.MAX_RESULTS,
            pageToken=channel.playlist_items_next_page_token.result.unwrap(),
        )
    )

    try:
        result = request.execute()
    except googleapiclient.errors.Error as err:
        return Err(f"[get_next_playlist_items_for] {str(err)}")
    except Exception as err:
        return Err(f"[get_next_playlist_items_for] {str(err)}")

    return Ok(result)


@api_response
async def get_videos_with(ids: str) -> ApiResult:

    client = youtube()
    if isinstance(client, Err):
        return Err(client.unwrap_err())

    await nap_before(f"getting videos {ids}")

    request = (
        client.unwrap()
        .videos()
        .list(
            part="snippet,contentDetails,status,id",
            id=ids,
            maxResults=settings.MAX_RESULTS,
        )
    )

    try:
        result = request.execute()
    except googleapiclient.errors.Error as err:
        return Err(f"[get_videos_with] {str(err)}")
    except Exception as err:
        return Err(f"[get_videos_with] {str(err)}")

    return Ok(result)


async def video_worker(worker_name: str, video_queue: asyncio.Queue) -> None:
    while True:
        channel, channel_semaphore, ids = await video_queue.get()

        channel_title = channel.title.result.unwrap()

        logging.info(f"[{worker_name}]: Getting a bunch of videos for {channel_title}")

        response: ApiResponseResult = await get_videos_with(ids)

        async with channel_semaphore:
            # update channel knowing we are the only worker doing it
            channel.ingest(response)

        logging.info(
            f"[{worker_name}]: Finished processing a bunch of videos for {channel_title}"
        )

        # notify queue we finished processing this chunk
        video_queue.task_done()


async def channel_worker(worker_name: str, channel_queue: asyncio.Queue) -> None:
    # initialize a video_queue for this channel_worker
    video_queue = asyncio.Queue()

    # Create some worker tasks to process the video_queue concurrently
    tasks = []
    for i in range(settings.VIDEO_WORKERS_PER_CHANNEL_WORKER):
        task = asyncio.create_task(
            video_worker(f"video-worker-{i}-{worker_name}", video_queue)
        )
        tasks.append(task)

    while True:
        # Get a ChannelId from the queue for processing
        channel = await channel_queue.get()
        logging.info(f"[{worker_name}]: Now processing channel {channel.id_}...")

        # Process one Channel
        response: ApiResponseResult = await get(channel)

        # populate Channel fields that are parseable from the response above
        channel.ingest(response)

        channel_title = channel.title.result.value

        # Get VideoIds for this channel
        number_of_calls_needed = math.ceil(
            settings.VIDEOS_NEEDED_PER_CHANNEL / settings.MAX_RESULTS
        )

        for i in range(number_of_calls_needed):
            # get playlist items for this channel
            # TODO: pass in remaining max_results instead of hard coding to max_results otherwise
            #  if needed isn't a multiple of max_results we'll end up with more videos than needed
            playlist_items_response: ApiResponseResult = (
                await get_next_playlist_items_for(channel)
            )
            logging.info(
                f"[{worker_name}]: Just got [{i + 1}/{number_of_calls_needed}] playlist items "
                f"for {channel_title}..."
            )

            # Ingest the playlist_items data
            channel.ingest(playlist_items_response)

        # channel.video_ids_to_query PipelineField now has all the VideoIds we need to pull
        # Add items to this channel_worker's video_queue. Note each item is a tuple of:
        # - chunk of settings.MAX_RESULTS VideoIds joined together like id1,id2,id3 ... id50
        # - channel instance that will get updated with the Videos
        # - channel_semaphore to coordinate the async work done on the shared channel instance

        # get a semaphore for the shared channel instance
        channel_semaphore = asyncio.BoundedSemaphore(1)
        for chunk in channel.video_ids_to_query.as_chunked_video_ids_strings():
            video_queue.put_nowait((channel, channel_semaphore, chunk))

        logging.info(
            f"[{worker_name}]: Processing videos for channel {channel_title}..."
        )

        await video_queue.join()

        logging.info(f"[{worker_name}]: Finished processing {channel_title}")

        # We're done, cancel the video worker tasks so they unlock
        for task in tasks:
            task.cancel()

        # Wait until all video worker tasks are cancelled
        await asyncio.gather(*tasks, return_exceptions=True)

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
        youtube()

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

    # Create some worker tasks to process the channel_queue concurrently
    tasks = []
    for i in range(settings.CHANNEL_WORKERS):
        task = asyncio.create_task(channel_worker(f"channel-worker-{i}", channel_queue))
        tasks.append(task)

    # Wait until the queue is fully processed, as in, all Channels have been processed
    await channel_queue.join()

    logging.info("Just finished processing all channels. Cleaning up...")

    # We're done, cancel the worker tasks so they unlock
    for task in tasks:
        task.cancel()

    # Wait until all worker tasks are cancelled
    await asyncio.gather(*tasks, return_exceptions=True)

    for channel in channels:
        pprint(channel)

    logging.info("All Done! Good bye...")


async def nap_before(action: str = "doing something") -> None:
    """
    Simulate latency in requests if DEBUG
    :param action: The action you're about to take
    """
    if settings.DEBUG:
        nap_time = random.randint(1, 3)
        logging.debug(f"Sleeping for {nap_time} seconds before {action}")
        await asyncio.sleep(nap_time)


if __name__ == "__main__":
    asyncio.run(main())
