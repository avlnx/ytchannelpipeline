from __future__ import annotations

import asyncio
import datetime
import logging
import math
import os
import random
import csv
import time
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
    Video,
    Populate,
    ReportRowFor,
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
            logging.debug(f"\n\n{client.value}")
            logging.debug("Now building youtube client...")
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

    channel_id = channel.id_.result.unwrap()
    await nap_before(f"getting {channel_id}")

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
        return Err(f"[get] {str(err)}")
    except Exception as err:
        return Err(f"[get] {str(err)}")

    # Check the channel was actually found since technically it's not an Error :/
    if result["pageInfo"]["totalResults"] == 0:
        return Err(f"[get] Channel not found {channel_id}")

    return Ok(result)


@api_response
async def get_next_playlist_items_for(
    channel: Channel, max_results: int = settings.MAX_RESULTS
) -> ApiResult:
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
            maxResults=max_results,
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
            part="snippet,contentDetails,status,id,statistics",
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

        # instead of blocking the channel instance which essentially makes the videos get processed
        # synchronously, let's populate the Videos with their own data
        if isinstance(response, Err):
            # /videos request failed
            logging.error(
                f"/videos request failed for {channel_title}: {str(response.unwrap_err())} "
            )
            video_queue.task_done()

        video_items = response.unwrap()["data"]["items"]
        video_items_responses: List[ApiResponseResult] = [
            Ok(ApiResponse(kind=video_item["kind"], data=video_item))
            for video_item in video_items
        ]

        videos = [
            Populate(Video()).using(video_item_response)
            for video_item_response in video_items_responses
        ]

        direct_set_videos_response = Ok(
            ApiResponse(kind="internal#directAccumulator", data={"value": videos})
        )

        async with channel_semaphore:
            # update channel knowing we are the only worker doing it
            Populate(channel).using(direct_set_videos_response)

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
        # Get a Channel from the queue for processing
        channel, output_file_semaphore = await channel_queue.get()
        logging.info(
            f"[{worker_name}]: Now processing channel {channel.id_.result.value}..."
        )

        # Process one Channel
        response: ApiResponseResult = await get(channel)

        if isinstance(response, Err):
            # request failed, move on
            logging.error(
                f"get/channel request failed with: \n{str(response.unwrap_err())}"
            )
            # Notify the queue this "work item" has been processed
            channel_queue.task_done()

        Populate(channel).using(response)

        channel_title = channel.title.result.value

        # Get VideoIds for this channel
        number_of_calls_needed = math.ceil(
            settings.VIDEOS_NEEDED_PER_CHANNEL / settings.MAX_RESULTS
        )

        videos_remaining = settings.VIDEOS_NEEDED_PER_CHANNEL

        for i in range(number_of_calls_needed):
            # get playlist items for this channel
            max_results = min(settings.MAX_RESULTS, videos_remaining)

            playlist_items_response: ApiResponseResult = (
                await get_next_playlist_items_for(channel, max_results)
            )

            if isinstance(playlist_items_response, Err):
                logging.error(
                    f"[{worker_name}] Could not get playlistItems: "
                    f"{str(playlist_items_response.unwrap_err())}"
                )
                break

            logging.info(
                f"[{worker_name}]: Just got [{i + 1}/{number_of_calls_needed}] playlist items "
                f"for {channel_title}..."
            )

            Populate(channel).using(playlist_items_response)

            logging.info(
                f"{channel_title}'s nextPageToken: {channel.playlist_items_next_page_token.result.value}"
            )

            videos_remaining -= max_results

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

        logging.info(f"[{worker_name}]: Finished building data for {channel_title}")

        # We're done, cancel the video worker tasks so they unlock
        for task in tasks:
            task.cancel()

        # Wait until all video worker tasks are cancelled
        await asyncio.gather(*tasks, return_exceptions=True)

        # The channel instance has all the data needed, generate a ReportRow
        report_row = ReportRowFor(channel)

        # Write ReportRow to file
        async with output_file_semaphore:
            file_empty = os.stat(output_file_name).st_size == 0
            with open(output_file_name, "a", newline="") as csv_file:
                report_dict = report_row.as_dict()
                fieldnames = list(report_dict.keys())
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

                # if file is empty also write the columns
                if file_empty:
                    writer.writeheader()

                writer.writerow(report_dict)
            logging.info(f"[{worker_name}]: Finished writing data for {channel_title}")
            print(f"Finished processing {channel_title}")

        # Notify the queue this "work item" has been processed
        channel_queue.task_done()


output_file_name = "results.csv"


input_file_name = "channel_ids.csv"


async def main():
    def check_channel_ids_input_file():
        try:
            with open(input_file_name, "r"):
                pass
        except FileNotFoundError:
            print(
                f"Could not find input file '{input_file_name}'."
                f"Please create the file in the same directory as this program.\n"
                f"It should contain one channel_id per line.\n"
            )
            exit(0)

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
        check_channel_ids_input_file()
        check_youtube_client_configuration()
        set_logging_level()
        youtube()

    initialize()

    start = time.time()

    channels: List[Channel] = []

    # Read in channel_ids from input file
    with open(input_file_name, newline="") as input_file:
        reader = csv.reader(input_file)
        for channel_ids in reader:
            try:
                channels.append(Channel(id_=ChannelId(channel_ids[0])))
            except IndexError:
                logging.error(
                    f"Could not read in row from {input_file_name}:\n{channel_ids}"
                )
                pass

    print(
        f"Processing {len(channels)} channels. We should take about "
        f"{str(datetime.timedelta(seconds=3 * len(channels)))}...\n"
    )

    # touch output file
    with open(output_file_name, "w+"):
        pass

    output_file_semaphore = asyncio.BoundedSemaphore(1)

    channel_queue = asyncio.Queue()
    for channel in channels:
        # Put all channels in the queue for processing along with a file semaphore so only one
        # coroutine writes at a time to it
        channel_queue.put_nowait((channel, output_file_semaphore))

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

    print(f"\nAll Done! Results were written to {output_file_name}.\n")

    total_time = datetime.timedelta(seconds=time.time() - start)

    print(
        f"We took {str(total_time)}. An average of {str(total_time / len(channels))} per channel.\n"
    )


async def nap_before(action: str = "doing something") -> None:
    """
    Simulate latency in requests if DEBUG
    :param action: The action you're about to take
    """
    if settings.DEBUG and settings.SIMULATE_LATENCY:
        nap_time = random.randint(1, 3)
        logging.debug(f"Sleeping for {nap_time} seconds before {action}")
        await asyncio.sleep(nap_time)


if __name__ == "__main__":
    asyncio.run(main())
