from typing import Callable, Dict, TypeVar, List, Union, Any
from datetime import datetime
import dateutil.parser
import logging
import itertools

from result import Result, Err, Ok
from typing_extensions import NewType, Protocol, TypedDict, Literal

import settings

T = TypeVar("T")

U = TypeVar("U")

TyperFunction = Callable[[T], U]

YoutubeClient = NewType("YoutubeClient", object)

YoutubeClientResult = Result[YoutubeClient, str]

YoutubeClientGetter = Callable[[], YoutubeClientResult]

ApiResult = Result[Dict, str]

DictPathKey = Union[str, int]

ApiResponseKind = Literal[
    "youtube#channelListResponse",
    "youtube#playlistItemListResponse",
    "youtube#videoListResponse",
]


class ApiResponse(TypedDict):
    kind: ApiResponseKind
    data: Dict


ApiResponseResult = Result[ApiResponse, str]


class ApiResponseParser(Protocol[T]):
    def __call__(self, current_result: T, data: ApiResponse) -> T:
        ...


class PipelineField(Protocol[T]):
    # set both in __init__ to bind the T variable and initialize parsers
    result: T
    parsers: Dict[ApiResponseKind, ApiResponseParser]


def string_parser_for_path(path: List[DictPathKey]) -> ApiResponseParser:
    def parser(current_result: T, data: ApiResponse) -> T:
        try:
            for field in path:
                data = data[field]
        except (KeyError, IndexError, ValueError) as err:
            logging.error(f"Could not traverse path {path} in {data}: {str(err)}")
            return current_result
        return Ok(data)

    return parser


def typing_parser_for_path(
    path: List[DictPathKey], typer: TyperFunction
) -> ApiResponseParser:
    dict_path_parser = string_parser_for_path(path)

    def typed_parser(current_result: T, data: ApiResponse) -> T:
        raw_string_result = dict_path_parser(current_result, data)
        if isinstance(raw_string_result, Err):
            return raw_string_result

        try:
            parsed_and_typed = typer(raw_string_result.unwrap())
        except ValueError as err:
            return Err(
                f"Could not convert {raw_string_result.unwrap()} with {typer.__name__}."
                f" {str(err)}"
            )
        return Ok(parsed_and_typed)

    return typed_parser


def typing_nested_dict_path_accumulator_parser(
    parent_path: List[DictPathKey],
    children_path: List[DictPathKey],
    children_typer: TyperFunction,
) -> ApiResponseParser:
    dict_path_parser = string_parser_for_path(parent_path)

    def nested_typed_parser(current_result: T, data: ApiResponse) -> T:
        parent_dict_result: T = Err("Could not parse parent")
        items = dict_path_parser(parent_dict_result, data)
        if isinstance(items, Err):
            return parent_dict_result

        child_path_parser = typing_parser_for_path(children_path, children_typer)
        # TODO: change Any into type of return of typer
        child_dict_initial_result: Result[Any, str] = Err("Could not parse child")
        items_data = items.unwrap()
        new_results = [
            child_path_parser(child_dict_initial_result, child_data)
            for child_data in items_data
        ]

        if isinstance(current_result, Err):
            logging.error(
                f"Parent had an Err! Resetting. {current_result.unwrap_err()}"
            )
            current_result = Ok([])

        all_results = current_result.unwrap() + new_results

        return Ok(all_results)

    return nested_typed_parser


def datetime_parser_for_path(path: List[DictPathKey]) -> ApiResponseParser:
    return typing_parser_for_path(path, dateutil.parser.isoparse)


def int_parser_for_path(path: List[DictPathKey]) -> ApiResponseParser:
    return typing_parser_for_path(path, int)


def noop_parser() -> ApiResponseParser:
    def parser(current_result: T, _: ApiResponse) -> T:
        return current_result

    return parser


# For each field you plan to parse add a class that conforms To the PipelineField Protocol and
# either use one of the above parsers or create another one that conforms to the ApiResponseParser
# protocol. See the example below for the title field.
# Then just add the field instance to the Channel class and instantiate it in __init__()


ChannelTitleResult = Result[str, str]


class ChannelTitlePipelineField:
    def __init__(self):
        self.result: ChannelTitleResult = Err("Not loaded yet")
        self.parsers = {
            "youtube#channelListResponse": string_parser_for_path(
                ["data", "items", 0, "brandingSettings", "channel", "title"]
            )
        }


DescriptionResult = Result[str, str]


class DescriptionPipelineField:
    def __init__(self):
        self.result: DescriptionResult = Err("Not loaded yet")
        self.parsers = {
            "youtube#channelListResponse": string_parser_for_path(
                ["data", "items", 0, "brandingSettings", "channel", "description"]
            )
        }


CountryResult = Result[str, str]


class CountryPipelineField:
    def __init__(self):
        self.result: CountryResult = Err("Not loaded yet")
        self.parsers = {
            "youtube#channelListResponse": string_parser_for_path(
                ["data", "items", 0, "brandingSettings", "channel", "country"]
            )
        }


PublishedAtResult = Result[datetime, str]


class PublishedAtPipelineField:
    def __init__(self):
        self.result: PublishedAtResult = Err("Not loaded yet")
        self.parsers = {
            "youtube#channelListResponse": datetime_parser_for_path(
                ["data", "items", 0, "snippet", "publishedAt"]
            )
        }


SubscriberCountResult = Result[int, str]


class SubscriberCountPipelineField:
    def __init__(self):
        self.result: SubscriberCountResult = Err("Not loaded yet")
        self.parsers = {
            "youtube#channelListResponse": int_parser_for_path(
                ["data", "items", 0, "statistics", "subscriberCount"]
            )
        }


ViewCountResult = Result[int, str]


class ViewCountPipelineField:
    def __init__(self):
        self.result: ViewCountResult = Err("Not loaded yet")
        self.parsers = {
            "youtube#channelListResponse": int_parser_for_path(
                ["data", "items", 0, "statistics", "viewCount"]
            )
        }


UploadsPlaylistId = NewType("UploadsPlaylistId", str)

UploadsPlaylistIdResult = Result[UploadsPlaylistId, str]


class UploadsPlaylistIdPipelineField:
    def __init__(self):
        self.result: UploadsPlaylistIdResult = Err("Not loaded yet")
        self.parsers = {
            "youtube#channelListResponse": string_parser_for_path(
                ["data", "items", 0, "contentDetails", "relatedPlaylists", "uploads"]
            )
        }


# From the playlistItems response we need to parse
# a list of video ids to be pulled in the videos request, note that request should clear the list
# after successfully getting said videos
# the pageToken for the next playlistItems request (if number of videos above = MAX_RESULTS)

# Note the playlistItems requests are run synchronously. We need the previous result for the
# nextPageToken and to figure out if we need to run it again to fulfill the requirements


NextPageTokenResult = Result[str, str]


class PlaylistItemsNextPageTokenPipelineField:
    def __init__(self):
        self.result: NextPageTokenResult = Ok("")
        self.parsers = {
            "youtube#playlistItemListResponse": string_parser_for_path(
                ["data", "nextPageToken"]
            )
        }


VideoId = NewType("VideoId", str)
VideoIdResult = Result[VideoId, str]
VideoIdsToQueryResult = Result[List[VideoIdResult], str]


# TODO: we can probably replace this with a videoIdChunk field that just holds the str chunks if we
#  don't need the ids later
class VideoIdsToQueryPipelineField:
    """
    A PipelineField that holds VideoIdResults that have not been queried yet for more information.
    Each call to /videos need to pop at most MAX_RESULTS (currently 50) out of this field for
    processing
    """

    def __init__(self):
        self.result: VideoIdsToQueryResult = Ok([])
        self.parsers = {
            "youtube#playlistItemListResponse": typing_nested_dict_path_accumulator_parser(
                ["data", "items"], ["snippet", "resourceId", "videoId"], VideoId
            )
        }

    def as_chunked_video_ids_strings(self) -> List[str]:
        def chunk(it, size):
            it = iter(it)
            return iter(lambda: tuple(itertools.islice(it, size)), ())

        video_ids_to_query = self.result

        if isinstance(video_ids_to_query, Err):
            logging.error(
                f"Could not chunk video_ids_to_query. {str(video_ids_to_query.unwrap_err())}"
            )
            return []

        pending_video_ids = [
            v.unwrap() for v in video_ids_to_query.unwrap() if isinstance(v, Ok)
        ]

        result = []
        for c in chunk(pending_video_ids, settings.MAX_RESULTS):
            result.append(",".join(c))

        return result


VideoTitleResult = Result[str, str]

VideoDescriptionResult = Result[str, str]


class Video:
    def __init__(self, id_: VideoId):
        self.id_ = id_
        self.title: VideoTitleResult = Err("Not parsed yet")
        self.description: VideoDescriptionResult = Err("Not parsed yet")

    def __repr__(self):
        video_info = "\nVideo(\n"
        field_reprs = []
        for field_name, field in vars(self).items():
            value = field if field_name == "id_" else f"{field.value[:30]}[...]"
            field_reprs.append(f"  {field_name}={value}")
        video_info += "\n".join(field_reprs)
        video_info += "\n)"
        return video_info


VideosResult = Result[List[Video], str]


def video_typer(video_item: Dict) -> Video:
    video = Video(id_=VideoId(video_item["id"]))
    video.title = string_parser_for_path(["snippet", "title"])(video.title, video_item)
    video.description = string_parser_for_path(["snippet", "description"])(
        video.description, video_item
    )
    return video


class VideoListPipelineField:
    def __init__(self):
        self.result: VideosResult = Ok([])
        self.parsers = {
            "youtube#videoListResponse": typing_nested_dict_path_accumulator_parser(
                ["data", "items"], [], video_typer
            )
        }


ChannelId = NewType("ChannelId", str)


class Channel:
    def __init__(self, id_: ChannelId):
        self.id_ = id_
        self.title = ChannelTitlePipelineField()
        self.description = DescriptionPipelineField()
        self.country = CountryPipelineField()
        self.published_at = PublishedAtPipelineField()
        self.subscriber_count = SubscriberCountPipelineField()
        self.view_count = ViewCountPipelineField()
        self.uploads_playlist_id = UploadsPlaylistIdPipelineField()
        self.playlist_items_next_page_token = PlaylistItemsNextPageTokenPipelineField()
        self.video_ids_to_query = VideoIdsToQueryPipelineField()
        self.videos = VideoListPipelineField()

    def ingest(self, api_response: ApiResponseResult) -> None:
        if isinstance(api_response, Err):
            logging.error(api_response.unwrap_err())
            return

        response = api_response.unwrap()

        for field in self._pipeline_fields():
            # Try to parse from this response
            parser = field.parsers.get(response["kind"], noop_parser())
            field.result = parser(field.result, response)

    def __repr__(self) -> str:
        channel_info = "\n\n** Channel data **\n\n"
        for field_name, field in vars(self).items():
            if field_name == "id_":
                channel_info += f"[id_]\n{field}\n"
                continue
            channel_info += f"[{field_name}]\n{field.result.value}\n"
        channel_info += "\n ** END of Channel Data **\n\n"
        return channel_info

    def _pipeline_fields(self):
        fields = vars(self)

        # Remove the id_ field since that's not a PipelineField and doesn't need to be processed
        id_ = self.id_

        return [f for f in fields.values() if f != id_]
