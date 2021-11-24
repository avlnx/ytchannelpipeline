from __future__ import annotations
from typing import Callable, Dict, TypeVar, List, Union, Any, Generic, Optional
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
    "youtube#video",
    "internal#directAccumulator",
]


class ApiResponse(TypedDict):
    kind: ApiResponseKind
    data: Dict


ApiResponseResult = Result[ApiResponse, str]


class ApiResponseParser(Protocol[T]):
    def __call__(self, current_result: T, data: ApiResponse) -> T:
        ...


PipelineFieldParsers = Dict[ApiResponseKind, ApiResponseParser]


class PipelineField(Protocol[T]):
    # set both in __init__ to bind the T variable and initialize parsers
    result: T
    parsers: PipelineFieldParsers


P = TypeVar("P", bound=PipelineField)


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


def limited_size_string_parser_for_path(
    path: List[DictPathKey], size: int = 30
) -> ApiResponseParser:
    string_parser = string_parser_for_path(path)

    def limited_string_parser(current_result: T, data: ApiResponse) -> T:
        raw_string_result = string_parser(current_result, data)
        if isinstance(raw_string_result, Err):
            return raw_string_result
        return Ok(f"{raw_string_result.unwrap()[:size]}[...]")

    return limited_string_parser


def direct_accumulator_parser() -> ApiResponseParser:
    def direct_accum(current_value: T, wrapped_value: ApiResponse) -> T:
        all_values = current_value.unwrap() + wrapped_value["data"]["value"]
        return Ok(all_values)

    return direct_accum


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
    result: ChannelTitleResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: ChannelTitleResult = Err("Not loaded yet")
        self.parsers = {
            "youtube#channelListResponse": string_parser_for_path(
                ["data", "items", 0, "brandingSettings", "channel", "title"]
            )
        }


DescriptionResult = Result[str, str]


class ChannelDescriptionPipelineField:
    result: DescriptionResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: DescriptionResult = Err("Not loaded yet")
        self.parsers = {
            "youtube#channelListResponse": limited_size_string_parser_for_path(
                ["data", "items", 0, "brandingSettings", "channel", "description"]
            )
        }


CountryResult = Result[str, str]


class CountryPipelineField:
    result: CountryResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: CountryResult = Err("Not loaded yet")
        self.parsers = {
            "youtube#channelListResponse": string_parser_for_path(
                ["data", "items", 0, "brandingSettings", "channel", "country"]
            )
        }


PublishedAtResult = Result[datetime, str]


class ChannelPublishedAtPipelineField:
    result: PublishedAtResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: PublishedAtResult = Err("Not loaded yet")
        self.parsers = {
            "youtube#channelListResponse": datetime_parser_for_path(
                ["data", "items", 0, "snippet", "publishedAt"]
            )
        }


SubscriberCountResult = Result[int, str]


class SubscriberCountPipelineField:
    result: SubscriberCountResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: SubscriberCountResult = Err("Not loaded yet")
        self.parsers = {
            "youtube#channelListResponse": int_parser_for_path(
                ["data", "items", 0, "statistics", "subscriberCount"]
            )
        }


ViewCountResult = Result[int, str]


class ChannelViewCountPipelineField:
    result: ViewCountResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: ViewCountResult = Err("Not loaded yet")
        self.parsers = {
            "youtube#channelListResponse": int_parser_for_path(
                ["data", "items", 0, "statistics", "viewCount"]
            )
        }


UploadsPlaylistId = NewType("UploadsPlaylistId", str)

UploadsPlaylistIdResult = Result[UploadsPlaylistId, str]


class ChannelUploadsPlaylistIdPipelineField:
    result: UploadsPlaylistIdResult
    parsers: PipelineFieldParsers

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


class ChannelPlaylistItemsNextPageTokenPipelineField:
    result: NextPageTokenResult
    parsers: PipelineFieldParsers

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


class ChannelVideoIdsToQueryPipelineField:
    """
    A PipelineField that holds VideoIdResults that have not been queried yet for more information.
    Each call to /videos need to pop at most MAX_RESULTS (currently 50) out of this field for
    processing
    """

    result: VideoIdsToQueryResult
    parsers: PipelineFieldParsers

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


VideoIdResult = Result[VideoId, str]


class VideoIdPipelineField:
    result: VideoIdResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: VideoIdResult = Err("Not set")
        self.parsers = {"youtube#video": string_parser_for_path(["data", "id"])}


VideoTitleResult = Result[str, str]


class VideoTitlePipelineField:
    result: VideoTitleResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: VideoTitleResult = Err("Not parsed yet")
        self.parsers = {
            "youtube#video": string_parser_for_path(["data", "snippet", "title"])
        }


VideoDescriptionResult = Result[str, str]


class VideoDescriptionPipelineField:
    result: VideoDescriptionResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: VideoDescriptionResult = Err("Not parsed yet")
        self.parsers = {
            "youtube#video": limited_size_string_parser_for_path(
                ["data", "snippet", "description"]
            )
        }


VideoViewCountResult = Result[int, str]


class VideoViewCountPipelineField:
    result: VideoViewCountResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: VideoDescriptionResult = Err("Not parsed yet")
        self.parsers = {
            "youtube#video": typing_parser_for_path(
                ["data", "statistics", "viewCount"], int
            )
        }


class Video:
    def __init__(self):
        self.id_ = VideoIdPipelineField()
        self.title = VideoTitlePipelineField()
        self.description = VideoDescriptionPipelineField()
        self.view_count = VideoViewCountPipelineField()

    def __repr__(self):
        return Represent(self).as_string()


VideosResult = Result[List[Video], str]


class ChannelVideoListPipelineField:
    result: VideosResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: VideosResult = Ok([])
        self.parsers = {"internal#directAccumulator": direct_accumulator_parser()}


ChannelId = NewType("ChannelId", str)
ChannelIdResult = Result[ChannelId, str]


class ChannelIdPipelineField:
    result: ChannelIdResult
    parsers: PipelineFieldParsers

    def __init__(self, id_: ChannelId):
        self.result: ChannelIdResult = Ok(id_)
        self.parsers = {
            "youtube#channelListResponse": string_parser_for_path(
                ["data", "items", 0, "id"]
            )
        }


class Channel:
    def __init__(self, id_: ChannelId):
        self.id_ = ChannelIdPipelineField(id_)
        self.title = ChannelTitlePipelineField()
        self.description = ChannelDescriptionPipelineField()
        self.country = CountryPipelineField()
        self.published_at = ChannelPublishedAtPipelineField()
        self.subscriber_count = SubscriberCountPipelineField()
        self.view_count = ChannelViewCountPipelineField()
        self.uploads_playlist_id = ChannelUploadsPlaylistIdPipelineField()
        self.playlist_items_next_page_token = (
            ChannelPlaylistItemsNextPageTokenPipelineField()
        )
        self.video_ids_to_query = ChannelVideoIdsToQueryPipelineField()
        self.videos = ChannelVideoListPipelineField()

    def __repr__(self) -> str:
        return Represent(self).as_string()


class Populate(Generic[T]):
    def __init__(self, item: T):
        self.item = item

    def using(self, api_response: ApiResponseResult) -> T:
        if isinstance(api_response, Err):
            logging.error(api_response.unwrap_err())
            return self.item

        response = api_response.unwrap()

        for field in vars(self.item).values():
            # Try to parse from this response
            parser = field.parsers.get(response["kind"], noop_parser())
            field.result = parser(field.result, response)

        return self.item


class Represent(Generic[T]):
    def __init__(self, item: T):
        self.item = item

    def as_string(self) -> str:
        info = f"\n{type(self.item).__name__.capitalize()}(\n"
        for field_name, field in vars(self.item).items():
            info += f"  {field_name}={field.result.value}\n"
        info += "\n)"
        return info


# Maps a key to a PipelineField this ReportField depends on
ReportFieldDependencies = Dict[str, PipelineField]

# A ReportFieldValue maps a column_name to a primitive value
# Note that a field might generate more than one column
AllowedReportFieldValue = Union[str, int, datetime]
ReportFieldValue = Dict[str, AllowedReportFieldValue]

ReportRow = List[ReportFieldValue]

ReportRowResult = Result[ReportRow, str]


class ReportField(Protocol):
    dependencies: ReportFieldDependencies

    def values(self) -> ReportRow:
        ...


class ReportFieldProxy:
    """
    If all you need is to unwrap the value from a PipelineField you can reuse this class to make
    a ReportField
    """

    dependencies: ReportFieldDependencies

    def __init__(self, field: PipelineField, column_name: Optional[str] = None):
        self.dependencies = {"field": field}
        self.column_name = column_name or field.__class__.__name__.replace(
            "PipelineField", ""
        )

    def values(self) -> ReportRow:
        return [{self.column_name: self.dependencies["field"].result.value}]


class ChannelLinkReportField:
    dependencies: ReportFieldDependencies

    def __init__(self, channel_id: ChannelIdPipelineField):
        self.dependencies = {"channel_id": channel_id}

    def values(self) -> ReportRow:
        return [
            {
                "ChannelLink": f"https://youtube.com/channel/"
                f"{self.dependencies['channel_id'].result.value}"
            }
        ]


class LatestVideoLinkReportField:
    dependencies: ReportFieldDependencies

    def __init__(self, channel_videos: ChannelVideoListPipelineField):
        self.dependencies = {"channel_videos": channel_videos}

    def values(self) -> ReportRow:
        videos = self.dependencies["channel_videos"].result
        if isinstance(videos, Err):
            video_link = videos.unwrap_err()
        else:
            try:
                video = next(iter(videos.unwrap()))
                video_link = f"https://youtube.com/video/{video.id_.result.value}"
            except StopIteration:
                video_link = "No videos found"
        return [{"LatestVideoLink": video_link}]


class Slice:
    count: int
    items: Result[List[Any]]
    field_key: str
    values: List[AllowedReportFieldValue]

    def __init__(self, count: int, items_result: Result[List[Any], str]):
        self.count = count
        self.items = items_result
        self.field_key = ""
        # Initialize to empty state (not found)
        self.values: List[AllowedReportFieldValue] = [
            "Not found" for _ in range(self.count)
        ]

    def and_unwrap_their(self, field_key: str) -> Slice:
        self.field_key = field_key

        if isinstance(self.items, Err):
            error_message = self.items.unwrap_err()
            # Fill with errors
            self.values = [error_message for _ in range(self.count)]
        else:
            up_to_count_items = itertools.islice(iter(self.items.unwrap()), self.count)
            for i, item in enumerate(up_to_count_items):
                # Replace as many empty states as possible
                self.values[i] = getattr(item, self.field_key).result.value
        return self

    def as_column(self, column_name: str) -> ReportRow:
        return [
            {f"{column_name}-{i + 1}/{self.count}": value}
            for i, value in enumerate(self.values)
        ]


class DescriptionOfTwoMostRecentVideosReportField:
    dependencies: ReportFieldDependencies

    def __init__(self, channel_videos: ChannelVideoListPipelineField):
        self.dependencies = {"channel_videos": channel_videos}

    def values(self) -> ReportRow:
        this_many = 2
        videos = self.dependencies["channel_videos"].result
        return (
            Slice(this_many, videos)
            .and_unwrap_their("description")
            .as_column("VideoDescription")
        )


class ViewCountForTenMostRecentVideosReportField:
    dependencies: ReportFieldDependencies

    def __init__(self, channel_videos: ChannelVideoListPipelineField):
        self.dependencies = {"channel_videos": channel_videos}

    def values(self) -> ReportRow:
        this_many = 10
        videos = self.dependencies["channel_videos"].result
        return (
            Slice(this_many, videos)
            .and_unwrap_their("view_count")
            .as_column("VideoViewCount")
        )


class ReportRowFor:
    def __init__(self, channel: Channel):
        self.channel_id = ReportFieldProxy(channel.id_)
        self.channel_title = ReportFieldProxy(channel.title)
        self.channel_description = ReportFieldProxy(channel.description)
        self.channel_link = ChannelLinkReportField(channel.id_)
        self.country = ReportFieldProxy(channel.country)
        self.subscriber_count = ReportFieldProxy(channel.subscriber_count)
        self.published_at = ReportFieldProxy(
            channel.published_at, column_name="JoinedDate"
        )
        self.view_count = ReportFieldProxy(
            channel.view_count, column_name="TotalNumberOfViews"
        )
        self.latest_video_link = LatestVideoLinkReportField(channel.videos)
        self.description_of_latest_two_videos = (
            DescriptionOfTwoMostRecentVideosReportField(channel.videos)
        )
        self.view_count_of_latest_ten_videos = (
            ViewCountForTenMostRecentVideosReportField(channel.videos)
        )

        # Build all ReportFields
        self.values = self._build()

    def _build(self) -> ReportRow:
        values: ReportRow = []
        for report_field in vars(self).values():
            values += report_field.values()
        return values

    def __repr__(self) -> str:
        return str(self.values)
