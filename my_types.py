from __future__ import annotations

import re
from typing import Callable, Dict, TypeVar, List, Union, Any, Generic, Optional
import datetime
import dateutil.parser
import logging
import itertools
import statistics
import parse

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


def parser_for_path(
    path: List[DictPathKey], custom_error: Optional[Err] = None
) -> ApiResponseParser:
    def parser(current_result: T, data: ApiResponse) -> T:
        try:
            for field in path:
                data = data[field]
        except (KeyError, IndexError, ValueError):
            if custom_error is not None:
                return custom_error
            logging.error(f"Could not traverse path {path} in {data.keys()}")
            return current_result
        return Ok(data)

    return parser


def direct_accumulator_parser() -> ApiResponseParser:
    def direct_accum(current_value: T, wrapped_value: ApiResponse) -> T:
        all_values = current_value.unwrap() + wrapped_value["data"]["value"]
        return Ok(all_values)

    return direct_accum


def typing_parser_for_path(
    path: List[DictPathKey], typer: TyperFunction
) -> ApiResponseParser:
    dict_path_parser = parser_for_path(path)

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
    dict_path_parser = parser_for_path(parent_path)

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
            "youtube#channelListResponse": parser_for_path(
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
            "youtube#channelListResponse": parser_for_path(
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
            "youtube#channelListResponse": parser_for_path(
                ["data", "items", 0, "brandingSettings", "channel", "country"]
            )
        }


PublishedAtResult = Result[datetime.datetime, str]


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


class ChannelSubscriberCountPipelineField:
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


ChannelTopicCategoriesResult = Result[List[str], str]


class ChannelTopicCategories:
    result: ChannelTopicCategoriesResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: ChannelTopicCategoriesResult = Err("Not loaded yet")
        self.parsers = {
            "youtube#channelListResponse": parser_for_path(
                ["data", "items", 0, "topicDetails", "topicCategories"]
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
            "youtube#channelListResponse": parser_for_path(
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
            "youtube#playlistItemListResponse": parser_for_path(
                ["data", "nextPageToken"], Err("Out of pages")
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
        self.parsers = {"youtube#video": parser_for_path(["data", "id"])}


VideoTitleResult = Result[str, str]


class VideoTitlePipelineField:
    result: VideoTitleResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: VideoTitleResult = Err("Not parsed yet")
        self.parsers = {"youtube#video": parser_for_path(["data", "snippet", "title"])}


VideoDescriptionResult = Result[str, str]


class VideoDescriptionPipelineField:
    result: VideoDescriptionResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: VideoDescriptionResult = Err("Not parsed yet")
        self.parsers = {
            "youtube#video": parser_for_path(["data", "snippet", "description"])
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


VideoPublishedAtResult = Result[datetime.datetime, str]


class VideoPublishedAtPipelineField:
    result: VideoPublishedAtResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: VideoPublishedAtResult = Err("Not parsed yet")
        self.parsers = {
            "youtube#video": datetime_parser_for_path(
                ["data", "snippet", "publishedAt"]
            )
        }


VideoLikeCountResult = Result[int, str]


class VideoLikeCountPipelineField:
    result: VideoLikeCountResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: VideoLikeCountResult = Err("Not parsed yet")
        self.parsers = {
            "youtube#video": typing_parser_for_path(
                ["data", "statistics", "likeCount"], int
            )
        }


VideoDislikeCountResult = Result[int, str]


class VideoDislikeCountPipelineField:
    result: VideoDislikeCountResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: VideoDislikeCountResult = Err("Not parsed yet")
        self.parsers = {
            "youtube#video": typing_parser_for_path(
                ["data", "statistics", "dislikeCount"], int
            )
        }


VideoDurationResult = Result[datetime.timedelta, str]


def youtube_duration(api_value: str) -> datetime.timedelta:
    """
    Parses strings like PT42M54S or PT3H57M46S or PT51M17S into a datetime.timedelta
    :param api_value:
    :return:
    """
    patterns = [
        "PT{days:d}D{hours:d}H{minutes:d}M{seconds:d}S",
        "PT{hours:d}H{minutes:d}M{seconds:d}S",
        "PT{minutes:d}M{seconds:d}S",
        "PT{seconds:d}S",
    ]

    result = None
    for p in patterns:
        # at least one pattern needs to succeed
        result = parse.parse(p, api_value)
        if result is not None:
            break

    if result is None:
        raise ValueError(f"youtube_duration parser failed on value {api_value}.")

    # <Result () {'hours': '3', 'minutes': '57', 'seconds': '46'}>
    return datetime.timedelta(**result.named)


class VideoDurationPipelineField:
    result: VideoDurationResult
    parsers: PipelineFieldParsers

    def __init__(self):
        self.result: VideoDurationResult = Err("Not parsed yet")
        self.parsers = {
            "youtube#video": typing_parser_for_path(
                ["data", "contentDetails", "duration"], youtube_duration
            )
        }


class Video:
    def __init__(self):
        self.id_ = VideoIdPipelineField()
        self.title = VideoTitlePipelineField()
        self.description = VideoDescriptionPipelineField()
        self.view_count = VideoViewCountPipelineField()
        self.published_at = VideoPublishedAtPipelineField()
        self.like_count = VideoLikeCountPipelineField()
        self.dislike_count = VideoDislikeCountPipelineField()
        self.duration = VideoDurationPipelineField()

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
            "youtube#channelListResponse": parser_for_path(["data", "items", 0, "id"])
        }


class Channel:
    def __init__(self, id_: ChannelId):
        self.id_ = ChannelIdPipelineField(id_)
        self.title = ChannelTitlePipelineField()
        self.description = ChannelDescriptionPipelineField()
        self.topic_categories = ChannelTopicCategories()
        self.country = CountryPipelineField()
        self.published_at = ChannelPublishedAtPipelineField()
        self.subscriber_count = ChannelSubscriberCountPipelineField()
        self.view_count = ChannelViewCountPipelineField()
        self.uploads_playlist_id = ChannelUploadsPlaylistIdPipelineField()
        self.playlist_items_next_page_token = (
            ChannelPlaylistItemsNextPageTokenPipelineField()
        )
        self.video_ids_to_query = ChannelVideoIdsToQueryPipelineField()
        self.videos = ChannelVideoListPipelineField()

    def __repr__(self) -> str:
        return Represent(self).as_string()


Resource = Union[Video, Channel]


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
AllowedReportFieldValue = Union[str, int, datetime.datetime, datetime.timedelta]
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
    videos: VideosResult
    field_key: str
    values: List[AllowedReportFieldValue]

    def __init__(self, count: int, videos_result: VideosResult):
        self.count = count
        self.videos = videos_result
        self.field_key = ""
        # Initialize to empty state (not found)
        self.values: List[AllowedReportFieldValue] = [
            "Not found" for _ in range(self.count)
        ]
        self.valid_results: List[Result] = []

    def and_their(self, field_key: str) -> Slice:
        self.field_key = field_key

        if isinstance(self.videos, Err):
            error_message = self.videos.unwrap_err()
            # Fill with errors
            self.values = [error_message for _ in range(self.count)]
            # On error don't put into results
            self.valid_results = []
        else:
            up_to_count_videos = itertools.islice(
                iter(self.videos.unwrap()), self.count
            )
            for i, video in enumerate(up_to_count_videos):
                # Replace as many empty states as possible
                field = getattr(video, self.field_key)
                self.values[i] = field.result.value
                # append the result if not err
                if not isinstance(field.result, Err):
                    self.valid_results.append(field.result)
        return self

    def released_between(
        self,
        start: datetime.datetime,
        end: datetime.datetime = datetime.datetime.now(datetime.timezone.utc),
        cap: int = 100,
    ) -> List[Video]:
        if isinstance(self.videos, Err):
            return []
        else:
            videos = self.videos.unwrap()

            videos_within_threshold: List[Video] = []

            for video in reversed(videos):
                if isinstance(video.published_at.result, Err):
                    continue

                published_at = video.published_at.result.unwrap()

                if start < published_at < end:
                    videos_within_threshold.append(video)

                if published_at > end:
                    # done
                    break

            return videos_within_threshold[::-1][:cap]

    def as_column(self, column_name: str) -> ReportRow:
        return [
            {f"{column_name}-{i + 1}/{self.count}": value}
            for i, value in enumerate(self.values)
        ]

    def as_valid_videos(self) -> List[Video]:
        if isinstance(self.videos, Err):
            logging.info(f"Could not parse videos {str(self.videos.unwrap_err())}")
            return []

        return self.videos.unwrap()[: self.count]


class DescriptionAndParsedLinksOfMostRecentVideosReportField:
    dependencies: ReportFieldDependencies

    def __init__(self, channel_videos: ChannelVideoListPipelineField):
        self.dependencies = {"channel_videos": channel_videos}

    def values(self) -> ReportRow:
        this_many = 2
        videos = self.dependencies["channel_videos"].result
        descriptions = Slice(this_many, videos).and_their("description").values
        return [
            {
                f"VideoDescription-{i+1}/{this_many}": description,
                f"VideoDescriptionLinks-{i+1}/{this_many}": self.parse_urls_from(
                    description
                ),
            }
            for i, description in enumerate(descriptions)
        ]

    @staticmethod
    def parse_urls_from(description) -> List[str]:
        return re.findall(r"(?P<url>https?://[^\s]+)", description)


class ViewCountForMostRecentVideosReportField:
    dependencies: ReportFieldDependencies

    def __init__(self, channel_videos: ChannelVideoListPipelineField):
        self.dependencies = {"channel_videos": channel_videos}

    def values(self) -> ReportRow:
        this_many = 10
        videos = self.dependencies["channel_videos"].result
        return (
            Slice(this_many, videos).and_their("view_count").as_column("VideoViewCount")
        )


class PublishedAtForMostRecentVideosReportField:
    dependencies: ReportFieldDependencies

    def __init__(self, channel_videos: ChannelVideoListPipelineField):
        self.dependencies = {"channel_videos": channel_videos}

    def values(self) -> ReportRow:
        this_many = 5
        videos = self.dependencies["channel_videos"].result
        return (
            Slice(this_many, videos).and_their("published_at").as_column("PublishedAt")
        )


class NumberOfVideosPublishedInSetPeriodsReportField:
    dependencies: ReportFieldDependencies

    def __init__(self, channel_videos: ChannelVideoListPipelineField):
        self.dependencies = {"channel_videos": channel_videos}

    def values(self) -> ReportRow:
        this_many = 100

        videos = self.dependencies["channel_videos"].result

        now = datetime.datetime.now(datetime.timezone.utc)

        days_ago_365 = now - datetime.timedelta(days=365)
        last_365_days = Slice(this_many, videos).released_between(days_ago_365, cap=100)

        days_ago_90 = now - datetime.timedelta(days=90)
        last_90_days = Slice(this_many, videos).released_between(days_ago_90, cap=20)

        return [
            {"VideosPublishedInLast365Days": len(last_365_days)},
            {"VideosPublishedInLast90Days": len(last_90_days)},
        ]


class MedianViewCountAndSubRatioForVideosWithinRangeReportField:
    dependencies: ReportFieldDependencies

    def __init__(
        self,
        channel_videos: ChannelVideoListPipelineField,
        channel_subscriber_count: ChannelSubscriberCountPipelineField,
    ):
        self.dependencies = {
            "channel_videos": channel_videos,
            "channel_subscriber_count": channel_subscriber_count,
        }

    def values(self) -> ReportRow:
        this_many = 100

        videos = self.dependencies["channel_videos"].result

        now = datetime.datetime.now(datetime.timezone.utc)

        days_ago_360 = now - datetime.timedelta(days=360)
        days_ago_30 = now - datetime.timedelta(days=30)
        videos_within_range = Slice(this_many, videos).released_between(
            days_ago_360, days_ago_30
        )

        median = statistics.median(
            [
                v.view_count.result.unwrap()
                for v in videos_within_range
                if v.view_count.result.is_ok()
            ]
        )

        row = [
            {f"MedianViewsForVideosPublishedBetween360And30DaysAgo": median},
        ]

        sub_count = self.dependencies["channel_subscriber_count"].result
        if isinstance(sub_count, Ok):
            row.append(
                {
                    f"SubscribersByMedianViewsForVideosPublishedBetween360And30DaysAgo": round(
                        sub_count.unwrap() / median
                    )
                }
            )

        return row


class MedianViewCountAndSubRatioForMostRecentVideosReportField:
    dependencies: ReportFieldDependencies

    def __init__(
        self,
        channel_videos: ChannelVideoListPipelineField,
        channel_subscriber_count: ChannelSubscriberCountPipelineField,
    ):
        self.dependencies = {
            "channel_videos": channel_videos,
            "channel_subscriber_count": channel_subscriber_count,
        }

    def values(self) -> ReportRow:
        this_many = 10

        videos = self.dependencies["channel_videos"].result

        view_counts = Slice(this_many, videos).and_their("view_count").values

        median = statistics.median(view_counts)

        row = [
            {f"MedianViewsForLatest{this_many}Videos": median},
        ]

        sub_count = self.dependencies["channel_subscriber_count"].result
        if isinstance(sub_count, Ok):
            row.append(
                {
                    f"SubscribersByMedianViewsRatioForLatestVideos": round(
                        sub_count.unwrap() / median
                    )
                }
            )

        return row


class LikeDislikeRatioForLatestVideosReportField:
    dependencies: ReportFieldDependencies

    def __init__(
        self,
        channel_videos: ChannelVideoListPipelineField,
    ):
        self.dependencies = {
            "channel_videos": channel_videos,
        }

    def values(self) -> ReportRow:
        this_many = 10

        video_results = self.dependencies["channel_videos"].result

        videos = Slice(this_many, video_results).as_valid_videos()

        results: List[str] = ["Not found" for _ in range(this_many)]

        result_index = 0

        for video in videos:
            like_count_result = video.like_count.result
            dislike_count_result = video.dislike_count.result

            # only calc if both numbers are available
            if like_count_result.is_err():
                results[result_index] = like_count_result.unwrap_err()
                result_index += 1
                continue

            if dislike_count_result.is_err():
                results[result_index] = dislike_count_result.unwrap_err()
                result_index += 1
                continue

            likes = like_count_result.unwrap()
            dislikes = dislike_count_result.unwrap()
            ratio = round(likes / (likes + dislikes) * 100)
            rounded_ratio = ratio - (ratio % 5)
            results[result_index] = f"{rounded_ratio}% Likes"
            result_index += 1

        return [
            {f"LikeDislikeRatioForLatestVideos-{i+1}/{this_many}": ratio}
            for i, ratio in enumerate(results)
        ]


class AverageVideoDurationOfLatestVideosReportField:
    dependencies: ReportFieldDependencies

    def __init__(self, channel_videos: ChannelVideoListPipelineField):
        self.dependencies = {"channel_videos": channel_videos}

    def values(self) -> ReportRow:
        this_many = 10
        videos = self.dependencies["channel_videos"].result
        duration_results = Slice(this_many, videos).and_their("duration").valid_results
        durations = [d.unwrap() for d in duration_results]

        return [
            {
                f"AverageVideoDurationOfLatestVideos": sum(
                    durations, datetime.timedelta()
                )
                / len(durations)
            }
        ]


class TopVideosByViewCountReportField:
    dependencies: ReportFieldDependencies

    def __init__(self, channel_videos: ChannelVideoListPipelineField):
        self.dependencies = {"channel_videos": channel_videos}

    def values(self) -> ReportRow:
        this_many = 2

        videos_result = self.dependencies["channel_videos"].result

        if isinstance(videos_result, Err):
            error_message = videos_result.unwrap_err()
            return [
                {f"TopVideosByViewCount-{i+1}/{this_many}": error_message}
                for i in range(this_many)
            ]

        results = ["Not found" for _ in range(this_many)]

        videos = videos_result.unwrap()
        sorted_videos = sorted(
            videos, key=lambda v: v.view_count.result.unwrap_or(0), reverse=True
        )
        video_slice = sorted_videos[:this_many]

        # replace as many placeholder results as possible with a valid result
        for index, video in enumerate(video_slice):
            results[index] = (
                f"({video.view_count.result.unwrap()}) {video.title.result.value}:"
                f" https://youtube.com/video/{video.id_.result.value}"
            )

        return [
            {f"TopVideosByViewCount-{i + 1}/{this_many}": result}
            for i, result in enumerate(results)
        ]


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
        self.topic_categories = ReportFieldProxy(channel.topic_categories)
        self.view_count = ReportFieldProxy(
            channel.view_count, column_name="TotalNumberOfViews"
        )
        self.latest_video_link = LatestVideoLinkReportField(channel.videos)
        self.description_of_latest_videos = (
            DescriptionAndParsedLinksOfMostRecentVideosReportField(channel.videos)
        )
        self.view_count_of_latest_videos = ViewCountForMostRecentVideosReportField(
            channel.videos
        )
        self.release_date_of_latest_videos = PublishedAtForMostRecentVideosReportField(
            channel.videos
        )
        self.videos_published_within_periods = (
            NumberOfVideosPublishedInSetPeriodsReportField(channel.videos)
        )
        self.median_views_and_sub_ratio_within_360_and_30_days = (
            MedianViewCountAndSubRatioForVideosWithinRangeReportField(
                channel.videos, channel.subscriber_count
            )
        )
        self.median_views_and_sub_ratio_of_latest_videos = (
            MedianViewCountAndSubRatioForMostRecentVideosReportField(
                channel.videos, channel.subscriber_count
            )
        )
        self.like_dislike_ratio_for_latest_videos = (
            LikeDislikeRatioForLatestVideosReportField(channel.videos)
        )
        self.average_duration_for_latest_videos = (
            AverageVideoDurationOfLatestVideosReportField(channel.videos)
        )
        self.top_videos_by_view_count = TopVideosByViewCountReportField(channel.videos)

        # Build all ReportFields
        self.values = self._build()

    def _build(self) -> ReportRow:
        values: ReportRow = []
        for report_field in vars(self).values():
            values += report_field.values()
        return values

    def as_dict(self) -> Dict:
        d = {}
        for report_field in self.values:
            for key, value in report_field.items():
                d[key] = value
        return d

    def __repr__(self) -> str:
        return str(self.values)
