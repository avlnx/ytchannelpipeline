from typing import Callable, Dict, TypeVar, List
from datetime import datetime
import dateutil.parser
import logging

from result import Result, Err, Ok
from typing_extensions import NewType, Protocol, TypedDict, Literal


YoutubeClient = NewType("YoutubeClient", object)

YoutubeClientResult = Result[YoutubeClient, str]

YoutubeClientGetter = Callable[[], YoutubeClientResult]

ApiResult = Result[Dict, str]

ApiResponseKind = Literal[
    "youtube#channelListResponse", "youtube#playlistItemListResponse"
]


class ApiResponse(TypedDict):
    kind: ApiResponseKind
    data: Dict


ApiResponseResult = Result[ApiResponse, str]


T = TypeVar("T")


class ApiResponseParser(Protocol[T]):
    @staticmethod
    def parse(current_result: T, api_response: ApiResponse) -> T:
        ...


class PipelineField(Protocol[T]):
    # set both in __init__ to bind the T variable and initialize parsers
    result: T
    parsers: Dict[ApiResponseKind, ApiResponseParser]


class NoOpParser:
    @staticmethod
    def parse(current_result: T, _: ApiResponse) -> T:
        return current_result


# For each field you plan to parse add a class that conforms To the PipelineField Protocol and a
# parser that conforms to the ApiResponseParser protocol. See the example below for the title field.
# Then just add the field instance to the Channel class and instantiate it in __init__()

TitleResult = Result[str, str]


class ChannelListTitleParser:
    @staticmethod
    def parse(_: TitleResult, api_response: ApiResponse) -> TitleResult:
        try:
            channel_data = api_response["data"]["items"][0]
            title = channel_data["brandingSettings"]["channel"]["title"]
            return Ok(title)
        except (KeyError, IndexError) as err:
            return Err(f"Could not parse title: {str(err)}")


class TitlePipelineField:
    def __init__(self):
        self.result: TitleResult = Err("Not loaded yet")
        self.parsers = {"youtube#channelListResponse": ChannelListTitleParser}


DescriptionResult = Result[str, str]


class ChannelListDescriptionParser:
    @staticmethod
    def parse(_: DescriptionResult, api_response: ApiResponse) -> DescriptionResult:
        try:
            channel_data = api_response["data"]["items"][0]
            description = channel_data["brandingSettings"]["channel"]["description"]
            return Ok(description)
        except (KeyError, IndexError) as err:
            return Err(f"Could not parse description: {str(err)}")


class DescriptionPipelineField:
    def __init__(self):
        self.result: DescriptionResult = Err("Not loaded yet")
        self.parsers = {"youtube#channelListResponse": ChannelListDescriptionParser}


CountryResult = Result[str, str]


class ChannelListCountryParser:
    @staticmethod
    def parse(_: CountryResult, api_response: ApiResponse) -> CountryResult:
        try:
            channel_data = api_response["data"]["items"][0]
            country = channel_data["brandingSettings"]["channel"]["country"]
            return Ok(country)
        except (KeyError, IndexError) as err:
            return Err(f"Could not parse country: {str(err)}")


class CountryPipelineField:
    def __init__(self):
        self.result: CountryResult = Err("Not loaded yet")
        self.parsers = {"youtube#channelListResponse": ChannelListCountryParser}


PublishedAtResult = Result[datetime, str]


class ChannelListPublishedAtParser:
    @staticmethod
    def parse(_: PublishedAtResult, api_response: ApiResponse) -> PublishedAtResult:
        try:
            channel_data = api_response["data"]["items"][0]
            published_at = channel_data["snippet"]["publishedAt"]
        except (KeyError, IndexError) as err:
            return Err(f"Could not parse published_at: {str(err)}")
        # parse date into timezone aware datetime. Format: ISO '2014-12-16T21:18:48Z'
        try:
            parsed_date = dateutil.parser.isoparse(published_at)
        except ValueError as err:
            return Err(f"Could not parse published_at {published_at}. {str(err)}")
        return Ok(parsed_date)


class PublishedAtPipelineField:
    def __init__(self):
        self.result: PublishedAtResult = Err("Not loaded yet")
        self.parsers = {"youtube#channelListResponse": ChannelListPublishedAtParser}


SubscriberCountResult = Result[int, str]


class ChannelListSubscriberCountParser:
    @staticmethod
    def parse(
        _: SubscriberCountResult, api_response: ApiResponse
    ) -> SubscriberCountResult:
        try:
            channel_data = api_response["data"]["items"][0]
            subscriber_count = channel_data["statistics"]["subscriberCount"]
            return Ok(int(subscriber_count))
        except (KeyError, IndexError, ValueError) as err:
            return Err(f"Could not parse subscriber_count: {str(err)}")


class SubscriberCountPipelineField:
    def __init__(self):
        self.result: SubscriberCountResult = Err("Not loaded yet")
        self.parsers = {"youtube#channelListResponse": ChannelListSubscriberCountParser}


ViewCountResult = Result[int, str]


class ChannelListViewCountParser:
    @staticmethod
    def parse(_: ViewCountResult, api_response: ApiResponse) -> ViewCountResult:
        try:
            channel_data = api_response["data"]["items"][0]
            view_count = channel_data["statistics"]["viewCount"]
            return Ok(int(view_count))
        except (KeyError, IndexError, ValueError) as err:
            return Err(f"Could not parse view_count: {str(err)}")


class ViewCountPipelineField:
    def __init__(self):
        self.result: ViewCountResult = Err("Not loaded yet")
        self.parsers = {"youtube#channelListResponse": ChannelListViewCountParser}


UploadsPlaylistId = NewType("UploadsPlaylistId", str)

UploadsPlaylistIdResult = Result[UploadsPlaylistId, str]


class ChannelListUploadsPlaylistIdParser:
    @staticmethod
    def parse(
        _: UploadsPlaylistIdResult, api_response: ApiResponse
    ) -> UploadsPlaylistIdResult:
        try:
            channel_data = api_response["data"]["items"][0]
            uploads_playlist_id = UploadsPlaylistId(
                channel_data["contentDetails"]["relatedPlaylists"]["uploads"]
            )
            return Ok(uploads_playlist_id)
        except (KeyError, IndexError) as err:
            return Err(f"Could not parse uploads_playlist_id: {str(err)}")


class UploadsPlaylistIdPipelineField:
    def __init__(self):
        self.result: UploadsPlaylistIdResult = Err("Not loaded yet")
        self.parsers = {
            "youtube#channelListResponse": ChannelListUploadsPlaylistIdParser
        }


# From the playlistItems response we need to parse
# a list of video ids to be pulled in the videos request, note that request should clear the list
# after successfully getting said videos
# the pageToken for the next playlistItems request (if number of videos above = MAX_RESULTS)

# Note the playlistItems requests are run synchronously. We need the previous result for the
# nextPageToken and to figure out if we need to run it again to fulfill the requirements


NextPageTokenResult = Result[str, str]


class PlaylistItemsNextPageTokenParser:
    @staticmethod
    def parse(_: NextPageTokenResult, api_response: ApiResponse) -> NextPageTokenResult:
        try:
            next_page_token = api_response["data"]["nextPageToken"]
            return Ok(next_page_token)
        except KeyError as err:
            return Err(f"Could not parse next_page_token: {str(err)}")


class PlaylistItemsNextPageTokenPipelineField:
    def __init__(self):
        self.result: NextPageTokenResult = Ok("")
        self.parsers = {
            "youtube#playlistItemListResponse": PlaylistItemsNextPageTokenParser
        }


VideoId = NewType("VideoId", str)
VideoIdsToQueryResult = Result[List[VideoId], str]


class PlaylistItemsVideoIdsToQueryResultParser:
    @staticmethod
    def parse(
        current_result: VideoIdsToQueryResult, api_response: ApiResponse
    ) -> VideoIdsToQueryResult:
        try:
            items = api_response["data"]["items"]
            resources = [r["snippet"]["resourceId"] for r in items]
        except KeyError as err:
            # Log the error and return the previous result which might have valid VideoIds from a
            # previous request
            logging.error(f"Could not parse items for video_ids_to_query: {str(err)}")
            return current_result

        if isinstance(current_result, Err):
            logging.error(
                f"VideoIdsToQuery had an Err! Resetting. {current_result.unwrap_err()}"
            )
            current_result = Ok([])

        video_ids: List[VideoId] = current_result.unwrap() + [
            VideoId(r["videoId"]) for r in resources if r["kind"] == "youtube#video"
        ]
        return Ok(video_ids)


class VideoIdsToQueryPipelineField:
    """
    A PipelineField that holds VideoIds that have not been queried yet for more information.
    Each call to /videos need to pop at most MAX_RESULTS (currently 50) out of this field for
    processing
    """

    def __init__(self):
        self.result: VideoIdsToQueryResult = Ok([])
        self.parsers = {
            "youtube#playlistItemListResponse": PlaylistItemsVideoIdsToQueryResultParser
        }


ChannelId = NewType("ChannelId", str)


class Channel:
    def __init__(self, id_: ChannelId):
        self.id_ = id_
        self.title = TitlePipelineField()
        self.description = DescriptionPipelineField()
        self.country = CountryPipelineField()
        self.published_at = PublishedAtPipelineField()
        self.subscriber_count = SubscriberCountPipelineField()
        self.view_count = ViewCountPipelineField()
        self.uploads_playlist_id = UploadsPlaylistIdPipelineField()
        self.playlist_items_next_page_token = PlaylistItemsNextPageTokenPipelineField()
        self.video_ids_to_query = VideoIdsToQueryPipelineField()

    def ingest(self, api_response: ApiResponseResult) -> None:
        if isinstance(api_response, Err):
            logging.error(api_response.unwrap_err())
            return

        response = api_response.unwrap()

        for field in self._pipeline_fields():
            # Try to parse from this response
            parser = field.parsers.get(response["kind"], NoOpParser)
            field.result = parser.parse(field.result, response)

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
