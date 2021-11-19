from typing import Callable, Dict, TypeVar
from datetime import datetime
import dateutil.parser

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


class YoutubeChannelListTitleParser:
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
        self.parsers = {"youtube#channelListResponse": YoutubeChannelListTitleParser}


DescriptionResult = Result[str, str]


class YoutubeChannelListDescriptionParser:
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
        self.parsers = {
            "youtube#channelListResponse": YoutubeChannelListDescriptionParser
        }


CountryResult = Result[str, str]


class YoutubeChannelListCountryParser:
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
        self.parsers = {"youtube#channelListResponse": YoutubeChannelListCountryParser}


PublishedAtResult = Result[datetime, str]


class YoutubeChannelListPublishedAtParser:
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
        self.parsers = {
            "youtube#channelListResponse": YoutubeChannelListPublishedAtParser
        }


SubscriberCountResult = Result[int, str]


class YoutubeChannelListSubscriberCountParser:
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
        self.parsers = {
            "youtube#channelListResponse": YoutubeChannelListSubscriberCountParser
        }


ViewCountResult = Result[int, str]


class YoutubeChannelListViewCountParser:
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
        self.parsers = {
            "youtube#channelListResponse": YoutubeChannelListViewCountParser
        }


UploadsPlaylistId = NewType("UploadsPlaylistId", str)

UploadsPlaylistIdResult = Result[UploadsPlaylistId, str]


class YoutubeChannelListUploadsPlaylistIdParser:
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
            "youtube#channelListResponse": YoutubeChannelListUploadsPlaylistIdParser
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

    def ingest(self, api_response: ApiResponseResult) -> None:
        if isinstance(api_response, Err):
            # TODO: log?
            return

        response = api_response.unwrap()

        for field in self._pipeline_fields():
            # If already parsed from a previous response skip this field
            if isinstance(field.result, Ok):
                continue

            # Try to parse from this response
            parser = field.parsers.get(response["kind"], NoOpParser)
            field.result = parser.parse(field.result, response)

    def __repr__(self) -> str:
        channel = "\n\n** Channel data **\n\n"
        for field_name, field in vars(self).items():
            if field_name == "id_":
                channel += f"[id_]\n{field}\n\n"
                continue
            channel += f"[{field_name}]\n{field.result.value}\n\n"
        return channel

    def _pipeline_fields(self):
        fields = vars(self)

        # Remove the id_ field since that's not a PipelineField and doesn't need to be processed
        id_ = self.id_

        return [f for f in fields.values() if f != id_]
