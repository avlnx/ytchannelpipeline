from dataclasses import dataclass
from typing import Any, Callable, Dict

from result import Result
from typing_extensions import NewType

YoutubeClient = NewType("YoutubeClient", Any)

YoutubeClientResult = Result[YoutubeClient, str]

YoutubeClientGetter = Callable[[], YoutubeClientResult]

ChannelId = NewType("ChannelId", str)

# TODO: Change Dict to Channel and update pipeline functions
ChannelResult = Result[Dict, str]

PlaylistId = NewType("PlaylistId", str)

PlaylistIdResult = Result[PlaylistId, str]

# TODO: Change PlaylistItems to PlaylistItem
PlaylistItemsResult = Result[Dict, str]

# TODO: Change to proper Video instead of Dict
VideosResult = Result[Dict, str]


@dataclass
class Channel:
    id: ChannelId
    name: str
    # Add a bunch of Result fields and initialize to Err('Not loaded') then try to fetch and
    # change the Channel.field to either the Err(str) or Ok(field.value)
    # playlist_items: Result[PlaylistItem, str]
