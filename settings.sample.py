import logging

DEBUG = False

LOGGING_LEVEL = logging.INFO if DEBUG else logging.WARNING

# Your Youtube Api Api Key, also called Developer Key
DEVELOPER_KEY = ""

# The number of concurrent tasks that will process channels
CHANNEL_WORKERS = 3

# The number of concurrent tasks that will process videos PER CHANNEL_WORKER
VIDEO_WORKERS_PER_CHANNEL_WORKER = 2

# How many videos we need per channel to fulfill our data needs
VIDEOS_NEEDED_PER_CHANNEL = 100

# Max results returned by the youtube api calls. Currently 50
MAX_RESULTS = 50

SIMULATE_LATENCY = False
