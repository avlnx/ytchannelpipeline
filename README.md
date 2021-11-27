# Usage guide (Mac OS or Linux)

### Open you terminal and clone this repo:
You might need to [install git.](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

`git clone git@github.com:brilliantorg/yt-channel-data-grabber.git`

### Change into repo's directory

`cd yt-channel-data-grabber`

### Create a new python3 environment

`python3 -m venv env`

### Activate the environment

`source env/bin/activate`

### Install dependencies into the environment

`pip install -r requirements.txt`

### Copy settings.sample.py into settings.py

`cp settings.sample.py settings.py`

### Open the settings.py file and set your `DEVELOPER_KEY`

- Edit `settings.py` and add your `DEVELOPER_KEY` (a Youtube Data Api key)
- You can probably copy it from [this link](https://console.cloud.google.com/apis/credentials?authuser=1&project=test-youtube-api-331922) with your brilliant google account. If not ask in Slack or [create one](https://developers.google.com/youtube/v3/getting-started)

### Add the channel ids to the `channel_ids.csv` file

- One channel_id per line. You already have three examples in the file to get started.

### Run the program with your environment active

- To activate your environment, see step 2. You should see a prompt like `(env) $ ` then run the command:

`python run.py`
