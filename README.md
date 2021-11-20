Still a work in progress.

For now:

- Clone repo:

`git clone git@github.com:brilliantorg/yt-channel-data-grabber.git`

- Change directory into repo

`cd yt-channel-data-grabber`

- Install dependencies into a python environment.

`python3 -m venv env`

`source env/bin/activate`

`pip install -r requirements.txt`

- Copy settings.sample.py into settings.py

`cp settings.sample.py settings.py`

- Edit `settings.py` and add your `DEVELOPER_KEY` which is a Youtube Data Api key

- Open `run.py` and change the `ChannelIds` if you want (or add others)

- Run (with your environment active):

`python run.py`
