#! /usr/bin/env python3

from luigi import Task, LocalTarget
from luigi.format import Nop
# from postgres_templates import TransactionFactTable, CopyWrapper
from spotify_api import check_for_refresh


class GetSavedTracks(Task):

    def output(self):
        import os

        file_location = '~/Temp/luigi/spotify/saved_songs.pckl'
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        from requests import get
        import pickle
        import time

        self.output().makedirs()

        access_token = check_for_refresh()

        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        url = 'https://api.spotify.com/v1/me/tracks'
        params = {'limit': 50}
        error_url = None

        songs = []

        while url:
            access_token = check_for_refresh()

            r = get(url, params=params, headers=headers)

            if r.status_code != 200:
                if error_url == url:
                    r.raise_for_status()
                print('Error accessing url: {}'.format(url))
                error_url = url
                time.sleep(1)
                continue

            data = r.json()
            songs.extend(data['items'])
            url = None  # currently testing

        with self.output().open() as f:
            pickle.dump(songs, f, protocol=-1)


# class GetAudioFeatures(Task):

#     auth_token = Parameter(visibility=ParameterVisibility.PRIVATE,
#                            significant=False)

#     def run(self):

#         songs = []

#         track_features_url = 'https://api.spotify.com/v1/audio-features/'

#         headers = {'Authorization': 'Bearer {}'.format(self.auth_token),
#                    }

#         params = {'ids': ','.join([x['track']['id']
#                                    for x in songs['items']]),
#                   }

#         r = requests.get(track_features_url,
#                          headers=headers,
#                          params=params,
#                          )

#         if r.status_code != 200:
#             print('error')

#         features = r.json()['audio_features']

#     def output(self):
#         pass
