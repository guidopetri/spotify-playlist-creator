#! /usr/bin/env python3

from luigi import Task, LocalTarget
from luigi.format import Nop
# from postgres_templates import TransactionFactTable, CopyWrapper
from spotify_api import check_for_refresh


class GetSavedTracks(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return [LocalTarget(file_location.format('saved_songs'), format=Nop),
                LocalTarget(file_location.format('mini_albums'), format=Nop),
                LocalTarget(file_location.format('mini_artists'), format=Nop),
                ]

    def run(self):
        from requests import get
        import pickle
        import time

        [x.makedirs() for x in self.output()]

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

        albums = set(song['track']['album']['id']
                     for song in songs)

        artists = set(artist['id'] for song in songs
                      for artist in song['track']['artists'])

        with self.output()[0].open('w') as f:
            pickle.dump(songs, f, protocol=-1)

        with self.output()[1].open('w') as f:
            pickle.dump(albums, f, protocol=-1)

        with self.output()[2].open('w') as f:
            pickle.dump(artists, f, protocol=-1)


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
