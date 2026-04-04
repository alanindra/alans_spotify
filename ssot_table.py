import pandas as pd
# import config
# import queries

class SsotTable:
    """
    Python class to transform tables
    """
    def __init__(self, df):
        self.df = df

    # def create_tracks_table(self, df):

    # def create_album_table(self, df):

    # def create_artist_table(self, df):

    # def create_streaming_history_table(self, df):

    # def create_extended_streaming_history_table(self, df):

    # def enrich_audio_features(self, df):

    # def create_ssot_table(self, df):
        """
        Create one table containing all information of the Spotify data
        Table is a daily snapshot of music stream
        """
        # df = create_tracks_table(self.df)