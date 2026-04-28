from tools import pipeline
from tools import config
import pandas as pd

path_master_table = config.path['master_tables']
pipeline = pipeline.Pipeline()

def retrieve_raw_data():
    pipeline.create_extended_stream_table()
    pipeline.create_albums_table()
    pipeline.create_artists_table()
    pipeline.create_tracks_table()

# def enrich_extended_stream_table():
#     # enrich table
#     if config.features['table_enrichment'] == 'true':
#         pipeline.enrich_table(pd.read_csv(path_master_table))

# def run_data_jobs():


if __name__ == "__main__":
    retrieve_raw_data()
    # enrich_extended_stream_table()
    # run_data_jobs()
    pipeline.move_to_processed_dir()