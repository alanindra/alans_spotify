from tools import pipeline
from tools import config
import pandas as pd

path_master_table = config.path['master_table']
pipeline = pipeline.Pipeline()

def run_pipeline():
    # create or update master table
    if not path_master_table.exists():
        pipeline.create_extended_stream_table(is_master_table=True)
    else:
        pipeline.update_master_table()

# def enrich_table():
#     # enrich table
#     if config.features['table_enrichment'] == 'true':
#         pipeline.enrich_table(pd.read_csv(path_master_table))

if __name__ == "__main__":
    run_pipeline()
    # enrich_table()
    pipeline.move_to_processed_dir()