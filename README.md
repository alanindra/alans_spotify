# [WIP] alans_spotify

Creating a unified table containing Spotify streamed data. Update master data by merging new data to master table every processing.

## Input
- metadata
    - Contains YourLibrary.json file
- streaming_history
    - Contains StreamingHistory_music*.json files
- extended_streaming_history
    - Contains Streaming_History_Audio*.json files
    - Contains Streaming_History_Video*.json files

Once processed, the data will be moved to processed dir

## Output
- output
    - master_table.csv
        - The master data
    - {time}
        - Contains transformed and enriched tables by times of processing
- processed
    - {time}
        - Contains untransformed and unenriched tables   
