with
    track_skipped_status as (
        select 
            master_metadata_track_name
            , master_metadata_album_artist_name
            , master_metadata_album_album_name
            , skipped
            , count(*) count
        from 
            {raw_extended_stream_table}
        where 
            master_metadata_album_artist_name is not null
        group by 
            master_metadata_track_name
            , master_metadata_album_artist_name
            , master_metadata_album_album_name
            , skipped
    )

select 
    master_metadata_track_name
    , master_metadata_album_artist_name
    , master_metadata_album_album_name
    , sum(case when skipped = 1 then count else 0 end) as count_skipped
    , sum(case when skipped = 0 then count else 0 end) as count_unskipped
    , sum(count) count_total
    , (
        cast(sum(case when skipped = 1 then count else 0 end) as real)
        / (sum(count))
    ) as skip_rate
from 
    track_skipped_status 
group by 
    master_metadata_track_name
    , master_metadata_album_album_name
    , master_metadata_album_artist_name
order by count_total desc

;