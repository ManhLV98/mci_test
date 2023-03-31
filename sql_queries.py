sql_queries = {
    "create_tb":"call public.proc_create_table()"
    ,"s3_to_redshift":{
        "song_data":"call public.proc_s3_to_tb('public.song_data','s3://manhlv-test/song_data/')"
       ,"log_data":"call public.proc_s3_to_tb('public.log_data','s3://manhlv-test/log_data/2018/11/')"
    }
    ,"stg_to_dwh":"call public.proc_music_sample()"
}