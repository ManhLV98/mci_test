CREATE OR REPLACE PROCEDURE public.proc_create_table()
 LANGUAGE plpgsql
AS $$
DECLARE
BEGIN

			-- 1.create table public.log_data
            DROP TABLE IF EXISTS public.log_data;
            CREATE TABLE public.log_data (
                artist character varying(200) ENCODE lzo,
                auth character varying(200) ENCODE lzo,
                firstname character varying(200) ENCODE lzo,
                gender character varying(50) ENCODE lzo,
                iteminsession integer ENCODE az64,
                lastname character varying(200) ENCODE lzo,
                length numeric(10, 6) ENCODE az64,
                level character varying(100) ENCODE lzo,
                location character varying(500) ENCODE lzo,
                method character varying(50) ENCODE lzo,
                page character varying(200) ENCODE lzo,
                registration bigint ENCODE az64,
                sessionid integer ENCODE az64,
                song character varying(200) ENCODE lzo,
                status integer ENCODE az64,
                ts bigint ENCODE az64,
                useragent character varying(500) ENCODE lzo,
                userid character varying(50) ENCODE lzo
            ) DISTSTYLE AUTO;
           -- 2.create table public.song_data
                DROP TABLE IF EXISTS public.song_data;
                CREATE TABLE public.song_data (
                    num_songs bigint ENCODE az64,
                    artist_id character varying(100) ENCODE lzo,
                    artist_latitude character varying(200) ENCODE lzo,
                    artist_longitude character varying(200) ENCODE lzo,
                    artist_location character varying(500) ENCODE lzo,
                    artist_name character varying(100) ENCODE lzo,
                    song_id character varying(100) ENCODE lzo,
                    title character varying(100) ENCODE lzo,
                    duration numeric(10, 6) ENCODE az64,
                    year integer ENCODE az64
                ) DISTSTYLE AUTO;
           -- 3.create table public.artists
                DROP TABLE IF EXISTS public.artists CASCADE;
                CREATE TABLE public.artists (
                    artist_id character varying(50) NOT NULL ENCODE lzo,
                        name character varying(200) ENCODE lzo,
                        location character varying(500) ENCODE lzo,
                        latitude numeric(10, 6) ENCODE az64,
                        longitude numeric(10, 6) ENCODE az64,
                        PRIMARY KEY (artist_id)
                ) DISTSTYLE AUTO
                SORTKEY
                    (artist_id);
	       -- 4.create table public.songs
	            DROP TABLE IF EXISTS public.songs CASCADE;
                CREATE TABLE public.songs (
                    song_id character varying(50) NOT NULL ENCODE raw,
                        title character varying(200) ENCODE lzo,
                        artist_id character varying(50) ENCODE lzo,
                        year integer ENCODE az64,
                        duration numeric(10, 6) ENCODE az64,
                        PRIMARY KEY (song_id)
                ) DISTSTYLE AUTO
                SORTKEY
                    (song_id);
           -- 5.create table public.time
            DROP TABLE IF EXISTS public.time CASCADE;
            CREATE TABLE public.time (
                start_time timestamp without time zone NOT NULL ENCODE az64,
                    hour integer ENCODE az64,
                    day integer ENCODE az64,
                    week integer ENCODE az64,
                    month integer ENCODE az64,
                    year integer ENCODE az64,
                    weekday integer ENCODE az64,
                    PRIMARY KEY (start_time)
            ) DISTSTYLE AUTO
            SORTKEY
                (start_time);
            -- 6.create table public.users
            DROP TABLE IF EXISTS public.users CASCADE;
            CREATE TABLE public.users (
                user_id integer NOT NULL ENCODE az64,
                    first_name character varying(150) ENCODE lzo,
                    last_name character varying(150) ENCODE lzo,
                    gener character(1) ENCODE lzo,
                    level character varying(50) ENCODE lzo,
                    PRIMARY KEY (user_id)
            ) DISTSTYLE AUTO
            SORTKEY
                (user_id);
            -- 7.create table public.songplays
            DROP TABLE IF EXISTS public.songplays ;
            CREATE TABLE public.songplays (
                songplay_id bigint NOT NULL identity(1, 1) ENCODE az64,
                    start_time timestamp without time zone ENCODE az64,
                    user_id integer ENCODE az64,
                    level character varying(50) ENCODE lzo,
                    song_id character varying(100) ENCODE lzo,
                    artist_id character varying(100) ENCODE lzo,
                    session_id integer ENCODE az64,
                    location character varying(500) ENCODE lzo,
                    user_agent character varying(200) ENCODE lzo,
                    PRIMARY KEY (songplay_id),
                    FOREIGN KEY (start_time) REFERENCES "time"(start_time),
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (song_id) REFERENCES songs(song_id),
                    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
            ) DISTSTYLE AUTO;
END;
$$

CREATE OR REPLACE PROCEDURE public.proc_s3_to_tb(nametb character varying(256), s3 character varying(256))
 LANGUAGE plpgsql
AS $$ DECLARE clean_sql varchar(max);
              copy_sql varchar(max);
BEGIN
copy_sql:= 'copy  '|| nametb || ' FROM ''' || s3 || ''' iam_role ''arn:aws:iam::********:role/service-role/AmazonRedshift-CommandsAccessRole-20220626T114004''   json ''auto ignorecase'';';
EXECUTE copy_sql;

END;
$$


CREATE OR REPLACE PROCEDURE public.proc_music_sample()
 LANGUAGE plpgsql
AS $$
DECLARE
BEGIN
    --1.table: songs
	DROP TABLE IF EXISTS songs_tmp;
	CREATE TEMP TABLE songs_tmp AS(
		select sd.song_id as song_id_tmp, sd.title as title_tmp, sd.artist_id artist_id_tmp, sd.year year_tmp, sd.duration duration_tmp, (case when ss.song_id is null then 1 else 0 end ) flag_songs
		from public.song_data sd
		left join public.songs ss on sd.song_id = ss.song_id
		where sd.song_id is not null
	);

	INSERT INTO public.songs
	( song_id, title, artist_id,year, duration)
	select
	song_id_tmp, title_tmp, artist_id_tmp, year_tmp, duration_tmp
	from songs_tmp
	where flag_songs = 1;


	UPDATE public.songs
	SET
	title = title_tmp
	, artist_id = artist_id_tmp
	, year = year_tmp
	, duration = duration_tmp
	FROM    songs_tmp
	WHERE     song_id_tmp      =      song_id
		AND flag_songs = 0
		;


	--2.table: artist
	DROP TABLE IF EXISTS artist_tmp;
	CREATE TEMP TABLE artist_tmp AS(
		select distinct sd.artist_id artist_id_tmp, sd.artist_name as name_tmp, sd.artist_location artist_location_tmp, sd.artist_latitude artist_latitude_tmp, sd.artist_longitude artist_longitude_tmp, (case when at.artist_id is null then 1 else 0 end ) flag_artist
		from public.song_data sd
		left join public.artists at on sd.artist_id = at.artist_id
		where sd.artist_id is not null and sd.artist_name is not null
	);

	INSERT INTO public.artists
	( artist_id,  name, location, latitude, longitude)
	select
	artist_id_tmp, name_tmp, artist_location_tmp, artist_latitude_tmp, artist_longitude_tmp
	from artist_tmp
	where flag_artist = 1;


	UPDATE public.artists
	SET
	latitude = artist_latitude_tmp
	, name = name_tmp
	, location = artist_location_tmp
	, longitude = artist_longitude_tmp
	FROM    artist_tmp
	WHERE     artist_id      =      artist_id_tmp
		AND flag_artist = 0
		;


	--3.table: time
	DROP TABLE IF EXISTS time_tmp;
	CREATE TEMP TABLE time_tmp AS(
    	select (timestamp 'epoch' + ld_1.ts/ 1000 * interval '1 second') start_time_tmp
        ,  DATE_PART(hour,(timestamp 'epoch' + ld_1.ts/ 1000 * interval '1 second')) hour_tmp
        , DATE_PART(day,(timestamp 'epoch' + ld_1.ts/ 1000 * interval '1 second')) day_tmp
        , DATE_PART(week,(timestamp 'epoch' + ld_1.ts/ 1000 * interval '1 second')) week_tmp
        ,  DATE_PART(month,(timestamp 'epoch' + ld_1.ts/ 1000 * interval '1 second')) month_tmp
        , DATE_PART(year,(timestamp 'epoch' + ld_1.ts/ 1000 * interval '1 second')) year_tmp
        , DATE_PART(dayofweek,(timestamp 'epoch' + ld_1.ts/ 1000 * interval '1 second')) week_day_tmp
        , (case when t_1.start_time is null then 1 else 0 end ) flag_time
        from public.log_data ld_1
        left join public.time t_1 on (timestamp 'epoch' + ld_1.ts/ 1000 * interval '1 second') = t_1.start_time
        where ld_1.ts is not null and ld_1.status = 200 and ld_1.artist is not null and ld_1.song is not null
	);

	INSERT INTO public.time
	( start_time,  hour, day, week, month, year, weekday)
	select
	start_time_tmp, hour_tmp, day_tmp, week_tmp, month_tmp, year_tmp, week_day_tmp
	from time_tmp
	where flag_time = 1;


	UPDATE public.time
	SET
	hour = hour_tmp
	, day = day_tmp
	, week = week_tmp
	, month = month_tmp
    , year = year_tmp
    , weekday = week_day_tmp
	FROM    time_tmp
	WHERE     start_time      =      start_time_tmp
		AND flag_time = 0
		;


	--4.table: users
	DROP TABLE IF EXISTS users_tmp;
	CREATE TEMP TABLE users_tmp AS(
        select distinct cast (ld_1.userid as bigint) user_id_tmp, ld_1.firstname first_name_tmp, ld_1.lastname last_name_tmp, ld_1.gender gener_tmp, ld_1.level level_tmp, (case when us_1.user_id is null then 1 else 0 end ) flag_user
        from public.log_data ld_1
        left join public.users us_1 on ld_1.userid = us_1.user_id
        where ld_1.ts is not null and ld_1.status = 200 and ld_1.artist is not null and ld_1.song is not null and ld_1.userid is not null
	);

	INSERT INTO public.users
	( user_id,  first_name, last_name, gener, level)
	select
	user_id_tmp, first_name_tmp, last_name_tmp, gener_tmp, level_tmp
	from users_tmp
	where flag_user = 1;


	UPDATE public.users
	SET
	 first_name = first_name_tmp
	, last_name = last_name_tmp
	, gener = gener_tmp
    , level = level_tmp
	FROM    users_tmp
	WHERE     user_id      =      user_id_tmp
		AND flag_user = 0
		;


	--5.table: songplays
	DROP TABLE IF EXISTS songplays_tmp;
	CREATE TEMP TABLE songplays_tmp AS(
        select (timestamp 'epoch' + ld.ts/ 1000 * interval '1 second') start_time_tmp, cast(ld.userid as bigint) user_id_tmp, ld.level level_tmp, sd.song_id song_id_tmp, sd.artist_id artist_id_tmp, ld.sessionid session_id_tmp, ld.location location_tmp, ld.useragent user_agent_tmp, (case when ss.songplay_id is null then 1 else 0 end ) flag_songplay
        from public.log_data  ld
        inner join public.song_data sd on upper(trim(artist_name)) = upper(trim(artist)) and upper(trim(song)) = upper(trim(title))
        left join public.songplays ss on ss.artist_id = sd.artist_id and ss.song_id = sd.song_id
        where ld.artist is not null and ld.song is not null and ld.status = 200

    );

	INSERT INTO public.songplays
	( start_time,  user_id, level, song_id, artist_id, session_id, location,user_agent )
	select
	start_time_tmp, user_id_tmp, level_tmp, song_id_tmp, artist_id_tmp, session_id_tmp, location_tmp, user_agent_tmp
	from songplays_tmp
	where flag_songplay = 1;


	UPDATE songplays
	SET
	 start_time = start_time_tmp
	, user_id = user_id_tmp
	, level = level_tmp
    , song_id = song_id_tmp
    , artist_id = artist_id_tmp
    , session_id = session_id_tmp
    , location = location_tmp
    , user_agent = user_agent_tmp
	FROM    songplays_tmp
	WHERE     artist_id = artist_id_tmp and song_id = song_id_tmp
		AND flag_songplay = 0
		;


END;
$$
