# Блок 1. Развертывание локального Hive

![img](screenshots/data_grip.png)
![img](screenshots/hue.png)

# Блок 2. Работа с Hive

Исполнителя с максимальным числом скробблов

```sql
USE default;

SELECT 
    artist_lastfm
    , scrobbles_lastfm
FROM hue__tmp_artists
ORDER BY scrobbles_lastfm DESC
LIMIT 1;
```

| artist_lastfm | scrobbles_lastfm |
| --- | --- |
| The Beatles | 517126254 |
____________________

Самый популярный тэг на ластфм

```sql
USE default;

SELECT 
    tag
    , COUNT(*) as cnt
FROM ( --tags
    SELECT
        trim(tag_dirty) as tag
        , artist_lastfm
    FROM hue__tmp_artists
        LATERAL VIEW explode(SPLIT(tags_lastfm, ';')) hue__tmp_artists AS tag_dirty
) as tags
WHERE tag != ''
GROUP BY tag
ORDER BY cnt DESC
LIMIT 1;
```

| tag | cnt |
| --- | --- |
| seen live | 99540 |

____________________

Самые популярные исполнители 10 самых популярных тегов ластфм

```sql
USE default;

WITH artist_tag_listners as (
    SELECT 
        tag
        , artist
        , listners
    FROM ( --tags
        SELECT
            trim(tag_dirty) as tag
            , artist_lastfm as artist
            , listeners_lastfm as listners
        FROM hue__tmp_artists
            LATERAL VIEW explode(SPLIT(tags_lastfm, ';')) hue__tmp_artists AS tag_dirty
    ) as tags
    WHERE tag != ''
),

top_tags as (
    SELECT 
        tag
        , COUNT(*) as cnt
    FROM artist_tag_listners
    GROUP BY tag
    ORDER BY cnt DESC
    LIMIT 10
)
	

SELECT
    artist
    , MAX(listners) AS listners
FROM artist_tag_listners
WHERE tag IN (SELECT tag FROM top_tags)
GROUP BY artist
ORDER BY listners DESC
LIMIT 10;
```

| id | artist | listners |
| --- | --- | --- |
| 1 | Coldplay | 5381567 |
| 2 | Radiohead | 4732528 |
| 3 | Red Hot Chili Peppers | 4620835 |
| 4 | Rihanna | 4558193 |
| 5 | Eminem | 4517997 |
| 6 | The Killers | 4428868 |
| 7 | Kanye West | 4390502 |
| 8 | Nirvana | 4272894 |
| 9 | Muse | 4089612 |
| 10 | Queen | 4023379 |
____________________

Топ 3 исполнителя от топ 20 стран по прослушиваниям

```sql
USE default;

WITH artist_country_listners as (
    SELECT 
        trim(country_dirty) as country
        , artist_lastfm as artist
        , MAX(listeners_lastfm) as listners
    FROM hue__tmp_artists
            LATERAL VIEW explode(SPLIT(country_lastfm, ';')) hue__tmp_artists AS country_dirty
    GROUP BY country_dirty, artist_lastfm
),

top_countries as (
    SELECT 
        country
        , SUM(listners) as cnt
    FROM artist_country_listners
    WHERE country != ''
    GROUP BY country
    ORDER BY cnt DESC
    LIMIT 20
),
	
artist_listners as (
    SELECT
        artist
        , country
        , MAX(listners) as listners
    FROM artist_country_listners
    WHERE country IN (SELECT country FROM top_countries)
    GROUP BY artist, country
)

SELECT
    country
    , artist
    , listners
FROM ( -- top_listners_window
    SELECT
        country
        , artist
        , listners
        , row_number() OVER (PARTITION BY country ORDER BY listners DESC) AS r_n
    FROM artist_listners
) as artist_listners
WHERE r_n <= 3
ORDER BY country, listners DESC
```
 
| id | country | artist | listners |
| --- | --- | --- | --- |
| 1  | Australia | AC/DC | 2691018 |
| 2 | Australia | Sia | 2124548 |
| 3 | Australia | Kylie Minogue | 1951310 |
| 4 | Brazil | Colbie Caillat | 1415771 |
| 5 | Brazil | Sepultura | 886301 |
| 6 | Brazil | Cansei de Ser Sexy | 868761 |
| 7 | Canada | Drake | 3379644 |
| 8 | Canada | Nickelback | 2832931 |
| 9 | Canada | Avril Lavigne | 2627363 |
| 10 | Finland | Nightwish | 1360111 |
| 11 | Finland | Apocalyptica | 1219619 |
| 12 | Finland | Him | 1043930 |
| 13 | France | Daft Punk | 3782404 |
| 14 | France | David Guetta | 2782756 |
| 15 | France | Air | 2155461 |
| 16 | Georgia | R.E.M. | 2886482 |
| 17 | Georgia | OutKast | 2508114 |
| 18 | Georgia | T.I. | 2422414 |
| 19 | Germany | Ludwig van Beethoven | 1836822 |
| 20 | Germany | Rammstein | 1809518 |
| 21 | Germany | Scorpions | 1652144 |
| 22 | Ireland | U2 | 3487345 |
| 23 | Ireland | Snow Patrol | 2982390 |
| 24 | Ireland | The Cranberries | 2276320 |
| 25 | Italy | Dean Martin | 1271988 |
| 26 | Italy | Benny Benassi | 1150247 |
| 27 | Italy | Antonio Vivaldi | 1133085 |
| 28 | Jamaica | Bob Marley | 2004655 |
| 29 | Jamaica | Bob Marley & The Wailers | 1921015 |
| 30 | Jamaica | Sean Paul | 1390177 |
| 31 | Japan | Far East Movement | 975724 |
| 32 | Japan | Blonde Redhead | 812628 |
| 33 | Japan | Bow Wow | 794885 |
| 34 | Netherlands | Armin van Buuren | 1111815 |
| 35 | Netherlands | Martin Garrix | 726254 |
| 36 | Netherlands | Armand van Helden | 609176 |
| 37 | Norway | Röyksopp | 1995533 |
| 38 | Norway | Justin Bieber | 1628031 |
| 39 | Norway | a-ha | 1595929 |
| 40 | Poland | Frédéric Chopin | 1238360 |
| 41 | Poland | Destroyer | 507089 |
| 42 | Poland | Behemoth | 402347 |
| 43 | Russia | Regina Spektor | 1836766 |
| 44 | Russia | Pyotr Ilyich Tchaikovsky | 990022 |
| 45 | Russia | t.A.T.u. | 926338 |
| 46 | Scotland | Franz Ferdinand | 3203026 |
| 47 | Scotland | Snow Patrol | 2982390 |
| 48 | Scotland | Calvin Harris | 2195535 |
| 49 | Spain | Shakira | 2346168 |
| 50 | Spain | Nelly Furtado | 2258851 |
| 51 | Spain | Jennifer Lopez | 2099109 |
| 52 | Sweden | The Cardigans | 1671502 |
| 53 | Sweden | ABBA | 1659292 |
| 54 | Sweden | José González | 1629952 |
| 55 | United Kingdom | Coldplay | 5381567 |
| 56 | United Kingdom | Radiohead | 4732528 |
| 57 | United Kingdom | Muse | 4089612 |
| 58 | United States | Red Hot Chili Peppers | 4620835 |
| 59 | United States | Rihanna | 4558193 |
| 60 | United States | Eminem | 4517997 |
