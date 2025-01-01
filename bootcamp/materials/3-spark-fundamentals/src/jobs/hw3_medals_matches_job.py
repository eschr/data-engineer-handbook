from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, split, lit
from pyspark.storagelevel import StorageLevel

def create_match_details_bucketed(spark, matchDetails):
    bucketedMatchDetails = """
        CREATE TABLE IF NOT EXISTS homework.match_details_bucketed (
            match_id STRING,
            player_gamertag STRING,
            player_total_kills INTEGER,
            player_total_deaths INTEGER
        )
        USING iceberg
        PARTITIONED BY (bucket(16, match_id));
    """
    spark.sql(bucketedMatchDetails)

    matchDetails.select("match_id", "player_gamertag", "player_total_kills", "player_total_deaths") \
        .write.mode("append") \
        .bucketBy(16, "match_id").saveAsTable("homework.match_details_bucketed")
    
def create_medals_players_bucketed(spark, medalsMatchesPlayers):
    bucketedMedalsMatchesPlayersDetails = """
    CREATE TABLE IF NOT EXISTS homework.medals_matches_players (
        match_id STRING,
        player_gamertag STRING,
        medal_id STRING,
        count INTEGER
    )
    USING iceberg
    PARTITIONED BY (bucket(16, match_id));
    """
    spark.sql(bucketedMedalsMatchesPlayersDetails)

    medalsMatchesPlayers.select("match_id", "player_gamertag", "medal_id", "count") \
        .write.mode("append") \
        .bucketBy(16, "match_id").saveAsTable("homework.medals_matches_players")
    
def create_matches_bucketed(spark, matches):
    distinctDates = matches.select("completion_date").distinct().collect()
    matchesBucketedDDL = """
    CREATE TABLE IF NOT EXISTS homework.matches_bucketed (
        match_id STRING,
        mapid STRING,
        is_team_game BOOLEAN,
        playlist_id STRING,
        completion_date TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (completion_date, bucket(16, match_id))
    """
    spark.sql(matchesBucketedDDL)

    for row in distinctDates:
        date = row['completion_date']
        filteredMatches = matches.filter(matches.completion_date == date)
        optimizedMatches = filteredMatches \
            .select("match_id", "mapid", "is_team_game", "playlist_id", "completion_date") \
            .repartition(16, "match_id") \
            .persist(StorageLevel.MEMORY_AND_DISK)

        optimizedMatches.write \
            .mode("append") \
            .bucketBy(16, "match_id") \
            .partitionBy("completion_date") \
            .saveAsTable("homework.matches_bucketed")
        
def join_tables(spark):
    createBucketJoinedDDL = """
    CREATE TABLE IF NOT EXISTS homework.final_matches_joined
        USING iceberg
        PARTITIONED BY (bucket(16, match_id))
        AS
            (
                SELECT mdb.match_id, mdb.player_gamertag, mdb.player_total_kills, mdb.player_total_deaths,
                    mb.mapid, mb.is_team_game, mb.playlist_id, mb.completion_date,
                    mmp.medal_id, mmp.count
                FROM ((homework.match_details_bucketed mdb
                INNER JOIN homework.matches_bucketed mb ON mdb.match_id = mb.match_id)
                INNER JOIN homework.medals_matches_players mmp ON mdb.match_id = mmp.match_id)
            )
    """
    spark.sql(createBucketJoinedDDL)

def aggregation_questions(spark):
    averageKills = """
        SELECT dfm.player_gamertag, 
            AVG(dfm.player_total_kills) OVER (PARTITION BY dfm.player_gamertag) AS average_kills
        FROM 
            (SELECT DISTINCT match_id, player_gamertag, player_total_kills FROM homework.final_matches_joined) AS dfm
        ORDER BY average_kills DESC
    """

    mostPlayedPlaylist = """
        SELECT dfm.playlist_id AS id, COUNT(dfm.playlist_id) AS playlist_count
        FROM
            (SELECT DISTINCT match_id, playlist_id FROM homework.final_matches_joined) AS dfm
        GROUP BY id
        ORDER BY playlist_count DESC
        LIMIT 3
    """

    mostPlayedMap = """
        SELECT dfm.mapid, name, COUNT(dfm.mapid) as map_count
        FROM
            (SELECT DISTINCT match_id, mapid FROM homework.final_matches_joined) AS dfm
            INNER JOIN mapsView
            ON dfm.mapid = mapsView.mapid
        GROUP BY dfm.mapid, name
        ORDER BY map_count DESC
        LIMIT 3
    """

    mostKillingSpreeMedals = """
        SELECT dfm.mapid, SUM(dfm.count) as total_killing_spree
        FROM
            (SELECT DISTINCT match_id, mapid, medal_id, count FROM homework.final_matches_joined) AS dfm
            INNER JOIN medalsView
            ON dfm.medal_id = medalsView.medal_id
        WHERE medalsView.name = 'Killing Spree'
        GROUP BY dfm.mapid
        ORDER BY total_killing_spree DESC
        LIMIT 3
    """

    spark.sql(averageKills).show()
    spark.sql(mostPlayedPlaylist).show()
    spark.sql(mostPlayedMap).show()
    spark.sql(mostKillingSpreeMedals).show()


def read_in_csvs_create_tables(spark):
    matches = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/matches.csv") \

    matchDetails = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/match_details.csv") \

    medalsMatchesPlayers = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/medals_matches_players.csv") \

    maps =  spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/maps.csv") \

    medals =  spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv("/home/iceberg/data/medals.csv")
    
    medals.join(broadcast(maps)).show()
    
    spark.sql('CREATE DATABASE IF NOT EXISTS homework')

    create_match_details_bucketed(spark, matchDetails)
    create_medals_players_bucketed(spark, medalsMatchesPlayers)
    create_matches_bucketed(spark, matches)
    join_tables(spark)

def sort_within_partitions(spark):
    check_size_original = """
        SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
        FROM homework.final_matches_joined.files
    """
    spark.sql(check_size_original).show()
    full_df = spark.read.table('homework.final_matches_joined')
    sort_playlist_df = full_df.sortWithinPartitions('playlist_id', ascending=False)

    create_sorted_playlist_ddl = """
    CREATE TABLE IF NOT EXISTS homework.sort_playlist (
        match_id STRING,
        player_gamertag STRING,
        player_total_kills INTEGER,
        player_total_deaths INTEGER,
        mapid STRING,
        is_team_game BOOLEAN,
        playlist_id STRING,
        completion_date TIMESTAMP,
        medal_id STRING,
        count INTEGER
    )
    USING iceberg
    PARTITIONED BY (playlist_id)    
    """
    spark.sql(create_sorted_playlist_ddl)

    sort_playlist_df.write.mode("append").saveAsTable("homework.sort_playlist")

    check_size_playlist = """
        SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
        FROM homework.sort_playlist.files
    """

    spark.sql(check_size_playlist).show()

    create_sorted_mapid_ddl = """
    CREATE TABLE IF NOT EXISTS homework.sort_mapid (
        match_id STRING,
        player_gamertag STRING,
        player_total_kills INTEGER,
        player_total_deaths INTEGER,
        mapid STRING,
        is_team_game BOOLEAN,
        playlist_id STRING,
        completion_date TIMESTAMP,
        medal_id STRING,
        count INTEGER
    )
    USING iceberg
    PARTITIONED BY (mapid)
    """

    spark.sql(create_sorted_mapid_ddl)

    map_id_df = full_df.sortWithinPartitions('mapid')

    map_id_df.write.mode("append").saveAsTable("homework.sort_mapid")

    check_size_mapid = """
        SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
        FROM homework.sort_mapid.files
    """

    spark.sql(check_size_mapid).show()


def main():
    spark = SparkSession.builder \
        .appName("IcebergTableManagement") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "1") \
        .config("spark.dynamicAllocation.maxExecutors", "50") \
        .getOrCreate()
    
    read_in_csvs_create_tables(spark)
    aggregation_questions(spark)
    sort_within_partitions(spark)