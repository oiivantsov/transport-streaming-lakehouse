from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
from pyspark.sql.functions import col, from_json

# Data: https://digitransit.fi/en/developers/apis/5-realtime-api/vehicle-positions/high-frequency-positioning/

'''
    -- Topic metadata (from MQTT topic path)

    prefix,                          -- e.g., "hfp" (root of topic tree)
    version,                         -- e.g., "v2" (topic version)
    journey_type,                    -- "journey", "deadrun", or "signoff"
    temporal_type,                   -- "ongoing" or "upcoming"
    event_type,                      -- e.g., "vp", "arr", "dep" (event type)
    transport_mode,                  -- e.g., "bus", "tram", "metro"
    operator_id,                     -- 4-digit operator ID (padded with zeroes)
    vehicle_number,                  -- 5-digit vehicle number (padded with zeroes)
    route_id,                        -- Route ID matching GTFS
    direction_id,                    -- "1" or "2" (direction of trip)
    headsign,                        -- Destination name (e.g., "Aviapolis")
    start_time,                      -- Scheduled trip start time (HH:mm)
    next_stop,                       -- Next stop ID (matches GTFS stop_id)
    geohash_level,                   -- Indicates GPS coordinate change level
    geohash,                         -- Encoded vehicle position (lat;long and more)
    sid,                             -- Junction ID (for traffic light events)

    -- Payload VP (Vehicle Position) data from message body

    desi,                            -- Route number (visible to passengers)
    dir,                             -- Route direction ("1" or "2")
    oper,                            -- Operator ID (no leading zeroes here)
    veh,                             -- Vehicle number (no leading zeroes here)
    tst,                             -- UTC timestamp (ISO 8601, e.g., "2023-07-08T12:00:00.000Z")
    tsi,                             -- Timestamp as Unix epoch seconds
    spd,                             -- Speed (m/s)
    hdg,                             -- Heading (degrees from North)
    lat,                             -- Latitude (WGS84)
    `long`,                          -- Longitude (WGS84)
    acc,                             -- Acceleration (m/s²)
    dl,                              -- Delay in seconds (ahead or behind schedule)
    odo,                             -- Odometer reading in meters
    drst,                            -- Door status: 0 = closed, 1 = open
    oday,                            -- Operating day ("YYYY-MM-DD")
    jrn,                             -- Internal journey ID (not usually useful)
    line,                            -- Internal line ID (not usually useful)
    start,                           -- Scheduled trip start time (HH:mm), same as topic
    loc,                             -- Location source: "GPS", "ODO", "MAN", "DR", "N/A"
    stop,                            -- Stop ID (GTFS stop_id or null)
    route,                           -- Route ID (matches GTFS)
    occu                             -- Occupancy level: 0 = space, 100 = full
'''

# Topic schema
topic_schema = StructType([
    StructField("prefix", StringType()),
    StructField("version", StringType()),
    StructField("journey_type", StringType()),
    StructField("temporal_type", StringType()),
    StructField("event_type", StringType()),
    StructField("transport_mode", StringType()),
    StructField("operator_id", StringType()),
    StructField("vehicle_number", StringType()),
    StructField("route_id", StringType()),
    StructField("direction_id", StringType()),
    StructField("headsign", StringType()),
    StructField("start_time", StringType()),
    StructField("next_stop", StringType()),
    StructField("geohash_level", StringType()),
    StructField("geohash", StringType()),
    StructField("sid", StringType())
])

# Payload → VP schema
vp_schema = StructType([
    StructField("desi", StringType()),
    StructField("dir", StringType()),
    StructField("oper", IntegerType()),
    StructField("veh", IntegerType()),
    StructField("tst", StringType()),
    StructField("tsi", LongType()),
    StructField("spd", DoubleType()),
    StructField("hdg", IntegerType()),
    StructField("lat", DoubleType()),
    StructField("long", DoubleType()),
    StructField("acc", DoubleType()),
    StructField("dl", IntegerType()),
    StructField("odo", IntegerType()),
    StructField("drst", IntegerType()),
    StructField("oday", StringType()),
    StructField("jrn", IntegerType()),
    StructField("line", IntegerType()),
    StructField("start", StringType()),
    StructField("loc", StringType()),
    StructField("stop", LongType()),
    StructField("route", StringType()),
    StructField("occu", IntegerType())
])

# Main schema
main_schema = StructType([
    StructField("topic", topic_schema),
    StructField("payload", StructType([
        StructField("VP", vp_schema)
    ]))
])

def parse_and_flatten(df_json):
    """
    Apply schema and flatten the JSON into a flat DataFrame
    """
    df_parsed = df_json.withColumn("data", from_json(col("json_str"), main_schema))

    df_flat = df_parsed.select(

        # Kafka metadata
        col("insert_time").alias("insert_time"),

        # Topic schema
        col("data.topic.prefix").alias("prefix"),
        col("data.topic.version").alias("version"),
        col("data.topic.journey_type").alias("journey_type"),
        col("data.topic.temporal_type").alias("temporal_type"),
        col("data.topic.event_type").alias("event_type"),
        col("data.topic.transport_mode").alias("transport_mode"),
        col("data.topic.operator_id").alias("operator_id"),
        col("data.topic.vehicle_number").alias("vehicle_number"),
        col("data.topic.route_id").alias("route_id"),
        col("data.topic.direction_id").alias("direction_id"),
        col("data.topic.headsign").alias("headsign"),
        col("data.topic.start_time").alias("start_time"),
        col("data.topic.next_stop").alias("next_stop"),
        col("data.topic.geohash_level").alias("geohash_level"),
        col("data.topic.geohash").alias("geohash"),
        col("data.topic.sid").alias("sid"),
        
        # Payload schema (VP)
        col("data.payload.VP.desi").alias("desi"),
        col("data.payload.VP.dir").alias("dir"),
        col("data.payload.VP.oper").alias("oper"),
        col("data.payload.VP.veh").alias("veh"),
        col("data.payload.VP.tst").alias("tst"),
        col("data.payload.VP.tsi").alias("tsi"),
        col("data.payload.VP.spd").alias("spd"),
        col("data.payload.VP.hdg").alias("hdg"),
        col("data.payload.VP.lat").alias("lat"),
        col("data.payload.VP.long").alias("long"),
        col("data.payload.VP.acc").alias("acc"),
        col("data.payload.VP.dl").alias("dl"),
        col("data.payload.VP.odo").alias("odo"),
        col("data.payload.VP.drst").alias("drst"),
        col("data.payload.VP.oday").alias("oday"),
        col("data.payload.VP.jrn").alias("jrn"),
        col("data.payload.VP.line").alias("line"),
        col("data.payload.VP.start").alias("start"),
        col("data.payload.VP.loc").alias("loc"),
        col("data.payload.VP.stop").alias("stop"),
        col("data.payload.VP.route").alias("route"),
        col("data.payload.VP.occu").alias("occu")
    )

    return df_flat
