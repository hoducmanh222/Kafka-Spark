"""Declarative Spark transformations. No Python UDFs in the streaming plan."""
from pyspark.sql import functions as F, types as T, Window

EVENT_SCHEMA = T.StructType([
    T.StructField('timestamp', T.LongType()),
    T.StructField('visitorid', T.LongType()),
    T.StructField('event', T.StringType()),
    T.StructField('itemid', T.LongType()),
    T.StructField('transactionid', T.LongType()),
    T.StructField('_corrupt_record', T.StringType()),
])


def parse_events(raw):
    parsed = raw.select(
        F.col('value').cast('string').alias('raw_json'),
        F.col('topic'), F.col('partition'), F.col('offset'),
        F.col('timestamp').alias('kafka_timestamp'),
        F.current_timestamp().alias('ingested_at'),
        F.from_json(F.col('value').cast('string'), EVENT_SCHEMA,
                    {'mode': 'PERMISSIVE', 'columnNameOfCorruptRecord': '_corrupt_record'}).alias('payload')
    ).select('*', 'payload.*').drop('payload')
    valid = (F.col('_corrupt_record').isNull() &
             F.col('timestamp').between(946684800000, 4102444799999) &
             (F.col('visitorid') >= 0) & (F.col('itemid') >= 0) &
             F.col('event').isin('view', 'addtocart', 'transaction') &
             (((F.col('event') == 'transaction') & F.col('transactionid').isNotNull() & (F.col('transactionid') >= 0)) |
              ((F.col('event') != 'transaction') & F.col('transactionid').isNull())))
    return (parsed.withColumn('is_valid', F.coalesce(valid, F.lit(False)))
            .withColumn('event_time', F.timestamp_millis('timestamp'))
            .withColumn('event_date', F.to_date('event_time'))
            .withColumn('ingest_date', F.to_date('ingested_at'))
            .withColumn('weight', F.when(F.col('event') == 'view', 1)
                        .when(F.col('event') == 'addtocart', 5)
                        .when(F.col('event') == 'transaction', 10).otherwise(0)))


def aggregate_intent(events):
    return (events.filter('is_valid')
            .withWatermark('event_time', '5 minutes')
            .groupBy(F.window('event_time', '10 minutes', '1 minute'), 'visitorid', 'itemid')
            .agg(F.sum('weight').alias('intent_score'),
                 F.count('*').alias('event_count'),
                 F.sum(F.when(F.col('event') == 'addtocart', 1).otherwise(0)).alias('cart_count'),
                 F.sum(F.when(F.col('event') == 'transaction', 1).otherwise(0)).alias('transaction_count'),
                 F.max('event_time').alias('last_event_time'))
            .select(F.col('window.start').alias('window_start'),
                    F.col('window.end').alias('window_end'), '*').drop('window'))


def merge_updates(previous, updates, event_max_ms):
    """Replace absolute update-mode totals, then drop windows behind the watermark."""
    new = updates.withColumn('_version', F.lit(1))
    old = previous.withColumn('_version', F.lit(0))
    keys = ['window_start', 'window_end', 'visitorid', 'itemid']
    rank = Window.partitionBy(*keys).orderBy(F.col('_version').desc())
    return (old.unionByName(new).withColumn('_row', F.row_number().over(rank))
            .filter('_row = 1').drop('_row', '_version')
            .filter(F.unix_millis('window_end') > event_max_ms - 300_000))


def recommendations(state, event_max_ms, threshold=5, top_k=3):
    """Current aligned window, strongest cart anchor and unseen live popular items.

    Only one window is ranked, so an event is never counted ten times in popularity.
    Returns a full replacement snapshot, including zero rows after retraction.
    """
    from retail.domain import active_window_start
    active = state.filter(F.unix_millis('window_start') == active_window_start(event_max_ms))
    anchors = active.filter((F.col('cart_count') > 0) &
                            (F.col('transaction_count') == 0) &
                            (F.col('intent_score') >= threshold))
    choice = Window.partitionBy('visitorid').orderBy(F.col('intent_score').desc(), F.col('itemid'))
    anchors = (anchors.withColumn('_anchor_rank', F.row_number().over(choice))
               .filter('_anchor_rank = 1')
               .select('visitorid', F.col('itemid').alias('anchor_itemid'),
                       'intent_score', 'window_start', 'window_end'))
    popularity = active.groupBy('itemid').agg(F.sum('intent_score').alias('popularity_score'))
    candidates = popularity.select(F.col('itemid').alias('recommended_itemid'), 'popularity_score')
    seen = active.select('visitorid', F.col('itemid').alias('recommended_itemid')).distinct()
    unseen = anchors.crossJoin(candidates).join(seen, ['visitorid', 'recommended_itemid'], 'left_anti')
    ranking = Window.partitionBy('visitorid').orderBy(F.col('popularity_score').desc(), F.col('recommended_itemid'))
    return (unseen.withColumn('rank', F.row_number().over(ranking)).filter(F.col('rank') <= top_k)
            .withColumn('reason', F.lit('active_cart_and_live_popularity'))
            .withColumn('event_date', F.to_date('window_start'))
            .select('visitorid', 'anchor_itemid', 'intent_score', 'recommended_itemid',
                    'popularity_score', 'rank', 'window_start', 'window_end', 'reason', 'event_date'))
