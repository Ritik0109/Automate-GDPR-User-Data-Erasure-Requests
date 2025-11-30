# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------


tables = spark.sql('show tables in dev.db_connectsphere').collect()
try:
    for table in tables:
        spark.sql(f'TRUNCATE TABLE dev.db_connectsphere.{table[1]}')
        print('done')
except Exception as e:
    print(f'Table not found: {e}')


# COMMAND ----------

class Historical:
    def __init__(self):
        self.Conf = Config()
        self.main_path = self.Conf.main_path
        self.historical_path = self.Conf.historical_path
        self.incremental_daily = self.Conf.incremental_daily
        self.db_name = self.Conf.db_name
        self.catalog = self.Conf.catalog


    def ingest_messaging_data(self):
        schema = 'message_id string, sender_id string, recipient_id string, message_text string, message_timestamp timestamp'
        df = spark.read\
            .format('csv')\
            .schema(schema)\
            .option('header','true')\
            .option('multiLine', 'true')\
            .load(f'{self.historical_path}/direct_messages')

        write_df = df.write\
            .format('delta')\
            .mode('append')\
            .saveAsTable(f'{self.catalog}.{self.db_name}.fact_dms')
        return True


    def support_tickets_data(self):
        schema = 'ticket_id	string, requester_email string,	ticket_subject string, creation_timestamp timestamp'
        df = spark.read\
            .format('csv')\
            .schema(schema)\
            .option('header','true')\
            .option('multiLine', 'true')\
            .load(f'{self.historical_path}/support_tickets')
        
        write_df = df.write\
            .format('delta')\
            .mode('append')\
            .saveAsTable(f'{self.catalog}.{self.db_name}.fact_tickets')
        return True

    
    def user_article_data(self):
        schema = 'article_id string, author_id string, article_title string, publish_timestamp timestamp'
        df = spark.read\
            .format('csv')\
            .schema(schema)\
            .option('header','true')\
            .option('multiLine', 'true')\
            .load(f'{self.historical_path}/user_articles')
        
        write_df = df.write\
            .format('delta')\
            .mode('append')\
            .saveAsTable(f'{self.catalog}.{self.db_name}.fact_user_articles')
        return True


    def user_engagement_data(self):
        schema = 'engagement_id string, engaging_user_id string, target_article_id string, engagement_type string,	comment_text string, engagement_timestamp timestamp'

        df = spark.read\
            .format('csv')\
            .schema(schema)\
            .option('header','true')\
            .option('multiLine', 'true')\
            .load(f'{self.historical_path}/user_engagements')

        write_df = df.write\
            .format('delta')\
            .mode('append')\
            .saveAsTable(f'{self.catalog}.{self.db_name}.fact_user_engagement')
        return True

    def user_profiles_data(self):
        schema = 'user_id string, username string, email string, signup_date timestamp, country string'
        from pyspark.sql import functions as F
        
        df = spark.read\
            .format('csv')\
            .schema(schema)\
            .option('header','true')\
            .option('multiLine', 'true')\
            .load(f'{self.historical_path}/user_profiles')\
            .withColumn('loadtime', F.current_timestamp())\
            .withColumn('is_active', F.expr('1'))
        
        write_df = df.write\
            .format('delta')\
            .mode('append')\
            .saveAsTable(f'{self.catalog}.{self.db_name}.dim_user_profile')
        return True


    def ingest_raw(self):
        import time
        start = int(time.time())
        print('Starting data ingestion from landing zone...')
        a = self.ingest_messaging_data()
        b = self.support_tickets_data()
        c = self.user_article_data()
        d = self.user_engagement_data()
        e = self.user_profiles_data()
        print(f'Completed initial ingestion in {int(time.time()) - start} seconds')

# COMMAND ----------

historical_backfill = Historical()
historical_backfill.ingest_raw()

# COMMAND ----------


tables = spark.sql('show tables in dev.db_connectsphere').select('tableName').collect()
for table in tables:
    count = spark.read.table(f'dev.db_connectsphere.{table[0]}').count()
    if count > 0:
        print(f'Data ingested in {table[0]}, count: {count}')
    else:
        print('data not ingested')