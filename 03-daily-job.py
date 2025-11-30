# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Daily_job():
    def __init__(self):
        self.Conf = Config()
        self.incremental_path = self.Conf.incremental_daily
        self.main_path = self.Conf.main_path
        self.catalog = self.Conf.catalog
        self.db_name = self.Conf.db_name

    def ingest_data(self):
        myTables = { 'dev.db_connectsphere.fact_dms': f'{self.incremental_path}/direct_messages', 'dev.db_connectsphere.fact_tickets': f'{self.incremental_path}/support_tickets', 'dev.db_connectsphere.fact_user_articles': f'{self.incremental_path}/user_articles', 'dev.db_connectsphere.fact_user_engagement': f'{self.incremental_path}/user_engagements'}


#Fact tables insertion:
        for table, path in myTables.items():
            try:
                daily_df = spark.read\
                    .format('csv')\
                    .option('header','true')\
                    .option('multiLine', 'true')\
                    .load(path)

                daily_df.createOrReplaceTempView(f'updates_{table.split('.')[-1]}')

                join_key = daily_df.columns[0]
                merge_query = f"""
                merge into {table} as target
                using updates_{table.split('.')[-1]} as source
                on target.{join_key} = source.{join_key}
                when matched then update set *
                when not matched then insert *
                """

                spark.sql(merge_query)

                spark.sql(f"drop table updates_{table.split('.')[-1]}")
                print(f'Successfully merged data for {table}.')

            except Exception as e:
                print(f'Error while merging fact table, error: {e}')

#Dimension table insertion:
        try:
            from pyspark.sql import functions as F
            table = 'dev.db_connectsphere.dim_user_profile' 
            path = f'{self.incremental_path}/user_profiles'

            daily_df = spark.read\
            .format('csv')\
            .option('header','true')\
            .option('multiLine', 'true')\
            .load(path)\
            .withColumn('loadtime', F.current_timestamp())\
            .withColumn('is_active', F.expr('1'))

            daily_df.createOrReplaceTempView(f'updates_{table.split('.')[-1]}')

            join_key1 = daily_df.columns[0]
            join_key2 = daily_df.columns[1]
            merge_query = f"""
            merge into {table} as target
            using updates_{table.split('.')[-1]} as source
            on target.{join_key1} = source.{join_key1}
            and target.{join_key2} = source.{join_key2}
            and target.is_active = 1
            when matched and source.loadtime > target.loadtime then update set is_active = 0
            when not matched then insert *
            """

            spark.sql(merge_query)

            spark.sql(f"drop table updates_{table.split('.')[-1]}")
            print(f'Successfully merged data for {table}.')

        except Exception as e:
            print(f'Error while merging dimensions table, error: {e}')

# COMMAND ----------

jobs = Daily_job()
jobs.ingest_data()