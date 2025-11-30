# Databricks notebook source
dbutils.widgets.text('user_id',"", 'user_id')

# COMMAND ----------

# MAGIC %run ./01-config

# COMMAND ----------

class DeletionReq:
    def __init__(self):
        self.Conf = Config()
        self.catalog = self.Conf.catalog
        self.db_name = self.Conf.db_name
        self._user_id  = dbutils.widgets.get('user_id')


    def delete_user_engagement (self):
        self.table = 'fact_user_engagement'
        query = f"""
                update {self.catalog}.{self.db_name}.{self.table}
                set engaging_user_id = 'DELETED',
                comment_text = 'DELETED'
                where engaging_user_id = '{self._user_id}'
                """
        self.run_delete_query(query)

        
    def delete_user_article (self):
        self.table = 'fact_user_articles'
        query = f"""
                update {self.catalog}.{self.db_name}.{self.table}
                set author_id = 'DELETED',
                article_title = 'DELETED'
                where author_id = '{self._user_id}'
                """
        self.run_delete_query(query)

    def delete_support_tickets (self):
        self.table = 'fact_tickets'
        query = f"""
                update {self.catalog}.{self.db_name}.{self.table}
                set ticket_subject = 'DELETED',
                requester_email = 'DELETED'
                where requester_email in (
                select email from  
                dev.db_connectsphere.dim_user_profile 
                where user_id = '{self._user_id}')
                """
        self.run_delete_query(query)


    def delete_direct_messages (self):
        self.table = 'fact_dms'
        query = f"""
                update {self.catalog}.{self.db_name}.{self.table}
                set sender_id  = 'DELETED',
                message_text = 'DELETED'
                where sender_id = '{self._user_id}'
                """
        self.run_delete_query(query)

        self.table = 'fact_dms'
        query = f"""
                update {self.catalog}.{self.db_name}.{self.table}
                set recipient_id   = 'DELETED'
                where recipient_id = '{self._user_id}'
                """
        self.run_delete_query(query)

        
    def delete_user_profile (self):
        self.table = 'dim_user_profile'
        query = f"""
                Delete from dev.db_connectsphere.dim_user_profile 
                where user_id = '{self._user_id}'
                """
        self.run_delete_query(query)


    def execute_deletion(self):
        print(f'deleting all user data for {self._user_id}...')
        self.delete_user_engagement()
        self.delete_user_article()
        self.delete_support_tickets()
        self.delete_direct_messages()
        self.delete_user_profile()
        print(f'all user data for {self._user_id} has been removed.')
        print('writing records in audit table')
        print(f'completed updates for {self._user_id}...',end='')


    def run_delete_query(self, query):
        self.query = query
        output = spark.sql(self.query)
        row_count =  output.first()['num_affected_rows']
        action = self.query.strip().split()[0].upper()
        self.update_audit_table(row_count, action, self.table)



    def update_audit_table(self, rows, action, table):
        try:
            import uuid
            from datetime import datetime
            from pyspark.sql import Row

            audit_record = Row (
                id = str(uuid.uuid4()),
                action = action,
                timestamp = datetime.now(),
                target_user_id = self._user_id,
                rows_affected = rows,
                table = table
            )

            df = spark.createDataFrame([audit_record])
            df.write.format('delta').mode('append').saveAsTable(f'{self.catalog}.{self.db_name}.audit_log_gdpr')
        
        except Exception as e:
            print(f'No record to delete {e}')


# COMMAND ----------

delete = DeletionReq()
delete.execute_deletion()

# COMMAND ----------

tables = spark.sql('show tables in dev.db_connectsphere').select('tableName').collect()

for table in tables:
    spark.sql(f'VACUUM dev.db_connectsphere.{table[0]} RETAIN 0 HOURS')