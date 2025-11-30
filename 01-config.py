# Databricks notebook source
class Config:
    def __init__(self):
        self.main_path = spark.sql("describe external location `data-connectsphere`").select('url').collect()[0][0]
        self.historical_path = self.main_path + '/historical'
        self.incremental_daily = self.main_path + '/incremental-daily'
        self.catalog = 'dev'
        self.db_name = 'db_connectsphere'