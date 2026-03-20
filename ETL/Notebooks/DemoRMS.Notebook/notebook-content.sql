-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "35f7e5b8-0e8e-4518-a998-c1e140176864",
-- META       "default_lakehouse_name": "DemoRMS",
-- META       "default_lakehouse_workspace_id": "fc294c47-40b8-4573-a523-359c63157b56",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "35f7e5b8-0e8e-4518-a998-c1e140176864"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

SELECT * FROM DemoRMS.dbo.BRONZE_TarifrechnerGas LIMIT 10

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Überschrift

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC 
-- MAGIC df = spark.sql("SELECT * FROM DemoRMS.dbo.BRONZE_TarifrechnerGas LIMIT 10")
-- MAGIC display(df)

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }
