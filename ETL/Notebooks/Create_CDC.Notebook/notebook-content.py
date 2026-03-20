# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a0c09f33-4770-4f7c-9511-a517d2c7924e",
# META       "default_lakehouse_name": "BronzeDemoLakehouse",
# META       "default_lakehouse_workspace_id": "fc294c47-40b8-4573-a523-359c63157b56",
# META       "known_lakehouses": [
# META         {
# META           "id": "a0c09f33-4770-4f7c-9511-a517d2c7924e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from delta.tables import DeltaTable

# Verbindung zum Delta Table
delta_table = DeltaTable.forPath(spark, "abfss://Demo_Fabric_ETL@onelake.dfs.fabric.microsoft.com/BronzeDemoLakehouse.Lakehouse/Tables/Transaktionen_last_30_days")

# Versionshistorie abrufen
history_df = delta_table.history()

# Anzeigen
display(history_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM Transaktionen_last_30_days

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC MERGE INTO test_transaktionshistorie as target
# MAGIC USING Transaktionen_last_30_days as source
# MAGIC ON target.TransaktionsID = source.TransaktionsID
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET
# MAGIC         target.Transaktionsdatum = source.Transaktionsdatum,
# MAGIC         target.Menge = source.Menge,
# MAGIC         target.Preis = source.Preis
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (TransaktionsID, Transaktionsdatum, Menge, Preis)
# MAGIC     VALUES (source.TransaktionsID, source.Transaktionsdatum, source.Menge, source.Preis)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

# Verbindung zum Delta Table
delta_table = DeltaTable.forPath(spark, "abfss://Demo_Fabric_ETL@onelake.dfs.fabric.microsoft.com/BronzeDemoLakehouse.Lakehouse/Tables/test_transaktionshistorie")

# Versionshistorie abrufen
history_df = delta_table.history()

# Anzeigen
display(history_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_old = spark.read.format("delta").option("versionAsOf", 0).load("abfss://Demo_Fabric_ETL@onelake.dfs.fabric.microsoft.com/BronzeDemoLakehouse.Lakehouse/Tables/test_transaktionshistorie")
display(df_old)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM test_transaktionshistorie

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Pfade zu den Delta-Tabellen im Lakehouse
table_source_path = "abfss://Demo_Fabric_ETL@onelake.dfs.fabric.microsoft.com/BronzeDemoLakehouse.Lakehouse/Tables/Transaktionen_last_30_days"
table_target_path = "abfss://Demo_Fabric_ETL@onelake.dfs.fabric.microsoft.com/BronzeDemoLakehouse.Lakehouse/Tables/test_transaktionshistorie"

# Lade die Quell-Tabelle (Transaktionen der letzten 30 Tage)
df_source = spark.read.format("delta").load(table_source_path)

# Prüfen, ob die Ziel-Tabelle existiert
try:
    delta_target = DeltaTable.forPath(spark, table_target_path)
    df_target = spark.read.format("delta").load(table_target_path)
except Exception as e:
    print("Ziel-Tabelle existiert nicht – wird jetzt mit dem Schema von Quelle erstellt.")
    df_source.write.format("delta").mode("overwrite").save(table_target_path)
    delta_target = DeltaTable.forPath(spark, table_target_path)
    df_target = spark.read.format("delta").load(table_target_path)

# Prüfe, ob neue Spalten existieren
new_columns = set(df_source.columns) - set(df_target.columns)

# Falls neue Spalten existieren, füge sie im target hinzu
if new_columns:
    print(f"Neue Spalten erkannt: {new_columns}. Füge sie zur Historie hinzu.")
    for col_name in new_columns:
        spark.sql(f"ALTER TABLE delta.`{table_target_path}` ADD COLUMNS ({col_name} STRING)")

    # Nach dem Schema-Update muss df_target erneut geladen werden
    df_target = spark.read.format("delta").load(table_target_path)

# Baue das MERGE dynamisch (inkl. neuer Spalten)
update_set = {col_name: col(f"source.{col_name}") for col_name in df_source.columns if col_name in df_target.columns}
insert_values = {col_name: col(f"source.{col_name}") for col_name in df_source.columns}

# Führe das MERGE durch
delta_target.alias("target").merge(
    df_source.alias("source"),
    "target.TransaktionsID = source.TransaktionsID"
).whenMatchedUpdate(set=update_set
).whenNotMatchedInsert(values=insert_values
).execute()

print("MERGE abgeschlossen – inklusive neuer Spalten.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * 
# MAGIC FROM test_transaktionshistorie

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM test_transaktionshistorie WHERE TransaktionsID = 1;
# MAGIC 
# MAGIC SELECT * FROM test_transaktionshistorie

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
