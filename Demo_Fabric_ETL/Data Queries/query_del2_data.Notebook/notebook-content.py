# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a0c09f33-4770-4f7c-9511-a517d2c7924e",
# META       "default_lakehouse_name": "BronzeDemoLakehouse",
# META       "default_lakehouse_workspace_id": "fc294c47-40b8-4573-a523-359c63157b56"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bibliotheken importieren

# CELL ********************

from bs4 import BeautifulSoup
import requests
import pandas as pd


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# url = "https://www.del-2.org/tabelle/"
# #https://www.del-2.org/stats/scorer/?round=142&club=&report={report}
# #reports = ["", "bio", "timeonice", "shots", "penaltyshots", "penalty"]
# #https://www.del-2.org/tabelle/
# 
# response = requests.get(url)
# 
# soup = BeautifulSoup(response.text, "html.parser")
# 
# table = soup.find("table", {"class": "table table-sm table-hover table-striped"})
# 
# header = [ele.text.strip() for ele in table.find_all("th")]
# header = [ele.replace('.', '_').replace('/', '_') for ele in header]
# header = [f"Col_{index}" if ele == "" else ele for index, ele in enumerate(header)]
# 
# df = pd.DataFrame(columns=header)
# header


# MARKDOWN ********************

# # Ligadaten

# CELL ********************

#Ligadaten abfragen

url = "https://www.del-2.org/tabelle/"

response = requests.get(url)

soup = BeautifulSoup(response.text, "html.parser")

table = soup.find("table", {"class": "table table-sm table-hover table-striped"})

header = [ele.text.strip() for ele in table.find_all("th")]
header = [ele.replace('.', '_').replace('+/-','Diff').replace('/', '_').replace("'", "min").replace('%','_pct').replace(' ','_') for ele in header]
header = [f"Col_{index}" if ele == "" else ele for index, ele in enumerate(header)]

df = pd.DataFrame(columns=header)

rows = table.find_all("tr")
for row in rows[1:]:
    cols = row.find_all("td")
    cols = [ele.text.strip() for ele in cols]

    length = len(df)
    df.loc[length] = cols

df["Abfragedatum"] = pd.Timestamp.now().strftime("%Y-%m-%d")

# Ligadaten in Lake speichern

date_of_request = pd.Timestamp.now().strftime("%Y-%m-%d")
filename = f"raw_ligatabelle_{date_of_request}.csv"
p = r"abfss://Demo_Fabric_ETL@onelake.dfs.fabric.microsoft.com/BronzeDemoLakehouse.Lakehouse/Files/DEL2_Eishockey_Data/Ligatabelle/"
df.to_csv(p + filename, index=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Spielerstatistiken

# CELL ********************

# Liste der Berichte, die abgerufen werden sollen
reports = ["", "bio", "timeonice", "shots", "penaltyshots", "penalty"]

# Schleife über die Berichte
for report in reports:
    url = f"https://www.del-2.org/stats/scorer/?round=142&club=&report={report}"

    response = requests.get(url)

    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find(
        "table",
        {"class": "table table-bordered table-sm table-striped table-hover small"},
    )

    header = [ele.text.strip() for ele in table.find_all("th")]
    header = [ele.replace('.', '_').replace('+/-','Diff').replace('/', '_').replace("'", "min").replace('%','_pct').replace(' ','_') for ele in header]
    header = [f"Col_{index}" if ele == "" else ele for index, ele in enumerate(header)]

    df = pd.DataFrame(columns=header)
    df = df.rename(columns={"#": "Rank"})

    rows = table.find_all("tr")
    for row in rows[1:]:
        cols = row.find_all("td")
        cols = [ele.text.strip() for ele in cols]
        length = len(df)
        df.loc[length] = cols

    df = df.drop(columns=["Rank"])
    df["Abfragedatum"] = pd.Timestamp.now().strftime("%Y-%m-%d")

    # Save to CSV
    date_of_request = pd.Timestamp.now().strftime("%Y-%m-%d")
    
    if report == "":
        filename = f"raw_spielerstatistiken_{date_of_request}.csv"
        p = r"abfss://Demo_Fabric_ETL@onelake.dfs.fabric.microsoft.com/BronzeDemoLakehouse.Lakehouse/Files/DEL2_Eishockey_Data/Spielerstatistiken/uebersicht/"
    else:
        filename = f"raw_spielerstatistiken_{report}_{date_of_request}.csv"
        p = rf"abfss://Demo_Fabric_ETL@onelake.dfs.fabric.microsoft.com/BronzeDemoLakehouse.Lakehouse/Files/DEL2_Eishockey_Data/Spielerstatistiken/{report}/"

    
    #filename = f"raw_spielerstatistiken_{report}_{date_of_request}.csv"
    #p = r"abfss://Demo_Fabric_ETL@onelake.dfs.fabric.microsoft.com/BronzeDemoLakehouse.Lakehouse/Files/DEL2_Eishockey_Data/Spielerstatistiken/"
    df.to_csv(p + filename, index=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
