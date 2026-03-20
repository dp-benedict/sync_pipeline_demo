[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared Kalender = let
  Source = Csv.Document(Web.Contents("https://datenpioniere-my.sharepoint.com/personal/benedict_altgassen_datenpioniere_de/Documents/Apps/Microsoft Power Query/Uploaded Files/Kalender.csv"), [Delimiter = ";", Columns = 14, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Datum", type date}, {"Jahr", Int64.Type}, {"Quartal", type text}, {"Monat", Int64.Type}, {"Monatsname", type text}, {"MonatKurz", type text}, {"Tag", Int64.Type}, {"TagImJahr", Int64.Type}, {"WochentagNr", Int64.Type}, {"WochentagName", type text}, {"KalenderwocheISO", Int64.Type}, {"IstWochenende", Int64.Type}, {"IstFeiertagDE", Int64.Type}, {"FeiertagName", type text}})
in
  #"Changed column type";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "fc294c47-40b8-4573-a523-359c63157b56"]}[Data]{[lakehouseId = "a0c09f33-4770-4f7c-9511-a517d2c7924e"]}[Data];
[BindToDefaultDestination = true]
shared #"TarifrechnerAbschlüsseGas 1" = let
  Source = Csv.Document(Web.Contents("https://datenpioniere-my.sharepoint.com/personal/benedict_altgassen_datenpioniere_de/Documents/Apps/Microsoft Power Query/Uploaded Files/TarifrechnerAbschlüsseGas 1.csv"), [Delimiter = ";", Columns = 14, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Zählernummer", Int64.Type}, {"Marktlokation", Int64.Type}, {"AngegebenerVerbrauch", Int64.Type}, {"Abschlussdatum", type date}, {"Belieferungsbeginn", type date}, {"Tarif", type text}, {"Vertragslaufzeit", type text}, {"Strasse", type text}, {"PLZ", Int64.Type}, {"Ort", type text}, {"Vorname", type text}, {"Nachname", type text}, {"Grundpreis", Int64.Type}, {"Arbeitspreis", Int64.Type}}),
  #"Divided column" = Table.TransformColumns(#"Changed column type", {{"Grundpreis", each _ / 100, type number}}),
  #"Divided column 1" = Table.TransformColumns(#"Divided column", {{"Arbeitspreis", each _ / 10000, type number}}),
  #"Added custom" = Table.AddColumn(#"Divided column 1", "Energie", each "Gas"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom", {{"Energie", type text}})
in
  #"Changed column type 1";
[BindToDefaultDestination = true]
shared TarifrechnerAbschlüsse = let
  Source = Csv.Document(Web.Contents("https://datenpioniere-my.sharepoint.com/personal/benedict_altgassen_datenpioniere_de/Documents/Apps/Microsoft Power Query/Uploaded Files/TarifrechnerAbschlüsseStrom.csv"), [Delimiter = ";", Columns = 14, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Zählernummer", Int64.Type}, {"Marktlokation", Int64.Type}, {"AngegebenerVerbrauch", Int64.Type}, {"Abschlussdatum", type date}, {"Belieferungsbeginn", type date}, {"Tarif", type text}, {"Vertragslaufzeit", type text}, {"Strasse", type text}, {"PLZ", Int64.Type}, {"Ort", type text}, {"Vorname", type text}, {"Nachname", type text}, {"Grundpreis", Int64.Type}, {"Arbeitspreis", Int64.Type}}),
  #"Added custom" = Table.AddColumn(#"Changed column type", "Energie", each "Strom"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom", {{"Energie", type text}}),
  #"Divided column" = Table.TransformColumns(#"Changed column type 1", {{"Grundpreis", each _ / 100, type number}}),
  #"Divided column 1" = Table.TransformColumns(#"Divided column", {{"Arbeitspreis", each _ / 10000, type number}}),
  #"Appended query" = Table.Combine({#"Divided column 1", #"TarifrechnerAbschlüsseGas 1"})
in
  #"Appended query";
