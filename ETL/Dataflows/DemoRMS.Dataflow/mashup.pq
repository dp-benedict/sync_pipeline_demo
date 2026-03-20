[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared BRONZE_TarifrechnerGas = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "fc294c47-40b8-4573-a523-359c63157b56"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "35f7e5b8-0e8e-4518-a998-c1e140176864"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "DemoDaten"]}[Content],
  #"Navigation 5" = #"Navigation 4"{[Name = "TarifrechnerAbschlüsseGas.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 5", [Delimiter = ";", Columns = 14, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Zählernummer", Int64.Type}, {"Marktlokation", Int64.Type}, {"AngegebenerVerbrauch", Int64.Type}, {"Abschlussdatum", type date}, {"Belieferungsbeginn", type date}, {"Tarif", type text}, {"Vertragslaufzeit", type text}, {"Strasse", type text}, {"PLZ", Int64.Type}, {"Ort", type text}, {"Vorname", type text}, {"Nachname", type text}, {"Grundpreis", Int64.Type}, {"Arbeitspreis", Int64.Type}}),
  #"Add column" = Table.AddColumn(#"Changed column type", "Jahreskosten", each [AngegebenerVerbrauch] * [Arbeitspreis] + [Grundpreis] * 12, type number)
in
  #"Add column";
[BindToDefaultDestination = true]
shared BRONZE_TarifrechnerStrom = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "fc294c47-40b8-4573-a523-359c63157b56"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "35f7e5b8-0e8e-4518-a998-c1e140176864"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "DemoDaten"]}[Content],
  #"Navigation 5" = #"Navigation 4"{[Name = "TarifrechnerAbschlüsseStrom.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 5", [Delimiter = ";", Columns = 14, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Zählernummer", Int64.Type}, {"Marktlokation", Int64.Type}, {"AngegebenerVerbrauch", Int64.Type}, {"Abschlussdatum", type date}, {"Belieferungsbeginn", type date}, {"Tarif", type text}, {"Vertragslaufzeit", type text}, {"Strasse", type text}, {"PLZ", Int64.Type}, {"Ort", type text}, {"Vorname", type text}, {"Nachname", type text}, {"Grundpreis", Int64.Type}, {"Arbeitspreis", Int64.Type}}),
  #"Add column" = Table.AddColumn(#"Changed column type", "Jahreskosten", each [AngegebenerVerbrauch] * [Arbeitspreis] + [Grundpreis] * 12, type number)
in
  #"Add column";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "fc294c47-40b8-4573-a523-359c63157b56"]}[Data]{[lakehouseId = "35f7e5b8-0e8e-4518-a998-c1e140176864"]}[Data];
