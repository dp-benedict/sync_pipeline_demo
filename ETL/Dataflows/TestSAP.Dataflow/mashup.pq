[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Transaktionen_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Transaktionen = let
  Quelle = Csv.Document(Web.Contents("https://datenpioniere-my.sharepoint.com/personal/benedict_altgassen_datenpioniere_de/Documents/Dokumente/Testordner/CDC_Testing_Fabric/Transaktionen.csv"), [Delimiter = ";", QuoteStyle = QuoteStyle.None]),
  #"Höher gestufte Header" = Table.PromoteHeaders(Quelle, [PromoteAllScalars = true]),
  #"Geänderter Spaltentyp" = Table.TransformColumnTypes(#"Höher gestufte Header", {{"TransaktionsID", Int64.Type}, {"Transaktionsdatum", type date}, {"Menge", Int64.Type}, {"Preis", Int64.Type}}, "de")
in
  #"Geänderter Spaltentyp";
shared Transaktionen_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "fc294c47-40b8-4573-a523-359c63157b56"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "a0c09f33-4770-4f7c-9511-a517d2c7924e"]}[Data],
  TableNavigation = Navigation_2{[Id = "Transaktionen_last_30_days", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
