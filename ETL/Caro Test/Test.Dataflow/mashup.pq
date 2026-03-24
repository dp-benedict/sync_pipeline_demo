[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Orders_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Orders = let
  Quelle = SharePoint.Files("https://datenpioniere.sharepoint.com/sites/Consulting", [ApiVersion = 15]),
  #"Gefilterte Zeilen" = Table.SelectRows(Quelle, each ([Name] = "Northwind_Demodaten.xlsx")),
  Navigation = #"Gefilterte Zeilen"{[Name = "Northwind_Demodaten.xlsx", #"Folder Path" = "https://datenpioniere.sharepoint.com/sites/Consulting/Freigegebene Dokumente/_03_Schulungsunterlagen/Werde Power BI Champion/EN/"]}[Content],
  #"Importierte Excel-Arbeitsmappe" = Excel.Workbook(Navigation, null, true),
  #"Navigation 1" = #"Importierte Excel-Arbeitsmappe"{[Item = "Orders", Kind = "Sheet"]}[Data],
  #"Höher gestufte Header" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Geänderter Spaltentyp" = Table.TransformColumnTypes(#"Höher gestufte Header", {{"Order ID", Int64.Type}, {"Customer", type text}, {"Employee", Int64.Type}, {"Order Date", type date}, {"Required Date", type date}, {"Shipped Date", type date}, {"Ship Via", Int64.Type}, {"Freight", type number}, {"Ship Name", type text}, {"Ship Address", type text}, {"Ship City", type text}, {"Ship Region", type text}, {"Ship Postal Code", type text}, {"Ship Country", type text}}, "de")
in
  #"Geänderter Spaltentyp";
shared Employees = let
  Quelle = SharePoint.Files("https://datenpioniere.sharepoint.com/sites/Consulting", [ApiVersion = 15]),
  #"Gefilterte Zeilen" = Table.SelectRows(Quelle, each ([Name] = "Northwind_Demodaten.xlsx")),
  Navigation = #"Gefilterte Zeilen"{[Name = "Northwind_Demodaten.xlsx", #"Folder Path" = "https://datenpioniere.sharepoint.com/sites/Consulting/Freigegebene Dokumente/_03_Schulungsunterlagen/Werde Power BI Champion/EN/"]}[Content],
  #"Importierte Excel-Arbeitsmappe" = Excel.Workbook(Navigation, null, true),
  #"Navigation 1" = #"Importierte Excel-Arbeitsmappe"{[Item = "Employees", Kind = "Sheet"]}[Data],
  #"Entfernte Spalten" = Table.RemoveColumns(#"Navigation 1", {"Column1"}),
  #"Höher gestufte Header" = Table.PromoteHeaders(#"Entfernte Spalten", [PromoteAllScalars = true]),
  #"Geänderter Spaltentyp" = Table.TransformColumnTypes(#"Höher gestufte Header", {{"Last Name", type text}, {"First Name", type text}, {"Title", type text}, {"Title Of Courtesy", type text}, {"Birth Date", type date}, {"Hire Date", type date}, {"Address", type text}, {"City", type text}, {"Region", type text}, {"Postal Code", type text}, {"Country", type text}, {"Home Phone", type text}, {"Extension", Int64.Type}, {"Notes", type text}, {"Reports To", type text}}, "de")
in
  #"Geänderter Spaltentyp";
shared Orders_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "fc294c47-40b8-4573-a523-359c63157b56"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "dcab2378-0f93-4761-ae00-76b3faa4b2d5"]}[Data],
  TableNavigation = Navigation_2{[Id = "Orders", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
