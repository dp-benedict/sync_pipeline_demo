[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared s_branchen2 = let
  Quelle = Lakehouse.Contents(null){[workspaceId = "fc294c47-40b8-4573-a523-359c63157b56"]}[Data]{[lakehouseId = "a0c09f33-4770-4f7c-9511-a517d2c7924e"]}[Data],
  #"Navigation 1" = Quelle{[Id = "raw_branchen", ItemKind = "Table"]}[Data],
  #"Umbenannte Spalten" = 
    Table.RenameColumns(#"Navigation 1", {
      {"Prop_0", "BranchenId"}, 
      {"Prop_1", "Branche"}, 
      {"Prop_2", "Zuordnung"}
    }),
  #"Ersetzter Wert" = 
    Table.ReplaceValue(#"Umbenannte Spalten", each 
      [Zuordnung], 
      each 
        if [Zuordnung] = "K" then "Klinik"
        else if [Zuordnung] = "N" then "Außerklinisch"
        else _, 
      Replacer.ReplaceValue, 
      {"Zuordnung"}
    ),
  #"Geänderter Spaltentyp" = 
    Table.TransformColumnTypes(#"Ersetzter Wert", {
      {"BranchenId", Int64.Type}, 
      {"Zuordnung", type text}
    })
in
  #"Geänderter Spaltentyp";
[BindToDefaultDestination = true]
shared s_produkt_hierarchie2 = let
  Quelle = Lakehouse.Contents(null){[workspaceId = "fc294c47-40b8-4573-a523-359c63157b56"]}[Data]{[lakehouseId = "a0c09f33-4770-4f7c-9511-a517d2c7924e"]}[Data],
  #"Navigation 1" = Quelle{[Id = "raw_umsatzhierarchie", ItemKind = "Table"]}[Data],
  // Hinzufügen einer benutzerdefinierten Spalte "Reihenfolge" basierend auf dem Wert in "LEVEL_1" (Sortierung von Heike Kreinsberger)
  #"Hinzugefügte benutzerdefinierte Spalte" = Table.AddColumn(
      #"Navigation 1", 
      "Reihenfolge", 
      each 
          if [LEVEL_1] = "ECP" then 1 
          else if [LEVEL_1] = "ECPm" then 2 
          else if [LEVEL_1] = "Patient" then 3 
          else if [LEVEL_1] = "EDP" then 4 
          else if [LEVEL_1] = "Dienstleistung" then 5 
          else if [LEVEL_1] = "Schnittstellen" then 6 
          else if [LEVEL_1] = "ViPa" then 7 
          else if [LEVEL_1] = "Digital Sonstiges" then 8 
          else if [LEVEL_1] = "EC-Portal" then 9 
          else if [LEVEL_1] = "Print" then 10 
          else 99
  ),

  // Ändern des Datentyps der Spalten "HIER_ID", "LEVEL_1", "LEVEL_2", "LEVEL_3" und "Reihenfolge"
  #"Geänderter Typ" = Table.TransformColumnTypes(
      #"Hinzugefügte benutzerdefinierte Spalte",
      {
          {"HIER_ID", Int64.Type},
          {"LEVEL_1", type text},
          {"LEVEL_2", type text},
          {"LEVEL_3", type text},
          {"Reihenfolge", Int64.Type}
      }
  ),

  // Umbenennen der Spalten zur besseren Verständlichkeit
  #"Umbenannte Spalten" = Table.RenameColumns(
      #"Geänderter Typ",
      {
          {"LEVEL_1", "Produkt"},
          {"LEVEL_2", "Kategorie"},
          {"LEVEL_3", "Detail"},
          {"HIER_ID", "Produkt ID"}
      }
  ),

  // Ersetzen leerer Werte in den Spalten "Kategorie" und "Detail" durch "N/A"
  #"Ersetzter Wert" = Table.ReplaceValue(
      #"Umbenannte Spalten",
      "",
      "N/A",
      Replacer.ReplaceValue,
      {"Kategorie", "Detail"}
  ),
  #"Ersetzter Wert 1" = Table.ReplaceValue(#"Ersetzter Wert", "b�hr", "buehr", Replacer.ReplaceText, {"Kategorie", "Detail"}),
  #"Ersetzter Wert 2" = Table.ReplaceValue(#"Ersetzter Wert 1", "ms�tze", "msaetze", Replacer.ReplaceText, {"Kategorie", "Detail"}),
  #"Ersetzter Wert 3" = Table.ReplaceValue(#"Ersetzter Wert 2", "B�gen", "Boegen", Replacer.ReplaceText, {"Kategorie", "Detail"}),
  #"Ersetzter Wert 4" = Table.ReplaceValue(#"Ersetzter Wert 3", "�bersetzung", "Uebersetzung", Replacer.ReplaceText, {"Kategorie", "Detail"})
in
  // Ausgabe der final transformierten Tabelle
  #"Ersetzter Wert 4";
[BindToDefaultDestination = true]
shared s_calendar2 = let
    /*
    ****This Calendar was created and provided by Avi Singh****
    ****This can be freely shared as long as this text comment is retained.****
    http://www.youtube.com/PowerBIPro
    www.LearnPowerBI.com by Avi Singh
    */
    #"LearnPowerBI.com by Avi Singh" = 1,
    StartDate = #date(2015, 1, 1),
    EndDate = Date.EndOfYear(DateTime.Date(DateTime.FixedLocalNow())) /*was "#date(2017, 1, 1)" Updated on 201802027: hard Coded End of Year caused some formulas to break, switching to dynamic date*/,
    //Used for 'Offset' Column calculations, you may Hard code CurrentDate for testing e.g. #date(2017,9,1)
    CurrentDate = DateTime.Date(DateTime.FixedLocalNow()),
    // Specify the last month in your Fiscal Year, e.g. if June is the last month of your Fiscal Year, specify 6
    FiscalYearEndMonth = 6,
    #"==SET PARAMETERS ABOVE==" = 1,
    #"==Build Date Column==" = #"==SET PARAMETERS ABOVE==",
    ListDates = List.Dates(StartDate, Number.From(EndDate - StartDate)+1, #duration(1,0,0,0)),
    #"Converted to Table" = Table.FromList(ListDates, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Renamed Columns as Date" = Table.RenameColumns(#"Converted to Table",{{"Column1", "Date"}}),
    // As far as Power BI is concerned, the 'Date' column is all that is needed :-) But we will continue and add a few Human-Friendly Columns
    #"Changed Type to Date" = Table.TransformColumnTypes(#"Renamed Columns as Date",{{"Date", type date}}),
    #"==Add Calendar Columns==" = #"Changed Type to Date",
    #"Added Calendar MonthNum" = Table.AddColumn(#"==Add Calendar Columns==", "MonthNum", each Date.Month([Date]), Int64.Type),
    #"Added Month Name" = Table.AddColumn(#"Added Calendar MonthNum", "Month", each Text.Start(Date.MonthName([Date]),3), type text),
    #"Added Month Name Long" = Table.AddColumn(#"Added Month Name", "MonthLong", each Date.MonthName([Date]), type text),
    #"Added Calendar Quarter" = Table.AddColumn(#"Added Month Name Long", "Quarter", each "Q" & Text.From(Date.QuarterOfYear([Date]))),
    #"Added Calendar Year" = Table.AddColumn(#"Added Calendar Quarter", "Year", each Date.Year([Date]), Int64.Type),
    #"==Add Fiscal Calendar Columns==" = #"Added Calendar Year",
    #"Added FiscalMonthNum" = Table.AddColumn(#"==Add Fiscal Calendar Columns==", "FiscalMonthNum", each if [MonthNum] > FiscalYearEndMonth
then [MonthNum] - FiscalYearEndMonth
else [MonthNum] + (12 - FiscalYearEndMonth), type number),
    #"Added FiscalMonth Name" = Table.AddColumn(#"Added FiscalMonthNum", "FiscalMonth", each [Month]),
    #"Added FiscalMonth Name Long" = Table.AddColumn(#"Added FiscalMonth Name", "FiscalMonthLong", each [MonthLong]),
    #"Added FiscalQuarter" = Table.AddColumn(#"Added FiscalMonth Name Long", "FiscalQuarter", each "FQ" & Text.From(Number.RoundUp([FiscalMonthNum] / 3,0))),
    #"Added FiscalYear" = Table.AddColumn(#"Added FiscalQuarter", "FiscalYear", each "FY" & 
Text.End(
  Text.From(
    if [MonthNum] > FiscalYearEndMonth
    then [Year] + 1
    else [Year]
  )
  , 2
)),

    #"==Add Calendar Date Offset Columns==" = #"Added FiscalYear",
    // Can be used to for example to show the past 3 months(CurMonthOffset = 0, -1, -2)
    #"Added CurMonthOffset" = Table.AddColumn(#"==Add Calendar Date Offset Columns==", "CurMonthOffset", each ( Date.Year([Date]) - Date.Year(CurrentDate) ) * 12
+ Date.Month([Date]) - Date.Month(CurrentDate), Int64.Type),
    // Can be used to for example to show the past 3 quarters (CurQuarterOffset = 0, -1, -2)
    #"Added CurQuarterOffset" = Table.AddColumn(#"Added CurMonthOffset", "CurQuarterOffset", each /*Year Difference*/
       ( Date.Year([Date]) - Date.Year(CurrentDate) )*4
       /*Quarter Difference*/
      + Number.RoundUp(Date.Month([Date]) / 3) 
      - Number.RoundUp(Date.Month(CurrentDate) / 3),
Int64.Type),
    // Can be used to for example to show the past 3 years (CurYearOffset = 0, -1, -2)
    #"Added CurYearOffset" = Table.AddColumn(#"Added CurQuarterOffset", "CurYearOffset", each Date.Year([Date]) - Date.Year(CurrentDate), Int64.Type),
    // Can be used to for example filter out all future dates
    #"Added FutureDate Flag" = Table.AddColumn(#"Added CurYearOffset", "FutureDate", each if [Date] > CurrentDate then "Future" else "Past" ),
    // FiscalYearOffset is the only Offset that is different.
    // FiscalQuarterOffset = is same as CurQuarterOffset
    // FiscalMonthOffset = is same as CurMonthOffset
    #"==Add FiscalYearOffset==" = #"Added FutureDate Flag",
    #"Filtered Rows to CurrentDate" = Table.SelectRows(#"==Add FiscalYearOffset==", each ([Date] = CurrentDate)),
    CurrentFiscalYear = #"Filtered Rows to CurrentDate"{0}[FiscalYear],
    #"Continue...Orig Table" = #"==Add FiscalYearOffset==",
    #"Added CurFiscalYearOffset" = Table.AddColumn(#"Continue...Orig Table", "CurFiscalYearOffset", each Number.From(Text.Range([FiscalYear],2,2)) - 
Number.From(Text.Range(CurrentFiscalYear,2,2))
/*Extract the numerical portion, e.g. FY18 = 18*/),
    #"==Add General Columns==" = #"Added CurFiscalYearOffset",
    // Used as 'Sort by Column' for MonthYear columns
    #"Added MonthYearNum" = Table.AddColumn(#"==Add General Columns==", "MonthYearNum", each [Year]*100 + [MonthNum] /*e.g. Sep-2016 would become 201609*/, Int64.Type),
    #"Added MonthYear" = Table.AddColumn(#"Added MonthYearNum", "MonthYear", each [Month] & "-" & Text.End(Text.From([Year]),2)),
    #"Added MonthYearLong" = Table.AddColumn(#"Added MonthYear", "MonthYearLong", each [Month] & "-" & Text.From([Year])),
    #"Added WeekdayNum" = Table.AddColumn(#"Added MonthYearLong", "WeekdayNum", each Date.DayOfWeek([Date]), Int64.Type),
    #"Added Weekday Name" = Table.AddColumn(#"Added WeekdayNum", "Weekday", each Text.Start(Date.DayOfWeekName([Date]),3), type text),
    #"Added WeekdayWeekend" = Table.AddColumn(#"Added Weekday Name", "WeekdayWeekend", each if [WeekdayNum] = 0 or [WeekdayNum] = 6
then "Weekend"
else "Weekday"),
    #"==Improve Ultimate Table" = #"Added WeekdayWeekend",
    #"----Add WeekSequenceNum----" = #"==Improve Ultimate Table",
    #"Filtered Rows Sundays Only (Start of Week)" = Table.SelectRows(#"----Add WeekSequenceNum----", each ([WeekdayNum] = 0)),
    #"Added Index WeekSequenceNum" = Table.AddIndexColumn(#"Filtered Rows Sundays Only (Start of Week)", "WeekSequenceNum", 2, 1),
    #"Merged Queries Ultimate Table to WeekSequenceNum" = Table.NestedJoin(#"==Improve Ultimate Table",{"Date"},#"Added Index WeekSequenceNum",{"Date"},"Added Index WeekNum",JoinKind.LeftOuter),
    #"Expanded Added Index WeekNum" = Table.ExpandTableColumn(#"Merged Queries Ultimate Table to WeekSequenceNum", "Added Index WeekNum", {"WeekSequenceNum"}, {"WeekSequenceNum"}),
    // somehow it ends up being unsorted after Expand Column, should not matter for the end table, but makes it harder to debug and check everything is correct. Thus sorting it.
    #"ReSorted Rows by Date" = Table.Sort(#"Expanded Added Index WeekNum",{{"Date", Order.Ascending}}),
    #"Filled Down WeekSequenceNum" = Table.FillDown(#"ReSorted Rows by Date",{"WeekSequenceNum"}),
    #"Replaced Value WeekSequenceNum null with 1" = Table.ReplaceValue(#"Filled Down WeekSequenceNum",null,1,Replacer.ReplaceValue,{"WeekSequenceNum"}),
    #"Inserted Start of Week (WeekDate)" = Table.AddColumn(#"Replaced Value WeekSequenceNum null with 1", "WeekDate", each Date.StartOfWeek([Date]), type date),
    // Added 2019-Oct
    #"Inserted Week of Year" = Table.AddColumn(#"Inserted Start of Week (WeekDate)", "Week of Year", each Date.WeekOfYear([Date]), Int64.Type),
    #"----WeekSequenceNum Complete----" = #"Inserted Week of Year",
    Current_WeekSequenceNum = #"----WeekSequenceNum Complete----"{[Date = CurrentDate]}?[WeekSequenceNum],
    #"Added Custom CurWeekOffset" = Table.AddColumn(#"----WeekSequenceNum Complete----", "CurWeekOffset", each [WeekSequenceNum] - Current_WeekSequenceNum, Int64.Type),
    // Adding a DayofYear 1 to 365
    // And YTD, QTD, MTD Columns (can help with showing YTD Numbers across multiple years)
    #"==Updates 2019-Feb DayofYear and YTD QTD MTD Columns" = #"Added Custom CurWeekOffset",
    // This maybe useful in some DAX Calculations
    #"Inserted Day of Year" = Table.AddColumn(#"==Updates 2019-Feb DayofYear and YTD QTD MTD Columns", "Day of Year", each Date.DayOfYear([Date]), Int64.Type),
    #"Added Flag_YTD" = Table.AddColumn(#"Inserted Day of Year", "Flag_YTD", each if Date.DayOfYear([Date]) <= Date.DayOfYear(CurrentDate)
 then "YTD"
 else null),
    #"Added Flag_MTD" = Table.AddColumn(#"Added Flag_YTD", "Flag_MTD", each if Date.Day([Date]) <= Date.Day(CurrentDate)
 then "MTD"
 else null),
    #"Added Flag_QTD" = Table.AddColumn(#"Added Flag_MTD", "Flag_QTD", each //Compare Month Number in Quarter (1,2,3) for [Date] and CurrentDate
if Number.Mod(Date.Month([Date])-1, 3) + 1
<= Number.Mod(Date.Month(CurrentDate)-1, 3) + 1
then "QTD"
else null),
    #"==Update 2019-Mar CurrentDatOffset" = #"Added Flag_QTD",
    #"Added CurrentDayOffset" = Table.AddColumn(#"==Update 2019-Mar CurrentDatOffset", "CurrentDayOffset", each [Date] - CurrentDate),
    #"Changed Type1" = Table.TransformColumnTypes(#"Added CurrentDayOffset", {{"CurrentDayOffset", Int64.Type}, {"Quarter", type text}, {"FiscalMonthNum", Int64.Type}, {"FiscalMonth", type text}, {"FiscalMonthLong", type text}, {"FiscalQuarter", type text}, {"FiscalYear", type text}, {"MonthYearLong", type text}, {"WeekdayWeekend", type text}, {"WeekSequenceNum", Int64.Type}, {"Flag_YTD", type text}, {"Flag_MTD", type text}, {"Flag_QTD", type text}, {"FutureDate", type text}, {"CurFiscalYearOffset", Int64.Type}, {"MonthYear", type text}})
in
    #"Changed Type1";
[BindToDefaultDestination = true]
shared s_geschaeftspartner2 = let
  Quelle = Lakehouse.Contents(null){[workspaceId = "fc294c47-40b8-4573-a523-359c63157b56"]}[Data]{[lakehouseId = "a0c09f33-4770-4f7c-9511-a517d2c7924e"]}[Data],
  #"Navigation 1" = Quelle{[Id = "raw_geschaeftspartner", ItemKind = "Table"]}[Data],
  #"Entfernte Spalten" = Table.RemoveColumns(#"Navigation 1", {"REGION"}),
  #"Geänderter Spaltentyp" = 
    Table.TransformColumnTypes(#"Entfernte Spalten", 
      {
        {"PARTNER", Int64.Type},
        {"NAME_ORG1", type text},
        {"NAME_ORG2", type text},
        {"NAME_ORG3", type text},
        {"NAME_ORG4", type text},
        {"IND_SECTOR", Int64.Type},
        {"STREET", type text},
        {"HOUSE_NO", type text},
        {"CITY", type text},
        {"POST_CODE", type text},
        {"COUNTRY", type text},
        {"POST_CODE_PO_BOX", type text},
        {"PO_BOX", type text},
        {"TEL_NOTEL", type text},
        {"TEL_NOMOB", type text},
        {"E_MAILSMT", type text},
        {"WS_MAIL", type text},
        {"WS_FAX", type text},
        {"WS_POST", type text},
        {"WS_TELEFON", type text},
        {"MA_ANZ_BETTEN", type text},
        {"MA_KIS", type text},
        {"MA_WLAN", type text},
        {"MA_ABC", type text},
        {"MA_ANZ_ARZT", Int64.Type},
        {"MA_BETREUUNG", type text},
        {"MA_ANZ_DOK", type text},
        {"MA_FALL", type text},
        {"MA_INFRASTRUKTUR", type text},
        {"MA_PORTALKUNDE", type text},
        {"MA_REF_KUN", type text},
        {"MA_SCHNITTST", type text},
        {"MA_BROWSER", type text},
        {"MA_CLIENT", type text},
        {"MA_DATENBANK", Int64.Type},
        {"MA_ECP", type text},
        {"MA_JAVA", type text},
        {"MA_LIZ_SEQ", Int64.Type},
        {"MA_OFFLINE", type text},
        {"MA_SERVER", type text},
        {"MA_AIRPRINT", type text},
        {"MA_TERMINAL", type text},
        {"MA_ENTLASS", type text},
        {"MA_KOM_SERVER", type text},
        {"MA_MDM", type text},
        {"MA_MOBIL", type text},
        {"MA_PAT_PORTAL", type text}
        //{"Branche", type text},
        //{"Zuordnung", type text},
        //{"Bundesland", type text}
      }),
    #"Umbenannte Spalten" = Table.RenameColumns(
        #"Geänderter Spaltentyp",
        {
            {"PARTNER", "Geschäftspartner ID"},
            {"NAME_ORG1", "Name 1"},
            {"NAME_ORG2", "Name 2"},
            {"NAME_ORG3", "Name 3"},
            {"NAME_ORG4", "Name 4"},
            {"IND_SECTOR", "Branchencode"},
            {"STREET", "Straße"},
            {"HOUSE_NO", "Hausnummer"},
            {"POST_CODE", "Postleitzahl"},
            {"CITY", "Ort"},
            {"POST_CODE_PO_BOX", "Plz. Postfach"},
            {"PO_BOX", "Postfach"},
            {"TEL_NOTEL", "Telefon"},
            {"TEL_NOMOB", "Mobiltelefon"},
            {"E_MAILSMT", "E-Mail"},
            {"WS_MAIL", "Werbesperre Mail"},
            {"WS_FAX", "Werbesperre Fax"},
            {"WS_POST", "Werbesperre Post"},
            {"WS_TELEFON", "Werbesperre Telefon"},
            {"MA_ANZ_BETTEN", "Anzahl Betten"},
            {"MA_KIS", "KIS/AIS/PVS"},
            {"MA_WLAN", "WLAN"},
            {"MA_ABC", "ABC Klassifizierung"},
            {"MA_ANZ_ARZT", "Anzahl Ärzte"},
            {"MA_BETREUUNG", "Betreuung"},
            {"MA_ANZ_DOK", "Anzahl EDP Dok."},
            {"MA_FALL", "Fallzahl"},
            {"MA_INFRASTRUKTUR", "Infrastruktur"},
            {"MA_PORTALKUNDE", "Portalkunde"},
            {"MA_REF_KUN", "Referenzkunde"},
            {"MA_SCHNITTST", "Schnittstellentechnologie"},
            {"MA_BROWSER", "Browser"},
            {"MA_CLIENT", "Clientplattform"},
            {"MA_DATENBANK", "Datenbank"},
            {"MA_ECP", "ECP"},
            {"MA_JAVA", "JAVA"},
            {"MA_LIZ_SEQ", "Lizenzsequenz"},
            {"MA_OFFLINE", "Offline"},
            {"MA_SERVER", "Server"},
            {"MA_AIRPRINT", "AirPrint Drucker"},
            {"MA_TERMINAL", "Bedside Terminals"},
            {"MA_ENTLASS", "Entlassmgmt System"},
            {"MA_KOM_SERVER", "Kommunikationsserver"},
            {"MA_MDM", "MDM-Lösung"},
            {"MA_MOBIL", "Mobilgeräte"},
            {"MA_PAT_PORTAL", "Patienten-Portal"},
            {"COUNTRY", "Ländercode"}
        }
    )
in
  #"Umbenannte Spalten";
[BindToDefaultDestination = true]
shared s_postleitzahlen2 = let
  Quelle = Lakehouse.Contents(null){[workspaceId = "fc294c47-40b8-4573-a523-359c63157b56"]}[Data]{[lakehouseId = "a0c09f33-4770-4f7c-9511-a517d2c7924e"]}[Data],
  Navigation = Quelle{[Id = "raw_postleitzahlen", ItemKind = "Table"]}[Data],
  #"Geänderter Spaltentyp" = Table.TransformColumnTypes(Navigation, {{"postal_code", type text}}),
  #"Umbenannte Spalten" = Table.RenameColumns(#"Geänderter Spaltentyp", {{"country_code", "Ländercode"}, {"postal_code", "Postleitzahl"}, {"admin_name1", "Bundesland"}, {"place_name", "Ort"}, {"latitude", "Breitengrad"}, {"longitude", "Laengengrad"}, {"coordinates", "Koordinaten"}})
in
  #"Umbenannte Spalten";
[BindToDefaultDestination = true]
shared s_heads2 = let
  Quelle = Lakehouse.Contents(null){[workspaceId = "fc294c47-40b8-4573-a523-359c63157b56"]}[Data]{[lakehouseId = "a0c09f33-4770-4f7c-9511-a517d2c7924e"]}[Data],
  #"Navigation 1" = Quelle{[Id = "raw_head", ItemKind = "Table"]}[Data],
  #"Doppelte Werte entfernt" = Table.Distinct(#"Navigation 1", {"BILL_DOC_ID"}),
  #"Geänderter Spaltentyp" = Table.TransformColumnTypes(#"Doppelte Werte entfernt", {{"BILL_DATE", type text}}),
  // Erfordert extra Schritt und kann nicht mit vorherigem Schritt kombiniert werden.
  #"Datum formatieren" = Table.TransformColumnTypes(#"Geänderter Spaltentyp", {{"BILL_DATE", type date}}),
  // Umbenennen der Spalten in benutzerdefinierte Namen
  #"Umbenannte Spalten" = Table.RenameColumns(
      #"Datum formatieren",
      {
          {"BILL_DOC_ID", "Faktura Nr."}, 
          {"CANCEL_FLAG", "Stornokennzeichen"}, 
          {"PAYER", "Regulierer"}, 
          {"SHIP_TO", "Warenempfänger"}, 
          {"SOLD_TO", "Auftragnehmer/Lizenznehmer"}, 
          {"DOC_CURRENCY", "Belegwährung"},
          {"BILL_DATE", "Datum"}
      }
  )
in
  // Rückgabe der umbenannten Spalten
  #"Umbenannte Spalten";
shared s_items2 = let
  Quelle = Lakehouse.Contents(null){[workspaceId = "fc294c47-40b8-4573-a523-359c63157b56"]}[Data]{[lakehouseId = "a0c09f33-4770-4f7c-9511-a517d2c7924e"]}[Data],
  #"Navigation 1" = Quelle{[Id = "raw_items", ItemKind = "Table"]}[Data],
  #"Ersetzter Wert" = Table.ReplaceValue(#"Navigation 1", ".", ",", Replacer.ReplaceText, {"NET_VALUE", "QUANTITY"}),
  #"Ersetzter Wert 1" = Table.ReplaceValue(#"Ersetzter Wert", "-", ":", Replacer.ReplaceText, {"time"}),
  #"Geänderter Spaltentyp" = Table.TransformColumnTypes(#"Ersetzter Wert 1", {{"NET_VALUE", type number}, {"QUANTITY", type number}, {"date", type date}, {"BILL_DOC_ID", Int64.Type}, {"BILL_DOC_ITEM_ID", Int64.Type}, {"HIER_ID", Int64.Type}}),
  #"Umbenannte Spalten" = Table.RenameColumns(
        #"Geänderter Spaltentyp",
        {
            {"BILL_DOC_ID", "Faktura Nr."},
            {"BILL_DOC_ITEM_ID", "Fakturaposition Nr."},
            {"NET_VALUE", "Nettowert in Belegwährung"},
            {"MATERIAL", "Produkt-ID"},
            {"QUANTITY", "Menge"},
            {"QTY_UNIT", "Mengeneinheit"},
            {"DOC_CURRENCY", "Belegwährung"},
            {"HIER_ID", "Produkt ID"}
        }
    )
in
  #"Umbenannte Spalten";
[BindToDefaultDestination = true]
shared s_bildurls2 = let
  Quelle = Lakehouse.Contents(null){[workspaceId = "fc294c47-40b8-4573-a523-359c63157b56"]}[Data]{[lakehouseId = "a0c09f33-4770-4f7c-9511-a517d2c7924e"]}[Data],
  #"Navigation 1" = Quelle{[Id = "raw_bildurls", ItemKind = "Table"]}[Data],
  #"Höher gestufte Header" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Geänderter Spaltentyp" = Table.TransformColumnTypes(#"Höher gestufte Header", {{"Bild", Int64.Type}}, "de")
in
  #"Geänderter Spaltentyp";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "fc294c47-40b8-4573-a523-359c63157b56"]}[Data]{[lakehouseId = "4d3a0e18-7f3a-4d42-9980-a1cf8cee006a"]}[Data];
