CREATE TABLE [dbo].[abrechnung_gefahrgut_demo] (

	[abrechnung_id] int NULL, 
	[rechnungsnummer] varchar(30) NULL, 
	[kundenname] varchar(100) NULL, 
	[versanddatum] date NULL, 
	[zugnummer] varchar(20) NULL, 
	[startbahnhof] varchar(100) NULL, 
	[zielbahnhof] varchar(100) NULL, 
	[un_nummer] varchar(10) NULL, 
	[stoffbezeichnung] varchar(150) NULL, 
	[gefahrklasse] varchar(20) NULL, 
	[brutto_gewicht_kg] decimal(10,2) NULL, 
	[transportpreis_eur] decimal(12,2) NULL, 
	[gefahrgutzuschlag_eur] decimal(12,2) NULL, 
	[gesamtbetrag_eur] decimal(12,2) NULL
);