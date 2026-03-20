CREATE TABLE [dbo].[demodaten] (

	[abrechnung_id] int NOT NULL, 
	[rechnungsnummer] varchar(30) NOT NULL, 
	[kundenname] varchar(100) NOT NULL, 
	[versanddatum] date NOT NULL, 
	[zugnummer] varchar(20) NOT NULL, 
	[startbahnhof] varchar(100) NOT NULL, 
	[zielbahnhof] varchar(100) NOT NULL, 
	[wagen_nummer] varchar(20) NOT NULL, 
	[un_nummer] varchar(10) NOT NULL, 
	[stoffbezeichnung] varchar(150) NOT NULL, 
	[gefahrklasse] varchar(20) NOT NULL, 
	[verpackungsgruppe] varchar(10) NULL, 
	[brutto_gewicht_kg] decimal(10,2) NOT NULL, 
	[transportpreis_eur] decimal(12,2) NOT NULL, 
	[gefahrgutzuschlag_eur] decimal(12,2) NOT NULL, 
	[sicherheitszuschlag_eur] decimal(12,2) NOT NULL, 
	[gesamtbetrag_eur] decimal(12,2) NOT NULL, 
	[abrechnungsstatus] varchar(20) NOT NULL, 
	[zahlungsziel] date NOT NULL
);