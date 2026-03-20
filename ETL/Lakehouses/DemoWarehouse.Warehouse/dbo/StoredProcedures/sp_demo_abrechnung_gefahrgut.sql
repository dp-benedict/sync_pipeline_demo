CREATE   PROCEDURE dbo.sp_demo_abrechnung_gefahrgut
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('dbo.abrechnung_gefahrgut_demo', 'U') IS NOT NULL
        DROP TABLE dbo.abrechnung_gefahrgut_demo;

    CREATE TABLE dbo.abrechnung_gefahrgut_demo
    (
        abrechnung_id           INT,
        rechnungsnummer         VARCHAR(30),
        kundenname              VARCHAR(100),
        versanddatum            DATE,
        zugnummer               VARCHAR(20),
        startbahnhof            VARCHAR(100),
        zielbahnhof             VARCHAR(100),
        un_nummer               VARCHAR(10),
        stoffbezeichnung        VARCHAR(150),
        gefahrklasse            VARCHAR(20),
        brutto_gewicht_kg       DECIMAL(10,2),
        transportpreis_eur      DECIMAL(12,2),
        gefahrgutzuschlag_eur   DECIMAL(12,2),
        gesamtbetrag_eur        DECIMAL(12,2)
    );

    INSERT INTO dbo.abrechnung_gefahrgut_demo
    (
        abrechnung_id,
        rechnungsnummer,
        kundenname,
        versanddatum,
        zugnummer,
        startbahnhof,
        zielbahnhof,
        un_nummer,
        stoffbezeichnung,
        gefahrklasse,
        brutto_gewicht_kg,
        transportpreis_eur,
        gefahrgutzuschlag_eur,
        gesamtbetrag_eur
    )
    VALUES
    (1, 'RG-2026-0001', 'ChemLogistik GmbH', '2026-03-01', 'DGX-4711', 'Hamburg Hafen', 'München Nord', '1203', 'Benzin', '3', 28500.00, 4200.00, 950.00, 5150.00),
    (2, 'RG-2026-0002', 'IndustrieTrans AG', '2026-03-02', 'DGX-4712', 'Ludwigshafen BASF', 'Köln Eifeltor', '1830', 'Schwefelsäure', '8', 31200.00, 4600.00, 1100.00, 5700.00),
    (3, 'RG-2026-0003', 'PetroRail Services', '2026-03-03', 'DGX-4713', 'Rostock Seehafen', 'Leipzig Wahren', '1017', 'Chlor', '2.3', 19800.00, 5100.00, 1800.00, 6900.00);
END;