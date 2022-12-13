TRUNCATE TABLE sttgaz.dds_optimatica_dealers;

INSERT INTO sttgaz.dds_optimatica_dealers
("dealer_name", "dealer_id", "dealer_city", "dealer_address")
SELECT DISTINCT
	FIRST_VALUE("Dealer") OVER(PARTITION BY Dealer_Id ORDER BY "Year" DESC)             AS "Dealer",
    "Dealer_Id",
    FIRST_VALUE("DealerCity") OVER(PARTITION BY Dealer_Id ORDER BY "Year" DESC)         AS "DealerCity",
    FIRST_VALUE("DealerLegalAddress") OVER(PARTITION BY Dealer_Id ORDER BY "Year" DESC) AS "DealerLegalAddress"
FROM sttgaz.stage_optimatica_YearPlan;
