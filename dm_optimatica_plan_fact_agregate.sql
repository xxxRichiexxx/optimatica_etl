CREATE OR REPLACE VIEW dm_optimatica_plan_fact_agregate AS
SELECT
	item_id,
	plan_id,
	dealer_id,
	deleted,
	frozen,
	"month",
	period_from,
	period_to,
	media,
	model,
	plan_price,
	SUM(placement_price) AS fact_rpice
FROM sttgaz.dm_optimatica_plan_fact
GROUP BY
	item_id,
	plan_id,
	dealer_id,
	deleted,
	frozen,
	"month",
	period_from,
	period_to,
	media,
	model,
	plan_price;