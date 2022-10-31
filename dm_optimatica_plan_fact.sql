CREATE OR REPLACE VIEW sttgaz.dm_optimatica_plan_fact AS
WITH sq AS(
	SELECT i.*, p.dealer_id 
	FROM sttgaz.aux_optimatica_year_plan_items 	AS i
	JOIN sttgaz.aux_optimatica_year_plans 		AS p 
		ON i.plan_id =p.id 
)
SELECT
	sq.id 									AS item_id,
	sq.plan_id,
	COALESCE(sq.dealer_id, p.dealer_id) 	AS dealer_id,
	COALESCE(sq.created_at, p.created_at) 	AS created_at,
	COALESCE(sq.deleted, p.deleted)			AS deleted,
	COALESCE(sq.frozen, p.frozen)			AS frozen,
	sq.month,
	COALESCE(sq.period_from, p.period_from) AS period_from,
	COALESCE(sq.period_to, p.period_to)		AS period_to,
	COALESCE(sq.media, p.media)				AS media,
	COALESCE(sq.model, p.model)				AS model,
	total_price,
	p.id									AS placement_id,
	(p.price / (DATEDIFF(day, p.period_from, p.period_to) + 1)) * (DATEDIFF(day, sq.period_from, sq.period_to) + 1) AS placement_price
FROM sq
FULL JOIN sttgaz.aux_optimatica_placements AS p 
	ON (sq.dealer_id = p.dealer_id 
	AND sq.media = p.media
	AND sq.model = p.model
	AND sq.period_from::date >= p.period_from::date
	AND sq.period_from::date < p.period_to::date);