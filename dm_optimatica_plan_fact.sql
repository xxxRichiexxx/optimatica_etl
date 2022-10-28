CREATE OR REPLACE VIEW sttgaz.dm_optimatica_plan_fact AS
WITH sq AS(
	SELECT i.*, p.dealer_id 
	FROM sttgaz.aux_optimatica_year_plan_items 	AS i
	JOIN sttgaz.aux_optimatica_year_plans 		AS p 
		ON i.plan_id =p.id 
)
SELECT
	sq.*,
	p.placement_id,
	(p.price / (DATEDIFF(day, p.period_from, p.period_to) + 1)) * (DATEDIFF(day, sq.period_from, sq.period_to) + 1) AS price_p_month
FROM sq
LEFT JOIN sttgaz.aux_optimatica_placements AS p 
	ON (sq.dealer_id = p.dealer_id 
	AND sq.media = p.media
	AND sq.model = p.model
	AND sq.period_from::date >= p.period_from::date
	AND sq.period_from::date < p.period_to::date);