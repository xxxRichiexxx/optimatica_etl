CREATE OR REPLACE VIEW sttgaz.dm_optimatica_plan_fact AS
WITH sq AS(
	SELECT i.*, p.dealer_id 
	FROM sttgaz.aux_optimatica_year_plan_items AS i
	JOIN sttgaz.aux_optimatica_year_plans AS p 
		ON i.plan_id =p.id 
)
SELECT sq.*, p.site, p.description, p.publish_count, p.measure_unit, p.price
FROM sq
LEFT JOIN sttgaz.aux_optimatica_placements AS p 
	ON sq.dealer_id = p.dealer_id 
	AND sq.media = p.media AND sq.model = p.model
	AND sq.period_from >= p.period_from
	AND sq.period_to <= p.period_to;