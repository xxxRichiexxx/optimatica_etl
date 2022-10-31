CREATE OR REPLACE VIEW sttgaz.dm_optimatica_plan_fact AS
WITH placements AS(
	SELECT
	p.*,
	plans.id AS plan_id,
	c.month,
	CASE
		WHEN (c.month <= p.period_from AND LAST_DAY(c.month) >= p.period_to) 
			THEN (DATEDIFF(day, p.period_from, p.period_to) + 1)
		WHEN (c.month >= p.period_from AND LAST_DAY(c.month) < p.period_to)
			THEN (DATEDIFF(day, c.month, LAST_DAY(c.month)) + 1)
		WHEN (c.month > p.period_from AND LAST_DAY(c.month) = p.period_to)
			THEN (DATEDIFF(day, c.month, LAST_DAY(c.month)) + 1)
		WHEN (c.month > p.period_from AND c.month < p.period_to AND LAST_DAY(c.month) > p.period_to)
			THEN (DATEDIFF(day, c.month, p.period_to) + 1)
		WHEN (c.month < p.period_from AND LAST_DAY(c.month) > p.period_from AND LAST_DAY(c.month) < p.period_to)
			THEN (DATEDIFF(day, p.period_from, LAST_DAY(c.month)))
	END AS days_count,
	(p.price / (DATEDIFF(day, p.period_from, p.period_to) + 1)) * days_count AS placement_price
	
	FROM sttgaz.aux_optimatica_placements AS p
	LEFT JOIN sttgaz.aux_optimatica_calendar AS c
		ON (c.month <= p.period_from AND LAST_DAY(c.month) >= p.period_to)
		OR (c.month >= p.period_from AND LAST_DAY(c.month) < p.period_to)
		OR (c.month > p.period_from AND LAST_DAY(c.month) = p.period_to)
		OR (c.month > p.period_from AND c.month < p.period_to AND LAST_DAY(c.month) > p.period_to)
		OR (c.month < p.period_from AND LAST_DAY(c.month) > p.period_from AND LAST_DAY(c.month) < p.period_to)
	LEFT JOIN sttgaz.aux_optimatica_year_plans AS plans 
		ON p.dealer_id = plans.dealer_id 
		AND (p.period_from >= plans.period_from AND p.period_to <= plans.period_to)
		AND p.specialization = plans.specialization
	WHERE p.state = 'Размещение завершено'
	ORDER BY p.id
),
items AS(
	SELECT i.*, p.dealer_id 
	FROM sttgaz.aux_optimatica_year_plan_items 		AS i
	JOIN sttgaz.aux_optimatica_year_plans 			AS p 
		ON i.plan_id =p.id 
)
SELECT
	i.id 											AS item_id,
	COALESCE(i.plan_id, p.plan_id)					AS plan_id,
	COALESCE(i.dealer_id, p.dealer_id) 				AS dealer_id,
	COALESCE(i.deleted, p.deleted)					AS deleted,
	COALESCE(i.frozen, p.frozen)					AS frozen,
	COALESCE(i.period_from, p.month)				AS month,
	COALESCE(i.period_from, p.period_from) 			AS period_from,
	COALESCE(i.period_to, p.period_to)				AS period_to,
	COALESCE(i.media, p.media)						AS media,
	COALESCE(i.model, p.model)						AS model,
	i.total_price									AS plan_price,
	p.id											AS placement_id,
	p.placement_price,
	p.state
FROM items 				AS i
FULL JOIN placements 	AS p 
	ON i.dealer_id = p.dealer_id
 	AND i.media = p.media
 	AND i.model = p.model
 	AND i.period_from = p.month
