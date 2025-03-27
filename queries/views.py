view_query = """
CREATE OR REPLACE VIEW public.item_prices_nok_view AS
SELECT 
	ip.id,
	ip.item,
	round(ip.price*cr.exchange_rate, 2) AS price,
	cr.target_currency AS currency
FROM public.item_prices ip
LEFT JOIN public.currency_conversion_rates AS cr 
	ON cr.base_currency=ip.currency
	AND cr.target_currency='NOK'
ORDER BY id ASC
; 
"""

all_columns_view_query = """
CREATE OR REPLACE VIEW public.full_item_prices_nok_view AS
SELECT 
	ip.*
	round(ip.price*cr.exchange_rate, 2) AS price_in_nok,
	cr.exchange_rate,
	cr.target_currency, 
FROM public.item_prices ip
LEFT JOIN public.currency_conversion_rates AS cr 
	ON cr.base_currency=ip.currency
	AND cr.target_currency='NOK'
ORDER BY id ASC
; 
"""