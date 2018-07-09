create or replace view v_reference_symbols as
SELECT productinterface.name AS symbol,
    reference_data.content,
    reference_field.result,
    reference_field.name AS field
   FROM ((productinterface
     JOIN symbol ON ((productinterface.id = symbol.id))
     JOIN reference_data ON ((reference_data.product_id = productinterface.id))
     JOIN reference_field ON ((reference_field.id = reference_data.field_id))))
  ORDER BY productinterface.name;


create or replace view v_symbols_state as
SELECT productinterface.name AS symbol,
    symbol.group,
    symbol.internal
   FROM ((productinterface
     JOIN symbol ON ((productinterface.id = symbol.id))))
  ORDER BY productinterface.name;

-- create or replace view v_symbols as
-- SELECT productinterface.name,
--     ts_name.jdata AS data,
--     ts_name.name  AS timeseries
--   FROM ((productinterface
--     LEFT JOIN ts_name ON ((productinterface.id = ts_name.product_id)))
--     JOIN symbol ON ((productinterface.id = symbol.id)));

-- create or replace view v_portfolio_nav as
-- SELECT portfolio.id,
--     productinterface.name,
--     ts_name.name AS timeseries,
--     ts_name.jdata AS data
--    FROM ((portfolio
--      JOIN productinterface ON ((productinterface.id = portfolio.id)))
--      JOIN ts_name ON ((ts_name.product_id = portfolio.id)))
--   WHERE ((ts_name.name)::text = 'nav'::text);
--
--
-- create or replace view v_portfolio_leverage as
-- SELECT portfolio.id,
--     productinterface.name,
--     ts_name.name AS timeseries,
--     ts_name.jdata AS data
--    FROM ((portfolio
--      JOIN productinterface ON ((productinterface.id = portfolio.id)))
--      JOIN ts_name ON ((ts_name.product_id = portfolio.id)))
--   WHERE ((ts_name.name)::text = 'leverage'::text);


-- create or replace view v_portfolio_sector as
-- SELECT portfolio.id,
--     p1.name,
--     p2.name AS symbol,
--     s."group",
--     ts_name.jdata AS data,
--     s.internal
--    FROM ((((portfolio
--      JOIN productinterface p1 ON ((p1.id = portfolio.id)))
--      JOIN ts_name ON ((ts_name.product_id = portfolio.id)))
--      JOIN productinterface p2 ON ((p2.id = ts_name.secondary_id)))
--      JOIN symbol s ON ((p2.id = s.id)))
--   WHERE ((ts_name.name)::text = 'weight'::text);


