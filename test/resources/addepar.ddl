create or replace view vx_owner_securities_pairs as
SELECT (p1.name)::integer AS owner,
    (p2.name)::integer AS security,
    p1.id AS owner_id,
    p2.id AS security_id,
    p3.id AS currency_id,
    p3.name AS currency
   FROM (((((security_owner so
     JOIN productinterface p1 ON ((p1.id = so.right_id)))
     JOIN productinterface p2 ON ((p2.id = so.left_id)))
     JOIN owner o ON ((o.id = p1.id)))
     JOIN currency ON ((currency.id = o.currency_id)))
     JOIN productinterface p3 ON ((p3.id = currency.id)))
  ORDER BY (p1.name)::integer, (p2.name)::integer;

create or replace view v_volatility_owner_security as
SELECT vosp.owner,
    vosp.security,
    vp.jdata AS data
   FROM (vx_owner_securities_pairs vosp
     JOIN ts_name vp ON (((vp.product_id = vosp.security_id) AND (vp.secondary_id = vosp.currency_id))))
  WHERE ((vp.name)::text = 'volatility'::text)
  ORDER BY vosp.owner, vosp.security;

create or replace view vx_product as
SELECT p1.name AS product,
    ts_name.jdata AS data,
    ts_name.name,
    p1.discriminator,
    ts_name.secondary_id
   FROM (ts_name
     JOIN productinterface p1 ON ((p1.id = ts_name.product_id)));

create or replace view v_prices as
SELECT (vp.product)::integer AS security,
    vp.data
   FROM vx_product vp
  WHERE (((vp.name)::text = 'price'::text) AND ((vp.discriminator)::text = 'Security'::text))
  ORDER BY (vp.product)::integer;

create or replace view v_volatility_owner as
SELECT (vp.product)::integer AS owner,
    vp.data
   FROM vx_product vp
  WHERE (((vp.name)::text = 'volatility'::text) AND ((vp.discriminator)::text = 'Owner'::text))
  ORDER BY (vp.product)::integer;

create or replace view v_volatility_security as
SELECT (vp.product)::integer AS security,
    c.name AS currency,
    vp.data
   FROM (vx_product vp
     JOIN productinterface c ON ((c.id = vp.secondary_id)))
  WHERE (((vp.name)::text = 'volatility'::text) AND ((vp.discriminator)::text = 'Security'::text) AND ((c.discriminator)::text = 'Currency'::text))
  ORDER BY (vp.product)::integer, c.name;

create or replace view v_position as
SELECT (vp.product)::integer AS owner,
    (p.name)::integer AS security,
    vp.data
   FROM (vx_product vp
     JOIN productinterface p ON ((p.id = vp.secondary_id)))
  WHERE (((vp.name)::text = 'position'::text) AND ((vp.discriminator)::text = 'Owner'::text) AND ((p.discriminator)::text = 'Security'::text))
  ORDER BY (vp.product)::integer, (p.name)::integer;

create or replace view v_reference_securities as
SELECT (productinterface.name)::integer AS security,
    reference_data.content,
    reference_field.result,
    reference_field.name AS field
   FROM (((productinterface
     JOIN security ON ((productinterface.id = security.id)))
     JOIN reference_data ON ((reference_data.product_id = security.id)))
     JOIN reference_field ON ((reference_data.field_id = reference_field.id)))
  ORDER BY (productinterface.name)::integer;

create or replace view v_reference_owner_securities as
SELECT vosp.owner,
    vosp.security,
    v_reference_securities.field,
    v_reference_securities.content,
    v_reference_securities.result
   FROM (vx_owner_securities_pairs vosp
     JOIN v_reference_securities ON ((v_reference_securities.security = vosp.security)))
  ORDER BY vosp.owner, vosp.security;

create or replace view v_reference_owner as
SELECT (productinterface.name)::integer AS owner,
    reference_data.content,
    reference_field.result,
    reference_field.name AS field
   FROM (((productinterface
     JOIN owner ON ((productinterface.id = owner.id)))
     JOIN reference_data ON ((reference_data.product_id = owner.id)))
     JOIN reference_field ON ((reference_data.field_id = reference_field.id)))
  ORDER BY (productinterface.name)::integer;

