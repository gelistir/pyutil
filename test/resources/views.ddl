create view v_reference as
SELECT security.entity_id,
    reference_data.content,
    reference_field.name
   FROM ((security
     JOIN reference_data ON ((reference_data.product_id = security.id)))
     JOIN reference_field ON ((reference_data.field_id = reference_field.id)));

create view v_owner as
SELECT owner.entity_id,
    reference_data.content,
    reference_field.name AS field
   FROM ((owner
     JOIN reference_data ON ((reference_data.product_id = owner.id)))
     JOIN reference_field ON ((reference_data.field_id = reference_field.id)))
  ORDER BY owner.entity_id;

create view v_volatility_owner as
SELECT owner.entity_id,
    ts_data.date,
    ts_data.value
   FROM ((owner
     JOIN ts_name ON ((ts_name.product_id = owner.id)))
     JOIN ts_data ON ((ts_name.id = ts_data.ts_id)))
  WHERE (ts_name.name = 'volatility');

create view v_prices as
SELECT security.entity_id,
    ts_data.date,
    ts_data.value
   FROM ((security
     JOIN ts_name ON ((ts_name.product_id = security.id)))
     JOIN ts_data ON ((ts_name.id = ts_data.ts_id)))
  WHERE (ts_name.name = 'price');

create view v_position as
SELECT owner.entity_id AS owner,
    security.entity_id AS security,
    ts_data.date,
    ts_data.value
   FROM (((owner
     JOIN ts_name ON ((ts_name.product_id = owner.id)))
     JOIN security ON ((security.id = ts_name.secondary_id)))
     JOIN ts_data ON ((ts_name.id = ts_data.ts_id)))
  WHERE (ts_name.name = 'position');

create view v_volatility_security as
SELECT security.entity_id AS security,
    ts_data.date,
    ts_data.value,
    currency.name AS currency
   FROM (((security
     JOIN ts_name ON ((ts_name.product_id = security.id)))
     JOIN ts_data ON ((ts_name.id = ts_data.ts_id)))
     JOIN currency ON ((ts_name.secondary_id = currency.id)))
  WHERE (ts_name.name = 'volatility');
