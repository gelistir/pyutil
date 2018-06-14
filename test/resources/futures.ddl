create or replace view refdata as
SELECT p.name AS product,
    field.name AS field,
    p.discriminator,
    field.result,
    field.type,
    reference_data.content
   FROM ((reference_data
     JOIN reference_field field ON ((reference_data.field_id = field.id)))
     JOIN productinterface p ON ((reference_data.product_id = p.id)));

create or replace view futures as
SELECT future.internal,
    future.quandl,
    future.fut_gen_month,
    p1.name AS future,
    p2.name AS exchange,
    p3.name AS category
   FROM (((future
     JOIN productinterface p1 ON ((future.id = p1.id)))
     JOIN productinterface p2 ON ((future.exchange_id = p2.id)))
     JOIN productinterface p3 ON ((future.category_id = p3.id)));

