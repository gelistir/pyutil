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
