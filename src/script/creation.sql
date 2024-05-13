CREATE TABLE IF NOT EXISTS public.fact_trip (
    trip_id SERIAL PRIMARY KEY,
    vendor_id integer REFERENCES public.vendor(vendor_id),
    pickup_datetime timestamp without time zone,
    dropoff_datetime timestamp without time zone,
    passenger_count integer,
    trip_distance double precision,
    rate_code_id integer REFERENCES public.rate_code(rate_code_id),
    store_and_fwd_flag boolean,
    payment_type integer REFERENCES public.payment_type(payment_type_id),
    fare_amount double precision,
    extra double precision,
    mta_tax double precision,
    tip_amount double precision,
    tolls_amount double precision,
    improvement_surcharge double precision,
    total_amount double precision,
    congestion_surcharge double precision,
    airport_fee double precision
);


CREATE TABLE IF NOT EXISTS public.vendor (
    vendor_id integer PRIMARY KEY,
    vendor_name text
);

INSERT INTO public.vendor (vendor_id, vendor_name)
VALUES (1, 'Creative Mobile Technologies, LLC'),
       (2, 'VeriFone Inc.');

CREATE TABLE IF NOT EXISTS public.rate_code (
    rate_code_id integer PRIMARY KEY,
    rate_description text
);

INSERT INTO public.rate_code (rate_code_id, rate_description)
VALUES (1, 'Standard rate'),
       (2, 'JFK'),
       (3, 'Newark'),
       (4, 'Nassau or Westchester'),
       (5, 'Negotiated fare'),
       (6, 'Group ride');

CREATE TABLE IF NOT EXISTS public.payment_type (
    payment_type_id integer PRIMARY KEY,
    payment_description text
);

INSERT INTO public.payment_type (payment_type_id, payment_description)
VALUES (1, 'Credit card'),
       (2, 'Cash'),
       (3, 'No charge'),
       (4, 'Dispute'),
       (5, 'Unknown'),
       (6, 'Voided trip');

CREATE INDEX idx_vendor_id ON public.fact_trip (vendor_id);
CREATE INDEX idx_rate_code_id ON public.fact_trip (rate_code_id);
CREATE INDEX idx_payment_type_id ON public.fact_trip (payment_type);

CREATE MATERIALIZED VIEW mv_dim_location AS
SELECT DISTINCT pulocationid AS location_id, 'Pickup' AS location_type
FROM public.nyc_raw
UNION
SELECT DISTINCT dolocationid AS location_id, 'Dropoff' AS location_type
FROM public.nyc_raw;

ALTER TABLE public.fact_trip ADD CONSTRAINT fk_vendor_id FOREIGN KEY (vendor_id) REFERENCES public.vendor(vendor_id);
ALTER TABLE public.fact_trip ADD CONSTRAINT fk_rate_code_id FOREIGN KEY (rate_code_id) REFERENCES public.rate_code(rate_code_id);
ALTER TABLE public.fact_trip ADD CONSTRAINT fk_payment_type_id FOREIGN KEY (payment_type) REFERENCES public.payment_type(payment_type_id);

ALTER TABLE mv_dim_location ADD PRIMARY KEY (location_id, location_type);