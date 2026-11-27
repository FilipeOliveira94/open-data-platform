-- olist-synth tables for the olist_synth_cdc contracts.
-- updated_at is the half-open extract watermark (timestamptz, not null).

CREATE SCHEMA IF NOT EXISTS olist_synth;

CREATE TABLE IF NOT EXISTS olist_synth.sellers (
    seller_id text PRIMARY KEY,
    seller_zip_code_prefix integer NOT NULL,
    seller_city text NOT NULL,
    seller_state text NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS olist_synth.customers (
    customer_id text PRIMARY KEY,
    customer_unique_id text NOT NULL,
    customer_zip_code_prefix integer NOT NULL,
    customer_city text NOT NULL,
    customer_state text NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS olist_synth.orders (
    order_id text PRIMARY KEY,
    customer_id text NOT NULL,
    order_status text NOT NULL,
    order_purchase_timestamp timestamptz NOT NULL,
    order_approved_at timestamptz,
    order_delivered_carrier_date timestamptz,
    order_delivered_customer_date timestamptz,
    order_estimated_delivery_date timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS olist_synth.order_items (
    order_id text NOT NULL,
    order_item_id integer NOT NULL,
    product_id text NOT NULL,
    seller_id text NOT NULL,
    shipping_limit_date timestamptz NOT NULL,
    price numeric(12, 2) NOT NULL,
    freight_value numeric(12, 2) NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);

-- P3-16: the database keeps updated_at honest. An UPDATE that does not change
-- updated_at gets the time the row was changed (clock_timestamp(), not the
-- transaction start), so a writer that forgets the column is still extracted.
-- A writer that sets updated_at explicitly keeps its value. INSERTs are not
-- touched: the column is NOT NULL, so every insert names it.
CREATE OR REPLACE FUNCTION olist_synth.set_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at THEN
        NEW.updated_at := clock_timestamp();
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE TRIGGER set_updated_at BEFORE UPDATE ON olist_synth.sellers
    FOR EACH ROW EXECUTE FUNCTION olist_synth.set_updated_at();
CREATE OR REPLACE TRIGGER set_updated_at BEFORE UPDATE ON olist_synth.customers
    FOR EACH ROW EXECUTE FUNCTION olist_synth.set_updated_at();
CREATE OR REPLACE TRIGGER set_updated_at BEFORE UPDATE ON olist_synth.orders
    FOR EACH ROW EXECUTE FUNCTION olist_synth.set_updated_at();
CREATE OR REPLACE TRIGGER set_updated_at BEFORE UPDATE ON olist_synth.order_items
    FOR EACH ROW EXECUTE FUNCTION olist_synth.set_updated_at();
