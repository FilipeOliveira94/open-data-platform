-- OLTP tables for the postgres_oltp contracts.
-- updated_at is the half-open extract watermark (timestamptz, not null).

CREATE SCHEMA IF NOT EXISTS oltp;

CREATE TABLE IF NOT EXISTS oltp.sellers (
    seller_id text PRIMARY KEY,
    seller_zip_code_prefix integer NOT NULL,
    seller_city text NOT NULL,
    seller_state text NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS oltp.customers (
    customer_id text PRIMARY KEY,
    customer_unique_id text NOT NULL,
    customer_zip_code_prefix integer NOT NULL,
    customer_city text NOT NULL,
    customer_state text NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS oltp.orders (
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

CREATE TABLE IF NOT EXISTS oltp.order_items (
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
