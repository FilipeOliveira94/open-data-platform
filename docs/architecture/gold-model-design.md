# Gold model design

Business models for the local lakehouse. dbt builds them in P2-06. This document names every Gold table, who it is for, and the grain. Engine, keys, and the snapshot profile follow the catalog.

D-012 is `approved` after P2-06 built the tables ([Gold layer](gold-layer.md)). Silver stays the Spark writer from P2-04.

## Domains

Seven business domains. One shared calendar. A domain is a schema: marts from one domain do not share a product key or a customer key with another domain unless the section "What connects them" says so.

Ingestion contracts label a source family on `governance.source_family`: `olist_analysis` or `product_analysis`. That field is not the Gold schema. Several Gold schemas can read one family.

| Domain | Who uses it | Question | Sources |
| --- | --- | --- | --- |
| `marketplace_operations` | Operations and finance | Did the order get paid, handed to the carrier, and delivered on the date we promised? | Olist orders, items, payments, products, sellers |
| `customer_crm` | CRM | Who buys again, what they say in reviews, and where they live? | Olist customers, reviews, and the purchase address on the order |
| `seller_acquisition` | Marketing | Which lead origins become sellers, and which of those sellers actually sell? | Marketing funnel, plus seller sales from items |
| `brazil_geography` | Every domain that places a person or a shipment | Which municipality and which ZIP prefix is this? | IBGE municipalities, Olist geolocation |
| `catalog_commerce` | Catalog manager | How are DummyJSON products priced, and how do their reviews look? | DummyJSON products |
| `storefront_behavior` | Storefront product and CRM | What does a shopper do in a session before buying? | Demo storefront clickstream |
| `oltp_operations` | Operations on the live store database | The same operating questions as the marketplace, on the PostgreSQL stream | `postgres_oltp` customers, orders, items, sellers |

`dim_date` sits outside those schemas. Every domain uses it.

## Models

### Shared calendar

| Model | Type | Grain | What you see |
| --- | --- | --- | --- |
| `dim_date` | Dimension | One day, from 2016-09-01 through 2026-12-31 | The day key for purchases, lead dates, deliveries, catalog timestamps, clickstream events, and OLTP orders |

### Marketplace operations

Historical Olist orders. The status column is the latest label. A delivered order usually passed approval and carrier handoff, and the status text is only `delivered`. Funnels use the timestamps.

A delivered order is **on time** when the customer delivery date is on or before the estimated delivery date. Early counts as on time. `mart_delivery_performance` leads with that rate: on-time delivered orders divided by delivered orders that have both dates. The late share can be the smaller slice and still be the slice that damages reviews. `mart_late_orders` is one row per missed promise so that slice can be opened order by order. Delivered orders missing the delivery timestamp stay out of the rate and appear in the quality fact.

Payments record method, installments, sequence, and amount. They do not record a refund status. Money at risk after approval is cancellation and unavailability, in `mart_order_cancellations`.

| Model | Type | Grain | What you see |
| --- | --- | --- | --- |
| `dim_products` | Dimension | One Olist product | Category in Portuguese and English. Two names missing from the published translation are filled by the platform: `pc_gamer` → `gaming_pc`, `portateis_cozinha_e_preparadores_de_alimentos` → `portable_kitchen_appliances_and_food_processors`. A later file row for the same Portuguese name wins |
| `dim_sellers` | Dimension | One seller | The seller who fulfilled items |
| `fct_orders` | Fact | One order | Milestones, current status, purchase address, municipality, ZIP prefix |
| `fct_order_items` | Fact | One item on an order | Price, freight, product, seller, seller municipality, seller ZIP, shipping deadline |
| `fct_payments` | Fact | One payment sequence on an order | Amount, method, installments |
| `mart_platform_sales` | Mart | Purchase date | Marketplace totals: orders, delivered orders, item GMV (`price + freight`), amount collected |
| `mart_seller_performance` | Mart | Seller | Item GMV, orders, on-time handoff to the carrier, freight relative to price, distinct buyer municipalities |
| `mart_seller_buyer_geography` | Mart | Seller and municipality | Where that seller's buyers are. A null municipality is its own row |
| `mart_order_milestones` | Mart | Purchase month and primary payment method | How many orders reached approval, carrier handoff, and customer delivery, and how many were canceled after approval or marked unavailable |
| `mart_payment_mix` | Mart | Payment method and installment count | How shoppers pay |
| `mart_order_cancellations` | Mart | Purchase month, status, primary payment method | Canceled and unavailable orders, item GMV, amount collected, and whether approval had already happened |
| `mart_delivery_performance` | Mart | Purchase month and customer state | **On-time rate** against the estimate, late rate, and the share handed to the carrier on or before the shipping deadline |
| `mart_late_orders` | Mart | One delivered order past the estimate | Days late, state, seller, review score, freight relative to price, primary payment method, and whether the carrier handoff also missed the shipping deadline |
| `mart_freight` | Mart | Customer state and distance band | Freight per item and freight relative to price. Bands are `0_50km`, `50_200km`, `200_500km`, `500km_plus`, and `unknown` |
| `fct_operations_quality_findings` | Quality | One finding | Invalid or broken order, item, payment, product, or seller rows. Reasons are listed under [Quality findings](#quality-findings) |

Primary payment method is the method with the largest amount on the order. A tie takes the smaller sequence number.

### Customer CRM

The person is `customer_unique_id`. `customer_id` changes every order. The order fact keeps the city, state, and ZIP of **that purchase**. `dim_customers` keeps the latest purchase address. A move between orders stays visible on the orders. It is not erased by the dimension.

| Model | Type | Grain | What you see |
| --- | --- | --- | --- |
| `dim_customers` | Dimension | One person | Latest city, state, and ZIP |
| `fct_reviews` | Fact | One review on one order | Score from 1 to 5, whether a comment was written, when it was written, when it was answered. The comment text stays on this fact |
| `mart_customer_retention` | Mart | One person | First and last delivered purchase, delivered order count, repeat flag |
| `mart_customer_geography` | Mart | Municipality | Buyers, repeat buyers, and mean review score from the purchase address. A null municipality is its own row |
| `mart_review_drivers` | Mart | Delay bucket and English category | Mean score, share of reviews with a comment, median days until an answer. Delay buckets are `late`, `on_time`, `early`, and `unknown` |
| `fct_crm_quality_findings` | Quality | One finding | Broken customer or review rows, and purchase addresses that do not match a municipality |

### Seller acquisition

Leads have no seller id. A closed deal has one. Item GMV is attached only when that seller id exists on `dim_sellers`. A won lead whose seller never appears in the historical orders stays in the mart with empty sales, which is the marketing result: the deal did not become a selling account.

Declared `business_segment` and the catalog category are different vocabularies. The mart shows both. It does not treat them as one list.

| Model | Type | Grain | What you see |
| --- | --- | --- | --- |
| `fct_leads` | Fact | One marketing qualified lead | First contact date, origin, landing page |
| `fct_closed_deals` | Fact | One closed lead | Won date, declared commercial attributes, seller id |
| `mart_lead_outcome` | Mart | One lead | Origin, won flag, days from first contact to won date, whether the seller matches the order history, item GMV after the match |
| `mart_marketing_acquisition` | Mart | Lead origin | Leads, wins, win rate, median days to win, item GMV of matched sellers, top declared segment |
| `fct_acquisition_quality_findings` | Quality | One finding | Leads and deals that break a key, a date order, or the seller match |

### Brazil geography

Reference data for the other domains. The municipality comes from the party's city and state, not from the city name stored on a ZIP prefix. A ZIP prefix supplies coordinates for freight distance.

| Model | Type | Grain | What you see |
| --- | --- | --- | --- |
| `dim_municipality` | Dimension | One IBGE municipality | Name, UF, macro region. A null microregion stays null |
| `dim_zip_prefix` | Dimension | One 5-character ZIP prefix | Modal state, modal city, mean latitude, mean longitude, and a flag when more than one state appears. The modal city is not the municipality name |
| `fct_geography_quality_findings` | Quality | One finding | Prefixes that contain more than one state, and contract violations on the IBGE file or the geolocation file |

### Catalog commerce

DummyJSON only. Product `id` is not an Olist product and not a storefront token.

| Model | Type | Grain | What you see |
| --- | --- | --- | --- |
| `dim_catalog_products` | Dimension | One catalog product | Title, category, brand, price, dimensions, created and updated timestamps of the current refresh |
| `fct_catalog_reviews` | Fact | One stored review | Position in the product's review list, starting at 1, plus rating, comment length, reviewer email, and review time. Email plus product is not unique |
| `mart_catalog_prices` | Mart | Category | Product count, minimum, median, and maximum price, and how many products omit brand |
| `mart_catalog_review_patterns` | Mart | Category and rating | Review count, mean comment length, share of reviews whose reviewer email appears twice on the same product |
| `fct_catalog_quality_findings` | Quality | One finding | Contract violations on product pages, and repeated reviewer emails on one product |

### Storefront behavior

Generated shoppers and generated product tokens. There is no page URL. The last event in the session, by event time then event id, stands in for the last page. A null product on a page view is allowed and is not an error. A purchase or add-to-cart without a product is an error.

These tokens do not join the catalog or the marketplace.

| Model | Type | Grain | What you see |
| --- | --- | --- | --- |
| `dim_storefront_users` | Dimension | One generated user | The shopper token. No location is collected |
| `dim_storefront_products` | Dimension | One generated product token | Products that appear on events |
| `fct_storefront_events` | Fact | One action | Time, type (`page_view`, `add_to_cart`, `purchase`), session, user, product |
| `mart_storefront_session` | Mart | One session | Events, whether the session carted and purchased, how many cart events happened before the first purchase, last event type, last product |
| `mart_storefront_product_behavior` | Mart | Storefront product | Views, carts, purchases, sessions that viewed it, sessions that purchased it, sessions that ended on it |
| `mart_storefront_user_behavior` | Mart | Storefront user | Sessions, purchases, products touched, sessions with a cart and no purchase, last session time |
| `fct_storefront_quality_findings` | Quality | One finding | Contract violations, and cart or purchase events with no product |

### OLTP operations

Same operating questions as the marketplace, on the PostgreSQL tables. Those ids (`customer-17`, `order-17`) are a different population from the historical file. There is no payment table and no review table here, so there is no payment mix and no review mart. City and state use the same municipality bridge. ZIP is an integer and is padded to 5 characters before it joins `dim_zip_prefix`.

The order fact copies city, state, and ZIP at purchase, so a later change to the customer row does not rewrite the address of the sale.

| Model | Type | Grain | What you see |
| --- | --- | --- | --- |
| `dim_oltp_customers` | Dimension | One order-scoped customer | Current customer attributes, plus `updated_at` |
| `dim_oltp_sellers` | Dimension | One seller | Current seller attributes, plus `updated_at` |
| `fct_oltp_orders` | Fact | One order | Milestones, status, purchase address |
| `fct_oltp_order_items` | Fact | One item | Price, freight, product, seller, shipping deadline |
| `mart_oltp_platform_sales` | Mart | Purchase date | Orders, delivered orders, item GMV |
| `mart_oltp_seller_performance` | Mart | Seller | Item GMV, orders, freight relative to price, on-time handoff |
| `mart_oltp_buyer_geography` | Mart | Seller and municipality | Buyer location, with a null-municipality row |
| `mart_oltp_order_milestones` | Mart | Purchase month | Approval, carrier, and delivery counts |
| `mart_oltp_delivery_performance` | Mart | Purchase month | On-time rate and late rate against the estimate, and on-time handoff |
| `mart_oltp_late_orders` | Mart | One delivered order past the estimate | Days late, state, seller, handoff miss |
| `mart_oltp_freight` | Mart | Customer state and distance band | Freight ratio. Unknown band when the padded ZIP misses the prefix dimension |
| `mart_oltp_cancellations` | Mart | Purchase month and status | Canceled and unavailable orders, and whether approval had already happened |
| `fct_oltp_quality_findings` | Quality | One finding | The operating breaks that this source can show: missing items, delivered without a delivery timestamp, unmatched city, ZIP prefix miss, contract violations |

## What connects them

| Link | From | To |
| --- | --- | --- |
| Day | Any fact date | `dim_date` |
| Municipality | Purchase or seller city + UF, after lowercase, accent stripping, and hyphen turned into a space | `dim_municipality` |
| ZIP prefix | Olist ZIP text, or OLTP ZIP padded to 5 characters | `dim_zip_prefix` |
| Seller sales | Closed deal `seller_id` | `dim_sellers`, only when the id exists |
| Distance | Customer prefix centroid to seller prefix centroid | Freight marts. Haversine. A missing prefix is the `unknown` band |
| Review and delay | `fct_reviews` and the delivery dates on `fct_orders` | `mart_review_drivers` and the score on `mart_late_orders` |

DummyJSON `id`, storefront `product_id`, storefront `user_id`, and OLTP ids do not point at Olist keys. Lead `mql_id` does not point at an order.

## Rules for implementers

Gold reads `iceberg.silver.*`. P2-06 does not replace the Spark Silver writer. A later change may re-express the same cast, JSON expansion, and survivor SQL as dbt, and only then retire that writer.

Every model is a full refresh. Dimensions are type 1: one current row per business key. A key that leaves Silver leaves Gold on the next overwrite. P2-06 does not add `valid_from` / `valid_to`.

A null foreign key stays. An orphan key stays. Rows are not dropped to make a rate look complete (D-013). Review comments are `pii` and stay on `fct_reviews`. CRM marts use the score, a comment-present flag, and the answer lag.

On time for `mart_delivery_performance` and `mart_oltp_delivery_performance`: customer delivery date on or before the estimated date, among delivered orders that have both timestamps. Handoff on time: carrier timestamp on or before the latest `shipping_limit_date` on the order's items.

`dim_customers` takes attributes from the latest `order_purchase_timestamp`. A tie takes the greater `order_id`.

### Quality findings

Each `fct_*_quality_findings` has one row per finding. The same source row can emit more than one finding.

| Column | Meaning |
| --- | --- |
| `finding_id` | Stable id of this finding inside the refresh |
| `issue_class` | `contract_violation` or `business_anomaly` |
| `reason` | Short code from the lists below |
| `source_dataset` | `source_id.dataset` |
| `business_key` | The key of the broken row, as text |
| `field` | Column involved, when there is one |
| `detail` | Short explanation. No comment body and no extra `pii` beyond the key |

`contract_violation` reads the quarantine sidecar for the datasets in that domain (`null_violation`, `type_mismatch`, `duplicate_primary_key`). P2-06 unions those lines into the fact. Reprocessing a quarantined row stays P3-07. `business_anomaly` is computed from Silver.

A late delivery is not a quality finding. It is `mart_late_orders`. An allowed null is not a finding: page-view product, nullable deal attributes, nullable brand, nullable microregion.

| Domain | `business_anomaly` reasons | Quarantine datasets |
| --- | --- | --- |
| `marketplace_operations` | `order_without_items`, `order_without_payment`, `payment_amount_mismatch` (item GMV and collected amount differ by more than 0.01), `delivered_without_timestamp`, `product_without_category` | `olist_brazilian_ecommerce` orders, order items, order payments, products, sellers, category translation |
| `customer_crm` | `address_without_municipality`, `zip_prefix_unknown`, `review_without_order` | customers, order reviews |
| `seller_acquisition` | `won_seller_not_in_orders`, `won_before_first_contact`, `lead_without_origin`, `deal_without_lead` | `olist_marketing_funnel` qualified leads, closed deals |
| `brazil_geography` | `zip_prefix_multi_state` | `ibge_localidades.municipios`, `olist_brazilian_ecommerce.geolocation` |
| `catalog_commerce` | `repeated_reviewer_email` | `dummyjson_products.products` |
| `storefront_behavior` | `cart_or_purchase_without_product` | `demo_storefront.clickstream` |
| `oltp_operations` | `order_without_items`, `delivered_without_timestamp`, `address_without_municipality`, `zip_prefix_unknown` | `postgres_oltp` customers, orders, order items, sellers |

### SCD Type 2

P4-08 is the only implementation issue.

Today's raw bucket has one execution per dataset. Silver overwrites and keeps the latest `updated_at`, which removes the previous version. Type 2 needs those versions kept.

| Application | Why | What P4-08 builds | What stays as designed here |
| --- | --- | --- | --- |
| OLTP customer, seller, order, order item | CDC will emit updates and deletes. This snapshot only repeats the same `updated_at` across the daily and weekly windows | History with `valid_from`, `valid_to`, and `is_current`. A delete closes the current row. OLTP dimensions read the current row | Purchase city, state, and ZIP copied on `fct_oltp_orders` |
| Olist person | Some people change city across orders in the one published file. The file has no effective date | Optional versions ordered by purchase time. `valid_from` is the purchase where the new address was observed, not the edit time of a master record | `fct_orders` keeps the purchase address. `dim_customers` stays the latest address |
| Olist seller, product, order status | One published row per key. Status milestones are columns | Nothing until a second snapshot or a CDC feed exists | Type 1 dimensions and milestone columns |
| DummyJSON product | Created and updated timestamps show change. The table stores the current row | Versions only after a later full refresh is kept | `dim_catalog_products` type 1 |
| IBGE municipality | A later refresh can rename or reclassify | Versions only when a prior refresh is kept | `dim_municipality` type 1 |
| Clickstream event, Olist review, catalog review | Immutable | Out of scope | Facts |
| Geolocation | Many coordinates per prefix, no change date | Out of scope | `dim_zip_prefix` is a collapse |

### Build order in P2-06

Shared calendar and `brazil_geography` first, then marketplace operations, customer CRM, seller acquisition, catalog commerce, storefront behavior, and OLTP operations. P2-06 chose **dbt-trino**; implementation detail: [Gold layer](gold-layer.md).

## Profile of the 2026-11-06 snapshot

Olist CSVs in gitignored `data/raw/` and Silver in Trino `iceberg.silver`, bucket `odp-raw`, `execution_id=run-001`. Counts match `live_counts`. These figures describe that snapshot. They are not a growth contract.

| Check | Result |
| --- | --- |
| Orders and order-scoped customers | 99_441 and 99_441 |
| Persons | 96_096. 2_997 have more than one order (maximum 17). Among them, 122 change city, 39 change UF, 250 change ZIP |
| Items | 112_650. Every product id and seller id matches. 775 orders have no item |
| Payments | 103_886. One order has no payment. 2_961 orders have more than one payment row. On 303 orders, item GMV and collected amount differ by more than 0.01 (median difference 5.90, maximum 182.81) |
| Milestones: purchase, approval, carrier, customer delivery | 99_441, 99_281, 97_658, 96_476 |
| Delivered vs estimate | 88_644 early, 1_292 on the estimate date, 6_534 late, 8 delivered with no customer-delivery timestamp. On-time rate is 93.2% of delivered orders that have both dates (89_936 / 96_470). Late rate is 6.8%. Median signed days is −12. Maximum lateness is 188 days |
| Review score vs lateness | Mean 2.272 on 6_382 reviewed late orders. Mean 4.290 on 89_448 reviewed on-time or early orders |
| Carrier after the shipping deadline | 8_849 late handoffs, 88_808 within the deadline |
| Canceled with approval already set | 484 of 625. Unavailable with approval set: 609 of 609 |
| Reviews | 99_224 rows. 789 review ids appear on more than one order. 768 orders have no review. 58_247 comment bodies are empty. Median answer lag 1.67 days |
| Municipality match | 98_817 / 99_441 customers and 2_986 / 3_095 sellers. No ambiguous city + UF |
| ZIP prefixes | 19_015. 8 span more than one state. 157 customer prefixes and 7 seller prefixes are absent. 23_995 customer prefixes start with `0` |
| Funnel | 8_000 leads, 842 wins (win rate 842 / 8_000). 380 closed sellers exist on the seller dimension. 462 do not. Median days to win: 14 (minimum −2, maximum 427). Declared segment and product category share no values |
| Products | 32_951. 610 have no category. Two category names need the English supplement above |
| DummyJSON | 194 products, 24 categories, 92 null brands, 582 reviews. Reviewer email repeats inside one product for one pair |
| Clickstream | 100 events, 5 users, 20 sessions, 16 page views with no product. No URL and no location |
| PostgreSQL Silver | 2 customers, 2 orders, 2 items, 3 sellers. The two extract windows repeat the same `updated_at` |
