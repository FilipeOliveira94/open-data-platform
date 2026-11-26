/**
 * Components and flows drawn by the home-page architecture scene.
 * Coordinates are world units: x runs from sources (left) to consumers
 * (right), z runs from the back (negative) to the front, y is height.
 */

/** Function of a component; each one has its own hue in the brand range. */
export type GroupId =
	| 'sources'
	| 'movement'
	| 'storage'
	| 'processing'
	| 'destinations'
	| 'orchestration'
	| 'observability';

export type FlowKind = 'batch' | 'stream' | 'control' | 'telemetry';

export type Vec3 = [number, number, number];

export interface SceneNode {
	id: string;
	label: string;
	detail: string;
	group: GroupId;
	pos: Vec3;
	glyph: string;
	core?: boolean;
}

export interface SceneEdge {
	from: string;
	to: string;
	kind: FlowKind;
}

export const GROUPS: { id: GroupId; label: string; color: string }[] = [
	{ id: 'sources', label: 'Sources', color: '#6FB2FF' },
	{ id: 'movement', label: 'Ingestion and CDC', color: '#8E8BFF' },
	{ id: 'storage', label: 'Databases and storage', color: '#B98AFF' },
	{ id: 'processing', label: 'Processing', color: '#E58BEA' },
	{ id: 'destinations', label: 'Destinations', color: '#FF6FA6' },
	{ id: 'orchestration', label: 'Orchestration and quality', color: '#E6DAFF' },
	{ id: 'observability', label: 'Observability and lineage', color: '#9C93B8' },
];

export const FLOWS: { kind: FlowKind; label: string }[] = [
	{ kind: 'batch', label: 'Batch data' },
	{ kind: 'stream', label: 'Change stream' },
	{ kind: 'control', label: 'Orchestration' },
	{ kind: 'telemetry', label: 'Metrics and lineage' },
];

export const NODES: SceneNode[] = [
	// Sources
	{ id: 'olist', label: 'Olist marketplace', detail: 'Historical Brazilian e-commerce dataset, loaded from CSV files.', group: 'sources', pos: [-9.4, 0, -3.4], glyph: 'CSV' },
	{ id: 'dummyjson', label: 'DummyJSON', detail: 'Product catalog read from a public REST API.', group: 'sources', pos: [-9.9, 0, -1.4], glyph: 'API' },
	{ id: 'ibge', label: 'IBGE', detail: 'Brazilian geography reference data from the IBGE API.', group: 'sources', pos: [-10.1, 0, 0.6], glyph: 'GEO' },
	{ id: 'bcb', label: 'BCB SGS', detail: 'Daily US dollar and Selic series from the Central Bank of Brazil.', group: 'sources', pos: [-9.9, 0, 2.6], glyph: 'FX' },
	{ id: 'storefront', label: 'Storefront', detail: 'Clickstream events from a fictional retail website, as JSONL.', group: 'sources', pos: [-9.4, 0, 4.6], glyph: 'EVT' },
	{ id: 'postgres', label: 'olist-synth-cdc', detail: 'Synthetic Olist-shaped operational database on PostgreSQL 16: windowed batch extracts and logical replication for CDC.', group: 'sources', pos: [-7.4, 0, 6.6], glyph: 'PG' },

	// Ingestion
	{ id: 'ingest', label: 'Ingestion', detail: 'Python writers with data contracts, a quarantine sidecar and an extract coverage ledger.', group: 'movement', pos: [-5.6, 0, 0.2], glyph: 'ING' },
	{ id: 'minio', label: 'MinIO raw zone', detail: 'Auditable raw objects by source, dataset, date and execution id, over the S3 API.', group: 'storage', pos: [-2.6, 0, -2.8], glyph: 'RAW' },

	// Streaming CDC
	{ id: 'debezium', label: 'Debezium', detail: 'Reads the PostgreSQL write-ahead log and emits row-level change events.', group: 'movement', pos: [-4.6, 0, 5.4], glyph: 'CDC' },
	{ id: 'kafka', label: 'Kafka', detail: 'Change-event topics that decouple the source from the lakehouse.', group: 'movement', pos: [-1.4, 0, 5.0], glyph: 'KFK' },

	// Lakehouse
	{ id: 'spark', label: 'Spark', detail: 'Batch jobs and Structured Streaming write Bronze and Silver Iceberg tables.', group: 'processing', pos: [-1.2, 0, 1.2], glyph: 'SPK' },
	{ id: 'lakehouse', label: 'Iceberg lakehouse', detail: 'Bronze keeps raw history, Silver conforms and deduplicates, Gold serves business marts.', group: 'storage', pos: [2.4, 0, 0], glyph: 'ICE', core: true },
	{ id: 'catalog', label: 'Iceberg catalog', detail: 'JDBC catalog on PostgreSQL shared by Spark and Trino.', group: 'storage', pos: [0.2, 0, -5.0], glyph: 'CAT' },
	{ id: 'dbt', label: 'dbt', detail: 'Builds and tests the Gold marts through Trino.', group: 'processing', pos: [5.6, 0, 3.8], glyph: 'DBT' },

	// Query and BI
	{ id: 'trino', label: 'Trino', detail: 'Distributed SQL engine that reads every Iceberg layer.', group: 'processing', pos: [6.4, 0, -0.4], glyph: 'SQL' },
	{ id: 'jupyter', label: 'JupyterLab', detail: 'Read-only exploration notebooks over Gold.', group: 'destinations', pos: [10.0, 0, -3.0], glyph: 'NB' },
	{ id: 'superset', label: 'Superset', detail: 'Dashboards as code over the Gold marts.', group: 'destinations', pos: [10.4, 0, -0.4], glyph: 'BI' },
	{ id: 'site', label: 'Report site', detail: 'Static multi-page report with the platform figures.', group: 'destinations', pos: [10.0, 0, 2.4], glyph: 'WEB' },

	// Orchestration and quality
	{ id: 'airflow', label: 'Airflow', detail: 'Asset-driven daily and weekly schedules, backfills and retries.', group: 'orchestration', pos: [0.2, 5.6, 1.6], glyph: 'AF' },
	{ id: 'contracts', label: 'Data contracts', detail: 'ODCS table contracts checked with the Data Contract CLI, with quarantine for invalid rows.', group: 'orchestration', pos: [5.8, 0, -4.0], glyph: 'DQ' },

	// Observability and lineage
	{ id: 'prometheus', label: 'Prometheus', detail: 'Pipeline metrics: lag, throughput, failures and refresh duration.', group: 'observability', pos: [-3.0, 0, -7.4], glyph: 'MET' },
	{ id: 'grafana', label: 'Grafana', detail: 'Operations dashboards, alerts and runbooks.', group: 'observability', pos: [0.6, 0, -7.8], glyph: 'OPS' },
	{ id: 'openlineage', label: 'OpenLineage', detail: 'Run-level lineage from orchestration, Spark and dbt.', group: 'observability', pos: [4.2, 0, -7.8], glyph: 'LIN' },
	{ id: 'finops', label: 'FinOps model', detail: 'Cost drivers, unit costs and Iceberg snapshot retention.', group: 'observability', pos: [7.8, 0, -7.4], glyph: 'FIN' },

	// CI
	{ id: 'actions', label: 'GitHub Actions', detail: 'Lint, unit tests and security checks on every change.', group: 'orchestration', pos: [-7.2, 0, -5.6], glyph: 'CI' },
];

export const EDGES: SceneEdge[] = [
	// Batch ingestion
	{ from: 'olist', to: 'ingest', kind: 'batch' },
	{ from: 'dummyjson', to: 'ingest', kind: 'batch' },
	{ from: 'ibge', to: 'ingest', kind: 'batch' },
	{ from: 'bcb', to: 'ingest', kind: 'batch' },
	{ from: 'storefront', to: 'ingest', kind: 'batch' },
	{ from: 'postgres', to: 'ingest', kind: 'batch' },
	{ from: 'ingest', to: 'minio', kind: 'batch' },
	{ from: 'minio', to: 'spark', kind: 'batch' },
	{ from: 'spark', to: 'lakehouse', kind: 'batch' },
	{ from: 'lakehouse', to: 'trino', kind: 'batch' },
	{ from: 'dbt', to: 'trino', kind: 'batch' },
	{ from: 'trino', to: 'jupyter', kind: 'batch' },
	{ from: 'trino', to: 'superset', kind: 'batch' },
	{ from: 'trino', to: 'site', kind: 'batch' },

	// Streaming CDC
	{ from: 'postgres', to: 'debezium', kind: 'stream' },
	{ from: 'debezium', to: 'kafka', kind: 'stream' },
	{ from: 'kafka', to: 'spark', kind: 'stream' },

	// Orchestration and checks
	{ from: 'airflow', to: 'ingest', kind: 'control' },
	{ from: 'airflow', to: 'spark', kind: 'control' },
	{ from: 'airflow', to: 'dbt', kind: 'control' },
	{ from: 'airflow', to: 'contracts', kind: 'control' },
	{ from: 'contracts', to: 'lakehouse', kind: 'control' },
	{ from: 'catalog', to: 'lakehouse', kind: 'control' },
	{ from: 'actions', to: 'ingest', kind: 'control' },

	// Telemetry and lineage
	{ from: 'airflow', to: 'prometheus', kind: 'telemetry' },
	{ from: 'spark', to: 'prometheus', kind: 'telemetry' },
	{ from: 'prometheus', to: 'grafana', kind: 'telemetry' },
	{ from: 'airflow', to: 'openlineage', kind: 'telemetry' },
	{ from: 'dbt', to: 'openlineage', kind: 'telemetry' },
	{ from: 'lakehouse', to: 'finops', kind: 'telemetry' },
];

/**
 * Portrait layout for narrow screens: data flows from the top (back, -z)
 * to the bottom (front, +z) instead of left to right.
 */
export const MOBILE_POS: Record<string, Vec3> = {
	olist: [-3.6, 0, -10.8],
	dummyjson: [-1.2, 0, -9.6],
	ibge: [1.2, 0, -10.8],
	bcb: [3.6, 0, -9.6],
	storefront: [-2.4, 0, -7.8],
	postgres: [2.4, 0, -7.8],
	ingest: [-2.4, 0, -5.2],
	debezium: [2.4, 0, -5.2],
	airflow: [0, 2.2, -2.6],
	minio: [-3.4, 0, -2.6],
	kafka: [3.4, 0, -2.6],
	spark: [-3.4, 0, 0.4],
	lakehouse: [0, 0, 0.8],
	catalog: [3.6, 0, 1.4],
	dbt: [-3.4, 0, 3.6],
	contracts: [0, 0, 4.0],
	trino: [3.4, 0, 3.6],
	jupyter: [-3.2, 0, 6.4],
	superset: [0, 0, 6.6],
	site: [3.2, 0, 6.4],
	actions: [-3.4, 0, 8.6],
	prometheus: [-1.7, 0, 10.0],
	grafana: [0, 0, 8.6],
	openlineage: [1.7, 0, 10.0],
	finops: [3.4, 0, 8.6],
};
