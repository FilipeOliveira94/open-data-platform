-- P2-01 probe. A second run overwrites the same two rows.
CREATE NAMESPACE IF NOT EXISTS poc;

CREATE TABLE IF NOT EXISTS poc.probe (
    id INT,
    label STRING
) USING iceberg;

INSERT OVERWRITE poc.probe VALUES (1, 'alpha'), (2, 'beta');

SELECT COUNT(*) AS row_count FROM poc.probe;
