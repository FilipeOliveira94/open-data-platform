-- P2-02 probe. A second run overwrites the same two rows.
CREATE NAMESPACE IF NOT EXISTS catalog_probe;

CREATE TABLE IF NOT EXISTS catalog_probe.probe (
    id INT,
    label STRING
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

INSERT OVERWRITE catalog_probe.probe VALUES (1, 'alpha'), (2, 'beta');

SELECT COUNT(*) AS row_count FROM catalog_probe.probe;
