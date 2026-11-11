# Spark secrets stay off the spark-sql argument list.
# Source this file. Do not execute it.
# S3 keys and the JDBC password are written to a mode-600 properties file
# and passed with --properties-file. Spark redacts password, secret, and
# access.key when it logs SparkConf.

odp_escape_property() {
  local value=$1
  local escaped=${value//\\/\\\\}
  escaped=${escaped//$'\n'/\\n}
  escaped=${escaped//$'\r'/\\r}
  printf '%s' "$escaped"
}

odp_write_secret_properties() {
  local mode=$1
  local file old_umask
  if [[ "$mode" != "jdbc" && "$mode" != "s3" ]]; then
    echo "odp_write_secret_properties: mode must be jdbc or s3" >&2
    return 1
  fi
  old_umask=$(umask)
  umask 077
  file="$(mktemp "${TMPDIR:-/tmp}/odp-spark-secrets.XXXXXXXX")"
  umask "$old_umask"
  {
    printf 'spark.hadoop.fs.s3a.access.key=%s\n' \
      "$(odp_escape_property "${MINIO_ROOT_USER}")"
    printf 'spark.hadoop.fs.s3a.secret.key=%s\n' \
      "$(odp_escape_property "${MINIO_ROOT_PASSWORD}")"
    if [[ "$mode" == "jdbc" ]]; then
      printf 'spark.sql.catalog.local.jdbc.password=%s\n' \
        "$(odp_escape_property "${POSTGRES_PASSWORD}")"
    fi
  } >"$file"
  chmod 600 "$file"
  printf '%s\n' "$file"
}
