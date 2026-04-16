# Spark DevContainer Configuration

## Configuration Files

| File                                | Purpose                                                                    |
| ----------------------------------- | -------------------------------------------------------------------------- |
| `spark-defaults-breakdown.yaml`     | Driver/executor resource allocation as % of host (RAM, cores, parallelism) |
| `spark-defaults.conf.tmpl`          | Spark configuration with variable substitution                             |
| `hive-site.xml.tmpl`                | Hive metastore configuration                                               |
| `livy.conf.tmpl`                    | Livy server configuration                                                  |
| `livy-server-log4j.properties.tmpl` | Livy server logging (log4j 1.x)                                            |
| `livy-spark-log4j.properties.tmpl`  | Spark session logging (log4j2)                                             |

## Override

Create `spark-devcontainer.yaml` at your repo root pointing to your config files (paths relative to repo root). 

If missing, these defaults are used. See `spark-devcontainer.yaml.example` for schema.