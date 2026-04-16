#!/bin/bash -e

export SPARK_HOME=/opt/spark
export LIVY_HOME=/opt/livy
export SCRIPT_DIR=$(realpath $(dirname $0))
source "${SCRIPT_DIR}/common.sh"

GIT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || echo "$(pwd)")
[ ! -d "$GIT_ROOT/.git" ] && echo "WARNING: Not inside a git repository. Using built-in defaults only."
export GIT_ROOT

# Speed up I/O as much as possible
#
if [ -d "$GIT_ROOT/.git" ]; then
    
    # >>> https://github.com/microsoft/vscode/issues/133215
    #
    git config oh-my-zsh.hide-info 1 2>/dev/null || true
    
    git config --global advice.detachedHead false 2>/dev/null || true
    git config --global advice.statusHints false 2>/dev/null || true

    if [ -f ~/.zshrc ]; then
        grep -q "DISABLE_AUTO_UPDATE" ~/.zshrc || echo "DISABLE_AUTO_UPDATE=true" >> ~/.zshrc || true
        grep -q "DISABLE_UPDATE_PROMPT" ~/.zshrc || echo "DISABLE_UPDATE_PROMPT=true" >> ~/.zshrc || true
    fi
fi

PROCESSED_FILES=()
CONFIG_SOURCES=()
USER_CONFIG_MSG=""

DEFAULT_CONFIG_DIR="/opt/spark-devcontainer/config/defaults"

if [ -d "$GIT_ROOT/.git" ]; then
    USER_CONFIG_FILE="${GIT_ROOT}/spark-devcontainer.yaml"
else
    USER_CONFIG_FILE=""
fi

if [ -d "$GIT_ROOT/.git" ]; then
    export LIVY_SPARK_LOG_DIR="${GIT_ROOT}/logs/livy"
else
    export LIVY_SPARK_LOG_DIR="/tmp/livy-logs"
fi

resolve_config_file() {
    local config_key=$1
    local default_filename=$2
    
    if [ -f "$USER_CONFIG_FILE" ]; then
        local user_path=$(yq eval ".config.${config_key}" "$USER_CONFIG_FILE")
        if [ "$user_path" != "null" ] && [ -n "$user_path" ]; then
            local full_path="${GIT_ROOT}/${user_path}"
            if [ -f "$full_path" ]; then
                echo "$full_path"
                return 0
            else
                echo "ERROR: Config file specified in spark-devcontainer.yaml not found: $full_path" >&2
                exit 1
            fi
        fi
    fi
    
    echo "${DEFAULT_CONFIG_DIR}/${default_filename}"
}

validate_range() {
    local value=$1
    local min=$2
    local max=$3
    local name=$4
    
    if [ "$value" -lt "$min" ] || [ "$value" -gt "$max" ]; then
        echo "ERROR: $name=$value is out of valid range [$min, $max]" >&2
        exit 1
    fi
}

calc_ram() {
    local pct=$1
    local total=$2
    echo $(( (total * pct) / 100 ))
}

calc_cores_clamped() {
    local pct=$1
    local total=$2
    local min=$3
    local result=$(( (total * pct) / 100 ))
    if [ "$result" -lt "$min" ]; then
        echo "$min"
    else
        echo "$result"
    fi
}

process_template() {
    local template_file=$1
    local output_file=$2
    
    PROCESSED_FILES+=("$(basename $template_file) → $(basename $output_file)")
    local temp_file=$(mktemp)
    envsubst < "$template_file" > "$temp_file"
    sudo install -m 644 -o vscode -g vscode "$temp_file" "$output_file"
    rm -f "$temp_file"
}

if [ -f "$USER_CONFIG_FILE" ]; then
    USER_CONFIG_MSG="Using user config: $(basename $USER_CONFIG_FILE)"

    if ! yq eval '.' "$USER_CONFIG_FILE" >/dev/null 2>&1; then
        echo "ERROR: Invalid YAML syntax in $USER_CONFIG_FILE" >&2
        exit 1
    fi
    
    version=$(yq eval '.version' "$USER_CONFIG_FILE")
    if [ "$version" != "1.0" ]; then
        echo "WARNING: Unexpected config version '$version', expected '1.0'" >&2
    fi
else
    USER_CONFIG_MSG="Using built-in defaults from devcontainer"
fi

SPARK_DEFAULTS_BREAKDOWN=$(resolve_config_file "spark_defaults_breakdown" "spark-defaults-breakdown.yaml")
SPARK_DEFAULTS_TEMPLATE=$(resolve_config_file "spark_defaults_template" "spark-defaults.conf.tmpl")
HIVE_SITE_TEMPLATE=$(resolve_config_file "hive_site_template" "hive-site.xml.tmpl")
LIVY_CONF_TEMPLATE=$(resolve_config_file "livy_conf_template" "livy.conf.tmpl")
LIVY_SERVER_LOG4J_TEMPLATE=$(resolve_config_file "livy_server_log4j_template" "livy-server-log4j.properties.tmpl")
LIVY_SPARK_LOG4J_TEMPLATE=$(resolve_config_file "livy_spark_log4j_template" "livy-spark-log4j.properties.tmpl")

CONFIG_SOURCES+=("spark-defaults-breakdown: $(basename $SPARK_DEFAULTS_BREAKDOWN)")
CONFIG_SOURCES+=("spark-defaults.conf:      $(basename $SPARK_DEFAULTS_TEMPLATE)")
CONFIG_SOURCES+=("hive-site.xml:            $(basename $HIVE_SITE_TEMPLATE)")
CONFIG_SOURCES+=("livy.conf:                $(basename $LIVY_CONF_TEMPLATE)")
CONFIG_SOURCES+=("livy-server-log4j:        $(basename $LIVY_SERVER_LOG4J_TEMPLATE)")
CONFIG_SOURCES+=("livy-spark-log4j:         $(basename $LIVY_SPARK_LOG4J_TEMPLATE)")

TOTAL_RAM_GB=$(free -g | awk '/^Mem:/{print $2}')
TOTAL_CORES=$(nproc)

DRIVER_PCT_RAM=$(yq eval '.driver.pct_ram' "$SPARK_DEFAULTS_BREAKDOWN")
DRIVER_PCT_CORES=$(yq eval '.driver.pct_cores' "$SPARK_DEFAULTS_BREAKDOWN")
EXECUTOR_PCT_RAM=$(yq eval '.executor.pct_ram' "$SPARK_DEFAULTS_BREAKDOWN")
EXECUTOR_PCT_CORES=$(yq eval '.executor.pct_cores' "$SPARK_DEFAULTS_BREAKDOWN")
MIN_CORES=$(yq eval '.resource_allocation.min_cores' "$SPARK_DEFAULTS_BREAKDOWN")
SHUFFLE_PARTITIONS=$(yq eval '.parallelism.shuffle_partitions' "$SPARK_DEFAULTS_BREAKDOWN")
DEFAULT_PARALLELISM=$(yq eval '.parallelism.default_parallelism' "$SPARK_DEFAULTS_BREAKDOWN")

validate_range "$DRIVER_PCT_RAM" 1 100 "driver.pct_ram"
validate_range "$DRIVER_PCT_CORES" 1 100 "driver.pct_cores"
validate_range "$EXECUTOR_PCT_RAM" 1 100 "executor.pct_ram"
validate_range "$EXECUTOR_PCT_CORES" 1 100 "executor.pct_cores"
validate_range "$MIN_CORES" 1 "$TOTAL_CORES" "resource_allocation.min_cores"
validate_range "$SHUFFLE_PARTITIONS" 1 10000 "parallelism.shuffle_partitions"
validate_range "$DEFAULT_PARALLELISM" 1 10000 "parallelism.default_parallelism"

export SPARK_DRIVER_MEMORY="$(calc_ram $DRIVER_PCT_RAM $TOTAL_RAM_GB)g"
export SPARK_DRIVER_CORES=$(calc_cores_clamped $DRIVER_PCT_CORES $TOTAL_CORES $MIN_CORES)
export SPARK_EXECUTOR_MEMORY="$(calc_ram $EXECUTOR_PCT_RAM $TOTAL_RAM_GB)g"
export SPARK_EXECUTOR_CORES=$(calc_cores_clamped $EXECUTOR_PCT_CORES $TOTAL_CORES $MIN_CORES)
export SPARK_SUBMIT_SHUFFLE_PARTITIONS=$SHUFFLE_PARTITIONS
export SPARK_SUBMIT_DEFAULT_PARALLELISM=$DEFAULT_PARALLELISM

sudo mkdir -p /opt/spark/conf
sudo chown -R vscode:vscode /opt/spark/conf

process_template "$SPARK_DEFAULTS_TEMPLATE" "/opt/spark/conf/spark-defaults.conf"
process_template "$HIVE_SITE_TEMPLATE" "/opt/spark/conf/hive-site.xml"

sudo mkdir -p /opt/livy/conf
sudo chown -R vscode:vscode /opt/livy/conf
mkdir -p "$LIVY_SPARK_LOG_DIR"

process_template "$LIVY_CONF_TEMPLATE" "/opt/livy/conf/livy.conf"
process_template "$LIVY_SERVER_LOG4J_TEMPLATE" "/opt/livy/conf/livy-server-log4j.properties"
process_template "$LIVY_SPARK_LOG4J_TEMPLATE" "/opt/livy/conf/livy-spark-log4j.properties"

sudo bash -c "cat > /opt/livy/conf/livy-env.sh" << EOF
export SPARK_HOME=/opt/spark
export SPARK_CONF_DIR=/opt/spark/conf
export LIVY_SERVER_JAVA_OPTS="-Dlog4j.configuration=file:/opt/livy/conf/livy-server-log4j.properties"
EOF
sudo chown vscode:vscode /opt/livy/conf/livy-env.sh
sudo chmod 644 /opt/livy/conf/livy-env.sh

LIVY_STATUS=""
if pgrep -f "livy.server.LivyServer" >/dev/null; then
    LIVY_STATUS="Already running"
else
    $LIVY_HOME/bin/livy-server start
    
    retries=0
    max_retries=30
    until curl -s http://localhost:8998/sessions >/dev/null 2>&1; do
        sleep 1
        retries=$((retries + 1))
        if [ $retries -ge $max_retries ]; then
            echo "ERROR: Livy failed to start within ${max_retries}s timeout" >&2
            echo "Check logs at: $LIVY_SPARK_LOG_DIR/livy-server.log" >&2
            exit 1
        fi
    done
    LIVY_STATUS="Started successfully"
fi

echo
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  CONFIGURATION"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  $USER_CONFIG_MSG"
echo
echo "  Config Sources:"
for source in "${CONFIG_SOURCES[@]}"; do
    echo "    $source"
done
echo
echo "  Processed Templates:"
for file in "${PROCESSED_FILES[@]}"; do
    echo "    $file"
done
echo
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  RESOURCES"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Host:     ${TOTAL_RAM_GB}GB RAM, ${TOTAL_CORES} cores"
echo
echo "  Allocated:"
echo "    Driver:   ${SPARK_DRIVER_MEMORY} RAM, ${SPARK_DRIVER_CORES} cores"
echo "    Executor: ${SPARK_EXECUTOR_MEMORY} RAM, ${SPARK_EXECUTOR_CORES} cores"
echo "    Shuffle partitions: ${SHUFFLE_PARTITIONS}"
echo "    Default parallelism: ${DEFAULT_PARALLELISM}"
echo
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  SPARK DEVCONTAINER READY"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Livy Server: http://localhost:8998 ($LIVY_STATUS)"
echo "  Logs:        $LIVY_SPARK_LOG_DIR"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo