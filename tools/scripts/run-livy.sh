#!/bin/bash -e
#
# Livy Session Management Functions
#
# Example usage:
#
#   ```bash
#   GIT_ROOT=$(git rev-parse --show-toplevel)
#   source "${GIT_ROOT}/projects/spark-scala/.scripts/run-livy.sh"
#
#   # Start the Livy server
#   start_livy
#
#   # Create a new Spark session (default) or PySpark session
#   SESSION_ID=$(create_session spark | tail -1)
#
#   # Execute Scala/Spark code with optional timeout (default: 120s)
#   execute_code "$SESSION_ID" 'spark.sql("SELECT 1 as test").show()' 60
#
#   # Execute SQL query with optional timeout (default: 120s)
#   execute_sql "$SESSION_ID" 'SELECT 1 AS foo' 60
#
#   # Delete a specific session
#   kill_session "$SESSION_ID"
#
#   # Delete all active sessions
#   kill_all_sessions
#
#   # Stop the Livy server
#   stop_livy
#   ```
#
# ---------------------------------------------------------------------------------------
#

export SPARK_HOME=/opt/spark
export LIVY_HOME=/opt/livy
export LIVY_URL="http://localhost:8998"

# Stop the Livy server and kill any running processes
stop_livy() {
    echo "Stopping Apache Livy..."
    
    if pgrep -f "livy.server.LivyServer" >/dev/null; then
        $LIVY_HOME/bin/livy-server stop 2>/dev/null || true
        sleep 2
        pkill -f "livy.server.LivyServer" 2>/dev/null || true
        echo "Livy Server stopped"
    else
        echo "Livy Server is not running"
    fi
}

# Start the Livy server and wait until it's ready
start_livy() {
    echo "Starting Apache Livy..."

    if [ ! -f "$LIVY_HOME/bin/livy-server" ]; then
        echo "ERROR: Livy is not installed. Rebuild the dev container."
        return 1
    fi

    if pgrep -f "livy.server.LivyServer" >/dev/null; then
        echo "Livy Server already running"
    else
        echo "Livy is not running. Starting..."
        $LIVY_HOME/bin/livy-server start

        echo "Waiting for Livy to be ready..."
        local retries=0
        local max_retries=30
        until curl -s http://localhost:8998/sessions >/dev/null 2>&1; do
            sleep 2
            retries=$((retries + 1))
            if [ $retries -ge $max_retries ]; then
                echo "ERROR: Livy failed to start within timeout"
                return 1
            fi
        done
        echo "Livy is ready!"
    fi

    echo
    echo "----------------------------------"
    echo "Livy UI:    http://localhost:8998"
    echo "----------------------------------"
}

# Create a new Livy session and wait until idle. Prints session ID to stdout.
# Supported kinds: spark (Scala, default), pyspark (Python), sparkr (R), sql (SQL-only)
create_session() {
    local KIND="${1:-spark}"
    
    echo "Creating Livy $KIND session..."
    RESPONSE=$(curl -s -X POST "${LIVY_URL}/sessions" \
        -H "Content-Type: application/json" \
        -d "{\"kind\": \"$KIND\"}")

    SESSION_ID=$(echo "$RESPONSE" | jq -r '.id')
    
    if [ "$SESSION_ID" = "null" ] || [ -z "$SESSION_ID" ]; then
        echo "ERROR: Failed to create session"
        echo "$RESPONSE" | jq .
        return 1
    fi
    
    echo "Session ID: $SESSION_ID"

    echo "Waiting for session to be idle..."
    local retries=0
    local max_retries=60
    while true; do
        STATE=$(curl -s "${LIVY_URL}/sessions/${SESSION_ID}" | jq -r '.state')
        echo "  Session state: $STATE"
        if [ "$STATE" = "idle" ]; then
            break
        elif [ "$STATE" = "dead" ] || [ "$STATE" = "error" ]; then
            echo "Session failed to start!"
            echo "Check logs: curl -s ${LIVY_URL}/sessions/${SESSION_ID}/log | jq '.log[]'"
            return 1
        fi
        sleep 2
        retries=$((retries + 1))
        if [ $retries -ge $max_retries ]; then
            echo "ERROR: Session creation timed out"
            return 1
        fi
    done

    echo "Session $SESSION_ID is ready!"
    echo "$SESSION_ID"
}

# Execute code on a Livy session with optional timeout (default: 120s). Polls until completion.
execute_code() {
    local SESSION_ID="$1"
    local CODE="$2"
    local TIMEOUT="${3:-120}"

    if [ -z "$SESSION_ID" ] || [ -z "$CODE" ]; then
        echo "Usage: execute_code <session_id> <code> [timeout_seconds]"
        return 1
    fi

    echo "Executing code on session $SESSION_ID (timeout: ${TIMEOUT}s)..."

    local ESCAPED_CODE=$(echo "$CODE" | jq -Rs .)
    
    RESPONSE=$(curl -s -X POST "${LIVY_URL}/sessions/${SESSION_ID}/statements" \
        -H "Content-Type: application/json" \
        -d "{\"code\": $ESCAPED_CODE}")

    STATEMENT_ID=$(echo "$RESPONSE" | jq -r '.id')
    
    if [ "$STATEMENT_ID" = "null" ] || [ -z "$STATEMENT_ID" ]; then
        echo "ERROR: Failed to submit statement"
        echo "$RESPONSE" | jq .
        return 1
    fi

    echo "Statement ID: $STATEMENT_ID"

    local start_time=$(date +%s)
    while true; do
        RESULT=$(curl -s "${LIVY_URL}/sessions/${SESSION_ID}/statements/${STATEMENT_ID}")
        STATE=$(echo "$RESULT" | jq -r '.state')

        if [ "$STATE" = "available" ]; then
            echo "Output:"
            echo "$RESULT" | jq '.output'
            break
        elif [ "$STATE" = "error" ] || [ "$STATE" = "cancelled" ]; then
            echo "Statement failed!"
            echo "$RESULT" | jq '.output'
            return 1
        fi
        
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        echo "  Elapsed: ${elapsed}s, state: $STATE)"
        
        if [ $elapsed -ge $TIMEOUT ]; then
            echo "ERROR: Statement execution timed out after ${elapsed}s"
            return 1
        fi
        
        sleep 5
    done
}

# Execute SQL query on a Livy session with optional timeout (default: 120s). Wrapper for execute_code.
execute_sql() {
    local SESSION_ID="$1"
    local SQL="$2"
    local TIMEOUT="${3:-120}"
    
    if [ -z "$SESSION_ID" ] || [ -z "$SQL" ]; then
        echo "Usage: execute_sql <session_id> <sql_query> [timeout_seconds]"
        return 1
    fi
    
    execute_code "$SESSION_ID" "$SQL" "$TIMEOUT"
}

# Delete a specific Livy session by ID
kill_session() {
    local SESSION_ID="$1"
    
    if [ -z "$SESSION_ID" ]; then
        echo "Usage: kill_session <session_id>"
        echo "List sessions: curl -s ${LIVY_URL}/sessions | jq '.sessions[] | {id, state}'"
        return 1
    fi
    
    echo "Killing session $SESSION_ID..."
    
    RESPONSE=$(curl -s -X DELETE "${LIVY_URL}/sessions/${SESSION_ID}")
    
    if [ -z "$RESPONSE" ] || [ "$RESPONSE" = "null" ]; then
        echo "Session $SESSION_ID deleted successfully"
        return 0
    else
        echo "Response: $RESPONSE"
        return 0
    fi
}

# Delete all active Livy sessions
kill_all_sessions() {
    echo "Killing all Livy sessions..."
    
    SESSION_IDS=$(curl -s "${LIVY_URL}/sessions" | jq -r '.sessions[].id')
    
    if [ -z "$SESSION_IDS" ]; then
        echo "No active sessions found"
        return 0
    fi
    
    for sid in $SESSION_IDS; do
        kill_session "$sid"
    done
    
    echo "All sessions killed"
}