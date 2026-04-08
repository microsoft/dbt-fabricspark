"""Test Livy DML scenarios for schema-enabled lakehouses."""
import json
import time
import sys
import requests

WORKSPACE_ID = "e4487eff-d67d-4b58-917c-ffbb61a5c05f"
LAKEHOUSE_ID = "529ef82e-a552-4b87-afd6-1023cd7c906f"
BASE_URL = f"https://msitapi.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/lakehouses/{LAKEHOUSE_ID}/livyapi/versions/2023-12-01"

TOKEN = sys.argv[1] if len(sys.argv) > 1 else input("Paste your token: ").strip()

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {TOKEN}",
}


def create_session():
    print("\n=== Creating Livy session ===")
    r = requests.post(
        f"{BASE_URL}/sessions",
        headers=HEADERS,
        json={"kind": "sql", "configuration": {"conf": {"spark.sql.session.timeZone": "UTC"}}},
    )
    r.raise_for_status()
    sid = str(r.json()["id"])
    print(f"Session ID: {sid}")
    # Wait for idle
    for _ in range(120):
        time.sleep(5)
        sr = requests.get(f"{BASE_URL}/sessions/{sid}", headers=HEADERS)
        state = sr.json().get("state")
        print(f"  Session state: {state}")
        if state == "idle":
            return sid
        if state in ("dead", "error", "killed"):
            raise RuntimeError(f"Session failed: {sr.json()}")
    raise TimeoutError("Session did not become idle")


def submit(session_id, code, kind="sql", label=""):
    print(f"\n--- {label} (kind={kind}) ---")
    print(f"  Code: {code[:120]}{'...' if len(code) > 120 else ''}")
    r = requests.post(
        f"{BASE_URL}/sessions/{session_id}/statements",
        headers=HEADERS,
        json={"code": code, "kind": kind},
    )
    r.raise_for_status()
    stmt_id = str(r.json()["id"])
    # Poll
    for _ in range(120):
        time.sleep(3)
        sr = requests.get(f"{BASE_URL}/sessions/{session_id}/statements/{stmt_id}", headers=HEADERS)
        res = sr.json()
        if res["state"] == "available":
            status = res["output"]["status"]
            if status == "ok":
                print(f"  RESULT: OK")
                data = res["output"].get("data", {})
                if "application/json" in data:
                    rows = data["application/json"].get("data", [])
                    if rows:
                        print(f"  Rows: {rows[:5]}{'...' if len(rows) > 5 else ''}")
                elif "text/plain" in data:
                    print(f"  Output: {data['text/plain'][:300]}")
            else:
                print(f"  RESULT: ERROR")
                print(f"  {res['output'].get('evalue', 'Unknown')[:200]}")
            return status
        if res["state"] in ("error", "cancelled"):
            print(f"  RESULT: {res['state']}")
            print(f"  {res.get('output', {}).get('evalue', 'Unknown')[:200]}")
            return res["state"]
    print("  RESULT: TIMEOUT")
    return "timeout"


def delete_session(session_id):
    print(f"\n=== Deleting session {session_id} ===")
    requests.delete(f"{BASE_URL}/sessions/{session_id}", headers=HEADERS)
    print("Done")


def main():
    sid = create_session()
    results = {}

    try:
        # Setup
        submit(sid, "CREATE SCHEMA IF NOT EXISTS dbt_lakehouse_with_schema.test_pyspark_dml", "sql", "S1: Create schema")
        submit(sid, "CREATE TABLE IF NOT EXISTS dbt_lakehouse_with_schema.test_pyspark_dml.target (id BIGINT, name STRING) USING delta", "sql", "S2: Create table")
        submit(sid, "INSERT INTO dbt_lakehouse_with_schema.test_pyspark_dml.target VALUES (1, 'Alice'), (2, 'Bob')", "sql", "S3: Seed data")
        submit(sid, "CREATE OR REPLACE TEMPORARY VIEW test_source AS SELECT 3 AS id, 'Charlie' AS name", "sql", "S4: Create temp view")

        # T1: SQL INSERT INTO SELECT (expected: FAILS)
        results["T1_sql_insert_select"] = submit(sid, "INSERT INTO dbt_lakehouse_with_schema.test_pyspark_dml.target SELECT * FROM test_source", "sql", "T1: SQL INSERT INTO SELECT (3-part)")

        # T2: PySpark insertInto with 2-part name
        results["T2_pyspark_insert_2part"] = submit(sid, "spark.table('test_source').write.mode('append').insertInto('test_pyspark_dml.target')", "pyspark", "T2: PySpark insertInto (2-part)")

        # T3: SQL MERGE INTO (expected: FAILS)
        results["T3_sql_merge"] = submit(sid, "MERGE INTO dbt_lakehouse_with_schema.test_pyspark_dml.target AS dest USING test_source AS src ON dest.id = src.id WHEN NOT MATCHED THEN INSERT *", "sql", "T3: SQL MERGE INTO (3-part)")

        # T4: PySpark DeltaTable.forName merge with 2-part name
        results["T4_pyspark_merge_2part"] = submit(sid, "from delta.tables import DeltaTable\ndt = DeltaTable.forName(spark, 'test_pyspark_dml.target')\ndt.alias('dest').merge(spark.table('test_source').alias('src'), 'dest.id = src.id').whenNotMatchedInsertAll().execute()", "pyspark", "T4: PySpark MERGE (2-part)")

        # T5: SQL INSERT OVERWRITE (expected: FAILS)
        results["T5_sql_overwrite"] = submit(sid, "INSERT OVERWRITE TABLE dbt_lakehouse_with_schema.test_pyspark_dml.target SELECT * FROM test_source", "sql", "T5: SQL INSERT OVERWRITE (3-part)")

        # T6: PySpark overwrite with 2-part name
        results["T6_pyspark_overwrite_2part"] = submit(sid, "spark.table('test_source').write.mode('overwrite').insertInto('test_pyspark_dml.target')", "pyspark", "T6: PySpark OVERWRITE (2-part)")

        # T7: PySpark spark.sql() DESCRIBE TABLE EXTENDED with 3-part name
        results["T7_pyspark_sparksql_describe"] = submit(sid, "result = spark.sql('DESCRIBE TABLE EXTENDED dbt_lakehouse_with_schema.test_pyspark_dml.target').collect()\nfor r in result:\n    print(r)", "pyspark", "T7: PySpark spark.sql() DESCRIBE (3-part)")

        # T8: PySpark spark.table() with 3-part name
        results["T8_pyspark_table_3part"] = submit(sid, "spark.table('dbt_lakehouse_with_schema.test_pyspark_dml.target').show()", "pyspark", "T8: PySpark spark.table() (3-part)")

        # T9: Verify final data
        submit(sid, "SELECT * FROM dbt_lakehouse_with_schema.test_pyspark_dml.target ORDER BY id", "sql", "T9: Verify data")

        # Cleanup
        submit(sid, "DROP TABLE IF EXISTS dbt_lakehouse_with_schema.test_pyspark_dml.target", "sql", "Cleanup: Drop table")
        submit(sid, "DROP SCHEMA IF EXISTS dbt_lakehouse_with_schema.test_pyspark_dml", "sql", "Cleanup: Drop schema")

    finally:
        delete_session(sid)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for k, v in results.items():
        status = "PASS" if v == "ok" else "FAIL"
        print(f"  {k:40s} {status}")


if __name__ == "__main__":
    main()
