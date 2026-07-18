expected_append_csv = """
id,msg
1,hello
2,goodbye
2,yo
3,anyway
""".lstrip()

expected_overwrite_csv = """
id,msg
2,yo
3,anyway
""".lstrip()

expected_partial_upsert_csv = """
id,msg,color
1,hello,blue
2,yo,red
3,anyway,purple
""".lstrip()

expected_upsert_csv = """
id,msg
1,hello
2,yo
3,anyway
""".lstrip()

expected_skip_matched_csv = """
id,msg,color
1,hello,blue
2,goodbye,red
3,anyway,purple
""".lstrip()

expected_skip_not_matched_csv = """
id,msg,color
1,hey,cyan
2,yo,green
""".lstrip()

expected_matched_condition_csv = """
id,first_name,last_name,v
1,Jessica,Atreides,2
2,Paul,Atreides,1
3,Dunkan,Aidaho,1
4,Baron,Harkonnen,1
""".lstrip()

expected_not_matched_by_source_delete_csv = """
id,first_name,last_name,v
2,Paul,Atreides,0
3,Dunkan,Aidaho,1
4,Baron,Harkonnen,1
""".lstrip()

expected_not_matched_by_source_update_csv = """
id,first_name,last_name,v
1,--,--,-1
2,Paul,Atreides,0
3,Dunkan,Aidaho,1
4,Baron,Harkonnen,1
""".lstrip()

expected_merge_schema_evolution_csv = """
id,first_name,last_name,v
1,Jessica,Atreides,1
2,Paul,Atreides,
3,Dunkan,Aidaho,2
""".lstrip()
