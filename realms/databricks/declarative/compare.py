"""Compare the declarative silver (schema declarative_proof) with the job-built silver, dataset by dataset:
current live rows (history) or all rows (append), version counts, and rows that differ on the business columns.

    python realms/databricks/declarative/compare.py      (databricks CLI, profile free)
"""
import json,subprocess
def q(sql):
    r=json.loads(subprocess.run(["databricks","api","post","/api/2.0/sql/statements","-p","free","--json",json.dumps({"warehouse_id":"62a5ea0c0e6e5701","statement":sql,"wait_timeout":"50s"})],capture_output=True,text=True).stdout)
    if r["status"]["state"]!="SUCCEEDED": raise SystemExit(sql[:80]+" -> "+r["status"].get("error",{}).get("message","")[:200])
    return r.get("result",{}).get("data_array") or []
DS={"dealer_api.dealer":"scd","erp.delivery":"scd","erp.sales_order_line":"scd","lims.inspection_result":"append","mes.production":"append","press_stream.press_reading":"append"}
allok=True
for ds,kind in DS.items():
    s,t=ds.split("."); J=f"dev_silver.{s}.{t}"; D=f"dev_silver.declarative_proof.{s}__{t}"
    jc=[r[0] for r in q(f"DESCRIBE {J}") if r[0] and not r[0].startswith("#")]
    dc=[r[0] for r in q(f"DESCRIBE {D}") if r[0] and not r[0].startswith("#")]
    cols=", ".join(f"`{c}`" for c in jc if not c.startswith("_") and c in dc)
    if kind=="scd":
        jw="_is_current AND NOT coalesce(_is_deleted,false)"; dw="__END_AT IS NULL" if "__END_AT" in dc else "true"
        n=(q(f"SELECT count(*) FROM {J} WHERE {jw}")[0][0], q(f"SELECT count(*) FROM {D} WHERE {dw}")[0][0])
        v=(q(f"SELECT count(*) FROM {J}")[0][0], q(f"SELECT count(*) FROM {D}")[0][0])
    else:
        # shiftleft: allow SL-SEC-01 -- developer script: table names come from the fixed DS list, columns are read back from our own tables
        jw=dw="true"; n=(q(f"SELECT count(*) FROM {J}")[0][0], q(f"SELECT count(*) FROM {D}")[0][0]); v=n
    # shiftleft: allow SL-SEC-01 -- developer script: table names come from the fixed DS list, columns are read back from our own tables
    a=q(f"SELECT count(*) FROM (SELECT {cols} FROM {J} WHERE {jw} EXCEPT ALL SELECT {cols} FROM {D} WHERE {dw})")[0][0]
    # shiftleft: allow SL-SEC-01 -- developer script: table names come from the fixed DS list, columns are read back from our own tables
    b=q(f"SELECT count(*) FROM (SELECT {cols} FROM {D} WHERE {dw} EXCEPT ALL SELECT {cols} FROM {J} WHERE {jw})")[0][0]
    ok = n[0]==n[1] and v[0]==v[1] and a=="0" and b=="0"
    allok&=ok
    print(f"{'EQUAL ' if ok else 'DIFFER'} {ds:28} {kind:6} current/rows job {n[0]:>7} decl {n[1]:>7} | versions {v[0]:>7}/{v[1]:>7} | only-in-job {a} only-in-decl {b} | {len(cols.split(','))} business columns")
print("ALL EQUAL" if allok else "SOME DIFFER")
