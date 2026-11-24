#!/usr/bin/env python3
"""Execute both shipped VRL transforms in an isolated Vector container; no sinks/network."""
import json, os, pathlib, subprocess, tempfile, tomllib
root=pathlib.Path(__file__).resolve().parents[1]
for filename in ['vector.toml','vector-logs.toml']:
 data=tomllib.loads((pathlib.Path(os.getenv('VECTOR_CONFIG_ROOT',str(root/'deploy/observability')))/filename).read_text())
 source=data['transforms']['cellhive_logs_vwork']['source']
 cases=[
  ('handler failure','cron','failed',{'cellhive.error':'Error: cleanup failed token=secret-value Bearer abc.def.xyz https://db.test/rpc?key=private','cellhive.logical_id':'cron:one','cellhive.scheduled_time_ms':1000},
   '.error_code == "CELLHIVE_INVOCATION_FAILED" && .severity == "error" && .status == 500 && contains(string!(.error_message), "cleanup failed") && !contains(string!(.error_message), "secret-value") && !contains(string!(.error_message), "abc.def.xyz") && !contains(string!(.error_message), "private") && .payload.logical_id == "cron:one" && .payload.scheduled_time_ms == 1000'),
  ('wrapper success','scheduled','ok',{'cellhive.exceptions':'[]'},'.category == "runtime_telemetry" && .payload.outcome == "transport_ok" && .payload.outcome_scope == "wrapper_transport"'),
  ('wrapper error envelope','scheduled','ok',{'cellhive.error':'cleanup failed'},'.category == "cron_dispatch" && .error_code == "CELLHIVE_INVOCATION_FAILED" && .payload.outcome == "failed"'),
 ('queue requested retry','queue','retry',{},'.error_code == null && .payload.failure_stage == null'),
 ('fetch return error','fetch','error',{'cellhive.failure_stage':'handler_response'},'.error_code == "CELLHIVE_INVOCATION_FAILED" && .payload.failure_stage == "handler_response" && .payload.error_origin == "unknown"'),
 ('capacity failure','fetch','error',{'cellhive.failure_stage':'runtime_capacity'},'.payload.failure_stage == "runtime_capacity" && .payload.error_origin == "platform"'),
 ('completed user handler','queue','failed',{'cellhive.function_result':'threw'},'.payload.function_completed == true && .payload.function_result == "threw" && .payload.error_origin == "user_function" && .payload.root_cause_origin == "unknown"'),
 ('not started','fetch','failed',{'cellhive.function_result':'not_started','cellhive.failure_stage':'worker_load'},'.payload.function_completed == false && .payload.error_origin == "unknown"'),
 ('safe diagnostics','cron','failed',{'cellhive.error':'INTERNAL_ERROR','cellhive.error_name':'Error','cellhive.error_frame':'tenant.js:49:20','cellhive.failure_stage':'handler','cellhive.error_origin':'user'},'.payload.error_name == "Error" && .payload.error_frame == "tenant.js:49:20" && .payload.failure_stage == "handler" && .payload.error_origin == "unknown"'),
 ('unsafe diagnostics','cron','failed',{'cellhive.error_name':'Bearer private','cellhive.error_frame':'https://private/tenant.js:1:1','cellhive.failure_stage':'private'},'.payload.error_name == null && .payload.error_frame == null && .payload.failure_stage == "unknown"'),
  ('handler success','cron','success',{},'.category == "cron_dispatch" && .payload.outcome == "success" && .error_code == null'),
  ('tail exception','scheduled','exception',{'cellhive.exceptions':'[{"name":"Error","message":"broken handler"}]'},'.error_code == "CELLHIVE_INVOCATION_FAILED" && .status == 500 && contains(string!(.error_message), "broken handler")'),
  ('failure without message','cron','failed',{},'.error_code == "CELLHIVE_INVOCATION_FAILED" && .severity == "error" && .status == 500'),
 ]
 tests=[]
 for name,kind,outcome,extra,condition in cases:
  attrs={'cellhive.invocation':True,'cellhive.namespace':'vwork-105','cellhive.worker':'pageverra-api','cellhive.request_id':'r1','cellhive.event_type':kind,'cellhive.outcome':outcome,**extra}
  tests.append({'name':filename+': '+name,'inputs':[{'insert_at':'normalize','type':'log','log_fields':{'attributes':attrs,'message':'invocation','timestamp':'2026-11-24T12:30:00Z','observed_timestamp':'2026-11-24T12:30:01Z'}}], 'outputs':[{'extract_from':'normalize','conditions':[{'type':'vrl','source':condition}]}]})
 config={'sources':{'fixture':{'type':'stdin'}},'transforms':{'normalize':{'type':'remap','inputs':['fixture'],'source':'.timestamp = parse_timestamp!(.timestamp, format: "%+")\n.observed_timestamp = parse_timestamp!(.observed_timestamp, format: "%+")\n'+source}},'sinks':{'discard':{'type':'blackhole','inputs':['normalize']}},'tests':tests}
 with tempfile.TemporaryDirectory() as tmp:
  pathlib.Path(tmp,'test.json').write_text(json.dumps(config))
  subprocess.run(['docker','run','--rm','--network','none','-v',tmp+':/test:ro',os.getenv('VECTOR_TEST_IMAGE','timberio/vector:latest-alpine'),'test','--config-json','/test/test.json'],check=True)
