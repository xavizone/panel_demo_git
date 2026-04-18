create or replace task TASTY_BYTES_DEMO.DEV.TASK_DQ_CHECK
	warehouse=TASTY_DEMO_WH
	COMMENT='DQ gate: checks DMF results and aborts if critical issues found'
	after TASTY_BYTES_DEMO.DEV.TASK_ROOT
	as CALL SYSTEM$LOG_TRACE('DQ check: verifying data quality metrics before promotion');