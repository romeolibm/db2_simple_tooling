# db2_simple_tooling
A collection of simple monitoring tools written mainly in python for monitoring db2 database systems

# srcPy/mon_sem_usage_log.py
Monitor semaphore usage for db2 processesa and the rest of the system and reaport in a .csv file 
with row structure (ts,db2inst_semcnt,db2fmp_semcnt,sys_semcnt,max_sem)
