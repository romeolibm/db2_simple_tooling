#!/usr/bin/python
"""
  @author: Romeo Lupascu
  @contact: romeol@ca.ibm.com
  @organization: IBM
  @since: 2020-06-15
    
    Log the current db2 node semaphore count and max sem count 
    for the db2 processes and the rest of the system.
    
    Execute the command from a cron schedule to produce a csv file
    with the default name db2andsyssems.csv and structure:
    
    ts,db2inst_semcnt,db2fmp_semcnt,sys_semcnt,max_sem
    
"""
import os, sys, subprocess, time

def get_db2_inst_fmp_user():
    """
    Retrieve the db2 instance and fmp user names
    """
    instusr = subprocess.Popen(
        ["ps", "-o", "user", "-C", "db2sysc"],
        stdout=subprocess.PIPE
    ).communicate()[0].strip().splitlines()[1].strip()
    fmpusr = [x for x in subprocess.Popen(
        ["ps", "-o", "user", "-C", "db2fmp"],
        stdout=subprocess.PIPE
    ).communicate()[0].strip().splitlines() if not x == instusr][1].strip()
    return instusr,fmpusr
    
def get_system_semaphores(*users):
    """
    List all semaphores in the system  ipcs -s
    as a list and if user names are provided filter by user name
    """
    l = [x.split()[2:] for x in subprocess.Popen(
        ["ipcs", "-s"],
        stdout=subprocess.PIPE
    ).communicate()[0].strip().splitlines()[2:]]
    m = {}
    for t in [(x[0], int(x[2])) for x in l if not users or x[0] in users]:
        if not m.has_key(t[0]):
            m[t[0]] = t[1]
        else:
            m[t[0]] = m[t[0]] + t[1]
    
    return m

def getSystemWideMaxSemaphores():
    """
    Retrieve the current maximum allowed semaphores
    in the system 
    """
    return int([x for x in subprocess.Popen(
            ["ipcs", "-sl"],
            stdout=subprocess.PIPE
            ).communicate()[0].strip().splitlines()[2:] 
                if x.startswith("max semaphores system wide")
        ][0].split("=")[1].strip()
    )
    
def getDB2OwnedSemaphores(db2instusr=None, db2fmpusr=None):
    """
    Return a map containing the current semaphore cout
    allocated by db2 and in the system (other then db2)
    {
      'sys':sem_cnt,
      db2instusr:sem_cnt,
      db2fmpusr:sem_cnt
    }
    """
    if not db2instusr:
        db2instusr,db2fmpusr=get_db2_inst_fmp_user()
    
    sm = get_system_semaphores()
    
    m = {'sys':0, db2instusr:0, db2fmpusr:0}
    for k in sm:
        if (k == db2instusr) or (k == db2fmpusr):
            m[k] = m[k] + sm[k]
        else:
            m['sys'] = m['sys'] + sm[k]
    
    return m

def logDB2OwnedSemaphores(logFn="db2andsyssems.csv"):
    """
    Retrieve the max semaphores in the system the 
    semaphore count allocated by the db2instance user
    and fmp user and the rest of the apps them log a record
    in csv format into the logFn file.
    
    Row structure: ts,db2inst_semcnt,db2fmp_semcnt,sys_semcnt,max_sem
    """
    db2instusr,db2fmpusr=get_db2_inst_fmp_user()    
    ts = time.time()
    maxsems = getSystemWideMaxSemaphores()
    m = getDB2OwnedSemaphores(db2instusr,db2fmpusr)
    
    if os.path.exists(logFn):
        out = open(logFn, "a")
    else:
        out = open(logFn, "w")
        print >> out, "ts,db2inst_semcnt,db2fmp_semcnt,sys_semcnt,max_sem"
    
    print >> out, "%s,%s,%s,%s,%s" % (
        ts, m[db2instusr], m[db2fmpusr], m['sys'], maxsems
    )
    out.flush()
    out.close()
    
def continousCapture(
        logFn="db2andsyssems.csv",
        timeIntervalSec=10,
        maxTimeMinutes=60
    ):
    """
    Call logDB2OwnedSemaphores in a loop and sleep timeIntervalSec
    between the calls, formaxTimeMinutes minutes
    """
    print "Start collecting information for %s minutes at every %s seconds into %s" % (
        "unlimited" if maxTimeMinutes==0 else maxTimeMinutes,timeIntervalSec,logFn
    )
    
    cnt=0
    starts = time.time()
    maxtime = starts + maxTimeMinutes * 60 if maxTimeMinutes else 0
    while True:
        if (maxtime>0) and (time.time() > maxtime):
            break
        logDB2OwnedSemaphores(logFn)
        cnt+=1
        time.sleep(timeIntervalSec)
    print "End collecting semaphore usage, %s rows added to %s" % (cnt,logFn)
    
def main():
    collectionIntervalSec = 0
    collectionTimeMin = 0
    logFn = "db2andsyssems.csv"
    
    if len(sys.argv) > 1:
        if sys.argv[1] in ('?','-h','--help'):
            print sys.argv[0] + " [csv-log-file-name def:db2andsyssems.csv] [interval-sec def:0(disable)] [max-collect-time-min def:60]"
            return
        logFn = sys.argv[1]
        
    if len(sys.argv) > 2:
        collectionIntervalSec = float(sys.argv[2])
    if len(sys.argv) > 3:
        collectionTimeMin = float(sys.argv[3])
        
    if collectionIntervalSec == 0:
        logDB2OwnedSemaphores(logFn)
    else:
        continousCapture(logFn, collectionIntervalSec, collectionTimeMin)
    
if __name__ == '__main__':
    main()
