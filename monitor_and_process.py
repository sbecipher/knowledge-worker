import os
import time
import subprocess
from google import genai

def main():
    project_id = "sbecipherio"
    location = "us-central1"
    os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
    os.environ['GOOGLE_CLOUD_LOCATION'] = location

    client = genai.Client(vertexai=True, project=project_id, location=location)
    job_id = "5293171279602384896"
    job_name = f"projects/268910786343/locations/{location}/batchPredictionJobs/{job_id}"

    with open("monitor_log.txt", "w") as f:
        f.write(f"Starting monitor for job {job_id}...\n")

    while True:
        try:
            job = client.batches.get(name=job_name)
            state_str = str(job.state)
            
            with open("monitor_log.txt", "a") as f:
                f.write(f"[{time.ctime()}] Job state: {state_str}\n")
                
            if "RUNNING" not in state_str and "PENDING" not in state_str and "INITIALIZING" not in state_str:
                # Job finished!
                with open("monitor_log.txt", "a") as f:
                    f.write(f"[{time.ctime()}] Job completed with state {state_str}, proceeding to pipeline.\n")
                break
                
        except Exception as e:
            with open("monitor_log.txt", "a") as f:
                f.write(f"[{time.ctime()}] Error checking job: {e}\n")
        
        time.sleep(60)
        
    # Now run the scripts sequentially
    scripts = ["batch_parse.py", "aggregate_to_prod.py", "audit_integrity.py"]
    for script in scripts:
        with open("monitor_log.txt", "a") as f:
            f.write(f"\n=========================================\n")
            f.write(f"[{time.ctime()}] Running {script}...\n")
        try:
            res = subprocess.run(["python", script], capture_output=True, text=True)
            with open("monitor_log.txt", "a") as f:
                f.write(f"[{time.ctime()}] {script} return code: {res.returncode}\n")
                f.write(f"STDOUT:\n{res.stdout}\n")
                if res.stderr:
                    f.write(f"STDERR:\n{res.stderr}\n")
        except Exception as e:
            with open("monitor_log.txt", "a") as f:
                f.write(f"[{time.ctime()}] Failed to run {script}: {e}\n")
                
    with open("monitor_log.txt", "a") as f:
        f.write(f"[{time.ctime()}] All done!\n")

if __name__ == '__main__':
    main()
