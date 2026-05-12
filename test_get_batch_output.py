import os
from google import genai

def main():
    project_id = "sbecipherio"
    location = "us-central1"
    os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
    os.environ['GOOGLE_CLOUD_LOCATION'] = location

    client = genai.Client(vertexai=True, project=project_id, location=location)

    job_id = "6124173371782463488"
    job = client.batches.get(name=f"projects/268910786343/locations/{location}/batchPredictionJobs/{job_id}")
    
    print("Job Information:")
    if hasattr(job, 'job_info'):
        print(f"job_info: {job.job_info}")
    
    # Try to find the output directory
    if hasattr(job, 'dest') and hasattr(job.dest, 'gcs_uri'):
        print(f"Dest GCS URI: {job.dest.gcs_uri}")
        
    print(job)

if __name__ == '__main__':
    main()
