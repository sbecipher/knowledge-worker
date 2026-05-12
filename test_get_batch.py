import os
from google import genai
from google.genai import types

def main():
    project_id = "sbecipherio"
    location = "us-central1"
    os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
    os.environ['GOOGLE_CLOUD_LOCATION'] = location

    client = genai.Client(vertexai=True, project=project_id, location=location)

    try:
        job_id = "5293171279602384896"
        job = client.batches.get(name=f"projects/268910786343/locations/{location}/batchPredictionJobs/{job_id}")
        print(f"Job Name: {job.name}")
        print(f"Job State: {job.state}")
        
        # print all properties to see where the output is
        for attr in dir(job):
            if not attr.startswith('_'):
                print(f"{attr}: {getattr(job, attr)}")
                
    except Exception as e:
        print(f"Error getting batch: {e}")

if __name__ == '__main__':
    main()
