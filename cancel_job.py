import os
from google import genai

def main():
    project_id = "sbecipherio"
    location = "us-central1"
    os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
    os.environ['GOOGLE_CLOUD_LOCATION'] = location

    client = genai.Client(vertexai=True, project=project_id, location=location)
    job_name = "projects/268910786343/locations/us-central1/batchPredictionJobs/9132349224447377408"
    print(f"Cancelling {job_name}...")
    client.batches.cancel(name=job_name)
    print("Cancelled.")

if __name__ == '__main__':
    main()
