import os
from google import genai
from google.genai import types

def main():
    project_id = "sbecipherio"
    location = "us-central1"
    os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
    os.environ['GOOGLE_CLOUD_LOCATION'] = location

    print(f"Listing batch jobs for project {project_id} in {location}...")

    client = genai.Client(vertexai=True, project=project_id, location=location)

    # Let's just try to list them
    try:
        # Note: the python SDK for genai batches doesn't seem to have a clear list method documented in the standard way,
        # but let's see what methods are available on client.batches
        print("Methods on client.batches:")
        print([m for m in dir(client.batches) if not m.startswith('_')])
        
        # Let's try iter or list if they exist
        if hasattr(client.batches, 'list'):
            jobs = client.batches.list()
            print(f"Found jobs via list():")
            for job in jobs:
                 print(f"Job Name: {job.name}, State: {job.state}")
        elif hasattr(client.batches, 'iter'):
             for job in client.batches.iter():
                 print(f"Job Name: {job.name}, State: {job.state}")
        else:
            print("No list or iter method found on client.batches")

    except Exception as e:
        print(f"Error listing batches: {e}")

if __name__ == '__main__':
    main()
