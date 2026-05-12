from google.cloud import storage

def main():
    client = storage.Client(project="sbecipherio")
    bucket_name = "sbecipher-intelligence"
    prefix = "stage/knowledge/batch_jobs/output/"
    
    bucket = client.bucket(bucket_name)
    
    # Get all blobs in the output directory
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    # Extract unique directories
    dirs = set()
    for blob in blobs:
        # Get the part after the prefix
        rel_path = blob.name[len(prefix):]
        if '/' in rel_path:
            dir_name = rel_path.split('/')[0]
            dirs.add(f"{prefix}{dir_name}/")
            
    print(f"Found {len(dirs)} directories:")
    for d in sorted(dirs):
        print(d)
        
    # Get the newest directory
    if dirs:
        newest_dir = sorted(dirs)[-1]
        print(f"\nContents of newest directory ({newest_dir}):")
        for blob in bucket.list_blobs(prefix=newest_dir):
            print(f"- {blob.name} (size: {blob.size})")
            
if __name__ == '__main__':
    main()
