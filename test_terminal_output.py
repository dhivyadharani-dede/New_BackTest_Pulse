#!/usr/bin/env python3
"""
Test script to verify terminal output functionality of the Flask app
"""
import requests
import time

def test_upload_and_process():
    """Test the file upload and processing with terminal output"""

    # Test file path
    test_file = 'test_strategies.csv'

    print("🧪 Testing Flask app terminal output functionality...")
    print(f"📁 Using test file: {test_file}")

    # First, upload the file
    print("\n📤 Uploading file...")
    with open(test_file, 'rb') as f:
        files = {'file': f}
        response = requests.post('http://127.0.0.1:5000/upload', files=files)

    if response.status_code == 200:
        print("✅ File uploaded successfully")

        # Start processing
        print("\n⚡ Starting strategy processing...")
        response = requests.post('http://127.0.0.1:5000/start_processing')

        if response.status_code == 200:
            result = response.json()
            if result.get('status') == 'started':
                print("✅ Processing started successfully")
                
                # Poll for progress
                print("\n📊 Monitoring progress...")
                while True:
                    progress_response = requests.get('http://127.0.0.1:5000/progress')
                    if progress_response.status_code == 200:
                        progress = progress_response.json()
                        print(f"[{progress['step'].upper()}] {progress['progress']}% - {progress['message']}")
                        if progress['progress'] >= 100:
                            print("✅ Processing completed!")
                            break
                    time.sleep(2)  # Poll every 2 seconds
            else:
                print(f"❌ Processing failed to start: {result.get('message', 'Unknown error')}")
        else:
            print(f"❌ Start processing request failed with status: {response.status_code}")

    else:
        print(f"❌ Upload failed with status: {response.status_code}")
        print(f"Response: {response.text}")

if __name__ == '__main__':
    test_upload_and_process()