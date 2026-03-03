#!/usr/bin/env python3
import os
import requests
import json
import time

def test_github_trigger():
    token = os.getenv('GITHUB_TOKEN')
    
    url = "https://api.github.com/repos/aniqahmed30/agentflo-adk-2/actions/workflows/deploy-adk-agent.yml/dispatches"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    payload = {
        "ref": "templates",
        "inputs": {
            "user_id": "test-user-001",
            "service_name": "test-adk-agent",
            "agent_config": json.dumps({
                "personality": "helpful and professional",
                "instructions": "Help users with their queries",
                "tools": ["search", "calculator"]
            })
        }
    }
    
    print("🚀 Triggering deployment...")
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 204:
        print("✅ Deployment triggered successfully!")
        print(f"🔗 Check status: https://github.com/aniqahmed30/agentflo-adk-2/actions")
        
        # Wait a bit and check runs
        time.sleep(3)
        runs_url = "https://api.github.com/repos/aniqahmed30/agentflo-adk-2/actions/runs"
        runs_response = requests.get(runs_url, headers=headers)
        if runs_response.status_code == 200:
            runs = runs_response.json().get('workflow_runs', [])
            if runs:
                latest_run = runs[0]
                print(f"\n📊 Latest Run:")
                print(f"   ID: {latest_run['id']}")
                print(f"   Status: {latest_run['status']}")
                print(f"   URL: {latest_run['html_url']}")
    else:
        print(f"❌ Failed: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    test_github_trigger()