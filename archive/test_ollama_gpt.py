#!/usr/bin/env python3
"""Test Ollama API with gpt-oss model"""
import requests
import json

def test_gpt_oss():
    url = "http://localhost:11434/api/generate"
    
    # Simple test prompt
    prompt = """
    Extract vessel data from this text and return JSON:
    
    IMO: 9872365
    MMSI: 253676000  
    Name: GALILEO GALILEI
    Flag: Luxembourg
    Type: Dredger
    
    Return only valid JSON with these fields.
    """
    
    payload = {
        "model": "gpt-oss:20b",
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.1,
            "num_predict": 1024
        }
    }
    
    print("Sending request to Ollama...")
    response = requests.post(url, json=payload, timeout=120)
    
    if response.status_code == 200:
        result = response.json()
        llm_response = result.get("response", "")
        print(f"Response length: {len(llm_response)}")
        print(f"Response:\n{llm_response[:500]}")
        
        # Try to extract JSON
        import re
        json_match = re.search(r'\{.*\}', llm_response, re.DOTALL)
        if json_match:
            try:
                parsed = json.loads(json_match.group())
                print(f"\nParsed JSON fields: {list(parsed.keys())}")
            except:
                print("\nFailed to parse JSON")
        else:
            print("\nNo JSON found in response")
    else:
        print(f"Error: {response.status_code}")

if __name__ == "__main__":
    test_gpt_oss()