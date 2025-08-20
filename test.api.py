import streamlit as st
import requests
import base64
import json

st.title("DataForSEO Credentials Tester")

# Get credentials from Streamlit secrets
DATAFORSEO_LOGIN = st.secrets.get("DATAFORSEO_LOGIN", "").strip()
DATAFORSEO_PASSWORD = st.secrets.get("DATAFORSEO_PASSWORD", "").strip()

if not DATAFORSEO_LOGIN or not DATAFORSEO_PASSWORD:
    st.error("❌ No credentials found in Streamlit Secrets")
    st.stop()

st.success(f"✅ Found credentials for: {DATAFORSEO_LOGIN}")

# Test button
if st.button("Test DataForSEO Connection"):
    
    # Method 1: Test with Base64 encoding
    st.subheader("Test 1: Using Base64 Authentication")
    cred_string = f"{DATAFORSEO_LOGIN}:{DATAFORSEO_PASSWORD}"
    st.code(f"Credential string: {DATAFORSEO_LOGIN}:{'*' * len(DATAFORSEO_PASSWORD)}")
    
    cred_bytes = cred_string.encode('utf-8')
    cred_b64 = base64.b64encode(cred_bytes).decode('utf-8')
    
    headers = {
        'Authorization': f'Basic {cred_b64}',
        'Content-Type': 'application/json'
    }
    
    # Test with simple endpoint first
    try:
        st.write("Testing /v3/merchant/google/locations endpoint...")
        response = requests.get(
            'https://api.dataforseo.com/v3/merchant/google/locations',
            headers=headers,
            timeout=10
        )
        st.write(f"Response Code: {response.status_code}")
        
        if response.status_code == 200:
            st.success("✅ Authentication successful!")
            st.json(response.json()[:500] if len(str(response.json())) > 500 else response.json())
        else:
            st.error(f"❌ Authentication failed: {response.status_code}")
            st.code(response.text[:500])
    except Exception as e:
        st.error(f"Error: {e}")
    
    # Test the SERP endpoint
    st.subheader("Test 2: SERP API Endpoint")
    
    data = [{
        "keyword": "test",
        "location_code": 2840,
        "language_code": "en",
        "device": "desktop",
        "os": "windows",
        "depth": 1,
        "calculate_rectangles": False
    }]
    
    try:
        st.write("Testing /v3/serp/google/organic/live/advanced endpoint...")
        response = requests.post(
            'https://api.dataforseo.com/v3/serp/google/organic/live/advanced',
            headers=headers,
            json=data,
            timeout=30
        )
        st.write(f"Response Code: {response.status_code}")
        
        if response.status_code == 200:
            st.success("✅ SERP API working!")
            result = response.json()
            if result.get("tasks"):
                st.json(result["tasks"][0] if result["tasks"] else result)
        else:
            st.error(f"❌ SERP API failed: {response.status_code}")
            st.code(response.text[:1000])
            
            # Try to parse error message
            try:
                error_data = response.json()
                if "status_message" in error_data:
                    st.error(f"API Message: {error_data['status_message']}")
            except:
                pass
                
    except Exception as e:
        st.error(f"Error: {e}")
    
    # Method 2: Try alternate authentication format
    st.subheader("Test 3: Alternative Auth Format")
    
    # Try using the credentials directly in the header
    alt_headers = {
        'Authorization': f'Basic {cred_b64}',
        'Content-Type': 'application/json'
    }
    
    # Simple ping endpoint
    try:
        st.write("Testing with ping endpoint...")
        response = requests.get(
            'https://api.dataforseo.com/v3/appendix/user_data',
            headers=alt_headers,
            timeout=10
        )
        st.write(f"Response Code: {response.status_code}")
        
        if response.status_code == 200:
            st.success("✅ User data retrieved!")
            user_data = response.json()
            if "data" in user_data:
                st.json(user_data["data"])
        else:
            st.error(f"❌ Failed: {response.status_code}")
            st.code(response.text[:500])
    except Exception as e:
        st.error(f"Error: {e}")

st.divider()
st.caption("This tester helps verify your DataForSEO API credentials are working correctly.")
