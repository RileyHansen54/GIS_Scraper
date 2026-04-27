import requests

# Notice we removed '/query' at the end. We are asking for the Layer Definition.
url = "https://gis.beaufortcountysc.gov/server/rest/services/EnerGov/MapServer/1?f=json"

try:
    response = requests.get(url)
    data = response.json()
    
    print("📊 ENERGOV LAYER 1 FIELDS:")
    print("-" * 30)
    
    if 'fields' in data:
        for field in data['fields']:
            print(f"Name: {field['name']}  |  Type: {field['type']}")
    else:
        print("No fields found. The layer might be empty or restricted.")
        
except Exception as e:
    print(f"Error: {e}")