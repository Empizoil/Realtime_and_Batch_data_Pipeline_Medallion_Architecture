import requests

url = "http://localhost:8081/contentListener"

data = '{"test": "hello from Python"}'

response = requests.post(
    url,
    data=data,
    headers={"Content-Type": "application/json"}
)

print("Status:", response.status_code)
print("Response:", response.text)