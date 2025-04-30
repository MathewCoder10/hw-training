import requests

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

response = requests.get('https://cx.static.heb.com/_next/static/chunks/pages/_app-5da7e8c36041877a.js',headers=headers)
if response.status_code == 200:
    # Write the JS to a file
    with open('app-5da7e8c36041877a.js', 'w', encoding='utf-8') as f:
        f.write(response.text)
    print("Saved response to app-5da7e8c36041877a.js")
else:
    print(f"Request failed: HTTP {response.status_code}")
