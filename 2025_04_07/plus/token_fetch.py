import requests
import re

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=1',
    'referer': 'https://www.plus.nl/',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'script',
    'sec-fetch-mode': 'no-cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

url = 'https://www.plus.nl/scripts/OutSystems.js?H4bR29NkZ15NFYcdxJmseg'
response = requests.get(url, headers=headers)

# Extract the content after 'e.AnonymousCSRFToken = "'
match = re.search(r'e\.AnonymousCSRFToken\s*=\s*"([^"]+)"', response.text)
if match:
    token = match.group(1)
    print("Extracted CSRF token:", token)
else:
    print("CSRF token not found.")
