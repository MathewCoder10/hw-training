import requests

headers = {
    'accept': '*/*',
    'accept-language': 'en-GB,en;q=0.9',
    'content-type': 'application/x-www-form-urlencoded',
    'origin': 'https://www.sijilat.bh',
    'priority': 'u=1, i',
    'referer': 'https://www.sijilat.bh/',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

data = {
    'username': 'sijilat',
    'password': 'af3ba08ce647fce9b2af578f43ad44905e6e74181f30abb8e43cb0ef3cf2e371',
    'grant_type': 'password',
}

response = requests.post('https://api.sijilat.bh/token', headers=headers, data=data)

print(response.status_code)
print(response.text)