import requests
from parsel import Selector
import requests

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'content-type': 'text/plain;charset=UTF-8',
    'origin': 'https://www.marksandspencer.com',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.marksandspencer.com/',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

data = '{"product":{"id":"P60638230"},"visitor":{"id":"b76b0cae-043c-40ba-9eed-879153b58a4b","sessionId":"538e6485-4644-484f-8ae2-8d11a037fbde"},"experience":{"id":"treatment-v9"}}'

response = requests.post('https://api.taggstar.com/api/v2/key/marksandspencercom/product/visit', headers=headers, data=data)

# print(response.status_code)
json_data = response.json()
# print(json_data)

# Loop through socialProof messages and extract desktop content with a gap
for proof in json_data.get('socialProof', []):
    for msg in proof.get('messages', []):
        message_html = msg.get('message', '')
        sel = Selector(text=message_html)
        # Use a space when joining text nodes so that there's a gap between them
        desktop_text_list = sel.css("span.tagg-desktop *::text").getall()
        desktop_text = " ".join(desktop_text_list).strip()
        print(desktop_text)