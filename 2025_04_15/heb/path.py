import requests

cookies = {
    'visid_incap_2302070': 'U4Tc0q+HQf6I3cteZ8RkayBuCGgAAAAAQUIPAAAAAABj5+GlaiBhBfVGxBmQB917',
    'AMP_MKTG_760524e2ba': 'JTdCJTdE',
    '_ga': 'GA1.1.652251946.1745382952',
    '_gcl_au': '1.1.417985956.1745382962',
    'incap_ses_770_2302070': 'mut4f5fHUjjAkwcwIZevCsR+CGgAAAAAkt50kwNo5vdf2XcndgeOLA==',
    'incap_ses_706_2302070': '7cFlXtCPohOVF2BwgTfMCbJXCmgAAAAAJm5RNCPD18xXgc+pqCbWUw==',
    'OptanonConsent': 'isGpcEnabled=0&datestamp=Thu+Apr+24+2025+20%3A55%3A23+GMT%2B0530+(India+Standard+Time)&version=202405.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=911b8bed-d060-439d-950a-6dd838411dd7&interactionCount=0&isAnonUser=1&landingPath=NotLandingPage&groups=C0004%3A1&AwaitingReconsent=false',
    'reese84': '3:6wsJykuskw90VGWAe/mKkw==:OKLQEfBVGbN5heOOjGmEA6ZRsx6irK4YBYDEPlZARrbPBnyJGmWMhjNbn9yq7+pp9iPztkO0epo4raAHt5CqC1zr/HfczOpNx5Yek3wdfXNQuPzQ4qZ3E+2bDlW2h3yDiZ2ZF0kpUP0lo8vAECWes+y7Qr965RAc7VVKGuu42q0nji77+n6JDl3dW1yaeGfxFSCb6hKGjg/mmc7ZAFK3HkPHME46pTDOhd59YdzkZ8UIYkjB/KE6RE/ovueQSBYfUMTdvZthqyNgUqxrlifrhfzeGReadKS71dKBYdi8KbM2d6yP9MDd1vbKiiIa7OfuPDYZRxi4/L7Iwka/A0GSlK/w7LTtHzXbYOd8gQ9EnI2LD+l+LF5cAkloVbm1Pp385eLI7Umdi1kVrNO23y8i4OdPIaMdDp0tYfixY9PXTFs/Qo0UIp8o6BZvRW97n8iltNuimxAYuxBEkH6AdPGRLrKyqbLMi1q3yJdF8WEwkYRplQMSCd+BbI53yYwSV1uG:ECe1wK5+ZgSYtzaF7CFCFtDCrGwlJdbVOXfeYllXRIs=',
    'incap_ses_712_2302070': 'OwLlIbjcOxzijrN9gojhCc5aCmgAAAAAF9gmk88kS+IdwcY5I7gXvg==',
    'AMP_760524e2ba': 'JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjJoLWM3OTUwNWM2LTI0YTctNDBhYi04YTBiLTM3NTIzN2VhNjU1NCUyMiUyQyUyMnNlc3Npb25JZCUyMiUzQTE3NDU1MDgyNzU3MTIlMkMlMjJvcHRPdXQlMjIlM0FmYWxzZSUyQyUyMmxhc3RFdmVudFRpbWUlMjIlM0ExNzQ1NTA5MjA2MzY5JTJDJTIybGFzdEV2ZW50SWQlMjIlM0E3MSUyQyUyMnBhZ2VDb3VudGVyJTIyJTNBMCU3RA==',
    '_ga_WKSH6HYPT4': 'GS1.1.1745508277.3.1.1745509208.0.0.0',
}

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    # 'cookie': 'visid_incap_2302070=U4Tc0q+HQf6I3cteZ8RkayBuCGgAAAAAQUIPAAAAAABj5+GlaiBhBfVGxBmQB917; AMP_MKTG_760524e2ba=JTdCJTdE; _ga=GA1.1.652251946.1745382952; _gcl_au=1.1.417985956.1745382962; incap_ses_770_2302070=mut4f5fHUjjAkwcwIZevCsR+CGgAAAAAkt50kwNo5vdf2XcndgeOLA==; incap_ses_706_2302070=7cFlXtCPohOVF2BwgTfMCbJXCmgAAAAAJm5RNCPD18xXgc+pqCbWUw==; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Apr+24+2025+20%3A55%3A23+GMT%2B0530+(India+Standard+Time)&version=202405.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=911b8bed-d060-439d-950a-6dd838411dd7&interactionCount=0&isAnonUser=1&landingPath=NotLandingPage&groups=C0004%3A1&AwaitingReconsent=false; reese84=3:6wsJykuskw90VGWAe/mKkw==:OKLQEfBVGbN5heOOjGmEA6ZRsx6irK4YBYDEPlZARrbPBnyJGmWMhjNbn9yq7+pp9iPztkO0epo4raAHt5CqC1zr/HfczOpNx5Yek3wdfXNQuPzQ4qZ3E+2bDlW2h3yDiZ2ZF0kpUP0lo8vAECWes+y7Qr965RAc7VVKGuu42q0nji77+n6JDl3dW1yaeGfxFSCb6hKGjg/mmc7ZAFK3HkPHME46pTDOhd59YdzkZ8UIYkjB/KE6RE/ovueQSBYfUMTdvZthqyNgUqxrlifrhfzeGReadKS71dKBYdi8KbM2d6yP9MDd1vbKiiIa7OfuPDYZRxi4/L7Iwka/A0GSlK/w7LTtHzXbYOd8gQ9EnI2LD+l+LF5cAkloVbm1Pp385eLI7Umdi1kVrNO23y8i4OdPIaMdDp0tYfixY9PXTFs/Qo0UIp8o6BZvRW97n8iltNuimxAYuxBEkH6AdPGRLrKyqbLMi1q3yJdF8WEwkYRplQMSCd+BbI53yYwSV1uG:ECe1wK5+ZgSYtzaF7CFCFtDCrGwlJdbVOXfeYllXRIs=; incap_ses_712_2302070=OwLlIbjcOxzijrN9gojhCc5aCmgAAAAAF9gmk88kS+IdwcY5I7gXvg==; AMP_760524e2ba=JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjJoLWM3OTUwNWM2LTI0YTctNDBhYi04YTBiLTM3NTIzN2VhNjU1NCUyMiUyQyUyMnNlc3Npb25JZCUyMiUzQTE3NDU1MDgyNzU3MTIlMkMlMjJvcHRPdXQlMjIlM0FmYWxzZSUyQyUyMmxhc3RFdmVudFRpbWUlMjIlM0ExNzQ1NTA5MjA2MzY5JTJDJTIybGFzdEV2ZW50SWQlMjIlM0E3MSUyQyUyMnBhZ2VDb3VudGVyJTIyJTNBMCU3RA==; _ga_WKSH6HYPT4=GS1.1.1745508277.3.1.1745509208.0.0.0',
    'pragma': 'no-cache',
    'referer': 'https://www.heb.com/',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'script',
    'sec-fetch-mode': 'no-cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

response = requests.get(
    'https://cx.static.heb.com/_next/static/chunks/pages/_app-5da7e8c36041877a.js',
    cookies=cookies,
    headers=headers)
if response.status_code == 200:
    # Write the JS to a file
    with open('app-5da7e8c36041877a.js', 'w', encoding='utf-8') as f:
        f.write(response.text)
    print("Saved response to app-5da7e8c36041877a.js")
else:
    print(f"Request failed: HTTP {response.status_code}")
