import requests
from parsel import Selector
cookies = {
    'aws-waf-token': 'e1e6981f-9f4f-4628-b339-4b373e4fe2cc:BgoAZfJQVeh6AAAA:ZIy9JoquzKkE40Jm2b8uAajTiw7whiZnOchydW+cyDYQyYFmk8lg5q/7VRHXGYu7iKNH1ZAm4qmGhv06G4vK9SvGrU8OpBYFFNEjzNiqRJuOep57IZvrTGSP9fHniFgWvwE4H1c16JoLo3r0x+CviQ9I2Cy6PWjdbBfNFbOFb45erdd63oZ6x+l07ZsSZ3Teh2KliBg=',
    'LanguagePreference': 'eng',
    'Currency': 'USD',
    'UnitSystem': 'Imperial',
    'TAsessionID': 'c49b1e51-3703-4725-85fa-d6437054b579|NEW',
    'notice_behavior': 'implied,us',
    'currentSearchQuery': 'int%2F180-a-df21030305521031362-agentid',
    '_fbp': 'fb.1.1742365922548.199578295208308822',
    '_gcl_au': '1.1.1031560927.1742365923',
    '_ga_07J12X0FK6': 'GS1.1.1742365922.1.0.1742365922.60.0.0',
    '_ga': 'GA1.1.953117992.1742365923',
    '_ga_0QLKQS5MQZ': 'GS1.1.1742365922.1.0.1742365922.0.0.0',
}
headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'referer': 'https://www.sothebysrealty.com/eng/associates/int',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

response = requests.get(
    'https://www.sothebysrealty.com/eng/associate/180-a-df22070710401011891/aisha-a-krechuniak',
    cookies=cookies,
    headers=headers,
)
print(response.status_code)
# Create a Selector from the response content
sel = Selector(response.text)

# Use the provided XPath to extract href attributes
agent_name = sel.xpath("//h1[contains(@class, 'Hero__agent-name')]/text()").get()
print(agent_name)
title = sel.xpath("//div[contains(@class, 'agent__agent-title u-text-uppercase')]/text()").get()
print(title)
image_url = sel.xpath("//div[contains(@class, 'Hero__agent-image palm--1-1 lap--1-1 desk--7-8')]//img/@src").get()
print(image_url)
description = sel.xpath("//h4[contains(@class, 'agent__long-video-description')]/text()").get()
print(description)
languages = sel.xpath("//p[normalize-space(.)='Languages']/following-sibling::h3/text()").get()
print(languages)
email = sel.xpath('//div[@class="m-listing-contact-info__agent-email"]/a/@href').get()
print(email)
agent_phone_numbers = sel.xpath('//li[@class="m-listing-contact-info__agent-phone"]/a/span/text()').getall()
print(agent_phone_numbers)
office_name = sel.xpath('//div[@class="m-listing-contact-info__office-name"]/a/text()').getall()
print(office_name)
website = sel.xpath("//a[contains(text(), 'Personal Website')]/@href").get()
print(website)

