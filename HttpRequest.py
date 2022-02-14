# importing the requests library
import requests

# api-endpoint
#URL = "https://www.propertyguru.com.sg/simple-listing/property-for-sale?freetext=Jurong+East%2C+Jurong+West&hdb_estate%5B%5D=13&hdb_estate%5B%5D=14&market=residential&property_type=H&property_type_code%5B%5D=4A&property_type_code%5B%5D=4I&property_type_code%5B%5D=4NG&property_type_code%5B%5D=4S&property_type_code%5B%5D=4STD"
URL = "https://in.finance.yahoo.com/quote/MSFT?p=MSFT"

# location given here
location = "delhi technological university"

# defining a params dict for the parameters to be sent to the API
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36",
    #"Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    #'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
    'Connection': 'keep-alive',
}

# sending get request and saving the response as response object
r = requests.get(url=URL, headers=headers)

# extracting data in json format
data = r.text

print(data)
