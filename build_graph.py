from streamz import Stream
import typing as T
import pandas as pd

from my_types import Fetch, Payload, Data, Fly


def parse_listing(payload: Payload) -> T.List[Fetch]:
    detail_urls = [Fetch(url)
                   for url
                   in payload.html.xpath('//table[@class="itemlist"]/tr[@id]/td[@class="title"]/a/@href')
                   ]
    return detail_urls


def parse_details(payload: Payload):
    data = Data()
    data['source_url'] = payload.response.url
    data['title'] = payload.html.xpath('//title/text() | //*[@title]/text() | //*/@title')[0]
    return data


def store(data: Payload):
    df = pd.DataFrame([data.precollected])
    df.to_csv("data.csv", index=False)
    print(df)


stream = Stream()

listing = stream.Fly(parse_listing).flatten()
details = listing.Fly(parse_details)
save = details.Fly(store)

seed = stream.emit((Fetch('https://news.ycombinator.com/'),))

# debug = listing.emit((Fetch('http://symbolflux.com/luciditystory.html'),))
