from scraper.scraper01 import ScraperL1
import aws_s3
import requests

def handler(event, context):
    # TODO configure this lambda handler, define event JSON format?
    """
    event: {'layout': 1, urls: []}
    :param event:
    :param context:
    :return:
    """
    layout = event['layout']
    urls = event['urls']

    if layout == 1:
        print("running scraper", layout)
        scrapper = ScraperL1(urls)
        print(scrapper.parse())
#
# def handler(event, context):
#     response = requests.get("https://jsonplaceholder.typicode.com/todos/1")
#     res = {
#         "event": event,
#         "output": response.json(),
#         "context": context
#     }
#     print(res)
#
#     return None