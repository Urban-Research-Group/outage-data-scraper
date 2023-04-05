import time
import json

from app.main import handler

def test_CA_INV():
    start = time.now()

    with open('events/ca/investor.json') as f:
        test_event = json.loads(f.read())

    result = handler(test_event)
    assert result['statusCode'] == 200

