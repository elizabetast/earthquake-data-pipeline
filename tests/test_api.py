"""Контракт API и разбиение: без сети и без Airflow."""
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'dags'))
from earthquake_common import api


def payload(count):
    return {'type': 'FeatureCollection', 'features': [
        {'id': f'e{i}', 'properties': {'updated': 1000}} for i in range(count)]}


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def test_utc(self):
        value = datetime(2026, 1, 1, 3, tzinfo=timezone(timedelta(hours=3)))
        self.assertEqual(api.iso(value), '2026-01-01T00:00:00.000000Z')

    def test_invalid_payload_fails(self):
        for value in ({}, {'type': 'FeatureCollection', 'features': [{}]},
                      {'type': 'FeatureCollection', 'features': [{'id': 'a', 'properties': {}}]}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                api.validate(value)

    def test_empty_response_is_valid(self):
        self.assertEqual(api.validate(payload(0)), [])

    def test_http_204_becomes_empty_collection(self):
        response = MagicMock()
        response.status = 204
        response.__enter__.return_value = response
        with patch.object(api, 'urlopen', return_value=response):
            url, value, fetched = api.request({'format': 'geojson'})
        self.assertEqual(value['features'], [])
        self.assertEqual(value['_http_status'], 204)

    def test_overflow_splits_without_losing_boundary(self):
        requests = []
        def fake(params):
            requests.append(params)
            count = 2 if len(requests) == 1 else 1
            return 'url', payload(count), self.start
        with patch.object(api, 'MAX_EVENTS', 2):
            result = list(api.fetch_window(self.start, self.start + timedelta(days=1),
                                          self.start, requester=fake))
        self.assertEqual(len(result), 2)
        self.assertEqual(requests[1]['endtime'], requests[2]['starttime'])
        self.assertEqual(requests[1]['updatedafter'], api.iso(self.start))

    def test_unsplittable_overflow_fails(self):
        def fake(params):
            return 'url', payload(1), self.start
        with patch.object(api, 'MAX_EVENTS', 1), self.assertRaises(ValueError):
            list(api.fetch_window(self.start, self.start + timedelta(seconds=1), requester=fake))


if __name__ == '__main__':
    unittest.main()
