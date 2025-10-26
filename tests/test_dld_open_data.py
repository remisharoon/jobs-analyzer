import asyncio
import importlib
import json
import sys
import types
import unittest
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


# Provide lightweight shims for the ``plombery`` runtime so the scraper module
# can be imported without the external scheduler installed in the test
# environment.
if "plombery" not in sys.modules or not hasattr(sys.modules.get("plombery"), "Trigger"):
    shim = types.ModuleType("plombery")
    shim.__path__ = [str(SRC_PATH / "plombery")]

    class Trigger:  # type: ignore[too-few-public-methods]
        def __init__(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - trivial shim
            self.args = args
            self.kwargs = kwargs

    def task(fn):  # pragma: no cover - trivial shim
        return fn

    def register_pipeline(*args: Any, **kwargs: Any) -> None:  # pragma: no cover - trivial shim
        return None

    shim.Trigger = Trigger
    shim.task = task
    shim.register_pipeline = register_pipeline
    sys.modules["plombery"] = shim


from plombery.dld_open_data import (  # noqa: E402
    BASE_SETTINGS,
    DatasetConfig,
    RecaptchaBlockedError,
    _build_request_headers,
    _compute_start_date,
    _download_dataset,
    _fetch_page,
    _extract_max_date_from_df,
    _find_dataset_node,
    _json_payload_to_df,
    _create_http_session,
    _load_next_data,
    _prepare_dataset_url,
    _table_to_dataframe,
    _fetch_dataset,
)


SAMPLE_NEXT_DATA = {
    "props": {
        "pageProps": {
            "pageData": {
                "tabs": [
                    {
                        "slug": "transactions",
                        "title": "Transactions",
                        "downloadUrl": "https://example.com/api/transactions?FromDate=2023-10-01&ToDate=2023-10-31",
                        "table": {
                            "columns": [
                                {"id": "Registration Date", "title": "Registration Date"},
                                {"id": "Value", "title": "Value"},
                            ],
                            "rows": [
                                ["2023-10-03", 100],
                                ["2023-10-05", 200],
                            ],
                        },
                    },
                    {
                        "slug": "rents",
                        "title": "Rents",
                        "downloadUrl": "https://example.com/api/rents?FromDate={fromDate}&ToDate={toDate}",
                    },
                ]
            }
        }
    }
}


class DummyResponse:
    def __init__(
        self,
        text: str,
        url: str,
        content_type: str = "application/json",
        status_code: int = 200,
    ) -> None:
        self._text = text
        self.url = url
        self.headers = {"Content-Type": content_type}
        self.status_code = status_code

    @property
    def text(self) -> str:
        return self._text

    def json(self) -> Any:
        return json.loads(self._text)

    def raise_for_status(self) -> None:  # pragma: no cover - always OK in tests
        return None


class DLDOpenDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = DatasetConfig(
            key="transactions",
            label="Transactions",
            slug="transactions",
            date_field="Registration Date",
            es_index="dld_transactions",
            buffer_days=2,
        )

    def test_load_next_data(self) -> None:
        html = (
            '<html><head></head><body><script id="__NEXT_DATA__" type="application/json">'
            + json.dumps(SAMPLE_NEXT_DATA)
            + "</script></body></html>"
        )
        parsed = _load_next_data(html)
        self.assertIn("props", parsed)
        self.assertIn("pageProps", parsed["props"])

    def test_find_dataset_node_by_slug(self) -> None:
        node = _find_dataset_node(SAMPLE_NEXT_DATA, self.cfg)
        self.assertIsNotNone(node)
        self.assertEqual(node.get("slug"), "transactions")

    def test_table_to_dataframe(self) -> None:
        node = _find_dataset_node(SAMPLE_NEXT_DATA, self.cfg)
        df = _table_to_dataframe(node["table"])
        self.assertEqual(list(df.columns), ["Registration Date", "Value"])
        self.assertEqual(len(df), 2)

    def test_prepare_dataset_url_updates_query(self) -> None:
        url = "https://example.com/api/rents?FromDate=2023-01-01&ToDate=2023-02-01&Page=1"
        start = date(2024, 1, 15)
        end = date(2024, 2, 1)
        prepared = _prepare_dataset_url(url, start, end)
        self.assertIn("FromDate=2024-01-15", prepared)
        self.assertIn("ToDate=2024-02-01", prepared)

    def test_compute_start_date_uses_buffer(self) -> None:
        today = date.today()
        state = {"transactions": {"max_date": (today - timedelta(days=5)).isoformat()}}
        computed = _compute_start_date(self.cfg, state)
        self.assertEqual(computed, today - timedelta(days=7))

    def test_extract_max_date_from_df(self) -> None:
        node = _find_dataset_node(SAMPLE_NEXT_DATA, self.cfg)
        df = _table_to_dataframe(node["table"])
        max_dt = _extract_max_date_from_df(df, "Registration Date")
        self.assertEqual(max_dt, date(2023, 10, 5))

    def test_json_payload_to_df_handles_nested_table(self) -> None:
        payload = {
            "dataTable": {
                "columns": ["Registration Date", "Value"],
                "rows": [["2024-01-01", 100]],
            }
        }
        df = _json_payload_to_df(payload)
        self.assertEqual(len(df), 1)

    def test_build_request_headers_contains_modern_fields(self) -> None:
        headers = _build_request_headers()
        self.assertIn("Sec-Ch-Ua", headers)
        self.assertIn("User-Agent", headers)
        self.assertEqual(headers["Referer"], BASE_SETTINGS.page_url)

    def test_create_http_session_prefers_curl_cffi_when_available(self) -> None:
        fake_session = mock.Mock()
        fake_session.headers = {}

        fake_module = mock.Mock()
        fake_module.Session.return_value = fake_session

        with mock.patch("plombery.dld_open_data.curl_requests", fake_module):
            session = _create_http_session()

        fake_module.Session.assert_called_once()
        self.assertIs(session, fake_session)
        self.assertTrue(hasattr(session, "h2"))

    def test_create_http_session_falls_back_when_curl_missing(self) -> None:
        with mock.patch("plombery.dld_open_data.curl_requests", None):
            session = _create_http_session()
        # ``requests.Session`` exposes ``request`` and ``headers`` attributes.
        self.assertTrue(hasattr(session, "request"))
        self.assertTrue(hasattr(session, "headers"))

    @mock.patch("plombery.dld_open_data.time.sleep", autospec=True)
    def test_download_dataset_detects_recaptcha(self, sleep_mock: mock.Mock) -> None:
        session = requests_session_with_response(
            DummyResponse(
                "<html><body><p>I'm not a robot</p></body></html>",
                url="https://example.com",  # type: ignore[arg-type]
                content_type="text/html",
            )
        )
        with self.assertRaises(RecaptchaBlockedError):
            _download_dataset(session, "https://example.com/api")
        self.assertEqual(sleep_mock.call_count, 2)

    @mock.patch("plombery.dld_open_data.time.sleep", autospec=True)
    def test_download_dataset_detects_http_block(self, sleep_mock: mock.Mock) -> None:
        session = requests_session_with_response(
            DummyResponse(
                json.dumps({"status": "blocked"}),
                url="https://example.com/api",  # type: ignore[arg-type]
                content_type="application/json",
                status_code=403,
            )
        )
        with self.assertRaises(RecaptchaBlockedError):
            _download_dataset(session, "https://example.com/api")
        self.assertEqual(sleep_mock.call_count, 2)

    @mock.patch("plombery.dld_open_data.time.sleep", autospec=True)
    def test_download_dataset_retries_after_recaptcha(self, _sleep_mock: mock.Mock) -> None:
        responses = [
            DummyResponse(
                "<html><body><p>recaptcha</p></body></html>",
                url="https://example.com/api",  # type: ignore[arg-type]
                content_type="text/html",
            ),
            DummyResponse(
                json.dumps(
                    {
                        "columns": ["Registration Date", "Value"],
                        "rows": [["2024-02-01", 9000]],
                    }
                ),
                url="https://example.com/api",
            ),
        ]

        session = mock.Mock()
        session.get.side_effect = responses

        df, source_url = _download_dataset(session, "https://example.com/api")

        self.assertEqual(len(df), 1)
        self.assertEqual(source_url, "https://example.com/api")
        self.assertEqual(session.get.call_count, 2)

    @mock.patch("plombery.dld_open_data.time.sleep", autospec=True)
    def test_fetch_page_retries_after_recaptcha(self, _sleep_mock: mock.Mock) -> None:
        html = (
            '<html><body><script id="__NEXT_DATA__" type="application/json">'
            + json.dumps(SAMPLE_NEXT_DATA)
            + "</script></body></html>"
        )

        responses = [
            DummyResponse(
                "<html><body>Recaptcha</body></html>",
                url=str(BASE_SETTINGS.page_url),
                content_type="text/html",
            ),
            DummyResponse(html, url=str(BASE_SETTINGS.page_url), content_type="text/html"),
        ]

        session = mock.Mock()
        session.get.side_effect = responses

        parsed = _fetch_page(session)
        self.assertIn("props", parsed)
        self.assertEqual(session.get.call_count, 2)

    def test_fetch_dataset_uses_download_url(self) -> None:
        rents_cfg = DatasetConfig(
            key="rents",
            label="Rents",
            slug="rents",
            date_field="Lease Date",
            es_index="dld_rents",
            buffer_days=None,
        )
        payload = {
            "columns": [
                {"dataIndex": "Lease Date"},
                {"dataIndex": "Value"},
            ],
            "rows": [["2024-01-01", 500]],
        }

        prepared_url_holder: list[str] = []

        def fake_get(url: str, timeout: int, headers: dict[str, str], **_: Any):
            prepared_url_holder.append(url)
            return DummyResponse(json.dumps(payload), url=url)

        session = mock.Mock()
        session.get.side_effect = fake_get

        df, source_url = _fetch_dataset(
            session, SAMPLE_NEXT_DATA, rents_cfg, start_date=date(2024, 1, 1), end_date=date(2024, 1, 31)
        )
        self.assertEqual(len(df), 1)
        self.assertTrue(prepared_url_holder)
        self.assertEqual(source_url, prepared_url_holder[0])


def requests_session_with_response(response: DummyResponse) -> Any:
    session = mock.Mock()
    session.get.return_value = response
    return session


class DLDIntegrationTest(unittest.TestCase):
    def test_pipeline_fetches_all_datasets(self) -> None:
        html = (
            '<html><body><script id="__NEXT_DATA__" type="application/json">'
            + json.dumps(SAMPLE_NEXT_DATA)
            + "</script></body></html>"
        )

        dataset_payloads = {
            "https://example.com/api/rents?FromDate=2024-01-01&ToDate=2024-01-31": {
                "columns": [
                    {"dataIndex": "Lease Date"},
                    {"dataIndex": "Value"},
                ],
                "rows": [["2024-01-15", 3000]],
            }
        }

        def fake_get(url: str, timeout: int, headers: dict[str, str], **_: Any):
            if url == BASE_SETTINGS.page_url:
                return DummyResponse(html, url=url, content_type="text/html")
            payload = dataset_payloads.get(url)
            if payload is None:
                raise AssertionError(f"Unexpected URL requested: {url}")
            return DummyResponse(json.dumps(payload), url=url)

        session = mock.Mock()
        session.get.side_effect = fake_get

        # Mock dependencies so we do not write to ES or disk.
        with mock.patch("plombery.dld_open_data._create_http_session", return_value=session), \
            mock.patch("plombery.dld_open_data.es_client"), \
            mock.patch("plombery.dld_open_data.ensure_index"), \
            mock.patch("plombery.dld_open_data.helpers.bulk"), \
            mock.patch("plombery.dld_open_data.DATASET_CONFIGS", [
                DatasetConfig(
                    key="rents",
                    label="Rents",
                    slug="rents",
                    date_field="Lease Date",
                    es_index="dld_rents",
                )
            ]), \
            mock.patch("plombery.dld_open_data._save_state") as save_state_mock, \
            mock.patch("plombery.dld_open_data._load_state", return_value={}) as load_state_mock:
            module = importlib.import_module("plombery.dld_open_data")
            asyncio.run(module.dld_open_data_pipeline())
            self.assertTrue(save_state_mock.called)
            self.assertTrue(load_state_mock.called)


if __name__ == "__main__":
    unittest.main()
