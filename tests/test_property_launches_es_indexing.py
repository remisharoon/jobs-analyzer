import json
from unittest.mock import MagicMock, patch, call

import numpy as np
import pandas as pd
import pytest

from property_launches_pipeline import (
    es_client,
    ensure_index,
    es_doc_exists,
    _is_completed_status,
    _split_projects_by_status,
    delete_projects_from_index,
    df_to_actions,
    index_projects_to_es,
    ES_INDEX_MAPPING,
    ES_INDEX,
    normalize_project_record,
)


class TestEnsureIndex:
    def test_creates_when_missing(self, mock_es_client):
        mock_es_client.indices.exists.return_value = False
        ensure_index(mock_es_client, ES_INDEX)
        mock_es_client.indices.create.assert_called_once()
        call_args = mock_es_client.indices.create.call_args
        assert call_args[1]["index"] == ES_INDEX
        assert "mappings" in call_args[1]["body"]

    def test_skips_when_exists(self, mock_es_client):
        mock_es_client.indices.exists.return_value = True
        ensure_index(mock_es_client, ES_INDEX)
        mock_es_client.indices.create.assert_not_called()


class TestEsDocExists:
    def test_exists(self, mock_es_client):
        mock_es_client.exists.return_value = True
        assert es_doc_exists(mock_es_client, ES_INDEX, "abc123") is True

    def test_not_exists(self, mock_es_client):
        mock_es_client.exists.return_value = False
        assert es_doc_exists(mock_es_client, ES_INDEX, "abc123") is False

    def test_exception_returns_false(self, mock_es_client):
        mock_es_client.exists.side_effect = Exception("connection error")
        assert es_doc_exists(mock_es_client, ES_INDEX, "abc123") is False


class TestDfToActions:
    def test_correct_ids(self):
        df = pd.DataFrame([
            {"id": "id1", "project_name": "Project 1"},
            {"id": "id2", "project_name": "Project 2"},
        ])
        actions = list(df_to_actions(df, ES_INDEX))
        assert len(actions) == 2
        assert actions[0]["_id"] == "id1"
        assert actions[1]["_id"] == "id2"
        assert actions[0]["_index"] == ES_INDEX
        assert actions[0]["_op_type"] == "index"

    def test_nan_replaced(self):
        df = pd.DataFrame([
            {"id": "id1", "project_name": "Project 1", "price_min": np.nan},
        ])
        actions = list(df_to_actions(df, ES_INDEX))
        assert len(actions) == 1
        assert actions[0]["_source"]["price_min"] is None

    def test_skip_no_id(self):
        df = pd.DataFrame([
            {"project_name": "No ID Project"},
        ])
        actions = list(df_to_actions(df, ES_INDEX))
        assert len(actions) == 0

    def test_op_type_is_index(self):
        df = pd.DataFrame([{"id": "id1", "project_name": "Project 1"}])
        actions = list(df_to_actions(df, ES_INDEX))
        assert actions[0]["_op_type"] == "index"


class TestIndexProjectsToEs:
    def test_index_projects(self, mock_es_client, sample_project_record):
        record = normalize_project_record(sample_project_record)
        with patch("property_launches_pipeline.es_client", return_value=mock_es_client):
            with patch("property_launches_pipeline.helpers") as mock_helpers:
                mock_helpers.bulk.return_value = (1, [])
                indexed = index_projects_to_es([record], es=mock_es_client)
                assert indexed == 1
                mock_helpers.bulk.assert_called_once()

    def test_empty_list_no_calls(self, mock_es_client):
        indexed = index_projects_to_es([], es=mock_es_client)
        assert indexed == 0
        mock_es_client.indices.create.assert_not_called()

    def test_deduplication_before_index(self, mock_es_client, sample_project_record):
        record1 = normalize_project_record(sample_project_record)
        record2 = normalize_project_record(sample_project_record)
        with patch("property_launches_pipeline.es_client", return_value=mock_es_client):
            with patch("property_launches_pipeline.helpers") as mock_helpers:
                mock_helpers.bulk.return_value = (1, [])
                index_projects_to_es([record1, record2], es=mock_es_client)
                call_args = mock_helpers.bulk.call_args
                actions = list(call_args[0][1])
                assert len(actions) == 1

    def test_ensure_index_called(self, mock_es_client, sample_project_record):
        record = normalize_project_record(sample_project_record)
        with patch("property_launches_pipeline.es_client", return_value=mock_es_client):
            with patch("property_launches_pipeline.helpers") as mock_helpers:
                mock_helpers.bulk.return_value = (1, [])
                index_projects_to_es([record], es=mock_es_client)
                mock_es_client.indices.exists.assert_called()

    def test_supports_custom_index(self, mock_es_client, sample_project_record):
        record = normalize_project_record(sample_project_record)
        with patch("property_launches_pipeline.helpers") as mock_helpers:
            mock_helpers.bulk.return_value = (1, [])
            indexed = index_projects_to_es([record], es=mock_es_client, index="property_completed_projects")
            assert indexed == 1
            call_args = mock_helpers.bulk.call_args
            actions = list(call_args[0][1])
            assert actions[0]["_index"] == "property_completed_projects"


class TestStatusSplitHelpers:
    def test_is_completed_status_variants(self):
        assert _is_completed_status("ready-to-move") is True
        assert _is_completed_status("ready to move") is True
        assert _is_completed_status("ready_to_move") is True
        assert _is_completed_status("new-launch") is False

    def test_split_projects_by_status(self):
        launches, completed = _split_projects_by_status([
            {"id": "1", "launch_status": "new-launch"},
            {"id": "2", "launch_status": "ready-to-move"},
            {"id": "3", "launch_status": "ready to move"},
        ])
        assert [r["id"] for r in launches] == ["1"]
        assert [r["id"] for r in completed] == ["2", "3"]


class TestDeleteProjectsFromIndex:
    def test_delete_projects_bulk(self, mock_es_client):
        mock_es_client.indices.exists.return_value = True
        mock_es_client.mget.return_value = {
            "docs": [
                {"_id": "a", "found": True},
                {"_id": "b", "found": True},
            ]
        }
        with patch("property_launches_pipeline.helpers") as mock_helpers:
            mock_helpers.bulk.return_value = (2, [])
            deleted = delete_projects_from_index(mock_es_client, "property_launches", ["a", "b", "a"])
            assert deleted == 2
            call_args = mock_helpers.bulk.call_args
            actions = list(call_args[0][1])
            assert len(actions) == 2
            assert actions[0]["_op_type"] == "delete"
            assert actions[0]["_index"] == "property_launches"

    def test_skips_missing_docs(self, mock_es_client):
        mock_es_client.indices.exists.return_value = True
        mock_es_client.mget.return_value = {"docs": [{"_id": "a", "found": False}, {"_id": "b", "found": False}]}
        with patch("property_launches_pipeline.helpers") as mock_helpers:
            deleted = delete_projects_from_index(mock_es_client, "property_launches", ["a", "b"])
            assert deleted == 0
            mock_helpers.bulk.assert_not_called()


class TestEsIndexMapping:
    def test_mapping_has_key_fields(self):
        props = ES_INDEX_MAPPING["mappings"]["properties"]
        assert "id" in props
        assert "project_name" in props
        assert "builder_name" in props
        assert "builder_tier" in props
        assert "launch_status" in props
        assert "price_min" in props
        assert "price_max" in props
        assert "rera_number" in props
        assert "locality" in props
        assert "source" in props

    def test_mapping_has_correct_types(self):
        props = ES_INDEX_MAPPING["mappings"]["properties"]
        assert props["id"]["type"] == "keyword"
        assert props["builder_tier"]["type"] == "keyword"
        assert props["launch_status"]["type"] == "keyword"
        assert props["price_min"]["type"] == "long"
        assert props["project_name"]["type"] == "text"

    def test_mapping_has_dynamic_templates(self):
        templates = ES_INDEX_MAPPING["mappings"]["dynamic_templates"]
        assert len(templates) > 0
        template_names = [list(t.keys())[0] for t in templates]
        assert "dates_iso" in template_names
        assert "strings" in template_names

    def test_settings(self):
        settings = ES_INDEX_MAPPING["settings"]
        assert settings["number_of_shards"] == 1
        assert settings["number_of_replicas"] == 0
