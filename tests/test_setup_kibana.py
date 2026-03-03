"""
test_setup_kibana.py
====================
Tests unitaires pour src/visualization/setup_kibana.py.

Toutes les dépendances réseau (requests) sont mockées.
"""

from unittest.mock import MagicMock, patch, call
import json

import pytest


# ──────────────────────────────────────────────
# _upsert
# ──────────────────────────────────────────────


class TestUpsert:
    @patch("src.visualization.setup_kibana.requests")
    def test_uses_put_when_successful(self, mock_requests) -> None:
        from src.visualization.setup_kibana import _upsert

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_requests.put.return_value = mock_resp

        _upsert("lens", "vis-test", {"title": "Test"})
        mock_requests.put.assert_called_once()
        mock_requests.post.assert_not_called()

    @patch("src.visualization.setup_kibana.requests")
    def test_falls_back_to_post_on_put_failure(self, mock_requests) -> None:
        from src.visualization.setup_kibana import _upsert

        mock_put_resp = MagicMock()
        mock_put_resp.status_code = 404
        mock_requests.put.return_value = mock_put_resp

        mock_post_resp = MagicMock()
        mock_post_resp.status_code = 200
        mock_requests.post.return_value = mock_post_resp

        _upsert("lens", "vis-test", {"title": "Test"})
        mock_requests.put.assert_called_once()
        mock_requests.post.assert_called_once()

    @patch("src.visualization.setup_kibana.requests")
    def test_raises_on_both_failures(self, mock_requests) -> None:
        from src.visualization.setup_kibana import _upsert

        mock_put_resp = MagicMock()
        mock_put_resp.status_code = 500
        mock_requests.put.return_value = mock_put_resp

        mock_post_resp = MagicMock()
        mock_post_resp.status_code = 500
        mock_post_resp.text = "Internal Error"
        mock_post_resp.raise_for_status.side_effect = Exception("500")
        mock_requests.post.return_value = mock_post_resp

        with pytest.raises(Exception):
            _upsert("lens", "vis-test", {"title": "Test"})

    @patch("src.visualization.setup_kibana.requests")
    def test_includes_references_in_body(self, mock_requests) -> None:
        from src.visualization.setup_kibana import _upsert

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_requests.put.return_value = mock_resp

        refs = [{"id": "dv-1", "name": "ref", "type": "index-pattern"}]
        _upsert("lens", "vis-test", {"title": "Test"}, references=refs)
        call_kwargs = mock_requests.put.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert "references" in body


# ──────────────────────────────────────────────
# _create_data_view
# ──────────────────────────────────────────────


class TestCreateDataView:
    @patch("src.visualization.setup_kibana.requests")
    def test_posts_data_view(self, mock_requests) -> None:
        from src.visualization.setup_kibana import _create_data_view

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_requests.post.return_value = mock_resp

        _create_data_view()
        mock_requests.post.assert_called_once()
        call_args = mock_requests.post.call_args
        url = call_args.args[0] if call_args.args else call_args.kwargs.get("url", "")
        assert "data_views/data_view" in url

    @patch("src.visualization.setup_kibana.requests")
    def test_raises_on_failure(self, mock_requests) -> None:
        from src.visualization.setup_kibana import _create_data_view

        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Error"
        mock_resp.raise_for_status.side_effect = Exception("500")
        mock_requests.post.return_value = mock_resp

        with pytest.raises(Exception):
            _create_data_view()


# ──────────────────────────────────────────────
# _create_lens_xy
# ──────────────────────────────────────────────


class TestCreateLensXy:
    @patch("src.visualization.setup_kibana._upsert")
    def test_calls_upsert_with_correct_type(self, mock_upsert) -> None:
        from src.visualization.setup_kibana import _create_lens_xy

        _create_lens_xy("vis-test", "Test Chart", ["Close"], "USD")
        mock_upsert.assert_called_once()
        args, kwargs = mock_upsert.call_args
        assert args[0] == "lens"
        assert args[1] == "vis-test"

    @patch("src.visualization.setup_kibana._upsert")
    def test_handles_multiple_y_fields(self, mock_upsert) -> None:
        from src.visualization.setup_kibana import _create_lens_xy

        _create_lens_xy("vis-multi", "Multi", ["geo_I_smoothed", "geo_B_smoothed"], "Score")
        args, kwargs = mock_upsert.call_args
        attrs = kwargs.get("attributes") or args[2]
        state = attrs["state"]
        accessors = state["visualization"]["layers"][0]["accessors"]
        assert len(accessors) == 2


# ──────────────────────────────────────────────
# _create_lens_dual_axis
# ──────────────────────────────────────────────


class TestCreateLensDualAxis:
    @patch("src.visualization.setup_kibana._upsert")
    def test_creates_dual_axis_vis(self, mock_upsert) -> None:
        from src.visualization.setup_kibana import _create_lens_dual_axis

        _create_lens_dual_axis(
            "vis-dual", "Dual Test",
            left_field="Close", right_field="score_pct_7d",
        )
        mock_upsert.assert_called_once()
        args, kwargs = mock_upsert.call_args
        attrs = kwargs.get("attributes") or args[2]
        state = attrs["state"]
        y_config = state["visualization"]["layers"][0]["yConfig"]
        assert len(y_config) == 1
        assert y_config[0]["axisMode"] == "right"


# ──────────────────────────────────────────────
# _create_lens_bar_horizontal
# ──────────────────────────────────────────────


class TestCreateLensBarHorizontal:
    @patch("src.visualization.setup_kibana._upsert")
    def test_creates_lens_visualization(self, mock_upsert) -> None:
        from src.visualization.setup_kibana import _create_lens_bar_horizontal

        _create_lens_bar_horizontal("vis-bar", "Top Actors", "period_actor_country")
        mock_upsert.assert_called_once()
        args, kwargs = mock_upsert.call_args
        obj_type = args[0]
        assert obj_type == "lens"
        attrs = kwargs.get("attributes") or args[2]
        assert attrs["visualizationType"] == "lnsXY"


# ──────────────────────────────────────────────
# _create_dashboard
# ──────────────────────────────────────────────


class TestCreateDashboard:
    @patch("src.visualization.setup_kibana._upsert")
    def test_creates_dashboard_with_6_panels(self, mock_upsert) -> None:
        from src.visualization.setup_kibana import _create_dashboard

        _create_dashboard()
        mock_upsert.assert_called_once()
        args, kwargs = mock_upsert.call_args
        assert args[0] == "dashboard"
        # Check references include all 6 vis/map panels
        refs = kwargs.get("references") or args[3]
        assert len(refs) == 6


# ──────────────────────────────────────────────
# main()
# ──────────────────────────────────────────────


class TestMain:
    @patch("src.visualization.setup_kibana._create_dashboard")
    @patch("src.visualization.setup_kibana._create_map_actors")
    @patch("src.visualization.setup_kibana._create_lens_bar_horizontal")
    @patch("src.visualization.setup_kibana._create_lens_dual_axis")
    @patch("src.visualization.setup_kibana._create_lens_xy")
    @patch("src.visualization.setup_kibana._create_data_view")
    @patch("src.visualization.setup_kibana.requests")
    def test_calls_all_steps(self, mock_req, mock_dv, mock_xy, mock_dual, mock_bar, mock_map, mock_dash) -> None:
        from src.visualization.setup_kibana import main

        # Mock DELETE calls for stale objects
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_req.delete.return_value = mock_resp

        main()
        mock_dv.assert_called_once()
        # 3 lens_xy calls (wti-close, volume, geo-scores)
        assert mock_xy.call_count == 3
        mock_dual.assert_called_once()
        mock_bar.assert_called_once()
        mock_map.assert_called_once()
        mock_dash.assert_called_once()
