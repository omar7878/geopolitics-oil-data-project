"""
setup_kibana.py
===============
Déploie automatiquement un dashboard Kibana « Oil Market — Stress Index »
via l'API Saved Objects CRUD (Kibana 8.x).

Ce script crée :
  1. Un Data View (ex « index pattern ») sur l'index `oil-market-analysis`
  2. Cinq visualisations Lens :
     - Cours WTI (Close) en ligne temporelle
     - Stress Index (score_pct_7d) en ligne temporelle
     - Volume de trading en barres
     - Scores géopolitiques smoothed (geo_I / geo_B / geo_S) superposés
     - Top acteurs géopolitiques (period_main_actor) en donut
  3. Un dashboard qui assemble les cinq panneaux

Usage :
  poetry run python -m src.visualization.setup_kibana

Idempotent : relancer le script écrase les objets existants (même IDs).

Pré-requis :
  - Kibana accessible sur http://localhost:5601
  - Index `oil-market-analysis` existant dans Elasticsearch
"""

from __future__ import annotations

import json
import logging
from typing import Any

import requests

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────

KIBANA_URL = "http://localhost:5601"
ES_INDEX = "oil-market-analysis"
DATA_VIEW_ID = "oil-market-dv"
DASHBOARD_ID = "oil-market-dashboard"

LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO)
log = logging.getLogger(__name__)

HEADERS = {
    "kbn-xsrf": "true",
    "Content-Type": "application/json",
}


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────


def _upsert(obj_type: str, obj_id: str,
            attributes: dict[str, Any],
            references: list[dict[str, Any]] | None = None) -> None:
    """Crée ou écrase un Saved Object via l'API CRUD Kibana."""
    body: dict[str, Any] = {"attributes": attributes}
    if references:
        body["references"] = references

    # Essayer PUT (update) d'abord
    url_put = f"{KIBANA_URL}/api/saved_objects/{obj_type}/{obj_id}"
    resp = requests.put(url_put, headers=HEADERS, json=body, timeout=15)
    if resp.status_code in (200, 201):
        log.info("   ✅ %s/%s (updated)", obj_type, obj_id)
        return

    # Sinon POST (create)
    resp2 = requests.post(url_put, headers=HEADERS, json=body, timeout=15)
    if resp2.status_code in (200, 201):
        log.info("   ✅ %s/%s (created)", obj_type, obj_id)
    else:
        log.error("   ❌ %s/%s — HTTP %s : %s",
                   obj_type, obj_id, resp2.status_code, resp2.text[:300])
        resp2.raise_for_status()


# ──────────────────────────────────────────────
# 1. DATA VIEW
# ──────────────────────────────────────────────


def _create_data_view() -> None:
    """Crée (ou met à jour) le Data View sur l'index oil-market-analysis."""
    url = f"{KIBANA_URL}/api/data_views/data_view"
    payload: dict[str, Any] = {
        "data_view": {
            "id": DATA_VIEW_ID,
            "title": ES_INDEX,
            "timeFieldName": "timestamp",
            "name": "Oil Market Analysis",
        },
        "override": True,
    }
    resp = requests.post(url, headers=HEADERS, json=payload, timeout=15)
    if resp.status_code in (200, 201):
        log.info("✅ Data View '%s' créé/mis à jour.", DATA_VIEW_ID)
    else:
        log.error("❌ Data View — HTTP %s : %s", resp.status_code, resp.text[:300])
        resp.raise_for_status()


# ──────────────────────────────────────────────
# 2. VISUALISATIONS LENS
# ──────────────────────────────────────────────


def _create_lens_xy(
    vis_id: str,
    title: str,
    y_fields: list[str],
    y_label: str = "Value",
    series_type: str = "line",
) -> None:
    """Crée une visualisation Lens XY (ligne ou barres) via l'API CRUD."""
    layer_id = "layer-main"
    columns: dict[str, Any] = {}
    col_order: list[str] = []

    # Axe X = date_histogram sur timestamp
    x_col = "col-x-ts"
    columns[x_col] = {
        "dataType": "date",
        "isBucketed": True,
        "label": "timestamp",
        "operationType": "date_histogram",
        "params": {"interval": "auto", "includeEmptyRows": True},
        "scale": "interval",
        "sourceField": "timestamp",
    }
    col_order.append(x_col)

    # Axes Y = average sur chaque champ
    accessors: list[str] = []
    for i, field in enumerate(y_fields):
        cid = f"col-y-{i}-{field}"
        columns[cid] = {
            "dataType": "number",
            "isBucketed": False,
            "label": field,
            "operationType": "average",
            "params": {},
            "scale": "ratio",
            "sourceField": field,
        }
        col_order.append(cid)
        accessors.append(cid)

    state: dict[str, Any] = {
        "visualization": {
            "axisTitlesVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
            "layers": [{
                "accessors": accessors,
                "layerId": layer_id,
                "layerType": "data",
                "seriesType": series_type,
                "xAccessor": x_col,
            }],
            "fittingFunction": "Linear",
            "legend": {"isVisible": True, "position": "right"},
            "preferredSeriesType": series_type,
            "title": title,
            "valueLabels": "hide",
            "yLeftExtent": {"mode": "full"},
            "yRightExtent": {"mode": "full"},
            "yTitle": y_label,
        },
        "query": {"language": "kuery", "query": ""},
        "filters": [],
        "datasourceStates": {
            "formBased": {
                "layers": {
                    layer_id: {
                        "columnOrder": col_order,
                        "columns": columns,
                        "indexPatternId": DATA_VIEW_ID,
                    }
                }
            }
        },
    }

    _upsert("lens", vis_id,
            attributes={"title": title, "visualizationType": "lnsXY", "state": state},
            references=[{
                "id": DATA_VIEW_ID,
                "name": f"indexpattern-datasource-layer-{layer_id}",
                "type": "index-pattern",
            }])


def _create_lens_dual_axis(
    vis_id: str,
    title: str,
    left_field: str,
    right_field: str,
    left_label: str = "Gauche",
    right_label: str = "Droite",
) -> None:
    """Crée une visualisation Lens XY à double échelle (axe gauche + axe droit)."""
    layer_id = "layer-main"
    x_col = "col-x-ts"
    y_left = f"col-y-left-{left_field}"
    y_right = f"col-y-right-{right_field}"

    columns: dict[str, Any] = {
        x_col: {
            "dataType": "date",
            "isBucketed": True,
            "label": "timestamp",
            "operationType": "date_histogram",
            "params": {"interval": "auto", "includeEmptyRows": True},
            "scale": "interval",
            "sourceField": "timestamp",
        },
        y_left: {
            "dataType": "number",
            "isBucketed": False,
            "label": left_field,
            "operationType": "average",
            "params": {},
            "scale": "ratio",
            "sourceField": left_field,
        },
        y_right: {
            "dataType": "number",
            "isBucketed": False,
            "label": right_field,
            "operationType": "average",
            "params": {},
            "scale": "ratio",
            "sourceField": right_field,
        },
    }

    state: dict[str, Any] = {
        "visualization": {
            "axisTitlesVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
            "layers": [{
                "accessors": [y_left, y_right],
                "layerId": layer_id,
                "layerType": "data",
                "seriesType": "line",
                "xAccessor": x_col,
                "yConfig": [
                    {"forAccessor": y_right, "axisMode": "right"},
                ],
            }],
            "fittingFunction": "Linear",
            "legend": {"isVisible": True, "position": "right"},
            "preferredSeriesType": "line",
            "title": title,
            "valueLabels": "hide",
            "yLeftExtent": {"mode": "full"},
            "yRightExtent": {"mode": "full"},
            "yTitle": left_label,
            "yRightTitle": right_label,
        },
        "query": {"language": "kuery", "query": ""},
        "filters": [],
        "datasourceStates": {
            "formBased": {
                "layers": {
                    layer_id: {
                        "columnOrder": [x_col, y_left, y_right],
                        "columns": columns,
                        "indexPatternId": DATA_VIEW_ID,
                    }
                }
            }
        },
    }

    _upsert("lens", vis_id,
            attributes={"title": title, "visualizationType": "lnsXY", "state": state},
            references=[{
                "id": DATA_VIEW_ID,
                "name": f"indexpattern-datasource-layer-{layer_id}",
                "type": "index-pattern",
            }])


def _create_lens_donut(vis_id: str, title: str, field: str) -> None:
    """Crée une visualisation Lens horizontal bar (top acteurs par count).

    Remplace un donut Pie (format lnsPie instable via l'API) par un bar chart
    horizontal Lens XY qui affiche les mêmes données : top N valeurs du champ
    keyword triées par nombre d'occurrences.
    """
    layer_id = "layer-bar"
    x_col = "col-terms"
    y_col = "col-count"

    state: dict[str, Any] = {
        "visualization": {
            "axisTitlesVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
            "layers": [{
                "accessors": [y_col],
                "layerId": layer_id,
                "layerType": "data",
                "seriesType": "bar_horizontal",
                "xAccessor": x_col,
            }],
            "legend": {"isVisible": False, "position": "right"},
            "preferredSeriesType": "bar_horizontal",
            "title": title,
            "valueLabels": "show",
            "yLeftExtent": {"mode": "full"},
            "yRightExtent": {"mode": "full"},
            "yTitle": "Nombre d'occurrences",
        },
        "query": {"language": "kuery", "query": ""},
        "filters": [],
        "datasourceStates": {
            "formBased": {
                "layers": {
                    layer_id: {
                        "columnOrder": [x_col, y_col],
                        "columns": {
                            x_col: {
                                "dataType": "string",
                                "isBucketed": True,
                                "label": f"Top {field}",
                                "operationType": "terms",
                                "params": {
                                    "orderBy": {"columnId": y_col, "type": "column"},
                                    "orderDirection": "desc",
                                    "size": 10,
                                },
                                "scale": "ordinal",
                                "sourceField": field,
                            },
                            y_col: {
                                "dataType": "number",
                                "isBucketed": False,
                                "label": "Count",
                                "operationType": "count",
                                "params": {},
                                "scale": "ratio",
                                "sourceField": "___records___",
                            },
                        },
                        "indexPatternId": DATA_VIEW_ID,
                    }
                }
            }
        },
    }

    _upsert("lens", vis_id,
            attributes={"title": title, "visualizationType": "lnsXY", "state": state},
            references=[{
                "id": DATA_VIEW_ID,
                "name": f"indexpattern-datasource-layer-{layer_id}",
                "type": "index-pattern",
            }])


# ──────────────────────────────────────────────
# 3. DASHBOARD
# ──────────────────────────────────────────────

# Kibana grid : 48 colonnes, hauteur en unités (~20 px)
PANEL_DEFS = [
    {"vis_id": "vis-wti-close",    "grid": {"x": 0,  "y": 0,  "w": 24, "h": 15, "i": "p1"}},
    {"vis_id": "vis-stress-index", "grid": {"x": 24, "y": 0,  "w": 24, "h": 15, "i": "p2"}},
    {"vis_id": "vis-price-vs-stress", "grid": {"x": 0, "y": 15, "w": 48, "h": 15, "i": "p6"}},
    {"vis_id": "vis-volume",       "grid": {"x": 0,  "y": 30, "w": 24, "h": 12, "i": "p3"}},
    {"vis_id": "vis-geo-scores",   "grid": {"x": 24, "y": 30, "w": 24, "h": 12, "i": "p4"}},
    {"vis_id": "vis-top-actors",   "grid": {"x": 0,  "y": 42, "w": 48, "h": 15, "i": "p5"}},
]


def _create_dashboard() -> None:
    """Crée le dashboard avec les 5 panneaux."""
    panels = []
    references = []
    for pdef in PANEL_DEFS:
        panels.append({
            "version": "8.10.2",
            "type": "lens",
            "gridData": pdef["grid"],
            "panelIndex": pdef["grid"]["i"],
            "embeddableConfig": {"enhancements": {}},
            "panelRefName": f"panel_{pdef['vis_id']}",
        })
        references.append({
            "id": pdef["vis_id"],
            "name": f"panel_{pdef['vis_id']}",
            "type": "lens",
        })

    attributes = {
        "title": "Oil Market — Stress Index",
        "description": "Dashboard de suivi du marché pétrolier et de l'indice de stress géopolitique.",
        "panelsJSON": json.dumps(panels),
        "timeRestore": True,
        "timeTo": "now",
        "timeFrom": "now-90d",
        "refreshInterval": {"pause": True, "value": 0},
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({
                "query": {"language": "kuery", "query": ""},
                "filter": [],
            })
        },
    }
    _upsert("dashboard", DASHBOARD_ID, attributes, references)


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────


def main() -> None:
    """Point d'entrée : Data View → Visualisations → Dashboard."""
    log.info("── Kibana Dashboard Setup ──")

    # 1. Data View
    _create_data_view()

    # 2. Visualisations
    log.info("Création des visualisations Lens …")
    _create_lens_xy("vis-wti-close",    "WTI — Cours (Close)",             ["Close"],                                                "USD/bbl")
    _create_lens_xy("vis-stress-index", "Stress Index (score_pct_7d)",     ["score_pct_7d"],                                         "Percentile 7j")
    _create_lens_xy("vis-volume",       "Volume de trading",               ["Volume"],              "Contrats",     "bar_stacked")
    _create_lens_xy("vis-geo-scores",   "Scores géopolitiques (smoothed)", ["geo_I_smoothed", "geo_B_smoothed", "geo_S_smoothed"],    "Score")
    _create_lens_dual_axis(
        "vis-price-vs-stress",
        "Prix WTI vs Stress Index (double échelle)",
        left_field="Close", right_field="score_pct_7d",
        left_label="USD/bbl", right_label="Percentile 7j",
    )
    _create_lens_donut("vis-top-actors", "Top acteurs géopolitiques",      "period_main_actor")

    # 3. Dashboard
    log.info("Création du dashboard …")
    _create_dashboard()

    log.info("── Terminé — ouvre http://localhost:5601/app/dashboards ──")


if __name__ == "__main__":
    main()
