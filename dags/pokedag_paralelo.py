# dags/recs_itunes_dag.py
from __future__ import annotations
import json, math, os, re, sqlite3, zipfile
from pathlib import Path
from typing import List, Dict

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.context import Context
import pendulum

# ---------- helpers ----------
ITUNES_URL = "https://itunes.apple.com/search"

COMMON_COLS = [
    "item_id", "type", "title", "genre", "release_year",
    "price_usd", "duration_min", "country", "content_rating",
    "store_url", "artwork_url", "search_term"
]

def _request_json(url: str, params: Dict) -> Dict:
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def _safe_year(ms: int | None) -> int | None:
    # iTunes devuelve releaseDate como ISO8601; a veces falta.
    if ms is None:
        return None
    return ms

def _norm_minutes(ms: int | None) -> float | None:
    if ms is None or ms <= 0:
        return None
    return round(ms / 60000.0, 2)

def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

# ---------- DAG ----------
@dag(
    start_date=pendulum.datetime(2025, 9, 1, tz="America/Argentina/Mendoza"),
    schedule=None,  # ejecución manual; podés programarlo luego
    catchup=False,
    tags=["recs", "itunes", "ing-datos"],
    params={
        # Semillas libres: podés cambiarlas al disparar el DAG.
        "music_terms": Param(["rock", "pop", "electronica"], type="array"),
        "movie_terms": Param(["science fiction", "comedy", "drama"], type="array"),
        "limit_per_term": Param(50, type="integer", minimum=10, maximum=200),
        "country": Param("US", type="string"),
    },
    default_args={"owner": "data-eng"},
)
def recs_itunes_dag():
    """Pipeline: extracción -> transformación -> validación -> dataset final."""

    @task
    def make_run_dir(context: Context) -> str:
        run_dir = Path(f"/tmp/recs_data/{context['ds_nodash']}")
        _ensure_dir(run_dir)
        return str(run_dir)

    @task
    def extract_itunes(media: str, terms: List[str], limit: int, country: str, run_dir: str) -> str:
        """
        media: 'music' o 'movie'
        Devuelve ruta a JSONL crudo.
        """
        out = Path(run_dir) / f"{media}_raw.jsonl"
        with out.open("w", encoding="utf-8") as f:
            for term in terms:
                payload = {
                    "media": media,
                    "term": term,
                    "limit": limit,
                    "country": country,
                    # para movies conviene entity=movie; para música usamos song
                    "entity": "movie" if media == "movie" else "song",
                }
                data = _request_json(ITUNES_URL, payload)
                for row in data.get("results", []):
                    row["_search_term"] = term  # trazabilidad
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
        return str(out)

    @task
    def transform_music(raw_path: str, run_dir: str) -> str:
        rows = []
        with open(raw_path, "r", encoding="utf-8") as f:
            for line in f:
                r = json.loads(line)
                rows.append({
                    "item_id": r.get("trackId") or r.get("collectionId"),
                    "type": "music",
                    "title": r.get("trackName") or r.get("collectionName"),
                    "genre": r.get("primaryGenreName"),
                    "release_year": int(str(r.get("releaseDate", ""))[:4]) if r.get("releaseDate") else None,
                    "price_usd": r.get("trackPrice") or r.get("collectionPrice"),
                    "duration_min": _norm_minutes(r.get("trackTimeMillis")),
                    "country": r.get("country"),
                    "content_rating": r.get("contentAdvisoryRating"),
                    "store_url": r.get("trackViewUrl") or r.get("collectionViewUrl"),
                    "artwork_url": r.get("artworkUrl100"),
                    "search_term": r.get("_search_term"),
                })
        df = pd.DataFrame(rows)
        out = Path(run_dir) / "music_clean.parquet"
        df.to_parquet(out, index=False)
        return str(out)

    @task
    def transform_movies(raw_path: str, run_dir: str) -> str:
        rows = []
        with open(raw_path, "r", encoding="utf-8") as f:
            for line in f:
                r = json.loads(line)
                rows.append({
                    "item_id": r.get("trackId") or r.get("collectionId"),
                    "type": "movie",
                    "title": r.get("trackName") or r.get("collectionName"),
                    "genre": r.get("primaryGenreName"),
                    "release_year": int(str(r.get("releaseDate", ""))[:4]) if r.get("releaseDate") else None,
                    "price_usd": r.get("trackHdPrice") or r.get("trackPrice") or r.get("collectionPrice"),
                    "duration_min": _norm_minutes(r.get("trackTimeMillis")),
                    "country": r.get("country"),
                    "content_rating": r.get("contentAdvisoryRating"),
                    "store_url": r.get("trackViewUrl") or r.get("collectionViewUrl"),
                    "artwork_url": r.get("artworkUrl100"),
                    "search_term": r.get("_search_term"),
                })
        df = pd.DataFrame(rows)
        out = Path(run_dir) / "movies_clean.parquet"
        df.to_parquet(out, index=False)
        return str(out)

    @task
    def merge_and_validate(music_parquet: str, movies_parquet: str, run_dir: str) -> dict:
        m1 = pd.read_parquet(music_parquet) if Path(music_parquet).exists() else pd.DataFrame(columns=COMMON_COLS)
        m2 = pd.read_parquet(movies_parquet) if Path(movies_parquet).exists() else pd.DataFrame(columns=COMMON_COLS)
        df = pd.concat([m1, m2], ignore_index=True)

        # Normalizaciones mínimas
        df["genre"] = df["genre"].astype("string").str.lower()
        df["title"] = df["title"].astype("string").str.strip()
        df = df.drop_duplicates(subset=["type", "item_id"]).reset_index(drop=True)

        # Validación simple
        assert not df.empty, "El dataset final quedó vacío."
        assert df["title"].notna().all(), "Hay títulos nulos."
        # guardar
        csv_path = Path(run_dir) / "recs_dataset.csv"
        pq_path = Path(run_dir) / "recs_dataset.parquet"
        db_path = Path(run_dir) / "recs_dataset.sqlite"

        df.to_csv(csv_path, index=False)
        df.to_parquet(pq_path, index=False)

        # SQLite para practicar SQL y alimentar BI
        with sqlite3.connect(db_path) as con:
            df.to_sql("items", con, if_exists="replace", index=False)

        return {"csv": str(csv_path), "parquet": str(pq_path), "sqlite": str(db_path), "rows": len(df)}

    @task
    def zip_artifacts(artifacts: dict, run_dir: str) -> str:
        zip_path = Path(run_dir) / "recs_artifacts.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for k, v in artifacts.items():
                if k in {"rows"}:  # solo archivos
                    continue
                z.write(v, arcname=Path(v).name)
        # también incluimos un README básico
        readme = Path(run_dir) / "README.txt"
        readme.write_text(
            "Recs dataset generado automáticamente.\n"
            f"Filas: {artifacts['rows']}\n"
            "Archivos: recs_dataset.csv, recs_dataset.parquet, recs_dataset.sqlite\n",
            encoding="utf-8",
        )
        with zipfile.ZipFile(zip_path, "a", compression=zipfile.ZIP_DEFLATED) as z:
            z.write(readme, arcname="README.txt")
        return str(zip_path)

    run_dir = make_run_dir()

    music_raw = extract_itunes.partial(
        media="music"
    ).expand(
        terms=[ [t] for t in "{{ params.music_terms }}".split(",") ]  # truco si quisieras mapear; aquí vamos simple
    )

    # versión simple (sin map): usa params directamente
    music_raw = extract_itunes.override(task_id="extract_music")(
        media="music",
        terms="{{ params.music_terms }}",  # Airflow serializa list Param
        limit="{{ params.limit_per_term }}",
        country="{{ params.country }}",
        run_dir=run_dir,
    )

    movies_raw = extract_itunes.override(task_id="extract_movies")(
        media="movie",
        terms="{{ params.movie_terms }}",
        limit="{{ params.limit_per_term }}",
        country="{{ params.country }}",
        run_dir=run_dir,
    )

    music_clean = transform_music(music_raw, run_dir)
    movies_clean = transform_movies(movies_raw, run_dir)
    artifacts = merge_and_validate(music_clean, movies_clean, run_dir)
    zip_path = zip_artifacts(artifacts, run_dir)

recs_itunes_dag()
