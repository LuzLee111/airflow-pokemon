from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import pandas as pd
import os
import json
import time
import logging
from requests.exceptions import ConnectionError, HTTPError, Timeout

logging.getLogger("airflow.hooks.base").setLevel(logging.ERROR)

# ========== Config ==========
MUSIC_LIMIT = int(os.getenv("MUSIC_LIMIT", "120"))
MOVIE_LIMIT = int(os.getenv("MOVIE_LIMIT", "120"))

DATA_DIR = "/tmp/reco_data"
MUSIC_DATA_PATH = f"{DATA_DIR}/music_data.json"
MOVIE_DATA_PATH = f"{DATA_DIR}/movie_data.json"
MERGED_RECS_PATH = f"{DATA_DIR}/recs_merged.csv"

OUTPUT_DIR = "/usr/local/airflow/output"
GROUP_NAME = os.getenv("GROUP_NAME", "Grupo 7")
DAG_ID = "recomendador_musica_peliculas_parallel"

# Preferencias/semillas (puedes setear por env o dejar defaults)
FAV_MUSIC_GENRES = [g.strip().lower() for g in os.getenv("FAV_MUSIC_GENRES", "rock,pop,latin").split(",")]
FAV_MOVIE_GENRES = [g.strip().lower() for g in os.getenv("FAV_MOVIE_GENRES", "action,comedy,drama").split(",")]
MUSIC_QUERY = os.getenv("MUSIC_QUERY", "queen")   # para endpoints de búsqueda
MOVIE_QUERY = os.getenv("MOVIE_QUERY", "matrix")

default_args = {
    'owner': 'Maria',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'depends_on_past': False,
}

# ========== Helpers ==========
def _fetch_with_retry(hook: HttpHook, endpoint: str, max_tries: int = 6, sleep_base: float = 0.5):
    for attempt in range(max_tries):
        try:
            res = hook.run(endpoint)
            status = res.status_code
            if status == 404:
                return None
            if status in (429,) or (500 <= status < 600):
                wait = sleep_base * (2 ** attempt)
                logging.warning(f"[WARN] {endpoint} -> {status}. Retry en {wait:.1f}s…")
                time.sleep(wait)
                continue
            res.raise_for_status()
            return res.json()
        except (ConnectionError, Timeout, HTTPError) as e:
            wait = sleep_base * (2 ** attempt)
            logging.warning(f"[WARN] Red/HTTP en {endpoint}: {e}. Retry en {wait:.1f}s…")
            time.sleep(wait)
        except Exception as e:
            logging.warning(f"[WARN] Error inesperado en {endpoint}: {e}. Se salta.")
            return None
    logging.error(f"[ERR] Reintentos agotados: {endpoint}")
    return None

def _norm(x, min_x, max_x):
    try:
        x = float(x)
        if max_x == min_x:
            return 0.0
        return (x - min_x) / (max_x - min_x)
    except Exception:
        return 0.0

# ========== DAG ==========
with DAG(
    dag_id=DAG_ID,
    description='DAG ETL paralelo para recomendaciones de música y películas (sin ZIP ni email)',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['recs', 'music', 'movies', 'parallel', 'etl']
) as dag:

    # A) Listas iniciales (paralelo)
    fetch_music_list = HttpOperator(
        task_id='fetch_music_list',
        http_conn_id='musicapi',  # p.ej. https://api.deezer.com
        endpoint=f'/search?q={MUSIC_QUERY}&limit={MUSIC_LIMIT}',
        method='GET',
        log_response=True,
        response_filter=lambda response: response.text,
        do_xcom_push=True,
    )

    fetch_movie_list = HttpOperator(
        task_id='fetch_movie_list',
        http_conn_id='movieapi',  # p.ej. https://api.themoviedb.org/3
        endpoint=f'/search/movie?query={MOVIE_QUERY}&include_adult=false&language=en-US&page=1',
        method='GET',
        log_response=True,
        response_filter=lambda response: response.text,
        do_xcom_push=True,
    )

    # B) Detalle música
    def download_music_data(**kwargs):
        ti = kwargs['ti']
        raw = ti.xcom_pull(task_ids='fetch_music_list')
        try:
            root = json.loads(raw)
        except Exception as e:
            raise RuntimeError(f"No pude parsear JSON de music list: {e}")

        items = root.get("data") or root.get("results") or []
        os.makedirs(os.path.dirname(MUSIC_DATA_PATH), exist_ok=True)

        if os.path.exists(MUSIC_DATA_PATH):
            try:
                with open(MUSIC_DATA_PATH, "r") as f:
                    music_data = json.load(f)
            except Exception:
                music_data = []
            done_ids = {str(x.get('id')) for x in music_data if isinstance(x, dict)}
        else:
            music_data, done_ids = [], set()

        hook = HttpHook(http_conn_id='musicapi', method='GET')

        procesados = 0
        for i, it in enumerate(items, 1):
            tid = str(it.get("id") or it.get("track", {}).get("id") or "")
            if not tid or tid in done_ids:
                continue

            endpoint = f"/track/{tid}"
            data = _fetch_with_retry(hook, endpoint)
            if not data:
                continue

            # Intentamos géneros vía artista
            genre_names = []
            try:
                artist_id = data.get("artist", {}).get("id")
                if artist_id:
                    ainfo = _fetch_with_retry(hook, f"/artist/{artist_id}")
                    if ainfo and isinstance(ainfo.get("genres"), dict):
                        for g in (ainfo["genres"].get("data") or []):
                            if g.get("name"):
                                genre_names.append(g["name"].lower())
            except Exception:
                pass

            music_data.append({
                "id": data.get("id"),
                "title": data.get("title"),
                "artist": (data.get("artist") or {}).get("name"),
                "album": (data.get("album") or {}).get("title"),
                "duration": data.get("duration"),
                "rank": data.get("rank"),
                "explicit_lyrics": data.get("explicit_lyrics"),
                "genres": list(set(genre_names)),
                "type": "music"
            })
            done_ids.add(tid)
            procesados += 1

            if (i % 100) == 0:
                with open(MUSIC_DATA_PATH, "w") as f:
                    json.dump(music_data, f)
                logging.info(f"[MUSIC] {i} vistos | {procesados} guardados")

            time.sleep(0.15)

        with open(MUSIC_DATA_PATH, "w") as f:
            json.dump(music_data, f)
        logging.info(f"[MUSIC] Final: {len(music_data)} tracks guardados.")

    download_music = PythonOperator(
        task_id='download_music_data',
        python_callable=download_music_data,
    )

    # C) Detalle películas
    def download_movie_data(**kwargs):
        ti = kwargs['ti']
        raw = ti.xcom_pull(task_ids='fetch_movie_list')
        try:
            root = json.loads(raw)
        except Exception as e:
            raise RuntimeError(f"No pude parsear JSON de movie list: {e}")

        items = root.get("results") or []
        os.makedirs(os.path.dirname(MOVIE_DATA_PATH), exist_ok=True)

        if os.path.exists(MOVIE_DATA_PATH):
            try:
                with open(MOVIE_DATA_PATH, "r") as f:
                    movie_data = json.load(f)
            except Exception:
                movie_data = []
            done_ids = {int(x.get('id')) for x in movie_data if isinstance(x, dict) and x.get('id') is not None}
        else:
            movie_data, done_ids = [], set()

        hook = HttpHook(http_conn_id='movieapi', method='GET')
        api_key = os.getenv("TMDB_API_KEY")

        procesados = 0
        for i, it in enumerate(items, 1):
            mid = it.get("id")
            if not mid or mid in done_ids:
                continue

            endpoint = f"/movie/{mid}"
            if api_key:
                endpoint += f"?api_key={api_key}&language=en-US"
            data = _fetch_with_retry(hook, endpoint)
            if not data:
                continue

            genres = [g.get("name", "").lower() for g in (data.get("genres") or []) if g.get("name")]

            movie_data.append({
                "id": data.get("id"),
                "title": data.get("title"),
                "overview": data.get("overview"),
                "release_date": data.get("release_date"),
                "vote_average": data.get("vote_average"),
                "vote_count": data.get("vote_count"),
                "popularity": data.get("popularity"),
                "genres": genres,
                "original_language": data.get("original_language"),
                "adult": data.get("adult", False),
                "type": "movie"
            })
            done_ids.add(mid)
            procesados += 1

            if (i % 100) == 0:
                with open(MOVIE_DATA_PATH, "w") as f:
                    json.dump(movie_data, f)
                logging.info(f"[MOVIE] {i} vistos | {procesados} guardados")

            time.sleep(0.15)

        with open(MOVIE_DATA_PATH, "w") as f:
            json.dump(movie_data, f)
        logging.info(f"[MOVIE] Final: {len(movie_data)} películas guardadas.")

    download_movies = PythonOperator(
        task_id='download_movie_data',
        python_callable=download_movie_data,
    )

    # D) Merge + ranking (SALIDA FINAL)
    def merge_and_rank_recommendations(**kwargs):
        ds = kwargs["ds"]

        with open(MUSIC_DATA_PATH, "r") as f:
            music_data = json.load(f)
        with open(MOVIE_DATA_PATH, "r") as f:
            movie_data = json.load(f)

        # Música
        if music_data:
            ranks = [m.get("rank") or 0 for m in music_data]
            rmin, rmax = min(ranks), max(ranks)
        else:
            rmin = rmax = 0

        music_rows = []
        for m in music_data:
            genres = [g.lower() for g in (m.get("genres") or [])]
            match = len(set(g.lower() for g in FAV_MUSIC_GENRES) & set(genres))
            score = 0.7 * _norm(m.get("rank") or 0, rmin, rmax) + 0.3 * (1 if match > 0 else 0)
            music_rows.append({
                "type": "music",
                "id": m.get("id"),
                "title": m.get("title"),
                "subtitle": m.get("artist"),
                "genres": ",".join(genres) if genres else None,
                "popularity_metric": m.get("rank"),
                "score": round(score, 4)
            })

        # Películas
        if movie_data:
            pops = [mv.get("popularity") or 0 for mv in movie_data]
            vavs = [mv.get("vote_average") or 0 for mv in movie_data]
            pmin, pmax = min(pops), max(pops)
            vmin, vmax = min(vavs), max(vavs)
        else:
            pmin = pmax = vmin = vmax = 0

        movie_rows = []
        for mv in movie_data:
            genres = [g.lower() for g in (mv.get("genres") or [])]
            match = len(set(g.lower() for g in FAV_MOVIE_GENRES) & set(genres))
            score = (
                0.5 * _norm(mv.get("popularity") or 0, pmin, pmax) +
                0.3 * _norm(mv.get("vote_average") or 0, vmin, vmax) +
                0.2 * (1 if match > 0 else 0)
            )
            movie_rows.append({
                "type": "movie",
                "id": mv.get("id"),
                "title": mv.get("title"),
                "subtitle": mv.get("release_date"),
                "genres": ",".join(genres) if genres else None,
                "popularity_metric": mv.get("popularity"),
                "score": round(score, 4)
            })

        df = pd.DataFrame(music_rows + movie_rows)
        if df.empty:
            raise RuntimeError("No hay datos para recomendar. Revisa conexiones o queries.")

        df["grupo"] = GROUP_NAME

        # Top 50 y salida final
        top_all = df.sort_values("score", ascending=False).head(50)
        os.makedirs(DATA_DIR, exist_ok=True)
        top_all.to_csv(MERGED_RECS_PATH, index=False)

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        final_path = os.path.join(OUTPUT_DIR, f"recs_final_{ds}.csv")
        top_all.to_csv(final_path, index=False)
        print(f"[INFO] CSV final guardado en: {final_path}")

    merge_rank = PythonOperator(
        task_id='merge_and_rank_recommendations',
        python_callable=merge_and_rank_recommendations,
    )

    # Dependencias: extracción paralela -> detalle paralelo -> merge/rank (FIN)
    [fetch_music_list, fetch_movie_list] >> [download_music, download_movies] >> merge_rank
