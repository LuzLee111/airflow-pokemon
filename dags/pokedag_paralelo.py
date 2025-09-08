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
from requests.exceptions import ConnectionError, HTTPError
logging.getLogger("airflow.hooks.base").setLevel(logging.ERROR)

POKEMON_LIMIT = 1000
OUTPUT_PATH = "/tmp/pokemon_data/pokemon_base.csv"
POKEMON_DATA_PATH = "/tmp/pokemon_data/pokemon_data.json"
SPECIES_DATA_PATH = "/tmp/pokemon_data/species_data.json"
MERGED_DATA_PATH = "/tmp/pokemon_data/pokemon_merged.csv"

default_args = {
    'owner': 'pablo',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'depends_on_past': False,
}

# Tarea A: /pokemon/{id}
def download_pokemon_data(**kwargs):
    import os
    import json
    import time
    import logging
    from airflow.providers.http.hooks.http import HttpHook

    # Obtener la lista de Pokémon desde el XCom de fetch_pokemon_list
    ti = kwargs['ti']
    results = json.loads(ti.xcom_pull(task_ids='fetch_pokemon_list'))['results']
    os.makedirs(os.path.dirname(POKEMON_DATA_PATH), exist_ok=True)

    # Si el archivo pokemon_data.json ya existe, lo cargamos para evitar repetir descargas
    if os.path.exists(POKEMON_DATA_PATH):
        with open(POKEMON_DATA_PATH, 'r') as f:
            pokemon_data = json.load(f)

        # Creamos un set con los nombres ya descargados para comparación rápida
        done_names = {p['name'] for p in pokemon_data}
        logging.info(f"[INFO] Ya existen {len(done_names)} pokémon descargados.")
    else:
        pokemon_data = []
        done_names = set()

    # Inicializamos el hook HTTP para hacer las requests a la pokeapi
    hook = HttpHook(http_conn_id='pokeapi', method='GET')

    try:
        # Iteramos sobre los Pokémon disponibles
        for i, entry in enumerate(results):
            name = entry['name']

            # Si ya lo descargamos antes, lo salteamos
            if name in done_names:
                continue

            url = entry['url']
            pokemon_id = url.strip('/').split('/')[-1]
            endpoint = f"/pokemon/{pokemon_id}/"

            # Hacemos la request a la API
            res = hook.run(endpoint)
            pokemon = res.json()

            # Guardamos el JSON crudo en la lista
            pokemon_data.append(pokemon)
            done_names.add(name)

            # Guardado parcial cada 100 Pokémon
            if (i + 1) % 100 == 0:
                with open(POKEMON_DATA_PATH, 'w') as f:
                    json.dump(pokemon_data, f)
                logging.info(f"[INFO] {i + 1} pokémon procesados (hasta ahora {len(pokemon_data)} guardados)")

            # Para no saturar la API
            time.sleep(0.5)

    except Exception as e:
        # Si hay error, guardamos lo que se pudo descargar y relanzamos el error
        logging.error(f"[ERROR] Interrupción en pokémon: {e}")
        with open(POKEMON_DATA_PATH, 'w') as f:
            json.dump(pokemon_data, f)
        raise e

    # Guardado final completo
    with open(POKEMON_DATA_PATH, 'w') as f:
        json.dump(pokemon_data, f)

    logging.info(f"[INFO] Descarga finalizada con {len(pokemon_data)} pokémon.")


# Tarea B: /pokemon-species/{id}
def download_species_data(**kwargs):
    import os
    import json
    import time
    import logging
    from airflow.providers.http.hooks.http import HttpHook

    # Obtener lista de Pokémon desde la tarea anterior (fetch_pokemon_list)
    ti = kwargs['ti']
    results = json.loads(ti.xcom_pull(task_ids='fetch_pokemon_list'))['results']
    os.makedirs(os.path.dirname(SPECIES_DATA_PATH), exist_ok=True)

    # Si el archivo species_data.json ya existe, lo cargamos para evitar repeticiones
    if os.path.exists(SPECIES_DATA_PATH):
        with open(SPECIES_DATA_PATH, 'r') as f:
            species_data = json.load(f)

        # Creamos un set con los nombres ya descargados para comparación rápida
        done_names = {s['name'] for s in species_data}
        logging.info(f"[INFO] Ya existen {len(done_names)} species descargadas.")
    else:
        species_data = []
        done_names = set()

    # Inicializamos el hook para hacer las requests a pokeapi
    hook = HttpHook(http_conn_id='pokeapi', method='GET')

    try:
        # Iteramos sobre todos los Pokémon recibidos en la lista original
        for i, entry in enumerate(results):
            name = entry['name']

            # Si ya descargamos esta species previamente, la salteamos
            if name in done_names:
                continue

            url = entry['url']
            pokemon_id = url.strip('/').split('/')[-1]
            endpoint = f"/pokemon-species/{pokemon_id}/"

            # Hacemos la request y parseamos la respuesta
            res = hook.run(endpoint)
            species = res.json()

            # Guardamos nombre, generación e info de legendario
            species_data.append({
                'name': species['name'],
                'generation': species['generation']['name'],
                'is_legendary': species['is_legendary']
            })
            done_names.add(species['name'])

            # Cada 100 species, escribimos un backup del archivo parcial
            if (i + 1) % 100 == 0:
                with open(SPECIES_DATA_PATH, 'w') as f:
                    json.dump(species_data, f)
                logging.info(f"[INFO] {i + 1} species procesadas (hasta ahora {len(species_data)} guardadas)")

            # Dormimos medio segundo para no saturar la API
            time.sleep(0.5)

    except Exception as e:
        # Si algo falla, guardamos lo que se haya descargado hasta ahora
        logging.error(f"[ERROR] Interrupción en species: {e}")
        with open(SPECIES_DATA_PATH, 'w') as f:
            json.dump(species_data, f)
        raise e  # relanzamos el error para que Airflow marque la tarea como fallida

    # Guardado final completo por si el total no es múltiplo de 100
    with open(SPECIES_DATA_PATH, 'w') as f:
        json.dump(species_data, f)

    logging.info(f"[INFO] Descarga finalizada con {len(species_data)} species.")


# Tarea C: combinar y transformar
def merge_and_transform_data(**kwargs):
    with open(POKEMON_DATA_PATH, 'r') as f:
        pokemon_data = json.load(f)
    with open(SPECIES_DATA_PATH, 'r') as f:
        species_data = json.load(f)
    species_lookup = {
        s['name']: {'generation': s['generation'], 'is_legendary': s['is_legendary']}
        for s in species_data
    }
    tidy_records = []
    for p in pokemon_data:
        p_info = species_lookup.get(p['name'], {})  # puede quedar como None
        stats = {s['stat']['name']: s['base_stat'] for s in p.get('stats', [])}
        types = sorted(p.get('types', []), key=lambda t: t['slot'])
        tidy_records.append({
            "id": p.get("id"),
            "name": p.get("name"),
            "height": p.get("height"),
            "weight": p.get("weight"),
            "base_experience": p.get("base_experience"),
            "generation": p_info.get("generation"),
            "is_legendary": p_info.get("is_legendary", False),
            "type_1": types[0]['type']['name'] if len(types) > 0 else None,
            "type_2": types[1]['type']['name'] if len(types) > 1 else None,
            "hp": stats.get("hp"),
            "attack": stats.get("attack"),
            "defense": stats.get("defense"),
            "special-attack": stats.get("special-attack"),
            "special-defense": stats.get("special-defense"),
            "speed": stats.get("speed"),
        })
    df = pd.DataFrame(tidy_records)
    os.makedirs(os.path.dirname(MERGED_DATA_PATH), exist_ok=True)
    df.to_csv(MERGED_DATA_PATH, index=False)
    print(f"[INFO] CSV guardado en: {MERGED_DATA_PATH}")

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

# DAG
with DAG(
    dag_id='pokemon_base_etl_parallel',
    description='DAG ETL paralelo que une data de /pokemon y /pokemon-species',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['pokemon', 'parallel', 'etl']
) as dag:

    fetch_pokemon_list = HttpOperator(
        task_id='fetch_pokemon_list',
        http_conn_id='pokeapi',
        endpoint=f'/pokemon?limit={POKEMON_LIMIT}',
        method='GET',
        log_response=True,
        response_filter=lambda response: response.text,
        do_xcom_push=True,
    )

    download_a = PythonOperator(
        task_id='download_pokemon_data',
        python_callable=download_pokemon_data,
    )

    download_b = PythonOperator(
        task_id='download_species_data',
        python_callable=download_species_data,
    )

    merge_transform = PythonOperator(
        task_id='merge_and_transform_data',
        python_callable=merge_and_transform_data,
    )

    fetch_pokemon_list >> [download_a, download_b] >> merge_transform
