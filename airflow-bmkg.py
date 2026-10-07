from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from datetime import datetime, timedelta

import requests
from pathlib import Path
import json
import pandas as pd
import mysql.connector


# ============================================================
# KONFIGURASI
# ============================================================

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# KODE ADM4 SULAWESI TENGGARA
# ============================================================

ADM4_CODES = [
    "74.01.01.1001",  # Kolaka
    "74.02.01.1001",  # Konawe
    "74.03.06.2003",  # Muna
    "74.04.11.2002",  # Buton
    "74.05.01.1001",  # Konawe Selatan
    "74.06.01.1007",  # Bombana
    "74.07.01.1002",  # Wakatobi
    "74.08.01.1001",  # Kolaka Utara
    "74.09.01.1005",  # Konawe Utara
    "74.10.01.2001",  # Buton Utara
    "74.11.01.2001",  # Kolaka Timur
    "74.12.01.1001",  # Konawe Kepulauan
    "74.13.01.2001",  # Muna Barat
    "74.14.01.2001",  # Buton Tengah
    "74.15.01.2001",  # Buton Selatan
    "74.71.02.1009",  # Kota Kendari
    "74.72.01.1007",  # Kota Baubau
]


# ============================================================
# KODE PERAIRAN SULAWESI TENGGARA
# ============================================================

MARINE_CODES = [
    "P.AF.01",
    "P.AF.02",
    "P.AF.03",
    "P.AF.04",
    "P.AF.05",
    "P.AF.06",
    "P.AF.07",
    "P.AF.08",
    "P.AF.09",
    "P.AF.10",
    "P.AF.11",
    "P.AF.12",
    "P.AF.13",
    "P.AF.14",
    "P.AF.15",
    "P.AF.16",
    "P.AF.17",
    "P.AF.18",
    "P.AF.19",
]


# ============================================================
# KODE PELABUHAN SULAWESI TENGGARA
# AF001 - AF036
# ============================================================

PORT_CODES = [
    f"AF{i:03d}" for i in range(1, 37)
]


# ============================================================
# FOLDER RAW DATA API BMKG
# ============================================================

RAW_DATA_DIR = Path("/usr/local/airflow/dags/data")
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="etl_bmkg_to_mysql",
    default_args=default_args,

    # JALAN OTOMATIS SETIAP HARI
    schedule="@daily",

    catchup=False,
    tags=["bmkg", "sulawesi-tenggara", "mysql", "etl"],
) as dag:

    # ========================================================
    # KONEKSI MYSQL
    # ========================================================

    def get_mysql_connection():

        conn = BaseHook.get_connection("mysql-local")

        return mysql.connector.connect(
            host=conn.host,
            port=conn.port or 3306,
            user=conn.login,
            password=conn.password,
            database=conn.schema,
        )


    # ========================================================
    # CREATE TABLES
    # ========================================================

    def create_tables():

        conn = get_mysql_connection()
        cursor = conn.cursor()

        # ----------------------------------------------------
        # WEATHER DATA
        # ----------------------------------------------------

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                adm1 VARCHAR(20),
                adm2 VARCHAR(20),
                adm3 VARCHAR(20),
                adm4 VARCHAR(20),
                provinsi VARCHAR(100),
                kotkab VARCHAR(100),
                kecamatan VARCHAR(100),
                desa VARCHAR(100),
                datetime DATETIME,
                suhu DECIMAL(5,2),
                tutupan_awaan DECIMAL(5,2),
                curah_hujan DECIMAL(6,2),
                weather_code INT,
                weather_desc VARCHAR(100),
                weather_desc_en VARCHAR(100),
                arah_angin_derajat DECIMAL(6,2),
                arah_angin VARCHAR(10),
                arah_angin_ke VARCHAR(10),
                kecepatan_angin DECIMAL(6,2),
                kelembapan DECIMAL(5,2),
                visibility DECIMAL(10,2),
                visibility_text VARCHAR(100),
                time_index VARCHAR(20),
                analysis_date DATETIME,
                image VARCHAR(255),
                utc_datetime DATETIME,
                local_datetime DATETIME,
                ingestion_date DATETIME
            )
        """)


        # ----------------------------------------------------
        # MARINE DATA
        # ----------------------------------------------------

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS marine_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                kode_perairan VARCHAR(50),
                nama_perairan VARCHAR(150),
                issued DATETIME,
                valid_from DATETIME,
                valid_to DATETIME,
                datetime DATETIME,
                weather VARCHAR(100),
                temp_avg DECIMAL(5,2),
                rh_avg DECIMAL(5,2),
                wind_from VARCHAR(50),
                wind_speed DECIMAL(6,2),
                wind_gust DECIMAL(6,2),
                wave_cat VARCHAR(50),
                wave_height DECIMAL(6,2),
                current_to VARCHAR(50),
                current_speed DECIMAL(6,2),
                ingestion_date DATETIME
            )
        """)


        # ----------------------------------------------------
        # PORT DATA
        # ----------------------------------------------------

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS port_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                kode_pelabuhan VARCHAR(20),
                nama_pelabuhan VARCHAR(150),
                issued DATETIME,
                valid_from DATETIME,
                valid_to DATETIME,
                datetime DATETIME,
                weather VARCHAR(100),
                visibility DECIMAL(6,2),
                temp_avg DECIMAL(5,2),
                rh_avg DECIMAL(5,2),
                wind_from VARCHAR(50),
                wind_speed DECIMAL(6,2),
                wind_gust DECIMAL(6,2),
                wave_cat VARCHAR(50),
                wave_height DECIMAL(6,2),
                current_to VARCHAR(50),
                current_speed DECIMAL(6,2),
                tides DECIMAL(6,2),
                ingestion_date DATETIME
            )
        """)


        # ----------------------------------------------------
        # TAMBAHKAN ingestion_date PADA TABEL LAMA
        # JIKA KOLOM BELUM ADA
        # ----------------------------------------------------

        tables = [
            "weather_data",
            "marine_data",
            "port_data"
        ]

        for table_name in tables:

            cursor.execute("""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = %s
                AND COLUMN_NAME = 'ingestion_date'
            """, (table_name,))

            column_exists = cursor.fetchone()[0]

            if column_exists == 0:

                cursor.execute(
                    f"""
                    ALTER TABLE {table_name}
                    ADD COLUMN ingestion_date DATETIME
                    """
                )

                print(
                    f"Kolom ingestion_date berhasil "
                    f"ditambahkan ke {table_name}."
                )

        conn.commit()

        cursor.close()
        conn.close()

        print("Semua tabel berhasil disiapkan.")


    # ========================================================
    # WEATHER ETL
    # ========================================================

    def process_weather():

        rows = []
        raw_weather = {}

        for adm4 in ADM4_CODES:

            url = (
                "https://api.bmkg.go.id/publik/prakiraan-cuaca"
                f"?adm4={adm4}"
            )

            try:

                response = requests.get(
                    url,
                    timeout=30
                )

                if response.status_code != 200:

                    print(
                        f"Gagal mengambil data weather "
                        f"{adm4}: HTTP {response.status_code}"
                    )

                    continue

                data = response.json()

                # Simpan response API asli BMKG
                raw_weather[adm4] = data

                lokasi = data.get("lokasi", {})

                provinsi = lokasi.get("provinsi")
                kotkab = lokasi.get("kotkab")
                kecamatan = lokasi.get("kecamatan")
                desa = lokasi.get("desa")

                adm1 = lokasi.get("adm1")
                adm2 = lokasi.get("adm2")
                adm3 = lokasi.get("adm3")
                adm4_result = lokasi.get("adm4")


                for forecast_group in data.get("data", []):

                    for item in forecast_group.get("cuaca", []):

                        if isinstance(item, list):
                            weather_items = item
                        else:
                            weather_items = [item]

                        for weather in weather_items:

                            rows.append({

                                "adm1": adm1,
                                "adm2": adm2,
                                "adm3": adm3,
                                "adm4": adm4_result,

                                "provinsi": provinsi,
                                "kotkab": kotkab,
                                "kecamatan": kecamatan,
                                "desa": desa,

                                "datetime": weather.get("datetime"),

                                "suhu": weather.get("t"),

                                "tutupan_awaan": weather.get("tcc"),

                                "curah_hujan": weather.get("tp"),

                                "weather_code": weather.get("weather"),

                                "weather_desc": weather.get(
                                    "weather_desc"
                                ),

                                "weather_desc_en": weather.get(
                                    "weather_desc_en"
                                ),

                                "arah_angin_derajat": weather.get(
                                    "wd_deg"
                                ),

                                "arah_angin": weather.get(
                                    "wd"
                                ),

                                "arah_angin_ke": weather.get(
                                    "wd_to"
                                ),

                                "kecepatan_angin": weather.get(
                                    "ws"
                                ),

                                "kelembapan": weather.get(
                                    "hu"
                                ),

                                "visibility": weather.get(
                                    "vs"
                                ),

                                "visibility_text": weather.get(
                                    "vs_text"
                                ),

                                "time_index": weather.get(
                                    "time_index"
                                ),

                                "analysis_date": weather.get(
                                    "analysis_date"
                                ),

                                "image": weather.get(
                                    "image"
                                ),

                                "utc_datetime": weather.get(
                                    "utc_datetime"
                                ),

                                "local_datetime": weather.get(
                                    "local_datetime"
                                ),
                            })


            except Exception as e:

                print(
                    f"Error mengambil weather {adm4}: {e}"
                )


        # ====================================================
        # SIMPAN RAW RESPONSE API BMKG KE JSON
        # ====================================================

        raw_file = (
            RAW_DATA_DIR /
            "bmkg_weather_sultra_2026.json"
        )

        with open(
            raw_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                raw_weather,
                f,
                ensure_ascii=False,
                indent=2
            )

        print(
            f"RAW Weather API berhasil disimpan ke: {raw_file}"
        )


        df = pd.DataFrame(rows)

        print(
            f"Jumlah data weather yang berhasil diambil: "
            f"{len(df)}"
        )

        if df.empty:

            print(
                "Tidak ada data weather yang berhasil diambil."
            )

            return


        # ====================================================
        # KONVERSI DATETIME
        # ====================================================

        datetime_columns = [
            "datetime",
            "analysis_date",
            "utc_datetime",
            "local_datetime",
        ]

        for column in datetime_columns:

            df[column] = pd.to_datetime(
                df[column],
                errors="coerce"
            )


        # ====================================================
        # UBAH NaN MENJADI None
        # AGAR KOMPATIBEL DENGAN MYSQL
        # ====================================================

        df = df.astype(object).where(
            pd.notna(df),
            None
        )


        # ====================================================
        # LOAD KE MYSQL
        # ====================================================

        conn = get_mysql_connection()
        cursor = conn.cursor()

        ingestion_date = datetime.now()


        insert_query = """

            INSERT INTO weather_data (

                adm1,
                adm2,
                adm3,
                adm4,

                provinsi,
                kotkab,
                kecamatan,
                desa,

                datetime,

                suhu,
                tutupan_awaan,
                curah_hujan,

                weather_code,
                weather_desc,
                weather_desc_en,

                arah_angin_derajat,
                arah_angin,
                arah_angin_ke,

                kecepatan_angin,
                kelembapan,

                visibility,
                visibility_text,

                time_index,
                analysis_date,

                image,

                utc_datetime,
                local_datetime,

                ingestion_date

            )

            VALUES (

                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s

            )

        """


        for _, row in df.iterrows():

            cursor.execute(

                insert_query,

                (

                    row.get("adm1"),
                    row.get("adm2"),
                    row.get("adm3"),
                    row.get("adm4"),

                    row.get("provinsi"),
                    row.get("kotkab"),
                    row.get("kecamatan"),
                    row.get("desa"),

                    row.get("datetime"),

                    row.get("suhu"),
                    row.get("tutupan_awaan"),
                    row.get("curah_hujan"),

                    row.get("weather_code"),
                    row.get("weather_desc"),
                    row.get("weather_desc_en"),

                    row.get("arah_angin_derajat"),
                    row.get("arah_angin"),
                    row.get("arah_angin_ke"),

                    row.get("kecepatan_angin"),
                    row.get("kelembapan"),

                    row.get("visibility"),
                    row.get("visibility_text"),

                    row.get("time_index"),
                    row.get("analysis_date"),

                    row.get("image"),

                    row.get("utc_datetime"),
                    row.get("local_datetime"),

                    ingestion_date,

                )

            )


        conn.commit()

        cursor.close()
        conn.close()

        print(
            f"Weather ETL selesai. "
            f"{len(df)} data masuk ke weather_data."
        )


    # ========================================================
    # MARINE ETL
    # ========================================================

    def process_marine():

        rows = []
        raw_marine = {}

        for marine_code in MARINE_CODES:

            url = (
                "https://maritim.bmkg.go.id/"
                f"marine2026-data/perairan/{marine_code}.json"
            )

            try:

                response = requests.get(
                    url,
                    timeout=30
                )

                if response.status_code != 200:

                    print(
                        f"Gagal mengambil marine "
                        f"{marine_code}: "
                        f"HTTP {response.status_code}"
                    )

                    continue

                data = response.json()

                # Simpan response API asli BMKG
                raw_marine[marine_code] = data

                kode_perairan = data.get("code")
                nama_perairan = data.get("name")

                issued = data.get("issued")
                valid_from = data.get("valid_from")
                valid_to = data.get("valid_to")


                for day in [
                    "forecast_day1",
                    "forecast_day2",
                    "forecast_day3",
                ]:

                    for item in data.get(day, []):

                        rows.append({

                            "kode_perairan": kode_perairan,

                            "nama_perairan": nama_perairan,

                            "issued": issued,

                            "valid_from": valid_from,

                            "valid_to": valid_to,

                            "datetime": item.get(
                                "time"
                            ),

                            "weather": item.get(
                                "weather"
                            ),

                            "temp_avg": item.get(
                                "temp_avg"
                            ),

                            "rh_avg": item.get(
                                "rh_avg"
                            ),

                            "wind_from": item.get(
                                "wind_from"
                            ),

                            "wind_speed": item.get(
                                "wind_speed"
                            ),

                            "wind_gust": item.get(
                                "wind_gust"
                            ),

                            "wave_cat": item.get(
                                "wave_cat"
                            ),

                            "wave_height": item.get(
                                "wave_height"
                            ),

                            "current_to": item.get(
                                "current_to"
                            ),

                            "current_speed": item.get(
                                "current_speed"
                            ),

                        })


            except Exception as e:

                print(
                    f"Error mengambil marine "
                    f"{marine_code}: {e}"
                )


        # ====================================================
        # SIMPAN RAW RESPONSE API MARINE KE JSON
        # ====================================================

        raw_file = (
            RAW_DATA_DIR /
            "bmkg_marine_sultra_2026.json"
        )

        with open(
            raw_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                raw_marine,
                f,
                ensure_ascii=False,
                indent=2
            )

        print(
            f"RAW Marine API berhasil disimpan ke: {raw_file}"
        )


        df = pd.DataFrame(rows)

        print(
            f"Jumlah data marine yang berhasil diambil: "
            f"{len(df)}"
        )

        if df.empty:

            print(
                "Tidak ada data marine."
            )

            return


        # ====================================================
        # KONVERSI DATETIME
        # ====================================================

        datetime_columns = [
            "issued",
            "valid_from",
            "valid_to",
            "datetime",
        ]

        for column in datetime_columns:

            df[column] = pd.to_datetime(
                df[column],
                errors="coerce"
            )


        # ====================================================
        # UBAH NaN MENJADI None
        # AGAR KOMPATIBEL DENGAN MYSQL
        # ====================================================

        df = df.astype(object).where(
            pd.notna(df),
            None
        )


        # ====================================================
        # LOAD KE MYSQL
        # ====================================================

        conn = get_mysql_connection()
        cursor = conn.cursor()

        ingestion_date = datetime.now()


        insert_query = """

            INSERT INTO marine_data (

                kode_perairan,
                nama_perairan,

                issued,
                valid_from,
                valid_to,

                datetime,

                weather,

                temp_avg,
                rh_avg,

                wind_from,
                wind_speed,
                wind_gust,

                wave_cat,
                wave_height,

                current_to,
                current_speed,

                ingestion_date

            )

            VALUES (

                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s

            )

        """


        for _, row in df.iterrows():

            cursor.execute(

                insert_query,

                (

                    row.get("kode_perairan"),
                    row.get("nama_perairan"),

                    row.get("issued"),
                    row.get("valid_from"),
                    row.get("valid_to"),

                    row.get("datetime"),

                    row.get("weather"),

                    row.get("temp_avg"),
                    row.get("rh_avg"),

                    row.get("wind_from"),
                    row.get("wind_speed"),
                    row.get("wind_gust"),

                    row.get("wave_cat"),
                    row.get("wave_height"),

                    row.get("current_to"),
                    row.get("current_speed"),

                    ingestion_date,

                )

            )


        conn.commit()

        cursor.close()
        conn.close()

        print(
            f"Marine ETL selesai. "
            f"{len(df)} data masuk ke marine_data."
        )


    # ========================================================
    # PORT ETL
    # ========================================================

    def process_ports():

        rows = []
        raw_ports = {}

        for port_code in PORT_CODES:

            url = (
                "https://maritim.bmkg.go.id/"
                f"marine2026-data/pelabuhan/{port_code}.json"
            )

            try:

                response = requests.get(
                    url,
                    timeout=30
                )

                if response.status_code != 200:

                    print(
                        f"Gagal mengambil port "
                        f"{port_code}: "
                        f"HTTP {response.status_code}"
                    )

                    continue

                data = response.json()

                # Simpan response API asli BMKG
                raw_ports[port_code] = data

                kode_pelabuhan = data.get("code")
                nama_pelabuhan = data.get("name")

                issued = data.get("issued")
                valid_from = data.get("valid_from")
                valid_to = data.get("valid_to")


                for day in [
                    "forecast_day1",
                    "forecast_day2",
                    "forecast_day3",
                ]:

                    for item in data.get(day, []):

                        rows.append({

                            "kode_pelabuhan":
                                kode_pelabuhan,

                            "nama_pelabuhan":
                                nama_pelabuhan,

                            "issued":
                                issued,

                            "valid_from":
                                valid_from,

                            "valid_to":
                                valid_to,

                            "datetime":
                                item.get("time"),

                            "weather":
                                item.get("weather"),

                            "visibility":
                                item.get("visibility"),

                            "temp_avg":
                                item.get("temp_avg"),

                            "rh_avg":
                                item.get("rh_avg"),

                            "wind_from":
                                item.get("wind_from"),

                            "wind_speed":
                                item.get("wind_speed"),

                            "wind_gust":
                                item.get("wind_gust"),

                            "wave_cat":
                                item.get("wave_cat"),

                            "wave_height":
                                item.get("wave_height"),

                            "current_to":
                                item.get("current_to"),

                            "current_speed":
                                item.get("current_speed"),

                            "tides":
                                item.get("tides"),

                        })


            except Exception as e:

                print(
                    f"Error mengambil port "
                    f"{port_code}: {e}"
                )


        # ====================================================
        # SIMPAN RAW RESPONSE API PORT KE JSON
        # ====================================================

        raw_file = (
            RAW_DATA_DIR /
            "bmkg_port_sultra_2026.json"
        )

        with open(
            raw_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                raw_ports,
                f,
                ensure_ascii=False,
                indent=2
            )

        print(
            f"RAW Port API berhasil disimpan ke: {raw_file}"
        )


        df = pd.DataFrame(rows)

        print(
            f"Jumlah data port yang berhasil diambil: "
            f"{len(df)}"
        )

        if df.empty:

            print(
                "Tidak ada data port."
            )

            return


        # ====================================================
        # KONVERSI DATETIME
        # ====================================================

        datetime_columns = [
            "issued",
            "valid_from",
            "valid_to",
            "datetime",
        ]

        for column in datetime_columns:

            df[column] = pd.to_datetime(
                df[column],
                errors="coerce"
            )


        # ====================================================
        # UBAH NaN MENJADI None
        # AGAR KOMPATIBEL DENGAN MYSQL
        # ====================================================

        df = df.astype(object).where(
            pd.notna(df),
            None
        )


        # ====================================================
        # LOAD KE MYSQL
        # ====================================================

        conn = get_mysql_connection()
        cursor = conn.cursor()

        ingestion_date = datetime.now()


        insert_query = """

            INSERT INTO port_data (

                kode_pelabuhan,
                nama_pelabuhan,

                issued,
                valid_from,
                valid_to,

                datetime,

                weather,
                visibility,

                temp_avg,
                rh_avg,

                wind_from,
                wind_speed,
                wind_gust,

                wave_cat,
                wave_height,

                current_to,
                current_speed,

                tides,

                ingestion_date

            )

            VALUES (

                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s

            )

        """


        for _, row in df.iterrows():

            cursor.execute(

                insert_query,

                (

                    row.get("kode_pelabuhan"),
                    row.get("nama_pelabuhan"),

                    row.get("issued"),
                    row.get("valid_from"),
                    row.get("valid_to"),

                    row.get("datetime"),

                    row.get("weather"),
                    row.get("visibility"),

                    row.get("temp_avg"),
                    row.get("rh_avg"),

                    row.get("wind_from"),
                    row.get("wind_speed"),
                    row.get("wind_gust"),

                    row.get("wave_cat"),
                    row.get("wave_height"),

                    row.get("current_to"),
                    row.get("current_speed"),

                    row.get("tides"),

                    ingestion_date,

                )

            )


        conn.commit()

        cursor.close()
        conn.close()

        print(
            f"Port ETL selesai. "
            f"{len(df)} data masuk ke port_data."
        )


    # ========================================================
    # TASK AIRFLOW
    # ========================================================

    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )


    process_weather_task = PythonOperator(
        task_id="process_weather",
        python_callable=process_weather,
    )


    process_marine_task = PythonOperator(
        task_id="process_marine",
        python_callable=process_marine,
    )


    process_ports_task = PythonOperator(
        task_id="process_ports",
        python_callable=process_ports,
    )


    # ========================================================
    # DEPENDENCY
    # ========================================================

    create_tables_task >> [
        process_weather_task,
        process_marine_task,
        process_ports_task,
    ]