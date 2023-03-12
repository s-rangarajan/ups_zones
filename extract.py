import io
import csv
import os
import pandas as pd
from uritemplate import expand
import psycopg2

def create_zone_mappings():
    try:
        conn = psycopg2.connect("dbname=ups_zones user=postgres")
        cursor = conn.cursor()

        cursor.execute("""
            TRUNCATE zone_mappings;

            WITH data AS MATERIALIZED (
                SELECT origin,
                        destination,
                        UNNEST(
                            ARRAY['ground', 'three_day_select', 'two_day_air', 'two_day_air_am', 'next_day_air_saver', 'next_day_air']
                        ) AS service_level,
                        UNNEST(
                            ARRAY[ground, three_day_select, two_day_air, two_day_air_am, next_day_air_saver, next_day_air]
                        ) AS zone
                FROM raw_data
            )
            INSERT INTO zone_mappings(
                origin_location,
                origin_country_code,
                destination_location,
                destination_country_code,
                service_level_id,
                zone_id
            )
            SELECT data.origin,
                    'US',
                    data.destination,
                    'US',
                    service_levels.service_level_id,
                    zones.zone_id
            FROM data
            JOIN service_levels ON service_levels.service_level = data.service_level
            JOIN zones ON zones.zone = data.zone
            ON CONFLICT DO NOTHING;
        """)

        conn.commit()
        cursor.close()
    finally:
        conn.close()

def create_data_structures():
    try:
        conn = psycopg2.connect("dbname=ups_zones user=postgres")
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS zones(
                zone_id BIGSERIAL PRIMARY KEY,
                zone TEXT UNIQUE NOT NULL,
                zone_sla TEXT,
                zone_price TEXT,
                zone_distance TEXT,
                zone_properties JSONB
            );

            CREATE TABLE IF NOT EXISTS service_levels(
                service_level_id BIGSERIAL PRIMARY KEY,
                service_level TEXT UNIQUE NOT NULL,
                service_level_properties JSONB
            );

            CREATE TABLE IF NOT EXISTS zone_mappings(
                zone_mapping_id BIGSERIAL PRIMARY KEY,
                origin_location TEXT,
                origin_country_code TEXT,
                destination_location TEXT,
                destination_country_code TEXT,
                service_level_id BIGINT REFERENCES service_levels(service_level_id),
                zone_id BIGINT REFERENCES zones(zone_id),
                UNIQUE(origin_location, origin_country_code, destination_location, destination_country_code, service_level_id, zone_id)
            );

            TRUNCATE zones CASCADE;
            TRUNCATE service_levels CASCADE;

            INSERT INTO service_levels(service_level) VALUES
            ('ground'), ('three_day_select'), ('two_day_air'), ('two_day_air_am'), ('next_day_air_saver'), ('next_day_air');

            INSERT INTO zones(zone)
            SELECT sq.zone FROM (
                SELECT DISTINCT (ground) AS zone FROM raw_data
                UNION
                SELECT DISTINCT (three_day_select) FROM raw_data
                UNION
                SELECT DISTINCT (two_day_air) FROM raw_data
                UNION
                SELECT DISTINCT (two_day_air_am) FROM raw_data
                union
                SELECT DISTINCT (next_day_air_saver) FROM raw_data
                UNION
                SELECT DISTINCT (next_day_air) FROM raw_data
            ) sq
            WHERE sq.zone <> '-'
            AND sq.zone IS NOT NULL;
        """)

        conn.commit()
        cursor.close()
    finally:
        conn.close()

def eat_ups_files():
    directory = "/Users/srangarajan/ups_zone_parsing/scraped_ups_zone_files/parsed"

    try:
        conn = psycopg2.connect("dbname=ups_zones user=postgres")
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_data(
                origin TEXT,
                idx INT,
                destination TEXT,
                ground TEXT,
                three_day_select TEXT,
                two_day_air TEXT,
                two_day_air_am TEXT,
                next_day_air_saver TEXT,
                next_day_air TEXT,
                unknown1 TEXT,
                unknown2 TEXT
            );

            TRUNCATE raw_data;
        """)

        for file in os.listdir(directory):
            filename = os.fsencode(os.path.join(directory, file))
            print(filename.decode("utf-8"))

            with open(filename, 'r') as fp:
                cursor.copy_expert(f"""COPY raw_data FROM '{filename.decode("utf-8")}' CSV HEADER""", fp)

        conn.commit()
        cursor.close()
    finally:
        conn.close()


def parse_ups_files():
    directory = "/Users/srangarajan/ups_zone_parsing/scraped_ups_zone_files"

    for file in os.listdir(directory):
        filename = os.fsencode(os.path.join(directory, file))
        parsed_file = os.fsencode(os.path.join(directory, "parsed", f"{os.path.splitext(file)[0]}.csv"))

        if os.path.isdir(filename) or os.path.exists(parsed_file):
            continue

        with open(filename, 'r') as raw_fp, open(parsed_file, 'w') as parsed_fp:
            print(filename.decode("utf-8"))
            parsed_fp.write("origin,idx,destination,ground,three_day_select,two_day_air,two_day_air_am,next_day_air_saver,next_day_air,unknown1,unknown2\n")

            for index, line in enumerate(raw_fp):
                if int(index) > 8:
                    if line.strip().split(',')[1] == "":
                        break

                    parsed_fp.write(f"{os.path.splitext(file)[0]},{line}")

def download_ups_files():
    ups_uri = "https://www.ups.com/media/us/currentrates/zone-csv/{post_code_prefix}.xls"
    save_directory = "/Users/srangarajan/ups_zone_parsing/scraped_ups_zone_files"

    for i in range(1, 1000):
        post_code_prefix = "{:03d}".format(i)
        csv_filename = os.path.join(save_directory, f"{post_code_prefix}.csv")
        if os.path.exists(csv_filename):
            print("Skipping {post_code_prefix}, processed")
            continue
        uri = expand(ups_uri, post_code_prefix=post_code_prefix)
        print(f"Processing {post_code_prefix} into {csv_filename}")
        try:
            df = pd.DataFrame(pd.read_excel(uri))
            df.to_csv(str(csv_filename))
        except ValueError:
            print("Errored {post_code_prefix}")
