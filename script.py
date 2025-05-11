import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os
import requests
from datetime import datetime

# Directorios
input_dir = "raw_parquet"
output_dir = "clean_parquet"
os.makedirs(input_dir, exist_ok=True)
os.makedirs(output_dir, exist_ok=True)

# Rango de fechas
start = datetime(2022, 1, 1)
end = datetime(2024, 12, 1)
current = start

while current <= end:
    year = current.year
    month = current.month
    filebase = f"{year}_{month:02d}"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    local_path = os.path.join(input_dir, f"{filebase}.parquet")
    output_path = os.path.join(output_dir, f"{filebase}.parquet")

    print(f"\n🔽 Descargando {filebase}.parquet...")
    try:
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    except Exception as e:
        print(f"❌ Error al descargar {filebase}: {e}")
        current = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
        continue

    print(f"🧼 Limpiando {filebase}.parquet...")

    try:
        # Leer parquet
        table = pq.read_table(local_path)
        df = table.to_pandas()

        # Normalizar columnas
        df.columns = [col.lower() for col in df.columns]
        df = df.dropna()

        if 'tpep_pickup_datetime' in df.columns:
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])

        if 'passenger_count' in df.columns:
            df['passenger_count'] = df['passenger_count'].astype(int)

        # Guardar como parquet limpio
        table_clean = pa.Table.from_pandas(df)
        pq.write_table(table_clean, output_path)
        print(f"✅ Guardado limpio en: {output_path}")

    except Exception as e:
        print(f"❌ Error al limpiar {filebase}: {e}")

    # Avanzar al siguiente mes
    current = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)

print("\n🏁 Todos los archivos fueron descargados y limpiados.")
