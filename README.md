# M+ Software - Airflow Project

Repository ini berisi tentang proses **ETL (Extract-Transform-Load)** workflow menggunakan **Apache Airflow**. Script DAG yang ada di project ini akan melakukan extract, transform, agregasi, dan output dari proses tersebut akan di convert ke beberapa format (CSV, Parquet, XLSX).

---

![Flow Chart M+ Software - Airflow Project](flow_chart.png)


## Project Structure

```
.
├── dags/                   -> berisi file DAG
│   └── stt_etl_process.py  -> script untuk melakukan extract, transform dan load
├── logs/                   -> berisi log yang dihasilkan setelah dag dijalankan
├── output/                 -> hasil output dari script DAG (CSV, Parquet, XLSX)
├── docker-compose.yaml     -> Docker Compose setup for Airflow
├── Dockerfile              -> Custom Airflow image with required Python packages
├── .gitignore
└── README.md
```
<hr>

## Requirements

- Docker & Docker Compose
- Python packages (installed inside Airflow Docker image):
  - pandas
  - openpyxl

<hr>

## Setup

1. **Mengambil template docker compose resmi dari airflow**:
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
```

2. **Build Docker compose**:
```bash
export AIRFLOW_UID=50000
docker compose build
```

3. **Instalasi Airflow**:
```bash
docker compose up airflow-init
```

4. **Run Docker**:
```bash
docker compose up -d
```

<hr>

## Usage

1. Copy semua file STT1.csv dan STT2.csv ke dalam folder **`data/`**.
2. Silahkan akses Web airflow melalui **`http://localhost:8080/`**.
3. Login dengan user: *airflow* dan password: *airflow*.
4. Cari DAG berdasarkan dag id *'stt__etl_process'*.
5. Geser *toggle* ke **ON** jika belum aktif.
6. Klik tombol **Trigger DAG** (ikon *play*) lalu klik tombol *Trigger* untuk memulai proses ETL.
7. Silahkan akses hasil transform di folder **`output/`**.

<hr>

## Code Explanation

1. Set file directory (folder **`data/`** untuk csv yang akan diextract dan folder **`output/`** untuk menyimpan hasil transform) lalu masukan semua file ke variable *files* secara berurutan
```bash
input_dir='/data' # set folder data sebagai input
output_dir='/output' # set folder outpu sebagai output
files=sorted([f for f in os.listdir(input_dir)]) #
```

2. Loop seluru file yang ada di variable *files* lalu di masukkan ke list *dfs*
```bash
dfs=[] # Set list kosong

for file in files: # loop setiap file yang ada di files
    path=os.path.join(input_dir,file) # ambil path dari setiap file
    df=pd.read_csv(path) # ubah data jadi dataframe
    dfs.append(df) # append (masukkan) dataframe ke list dfs
    
    print(f"Extracted from {file}, number of rows: {len(df)}") # digunakan untuk logging -> print pesan bahwa extraction sudah berhasil
```

3. Menggabungkan dataframe yang ada di list *dfs*. Penggabungan dataframe akan di append berdasarkan urutan yang ada di list.
```bash
df_combined=pd.concat(dfs, ignore_index=True)
```

4. Menghapus duplicate berdasarkan kolom number dengan metode keep data terakhir menggunakan parameter **`keep='last'`**.
```bash
df_clean=df_combined.drop_duplicates(subset=['number'], keep='last')
```

5. Menghapus data yang kosong (null value) agar tidak diproses.
```bash
df_clean=df_clean.dropna()
```

6. Untuk kebutuhan agregasi, code berikut untuk menghitung jumlah record dan menjumlahkan amount dan di group berdasarkan kolom *date*, *client_code*, *client_type*.
```bash
df_group=df_clean.groupby(['date','client_code','client_type']).agg(
    stt_count=('number','count'),
    total_amount=('amount','sum')
).reset_index()
```

7. Memindahkan *total_amount* ke kolom baru *debit* dan *credit*.
```bash
df_group['debit']=df_group.apply(
    lambda x: x['total_amount'] if x['client_type']=='C' else None,
    axis=1
) # Jika client_type adalah C maka total_amount dimasukkan ke kolom debit

df_group['credit']=df_group.apply(
    lambda x: x['total_amount'] if x['client_type']=='V' else None,
    axis=1
) # Jika client_type adalah V maka total_amount dimasukkan ke kolom credit
```

8. Hapus kolom *client_type* dan *total_amount* karena tidak dibutuhkan
```bash
df_final=df_group.drop(['client_type','total_amount'], axis=1)
```

9. Load hasil akhir dataframe ke folder output dan simpan hasil dalam format *parquet*,*csv*, dan *excel*.
```bash
parquet_path=os.path.join(output_dir, 'df_final.parquet') # Menentukan direktori untuk file parquet
csv_path=os.path.join(output_dir, 'df_final.csv') # Menentukan direktori untuk file csv
excel_path = os.path.join(output_dir, 'df_final.xlsx') # Menentukan direktori untuk file excel

df_final.to_parquet(parquet_path, index=False) # Save dataframe menjadi file parquet
df_final.to_csv(csv_path, index=False) # Save dataframe menjadi file csv
df_final.to_excel(excel_path, index=False) # Save dataframe menjadi file excel
```

10. Logging: Untuk menampilkan hasil transform ke dalam log airflow
```bash
print(df.head()) # Menampilkan 5 data teratas dari hasil dataframe
print(f"NUMBER OF ROWS -------> {len(df)}") # Menamplikan jumlah rows dari dataframe
print("Check client_code == 'MS10001' after combine") 
print(df_combined[df_combined['client_code'] == 'MS10001']) # Menampilkan contoh dari client_code MS10001

print('Columns name -----> ',df_group.columns) # Menamplikan list kolom dari dataframe
```

11. Airflow code:
```bash
with DAG(
    dag_id="stt__etl_process", # Menentukan id DAG yang akan muncul di web airflow
    description="STT Aggregation Process: extract files → transform (combined->count and sum) → load to storage",
    start_date=datetime(2025, 11, 8), 
    schedule='0 7 * * *', # Cron job: dag akan jalan setiap jam 7 pagi
    catchup=False,
    tags=['STT','ETL']
) as dag:

    etl_task=PythonOperator(
        task_id="stt__etl_process",
        python_callable=etl_process, # Memanggil function yg akan dijalankan
    )
    etl_process
```
<hr>