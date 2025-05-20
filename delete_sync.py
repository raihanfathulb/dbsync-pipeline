import pandas as pd
import sqlalchemy 
import pytz
import requests
import datetime as dt
from urllib.parse import urlencode, quote
from sqlalchemy import create_engine
import sys
from sqlalchemy import text

# Mulai pencatatan waktu proses
first_time = dt.datetime.now()
dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 

# Koneksi ke database sumber
postgres_str = ('postgresql://<source_user>:<password>@<source_host>:<port>/<source_db>')
con_source = create_engine(postgres_str)

# Koneksi ke database tujuan
postgres_str_dest = 'postgresql://<dest_user>:<password>@<dest_host>:<port>/<dest_db>'
connect_dest = create_engine(postgres_str_dest)
con_dest = connect_dest.connect()

# Koneksi ke database log
mysql_str_cs2 = 'mysql+pymysql://<log_user>:<password>@<log_host>:<port>/<log_db>'
con_log_cs2 = create_engine(mysql_str_cs2)

# Baca dan perbarui status checkpoint
sch_status = pd.read_csv('delete_checkpoint_file.csv')
sch_status['status'] = 'active'
sch_status.to_csv('delete_checkpoint_file.csv', index=False)

# Update status cron menjadi "Running"
with con_log_cs2.connect() as con_status:
    con_status.execute("UPDATE <cron_table> SET dtu = NOW(), note = 'Running' WHERE id = '<cron_id>';")

# Ambil waktu checkpoint dari file
load_date_cron = pd.read_csv('delete_checkpoint_file.csv')
ld = load_date_cron['date'].iloc[0]
ld = '"' + ld + '"'

# Hitung batas waktu untuk query terbaru (kurangi 1 menit dari sekarang)
save_date_cron = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
current_time = dt.datetime.strptime(save_date_cron, "%Y-%m-%d %H:%M:%S")
save_date_cron = str(current_time - dt.timedelta(minutes=1))

# Ambil data log dari tabel log sumber
dflog = pd.read_sql_query('''
SELECT
    <id_column>,
    <action_column>,
    <timestamp_column>
FROM <log_table>
WHERE <timestamp_column> >= {} and <action_column> = '<delete_action>'
'''.format(ld), con_source)

# Jika tidak ada data log baru, update file checkpoint dan keluar
if len(dflog) == 0:
    save_date_cron = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sv = pd.DataFrame({'date': [save_date_cron]})
    sv['status'] = 'non_active'
    sv.to_csv('delete_checkpoint_file.csv', index=False)
    sv

    first_time = dt.datetime.now()
    later_time = dt.datetime.now()
    difference = later_time - first_time
    seconds_in_day = 24 * 60 * 60
    x_time = divmod(difference.days * seconds_in_day + difference.seconds, 60)
    print(" processing_time, {}:{} seconds".format(x_time[0], x_time[1]))
    x_ms = str(round(difference.total_seconds() * 1000, 2)) + ' ms'

    # Update status cron: selesai tanpa data
    con_log_cs2 = create_engine(mysql_str_cs2)
    with con_log_cs2.connect() as con_status:
        con_status.execute("UPDATE <cron_table> SET dtu = NOW(), note = 'Done - No Delete Data' WHERE id = '<cron_id>';")
    sys.exit()

# Persiapkan daftar ID dari log
log_id = str(dflog['<id_column>'].values.tolist()).replace('[', '(').replace(']', ')')
list_log = dflog['<id_column>'].values.tolist()

# Ambil data terkait dari tabel utama di database sumber
df = pd.read_sql_query('''
    SELECT <id_column>
    FROM <main_table>
    WHERE <id_column> in {}
'''.format(log_id), con_source)

# Bandingkan ID dari log dan data utama
list_dev3 = df['<id_column>'].values.tolist()
list_del = []

for i in list_log:
    if i not in list_dev3:
        list_del.append(i)

for j in list_dev3:
    if j not in list_log:
        list_del.append(j)

# Ambil data log yang ID-nya cocok untuk diproses
df_wh_customer_log = dflog[dflog["<id_column>"].isin(list_del)]
if len(df_wh_customer_log) == 0:
    print("no data")
else:
    df_wh_customer_log['destination'] = '<main_table>'

# Simpan log ke database tujuan (opsional, nonaktif secara default)
# df_wh_customer_log.to_sql('<log_table_dest>', con=con_dest, if_exists='append', index=False, method='multi')

# Eksekusi DELETE untuk setiap ID yang ditemukan
con_dest = create_engine(postgres_str_dest)
con_delete = con_dest.connect()

for i in list_del:
    con_delete.execute("DELETE FROM <main_table> WHERE <id_column> = '{}';".format(i))
    print("DELETE FROM <main_table> WHERE <id_column> = '{}';".format(i))

# Simpan checkpoint terbaru
sv = pd.DataFrame({'date': [save_date_cron]})
sv['status'] = 'non_active'
sv.to_csv('delete_checkpoint_file.csv', index=False)
sv

# Hitung waktu proses
later_time = dt.datetime.now()
difference = later_time - first_time
seconds_in_day = 24 * 60 * 60
save_date_cron = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
x_time = divmod(difference.days * seconds_in_day + difference.seconds, 60)
print(" processing_time, {}:{} seconds".format(x_time[0], x_time[1]))
x_ms = str(round(difference.total_seconds() * 1000, 2)) + ' ms'

# Update status cron: selesai dan berhasil
con_log_cs2 = create_engine(mysql_str_cs2)
with con_log_cs2.connect() as con_status:
    con_status.execute("UPDATE <cron_table> SET dtu = NOW(), note = 'Done - Successfully Deleted' WHERE id = '<cron_id>';")
