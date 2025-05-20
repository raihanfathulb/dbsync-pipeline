# dbsync-pipeline

ID

Sinkronisasi data otomatis antar-database melalui proses penghapusan (delete) dan pembaruan (upsert).  
Cocok digunakan untuk menjaga konsistensi data secara rutin di lingkungan terotomatisasi seperti cron job.

---

## 🧩 Deskripsi Proyek

Proyek ini menyediakan pipeline ringan untuk menjaga sinkronisasi antara dua sistem database.  
Script ini bisa dijalankan secara terjadwal (misalnya melalui cron) untuk:

- Menghapus data yang sudah ditandai sebagai dihapus di sumber.
- Memperbarui atau menambahkan data baru ke tujuan berdasarkan data sumber.

---

## 🚀 Fitur Utama

- Sinkronisasi data antar-database (delete dan upsert)
- Dukungan untuk berbagai jenis database (PostgreSQL, MySQL, dll.)
- Menggunakan file checkpoint (CSV) untuk pencatatan waktu terakhir eksekusi
- Logging waktu eksekusi dan status proses
- Mudah dikonfigurasi via placeholder
----------------------------------------------------------------------------------
ENG

Automated cross-database data synchronization using delete and upsert operations. 
Designed to help keep your systems in sync via scheduled jobs like cron.\

🧩 Project Description

This project provides a simple data pipeline to synchronize records between two databases.
It can be scheduled to run periodically (e.g., via cron) to:

- Remove data that has been marked as deleted in the source
- Insert or update records in the destination based on source data

🚀 Key Features
- Synchronizes records via delete and upsert
- Supports multiple databases (PostgreSQL, MySQL, etc.)
- Uses CSV checkpoint files to track last run timestamp
- Logs processing time and status
- Easy to configure via placeholders




