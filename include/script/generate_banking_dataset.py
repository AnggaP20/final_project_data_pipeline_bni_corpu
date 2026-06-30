"""
generate_banking_dataset.py
============================
Script untuk generate semua dataset studi kasus Banking ETL Pipeline.
Tidak perlu download dari Kaggle — semua data di-generate di sini.

Cara pakai:
    pip install pandas
    python generate_banking_dataset.py

Output (disimpan di folder ./dataset/):
    branches.csv        -> sumber dim_branch
    channels.csv        -> sumber dim_channel
    dim_date.csv        -> sumber dim_date
    customers.csv       -> sumber dim_customer
    accounts.csv        -> sumber dim_account
    transactions.csv    -> sumber fact_transactions
    fraud_labels.csv    -> sumber flag fraud
"""

import os
import random
import string
import pandas as pd
from datetime import date, datetime, timedelta

# ─── Setup ────────────────────────────────────────────────────────────────────
random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dataset")
os.makedirs(OUTPUT_DIR, exist_ok=True)

DATE_START = date(2023, 1, 1)
DATE_END   = date(2025, 12, 31)

print("=" * 60)
print("  Banking ETL Case Study — Dataset Generator")
print("=" * 60)


# ─── Helper functions ─────────────────────────────────────────────────────────
def rand_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def rand_phone():
    prefix = random.choice(["0812","0813","0821","0822","0851","0852","0857","0858","0877","0878"])
    return prefix + "".join([str(random.randint(0,9)) for _ in range(8)])

def rand_email(name):
    domains = ["gmail.com","yahoo.com","outlook.com","mail.com","icloud.com"]
    slug = name.lower().replace(" ","").replace(".","")[:10]
    return f"{slug}{random.randint(1,999)}@{random.choice(domains)}"

FIRST_NAMES_M = ["Budi","Agus","Eko","Doni","Rizki","Fajar","Hendra","Ivan","Joko","Kevin",
                 "Lukman","Maman","Nanda","Oki","Putra","Qori","Rudi","Sandi","Tono","Umar",
                 "Vino","Wahyu","Xander","Yanuar","Zaki","Aditya","Bagas","Cahyo","Dimas","Eko"]
FIRST_NAMES_F = ["Siti","Dewi","Rina","Fitri","Ayu","Bella","Citra","Diana","Eka","Feni",
                 "Gita","Hana","Indah","Julia","Kartika","Liana","Maya","Nita","Olga","Putri",
                 "Qita","Rini","Sari","Tini","Ulfa","Vera","Winda","Xena","Yuli","Zara"]
LAST_NAMES   = ["Santoso","Wijaya","Kusuma","Prasetyo","Susanto","Rahayu","Wibowo","Setiawan",
                "Hartono","Nugroho","Saputra","Hidayat","Purnomo","Wahyudi","Mulyono",
                "Gunawan","Hermawan","Siregar","Nasution","Lubis","Simanjuntak","Harahap",
                "Panjaitan","Manurung","Sihombing","Lestari","Anggraini","Permata","Safitri"]
CITIES       = ["Jakarta","Surabaya","Bandung","Medan","Bekasi","Tangerang","Depok","Semarang",
                "Makassar","Palembang","Bogor","Pekanbaru","Batam","Denpasar","Yogyakarta",
                "Malang","Solo","Banjarmasin","Pontianak","Balikpapan","Manado","Cirebon"]
PROVINCES    = ["DKI Jakarta","Jawa Barat","Jawa Tengah","Jawa Timur","Banten",
                "Sumatera Utara","Sumatera Selatan","Riau","Bali","Sulawesi Selatan",
                "Kalimantan Timur","DI Yogyakarta","Kepulauan Riau","Kalimantan Selatan"]


# ══════════════════════════════════════════════════════════════════
# 1. BRANCHES  (25 rows)
# ══════════════════════════════════════════════════════════════════
print("\n[1/7] Generating branches.csv ...")

BRANCH_DATA = [
    (1,"KCU-JKT01","KCU Jakarta Pusat",      "Jakarta",      "DKI Jakarta",        "JABODETABEK","KCU","2010-01-15"),
    (2,"KCU-JKT02","KCU Jakarta Selatan",    "Jakarta",      "DKI Jakarta",        "JABODETABEK","KCU","2011-03-10"),
    (3,"KCU-JKT03","KCU Jakarta Utara",      "Jakarta",      "DKI Jakarta",        "JABODETABEK","KCU","2012-06-01"),
    (4,"KCP-BKS01","KCP Bekasi Jaya",        "Bekasi",       "Jawa Barat",         "JABODETABEK","KCP","2013-08-20"),
    (5,"KCP-TGR01","KCP Tangerang Selatan",  "Tangerang",    "Banten",             "JABODETABEK","KCP","2014-02-14"),
    (6,"KCP-DPK01","KCP Depok Margonda",     "Depok",        "Jawa Barat",         "JABODETABEK","KCP","2015-05-05"),
    (7,"KK-BGR01", "KK Bogor Pajajaran",     "Bogor",        "Jawa Barat",         "JABODETABEK","KK", "2016-09-12"),
    (8,"KCU-BDG01","KCU Bandung Asia Afrika","Bandung",      "Jawa Barat",         "JABAR",      "KCU","2009-07-01"),
    (9,"KCP-BDG02","KCP Bandung Dago",       "Bandung",      "Jawa Barat",         "JABAR",      "KCP","2014-11-03"),
    (10,"KCP-CRM01","KCP Cirebon Kartini",   "Cirebon",      "Jawa Barat",         "JABAR",      "KCP","2017-03-20"),
    (11,"KCU-SBY01","KCU Surabaya Tunjungan","Surabaya",     "Jawa Timur",         "JATIM",      "KCU","2008-04-15"),
    (12,"KCU-SBY02","KCU Surabaya Rungkut",  "Surabaya",     "Jawa Timur",         "JATIM",      "KCU","2012-08-08"),
    (13,"KCP-MLG01","KCP Malang Kawi",       "Malang",       "Jawa Timur",         "JATIM",      "KCP","2016-01-10"),
    (14,"KCP-JBR01","KCP Jember Gajah Mada", "Jember",       "Jawa Timur",         "JATIM",      "KCP","2018-06-25"),
    (15,"KCU-SMG01","KCU Semarang Pemuda",   "Semarang",     "Jawa Tengah",        "JATENG",     "KCU","2007-12-01"),
    (16,"KCP-SOL01","KCP Solo Slamet Riyadi","Solo",         "Jawa Tengah",        "JATENG",     "KCP","2013-04-17"),
    (17,"KCP-YGY01","KCP Yogyakarta Malioboro","Yogyakarta", "DI Yogyakarta",      "JATENG",     "KCP","2011-10-10"),
    (18,"KCU-MKS01","KCU Makassar Ahmad Yani","Makassar",    "Sulawesi Selatan",   "SULAWESI",   "KCU","2009-02-28"),
    (19,"KCP-MKS02","KCP Makassar Panakukang","Makassar",    "Sulawesi Selatan",   "SULAWESI",   "KCP","2015-07-07"),
    (20,"KCU-MDN01","KCU Medan Imam Bonjol", "Medan",        "Sumatera Utara",     "SUMUT",      "KCU","2006-11-11"),
    (21,"KCP-MDN02","KCP Medan Helvetia",    "Medan",        "Sumatera Utara",     "SUMUT",      "KCP","2014-09-30"),
    (22,"KCU-PLG01","KCU Palembang Sudirman","Palembang",    "Sumatera Selatan",   "SUMSEL",     "KCU","2010-05-20"),
    (23,"KCU-PKB01","KCU Pekanbaru Sudirman","Pekanbaru",    "Riau",               "SUMSEL",     "KCU","2012-03-15"),
    (24,"KCU-DPS01","KCU Denpasar Gajah Mada","Denpasar",    "Bali",               "BALI-NUSA",  "KCU","2008-08-08"),
    (25,"KCP-DPS02","KCP Kuta Legian",       "Badung",       "Bali",               "BALI-NUSA",  "KCP","2016-12-01"),
]

df_branches = pd.DataFrame(BRANCH_DATA, columns=[
    "branch_id","branch_code","branch_name","city","province",
    "region","branch_type","open_date"
])
df_branches["is_active"] = True
df_branches.to_csv(f"{OUTPUT_DIR}/branches.csv", index=False)
print(f"    -> {len(df_branches)} rows  |  {OUTPUT_DIR}/branches.csv")


# ══════════════════════════════════════════════════════════════════
# 2. CHANNELS  (6 rows)
# ══════════════════════════════════════════════════════════════════
print("\n[2/7] Generating channels.csv ...")

df_channels = pd.DataFrame([
    (1,"ATM",    "ATM",                "PHYSICAL",False, "Anjungan Tunai Mandiri"),
    (2,"MB",     "Mobile Banking",     "DIGITAL", True,  "Transaksi via aplikasi mobile"),
    (3,"IB",     "Internet Banking",   "DIGITAL", True,  "Transaksi via browser / web"),
    (4,"TELLER", "Teller",             "PHYSICAL",False, "Transaksi di counter cabang"),
    (5,"EDC",    "EDC / Mesin Kasir",  "PHYSICAL",False, "Electronic Data Capture di merchant"),
    (6,"CS",     "Call Center / IVR",  "DIGITAL", True,  "Transaksi via telepon"),
], columns=["channel_id","channel_code","channel_name",
            "channel_category","is_digital","description"])
df_channels.to_csv(f"{OUTPUT_DIR}/channels.csv", index=False)
print(f"    -> {len(df_channels)} rows  |  {OUTPUT_DIR}/channels.csv")


# ══════════════════════════════════════════════════════════════════
# 3. DIM DATE  (2023-01-01 s/d 2025-12-31  =  1095 rows)
# ══════════════════════════════════════════════════════════════════
print("\n[3/7] Generating dim_date.csv ...")

ID_HOLIDAYS = {
    date(2023,1,1),date(2023,1,22),date(2023,3,22),date(2023,4,7),
    date(2023,4,21),date(2023,4,22),date(2023,4,23),date(2023,5,1),
    date(2023,5,18),date(2023,5,29),date(2023,6,1),date(2023,6,29),
    date(2023,8,17),date(2023,12,25),
    date(2024,1,1),date(2024,2,8),date(2024,2,9),date(2024,3,11),
    date(2024,3,29),date(2024,4,10),date(2024,4,11),date(2024,5,1),
    date(2024,5,9),date(2024,5,23),date(2024,6,1),date(2024,8,17),
    date(2024,12,25),
    date(2025,1,1),date(2025,3,31),date(2025,4,18),date(2025,5,1),
    date(2025,5,29),date(2025,8,17),date(2025,12,25),
}
MONTH_ID = ["","Januari","Februari","Maret","April","Mei","Juni",
            "Juli","Agustus","September","Oktober","November","Desember"]
DAY_ID   = ["","Senin","Selasa","Rabu","Kamis","Jumat","Sabtu","Minggu"]

rows_date = []
d = DATE_START
while d <= DATE_END:
    rows_date.append({
        "date_id"      : int(d.strftime("%Y%m%d")),
        "full_date"    : str(d),
        "year"         : d.year,
        "quarter"      : (d.month - 1) // 3 + 1,
        "month"        : d.month,
        "month_name"   : MONTH_ID[d.month],
        "week_of_year" : d.isocalendar()[1],
        "day_of_month" : d.day,
        "day_of_week"  : d.isoweekday(),
        "day_name"     : DAY_ID[d.isoweekday()],
        "is_weekend"   : d.isoweekday() >= 6,
        "is_holiday"   : d in ID_HOLIDAYS,
    })
    d += timedelta(days=1)

df_dates = pd.DataFrame(rows_date)
df_dates.to_csv(f"{OUTPUT_DIR}/dim_date.csv", index=False)
print(f"    -> {len(df_dates)} rows  |  {OUTPUT_DIR}/dim_date.csv")


# ══════════════════════════════════════════════════════════════════
# 4. CUSTOMERS  (2.000 rows)
# ══════════════════════════════════════════════════════════════════
print("\n[4/7] Generating customers.csv  (2.000 rows) ...")

SEGMENTS = ["RETAIL","PRIORITY","VIP"]
SEG_W    = [80, 15, 5]
JOBS     = ["PNS","TNI/POLRI","Karyawan Swasta","Wirausaha",
            "Pensiunan","Profesional","Pelajar/Mahasiswa","Ibu Rumah Tangga"]

branch_ids = df_branches["branch_id"].tolist()

rows_cust = []
for i in range(1, 2001):
    gender = random.choice(["M","F"])
    fname  = random.choice(FIRST_NAMES_M if gender=="M" else FIRST_NAMES_F)
    lname  = random.choice(LAST_NAMES)
    name   = f"{fname} {lname}"
    birth  = rand_date(date(1955,1,1), date(2005,12,31))
    reg    = rand_date(date(2010,1,1), date(2023,12,31))
    rows_cust.append({
        "customer_id"      : i,
        "customer_code"    : f"CST{i:06d}",
        "full_name"        : name,
        "gender"           : gender,
        "birth_date"       : str(birth),
        "email"            : rand_email(name),
        "phone"            : rand_phone(),
        "segment"          : random.choices(SEGMENTS, weights=SEG_W)[0],
        "job_segment"      : random.choice(JOBS),
        "city"             : random.choice(CITIES),
        "province"         : random.choice(PROVINCES),
        "registration_date": str(reg),
        "branch_id"        : random.choice(branch_ids),
        "is_active"        : random.choices([True,False], weights=[90,10])[0],
        "credit_score"     : random.randint(400, 900),
        "estimated_salary" : random.randrange(3_000_000, 50_000_001, 500_000),
    })

df_customers = pd.DataFrame(rows_cust)
df_customers.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)
print(f"    -> {len(df_customers)} rows  |  {OUTPUT_DIR}/customers.csv")


# ══════════════════════════════════════════════════════════════════
# 5. ACCOUNTS  (~3.500 rows)
# ══════════════════════════════════════════════════════════════════
print("\n[5/7] Generating accounts.csv  (~3.500 rows) ...")

PRODUCTS = {
    "TABUNGAN" : ["Tabungan Reguler","Tabungan Junior","Tabungan Bisnis","Tabungan Online"],
    "GIRO"     : ["Giro Reguler","Giro Bisnis","Giro Valas"],
    "DEPOSITO" : ["Deposito 1 Bulan","Deposito 3 Bulan","Deposito 6 Bulan","Deposito 12 Bulan"],
}
ATYPES = ["TABUNGAN","TABUNGAN","TABUNGAN","GIRO","DEPOSITO"]

rows_acc = []
acc_id = 1
for cust_row in rows_cust:
    n = random.choices([1,2,3], weights=[60,30,10])[0]
    for _ in range(n):
        atype   = random.choice(ATYPES)
        open_d  = rand_date(date(2010,1,1), date(2024,6,30))
        closed  = random.random() < 0.05
        close_d = rand_date(open_d, date(2025,6,30)) if closed else None
        rows_acc.append({
            "account_id"   : acc_id,
            "account_no"   : f"000{acc_id:08d}",
            "account_type" : atype,
            "product_name" : random.choice(PRODUCTS[atype]),
            "currency"     : random.choices(["IDR","USD"], weights=[95,5])[0],
            "open_date"    : str(open_d),
            "close_date"   : str(close_d) if close_d else None,
            "status"       : "CLOSED" if closed else random.choices(["ACTIVE","DORMANT"], weights=[90,10])[0],
            "interest_rate": round(random.uniform(0.5, 7.5), 2),
            "customer_id"  : cust_row["customer_id"],
            "branch_id"    : cust_row["branch_id"],
        })
        acc_id += 1

df_accounts = pd.DataFrame(rows_acc)
df_accounts.to_csv(f"{OUTPUT_DIR}/accounts.csv", index=False)
print(f"    -> {len(df_accounts)} rows  |  {OUTPUT_DIR}/accounts.csv")


# ══════════════════════════════════════════════════════════════════
# 6. TRANSACTIONS  (50.000 rows)
# ══════════════════════════════════════════════════════════════════
print("\n[6/7] Generating transactions.csv  (50.000 rows) ...")

active_accs = df_accounts[df_accounts["status"]=="ACTIVE"]["account_id"].tolist()
acc_to_cust  = df_accounts.set_index("account_id")["customer_id"].to_dict()
acc_to_branch= df_accounts.set_index("account_id")["branch_id"].to_dict()

TRX_TYPES = ["DEBIT","KREDIT","TRANSFER_OUT","TRANSFER_IN","PEMBAYARAN","TARIK_TUNAI","SETOR_TUNAI"]
TRX_W     = [25,25,15,15,10,5,5]
STATUSES  = ["SUCCESS","SUCCESS","SUCCESS","SUCCESS","SUCCESS","FAILED","PENDING"]
CH_IDS    = df_channels["channel_id"].tolist()
CH_W      = [20,35,15,20,5,5]

all_dates = [DATE_START + timedelta(days=i) for i in range((DATE_END - DATE_START).days + 1)]
HOUR_W    = [1,1,1,1,1,1,2,5,8,8,8,8,8,8,7,7,6,5,5,4,3,2,2,1]

rows_trx = []
for trx_id in range(1, 50_001):
    acc_id    = random.choice(active_accs)
    cust_id   = acc_to_cust[acc_id]
    branch_id = acc_to_branch[acc_id]
    trx_date  = random.choice(all_dates)
    hour      = random.choices(range(24), weights=HOUR_W)[0]
    trx_ts    = datetime(trx_date.year, trx_date.month, trx_date.day,
                         hour, random.randint(0,59), random.randint(0,59))
    ttype     = random.choices(TRX_TYPES, weights=TRX_W)[0]
    ch_id     = random.choices(CH_IDS, weights=CH_W)[0]
    status    = random.choices(STATUSES)[0]

    if ttype in ("TRANSFER_OUT","TRANSFER_IN","PEMBAYARAN"):
        amount = round(random.uniform(50_000, 10_000_000))
    elif ttype in ("TARIK_TUNAI","SETOR_TUNAI"):
        amount = random.choice([100_000,200_000,300_000,500_000,1_000_000,2_000_000])
    else:
        amount = round(random.uniform(10_000, 5_000_000))

    bal_before = round(random.uniform(100_000, 100_000_000))
    bal_after  = bal_before - amount if ttype in ("DEBIT","TRANSFER_OUT","TARIK_TUNAI","PEMBAYARAN") \
                 else bal_before + amount

    rows_trx.append({
        "transaction_id"   : trx_id,
        "transaction_code" : f"TRX{trx_id:010d}",
        "account_id"       : acc_id,
        "customer_id"      : cust_id,
        "branch_id"        : branch_id,
        "channel_id"       : ch_id,
        "transaction_date" : str(trx_date),
        "transaction_at"   : trx_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "transaction_type" : ttype,
        "amount"           : amount,
        "balance_before"   : bal_before,
        "balance_after"    : bal_after,
        "status"           : status,
        "reference_no"     : f"REF{trx_date.strftime('%Y%m%d')}{trx_id:08d}",
    })

df_transactions = pd.DataFrame(rows_trx)
df_transactions.to_csv(f"{OUTPUT_DIR}/transactions.csv", index=False)
print(f"    -> {len(df_transactions)} rows  |  {OUTPUT_DIR}/transactions.csv")


# ══════════════════════════════════════════════════════════════════
# 7. FRAUD LABELS  (~1% of SUCCESS transactions)
# ══════════════════════════════════════════════════════════════════
print("\n[7/7] Generating fraud_labels.csv ...")

FRAUD_TYPES = ["CARD_SKIMMING","PHISHING","ACCOUNT_TAKEOVER",
               "UNUSUAL_AMOUNT","VELOCITY_ABUSE","SIM_SWAP"]

rows_fraud = []
for row in rows_trx:
    if row["status"] == "SUCCESS" and random.random() < 0.012:
        rows_fraud.append({
            "transaction_id"  : row["transaction_id"],
            "transaction_code": row["transaction_code"],
            "is_fraud"        : True,
            "fraud_type"      : random.choice(FRAUD_TYPES),
            "fraud_score"     : round(random.uniform(0.70, 0.99), 4),
            "flagged_at"      : row["transaction_at"],
        })

df_fraud = pd.DataFrame(rows_fraud)
df_fraud.to_csv(f"{OUTPUT_DIR}/fraud_labels.csv", index=False)
print(f"    -> {len(df_fraud)} rows  |  {OUTPUT_DIR}/fraud_labels.csv")


# ══════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("  SELESAI! Semua dataset berhasil dibuat.")
print("=" * 60)
print(f"\n  Output: {os.path.abspath(OUTPUT_DIR)}/\n")
sizes = [
    ("branches.csv",    len(df_branches),    "-> dim_branch"),
    ("channels.csv",    len(df_channels),    "-> dim_channel"),
    ("dim_date.csv",    len(df_dates),       "-> dim_date  (2023-2025)"),
    ("customers.csv",   len(df_customers),   "-> dim_customer"),
    ("accounts.csv",    len(df_accounts),    "-> dim_account"),
    ("transactions.csv",len(df_transactions),"-> fact_transactions (source)"),
    ("fraud_labels.csv",len(df_fraud),       "-> fraud flag (~1%)"),
]
for fname, n, note in sizes:
    print(f"  {fname:<22} {n:>7} rows  {note}")

print(f"\n  Dependency: pip install pandas")
print(f"  Tidak perlu Faker / Kaggle — semua pure Python stdlib + pandas")
print("\n  Next step:")
print("  1. Letakkan folder dataset/ di root project Airflow")
print("  2. Jalankan DAG: dag_banking_etl")
print("  3. Cek hasil di PostgreSQL schema staging & dw")
print("=" * 60)
