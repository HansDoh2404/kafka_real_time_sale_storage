# Paramètres
DATA_DIR = "./data"
STREAM_INTERVAL = 10  # secondes entre les batches
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 50
NUM_STORES = 20
SALES_PER_BATCH = 1000  # nombre de ventes par fichier
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092" # "192.168.100.104:9092"
TOPIC = "sales"
S3_URL = "localhost:9000"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password"

producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "linger.ms": 10,
    "batch.num.messages": 1000,
    "queue.buffering.max.messages": 100000,  # taille du buffer en messages
    "queue.buffering.max.kbytes": 1048576,   # buffer max en Ko
    "acks": "all",
}