# ITO5202 Assessment 2: Machine Learning and Real-Time Streaming

| | |
|---|---|
| **Student ID** | 29701201 |
| **Unit code** | ITO5202 |
| **Dataset** | Brazilian E-Commerce Public Dataset by Olist |
| **Source** | https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce |

## Project overview


- **Part A** trains a Spark MLlib model that predicts, when an order is placed, whether it will be **delivered after the date Olist promised** (`is_late`, binary classification). Three models are compared (Logistic Regression, Random Forest, Gradient-Boosted Trees). The best one is saved as a `PipelineModel` in `models/a2_model`.
- **Part B** replays orders the model has never seen through **Kafka** (`producer.py`). A **Spark Structured Streaming** consumer scores each order with the saved model and counts predicted late orders in **10-minute sliding windows** with a watermark.

## Key results
| | |
|---|---|
| Orders (delivered, 2017 Q1 – 2018 Q3) | 96,203, split 70/30 with `seed=42`: 67,274 training, 28,929 streaming |
| Late-delivery rate | 6.7% (imbalanced) |
| Chosen model | Gradient-Boosted Trees, with class weighting |
| Validation | AUC-ROC 0.7174, AUC-PR 0.1703, late-class recall 0.618, precision 0.136 |
| Stream (28,929 unseen orders) | Recall 0.637, precision 0.134. Streaming predictions identical to batch scoring (0 mismatches). |
| Windowed output | 3,618 – 4,181 predicted late orders per full 10-minute window |

## Repository structure
| Path | Purpose |
|---|---|
| `README.md` | This file |
| `assessment2.ipynb` | Main notebook: all code, outputs and analysis for Parts A and B, including the reflection |
| `producer.py` | Kafka producer that replays the streaming subset (Part B.2) |
| `models/a2_model/` | Persisted Spark ML `PipelineModel` (7 stages: feature pipeline + GBT), created in A.5 and loaded in Part B |
| `data/README.md` | How to download the dataset, and the files the notebook generates |
| `.gitignore` | Excludes raw data, generated Parquet, streaming output/checkpoints and system files |

**Not committed** (recreated by running the notebook): the raw CSVs and `data/*.parquet` (see `data/README.md`), `output/` (Parquet sink) and `checkpoints/` (streaming checkpoints).

## Environment used
| Setting | Value |
|---|---|
| Spark / Jupyter container | `monashfit/fit5202-pyspark` (Spark 4.1.1, Python 3.13.12; Jupyter on port 5202, Spark UI on port 4040) |
| Kafka container | `monashfit/fit5202-kafka` |
| ZooKeeper container | `monashfit/fit5202-zookeeper` |
| Docker network | `FIT5202` (the name is case-sensitive) |
| Execution mode | Spark local mode (`local[*]`), 8 cores, 4 GB driver memory, 16 shuffle partitions |
| Session time zone | UTC (the producer's `event_timestamp` is also UTC) |
| Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1`, loaded via `spark.jars.packages` in the notebook's first Spark cell (downloaded automatically on first start, needs internet) |
| Python Kafka client | `kafka-python` 3.0.11 (`producer.py` also works with 2.x) |

## Reproducing the results

### 1. Prerequisites
- **Docker Desktop**, with at least **8 CPUs** and **6 GB of memory** under *Settings → Resources*.
- **Git**, and a **Kaggle account** to download the dataset.

### 2. Clone the repository and download the data
```bash
git clone https://github.com/ndec0001-max/ITO5202_A2.git
cd ITO5202_A2
```
Download the dataset from Kaggle and unzip the nine CSV files directly into `data/` (see [`data/README.md`](data/README.md)).

### 3. Start the containers (exact commands)
Run these in a terminal on the host machine, from the repository folder. **Start them in this order: network, ZooKeeper, Kafka, Spark.**

```bash
# 1. Docker network shared by all three containers (skip if it already exists)
docker network create FIT5202

# 2. ZooKeeper
docker run --network FIT5202 --name zookeeper -d -p 2181:2181 monashfit/fit5202-zookeeper

# 3. Kafka (wait ~10 seconds after ZooKeeper starts)
docker run --network FIT5202 --name kafka -d -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_ADVERTISED_HOST_NAME=kafka -p 9092:9092 monashfit/fit5202-kafka

# 4. Spark + Jupyter (Jupyter on port 5202, Spark UI on 4040), with this repository
#    mounted inside the container at /home/student/ITO5202_A2
docker run --network FIT5202 --name spark -d -p 5202:5202 -p 4040:4040 -v "$PWD":/home/student/ITO5202_A2 monashfit/fit5202-pyspark
```

If a Spark container already exists (for example from Assessment 1), attach it to the network instead of creating a new one:
```bash
docker network connect FIT5202 <spark-container-name>
```

**Check that everything is running:**
```bash
docker network inspect FIT5202 --format '{{range .Containers}}{{.Name}} {{end}}'   # zookeeper kafka spark
docker ps                                                                         # all three "Up"
docker logs kafka --tail 20                                                       # Kafka started, no connection errors
```

Open Jupyter at **http://localhost:5202**. If it asks for a token, run `docker logs spark` to find the login URL. The repository appears in Jupyter as the folder `ITO5202_A2`.

*(For reference, the original development container mounted the parent folder instead: `-v ~/ITO5202:/home/student`, with the project in `/home/student/Ass2`. Either layout works, as long as the notebook and `producer.py` are run from the repository folder.)*

### 4. Install the Kafka client in the Spark container (one-off)
In Jupyter, open **File → New → Terminal**. This terminal runs inside the Spark container, on the same network as Kafka.
```bash
cd ITO5202_A2
pip install --upgrade kafka-python
python producer.py --dry-run        # prints one sample record; no Kafka needed
```

### 5. Run Part A (notebook)
Open `assessment2.ipynb` and run all cells **from the top down to the end of A.5** (*Kernel → Restart*, then run the cells in order). The Environment Setup cells must run first after a restart, because Spark's driver memory and the Kafka connector can only be set when Spark starts.

This:
- builds the order-level record and the reproducible 70/30 split (`data/train_data.parquet`, `data/stream_data.parquet`);
- screens features and builds the feature pipeline (A.2);
- trains and compares three models with 3-fold cross-validation (A.3) and evaluates them (A.4);
- refits Gradient-Boosted Trees on the full training subset and saves it to `models/a2_model` (A.5).

A.3 takes the longest (12 model fits, a few minutes on 8 cores). The train/stream split is deterministic and reproduces exactly. Model metrics can differ in the last decimal places between machines with a different number of cores, because tree training samples per partition.

### 6. Run Part B (notebook + producer)
Part B needs only the Environment Setup cells, because it loads the saved model from disk. After a kernel restart:

1. Run the **Environment Setup** cells at the top of the notebook.
2. Run the Part B cells in order, from **B.3** (schema, Kafka source and model, start the prediction query) through **B.4** (windowed aggregation, start the console and memory queries). The last of these prints three running queries:
   `late_delivery_predictions`, `late_count_console`, `late_count_windows`.
3. In the **Jupyter terminal**, start the producer:
   ```bash
   cd ITO5202_A2
   python producer.py --batch-size 100
   ```
   It publishes 290 batches of 100 orders, 5 seconds apart (about 24 minutes), and logs each batch's record count and timestamp.
4. Straight away, run the **monitoring cell** under *Running the stream*. It stops automatically about 45 seconds after the producer finishes.
5. Run the remaining cells: stream results, the streaming-vs-batch check, the window table, the trend chart, and finally **stop all queries**.

**Optional checks while the stream runs:**
- Spark UI, *Structured Streaming* tab (named queries): http://localhost:4040
- Console sink output (windowed counts): `docker logs -f spark` on the host

**Producer options:**
| Option | Default | Meaning |
|---|---|---|
| `--batch-size` | 500 | Records per batch (100–1000). The final run uses **100**. |
| `--interval` | 5 | Seconds between batches |
| `--max-batches` | all | Stop early, e.g. `--max-batches 3` for a quick test |
| `--topic` | `events` | Kafka topic, created automatically if missing |
| `--bootstrap` | `kafka:9092` | Kafka address on the Docker network |
| `--dry-run` | off | Print one record and exit, without Kafka |

### 7. Shut down
On the host:
```bash
docker stop spark kafka zookeeper       # stop (keeps the containers)
docker start zookeeper                  # restart later: ZooKeeper first,
docker start kafka                      # then Kafka (wait ~10 s),
docker start spark                      # then Spark
```
To remove everything completely:
```bash
docker rm -f spark kafka zookeeper
docker network rm FIT5202
```

## Notes
- **The streaming subset is never re-split at runtime.** `producer.py` reads `data/stream_data.parquet`, created once by the notebook.
- **No model is trained in the stream.** Part B only loads `models/a2_model` and calls `.transform()`.
- Each Kafka message is one JSON array (one producer batch). `event_timestamp` (publish time, UTC) is used for windowing and the 30-second watermark. The original `order_purchase_timestamp` is kept as a separate field.
- Each run of the prediction query deletes its previous output and checkpoint and reads from the `latest` offset, so messages from earlier test runs are ignored.
- `is_late` is sent in each record only to compare predicted and actual late orders. It is not a model input.
