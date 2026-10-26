"""Création de la SparkSession (partagée par toutes les parties).

Par défaut : Spark Connect (Spark dans Docker, client Python local, sans Java/winutils).
Si SPARK_REMOTE est vide : session Spark classique avec config s3a appliquée ici.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from . import config


def build_spark(app_name: str = "UrbanHub") -> SparkSession:
    if config.SPARK_REMOTE:
        print(f"Connexion à Spark Connect ({config.SPARK_REMOTE}) ... "
              f"(si ça bloque : vérifier `docker compose ps` -> spark healthy)", flush=True)
        spark = SparkSession.builder.appName(app_name).remote(config.SPARK_REMOTE).getOrCreate()
        print(f"Connecté. Spark {spark.version}", flush=True)
        return spark

    # Mode classique (tout en local/conteneur) : on configure s3a nous-mêmes.
    spark = (SparkSession.builder
             .appName(app_name)
             .config("spark.jars.packages",
                     "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
             .config("spark.hadoop.fs.s3a.endpoint", f"http://{config.MINIO_ENDPOINT}")
             .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
             .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark {spark.version} (session locale), MinIO sur {config.MINIO_ENDPOINT}")
    return spark
