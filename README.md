# Telecom Big Data — démarrage

## 1. Préparer le dossier
Copie ce dossier sur Windows (ex. C:\telecom-bigdata). Crée aussi deux sous-dossiers vides : `flume` et `logs`.

## 2. (Optionnel, si le PC rame) limiter WSL
Copie `wslconfig.txt` vers `C:\Users\<toi>\.wslconfig` (renomme-le en `.wslconfig`), puis dans PowerShell :
    wsl --shutdown
et redémarre Docker Desktop.

## 3. Lancer
Dans PowerShell, depuis le dossier :
    docker compose up -d --build
    docker compose ps
    docker stats --no-stream

## 4. Vérifier
- HDFS : http://localhost:9870 (1 datanode vivant)
- MySQL : docker compose exec mysql mysql -uroot -proot -e "show databases;"
- Kafka : docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic usage_events --bootstrap-server localhost:9092
- Sqoop : docker compose exec client sqoop version
- Flume : docker compose exec client flume-ng version
- HDFS dossiers : docker compose exec namenode hdfs dfs -mkdir -p /data/events

## 5. Arrêter
    docker compose stop      (garde les données)
    docker compose down      (supprime les conteneurs)
