import sys, time, datetime
from kafka import KafkaProducer

rate = int(sys.argv[1]) if len(sys.argv) > 1 else 500
mx = int(sys.argv[2]) if len(sys.argv) > 2 else 0
p = KafkaProducer(bootstrap_servers="kafka:9092", linger_ms=50, acks=1)
n = 0
with open("/data/part_stream.csv", encoding="utf-8") as f:
    next(f)  # en-tete
    t = time.time()
    for line in f:
        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        p.send("network_flows", (ts + "," + line.rstrip("\r\n")).encode())
        n += 1
        if n % rate == 0:
            p.flush()
            print(n, "messages envoyes", flush=True)
            dt = 1 - (time.time() - t)
            if dt > 0:
                time.sleep(dt)
            t = time.time()
        if mx and n >= mx:
            break
p.flush()
print("Termine :", n)