import random 
import time 
from datetime import datetime 
import psycopg2 
import simplejson as json 
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer 
from main import delivery_report 

conf = {
    "bootstrap.servers": "localhost:9092", 
}
consumer = Consumer(conf | {
    "group.id": "voting-group", 
    "auto.offset.reset": "earliest", 
    "enable.auto.commit": False
})
producer = SerializingProducer(conf)
def consume_message(): 
    res = []
    consumer.subscribe(["Candidates_topic"])
    try: 
        while True: 
            msg = consumer.poll()
            if msg is None:
                continue 
            elif msg.error(): 
                if msg.error().code() == KafkaError._PARTITION_EOF: 
                    continue 
                else: 
                    print(msg.error())
                    break 
            else: 
                res.append(json.loads(msg.value().decode("utf-8")))
                if len(res) == 3: 
                    return res 
    except KafkaException as e: 
        print(e)

if __name__ == "__main__": 
    conn = psycopg2.connect("host=localhost dbname=Voting user=postgres password=postgres")
    cur = conn.cursor()
    #Candidates 
    q = cur.execute("""SELECT row_to_json(t) FROM (SELECT * FROM candidates) t;""")
    candidates = cur.fetchall()
    candidates = [candidate[0] for candidate in candidates]
    if len(candidates) == 0: 
        raise Exception("No candidates found..")
    else: 
        print(candidates)

    consumer.subscribe(['Voters_topic'])
    try: 
        while True: 
            msg = consumer.poll()
            if msg is None: 
                continue 
            elif msg.error(): 
                if msg.error().code() == KafkaError._PARTITION_EOF: 
                    continue 
                else: 
                    print(msg.error())
                    break 
            else: 
                voter = json.loads(msg.value().decode("utf-8"))
                chosen_candidate = random.choice(candidates)
                vote = voter | chosen_candidate | {
                    "voting_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                    "vote": 1
                }
                try: 
                    print("User {} is voting for candidate : {}".format(vote['voter_id'], vote['candidate_id']))
                    cur.execute("""
                    INSERT INTO votes (voter_id, candidate_id, voting_time) VALUES (%s,%s,%s)
                    """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))
                    conn.commit()
                    
                    producer.produce(
                        "Votes_topic", 
                        key = vote['voter_id'], 
                        value = json.dumps(vote), 
                        on_delivery = delivery_report
                    )
                    producer.poll(0)
                except Exception as e: 
                    print("Error: {}".format(e))
                    continue 
            time.sleep(0.2)
    except KafkaException as e: 
        print(e)