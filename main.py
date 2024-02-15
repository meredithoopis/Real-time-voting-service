import requests 
import random 
import psycopg2 
import simplejson as json 
from confluent_kafka import SerializingProducer 


base_url = "https://randomuser.me/api/?nat=gb"
parties = ["NLP Party", "Computer Vision Party", "Reinforcement Learning Party"]
random.seed(2004)

def generate_voter_data(): 
    response = requests.get(base_url)
    if response.status_code == 200: 
        user = response.json()['results'][0] 
        return {
            "voter_id": user['login']['uuid'], 
            "voter_name": f"{user['name']['first']} {user['name']['last']}", 
            "date_of_birth": user['dob']['date'], 
            "gender": user['gender'], 
            "registration_number": user['login']['username'], 
            "email": user['email'], 
            "registered_age": user['registered']['age']
        }
    else: 
        return "Error fetching data"
    

def generate_candidate_data(candidate_num, total_parties): 
    response = requests.get(base_url + "&gender=" + ('female' if candidate_num % 2 == 1 else 'male'))
    if response.status_code == 200: 
        candidate_data = response.json()['results'][0]
        return {
            "candidate_id": candidate_data['login']['uuid'], 
            "candidate_name": f"{candidate_data['name']['first']} {candidate_data['name']['last']}", 
            "party": parties[candidate_num % total_parties], 
            "biography": "I am very passionate about AI", 
            "campaign_platform": "Using social media ehehehe", 
            "photo_url": candidate_data['picture']['large']
        }
    else: 
        return "Error fetching data"

def delivery_report(err, message): 
    if err is not None: 
        print(f"Message delivery failed: {err}")
    else: 
        print(f"Message delivered to {message.topic()} [{message.partition()}]")

#w = generate_candidate_data(3,3)
#print(w)
#print(generate_voter_data())
#Kafka topics 
voters_topic = "Voters_topic"
candidates_topic = "Candidates_topic"



def create_table(conn,cur): 
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY, 
            candidate_name VARCHAR(255), 
            party VARCHAR(255), 
            biography TEXT, 
            campaign_platform TEXT, 
            photo_url TEXT
        )
        """)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS voters(
            voter_id VARCHAR(255) PRIMARY KEY, 
            voter_name VARCHAR(255), 
            date_of_birth VARCHAR(255), 
            gender VARCHAR(255), 
            registration_number VARCHAR(255), 
            email VARCHAR(255), 
            registered_age INTEGER      
        )
        """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
        voter_id VARCHAR(255) UNIQUE, 
        candidate_id VARCHAR(255), 
        voting_time TIMESTAMP, 
        vote int DEFAULT 1, 
        PRIMARY KEY (voter_id, candidate_id)
        )""")
    conn.commit()


def insert_voters(conn,cur,voter): 
    cur.execute("""
    INSERT INTO voters(voter_id, voter_name, date_of_birth, gender,registration_number, email, registered_age)
    VALUES (%s, %s, %s, %s, %s, %s,%s)""", 
    (voter['voter_id'], voter['voter_name'], voter['date_of_birth'], voter['gender'], voter['registration_number'], 
     voter['email'], voter['registered_age']))
    conn.commit()

if __name__ == "__main__": 
    #If there is error on authentication, use psql, create a database and grant all privileges on database "db_name" to postgres; 
    conn = psycopg2.connect("host=localhost dbname=Voting user=postgres password=postgres")
    cur = conn.cursor()
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092', })
    create_table(conn,cur)
    # Get candidates 
    cur.execute("""SELECT * FROM candidates""")
    candidates = cur.fetchall()
    print(candidates)
    if len(candidates) == 0: 
        for i in range(3): 
            candidate = generate_candidate_data(i,3)
            print(candidate)
            try:   
                cur.execute("""
        INSERT INTO candidates (candidate_id, candidate_name, party, biography, campaign_platform, photo_url)
        VALUES(%s, %s, %s, %s, %s, %s)
        """, (candidate['candidate_id'], candidate['candidate_name'], candidate['party'], 
              candidate['biography'], candidate['campaign_platform'], candidate['photo_url']))
                conn.commit()
            except psycopg2.IntegrityError as e: 
                conn.rollback()
    for i in range(1000): 
        voter_data = generate_voter_data()
        insert_voters(conn,cur,voter_data)
        producer.produce(
            voters_topic, 
            key = voter_data["voter_id"], 
            value = json.dumps(voter_data), 
            on_delivery=delivery_report
        )
        print("Produced voter {}, data: {}".format(i, voter_data))
        producer.flush()
