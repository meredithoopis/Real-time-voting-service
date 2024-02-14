Real-time streaming system: Using fake data 
===============================

This is a real-time voting system for illustration purposes only. In my case, there are three candidates and voters' data is generated randomly. Data about the candidates and voters as well as their votes are stored in a PostgreSQL database alongside being streamed to Kafka. Data from Kafka will be read and aggregated using Spark streaming and finally voting results are visible through Streamlit. All services are containerzied. 


## About 
- **main.py**: Create tables in PostgreSQL and topics in Kafka 
- **voting.py**: Consuming votes to Kafka
- **spark-streaming.py**: Reading streams from Kafka to aggregate data 
- **streamlit-app.py**: Showcase results 

### Requirements
- Python >= 3.9
- Docker + Docker Compose 

### How-to
Clone this repository and run: 

```bash
docker-compose up -d
```

### Running each service 
1. Install packages: 

```bash
pip install -r requirements.txt
```

2. Creating tables on PostgreSQL and topics on Kafka: 

```bash
python main.py
```

3. Consuming the voter information from Kafka topic, generating voting data and producing data to Kafka topic:

```bash
python voting.py
```

4. Aggregating data using Spark:

```bash
python spark-streaming.py
```

5. Using Streamlit as the interface:

```bash
streamlit run streamlit-app.py
```

## Illustrations
### Candidates
![Candidates.png](imgs%2FCandidates.png)

### Voters
![voters.png](imgs%2Fvoters.png)

### Voting
![voting.png](imgs%2Fvoting.png)

### Spark Jobs 
![spark.png](imgs%2Fspark.png)

### UI
![streamlit.png](imgs%2Fstreamlit.png)

