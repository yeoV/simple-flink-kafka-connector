# Simple Flink-Kafka Connector

Kafa topic에서 데이터를 가져오고 2개의 Topic을 조인할 수 있는 간단한 커넥터.

Flink의 Session mode로 실행

Read Kafka topic, Sink kafka Topic, Join Topics log data using flink

- kafka source / kafka sink

### Architecture
<img src="https://user-images.githubusercontent.com/59818703/225208630-e3f00419-5acb-430b-a6f9-1e1fe51589a1.png" width="50%" height="50%">


### Environment

- Java : 11
- Flink : 1.16.1-bin-scala_2.12

## How to use

### Using JoinTopic

`src/main/java/examples/JoinTopics`

1. Java Class 내 Kafka Property 값을 변경
- Change kafka Property for your system

```java
prop.setProperty("BOOTSTRAP_SERVERS", "localhost:9092");
prop.setProperty("FIRST_TOPIC", { your source topic name });
prop.setProperty("SECOND_TOPIC", { your source topic name });
prop.setProperty("JOIN_TOPIC", { your sink topic name });
```

2. 사용할 Join SQL 쿼리문 변경
- Change Join SQL query

```java
Table result = tableEnv.sqlQuery( "{ set your Query }");
```

3. model, schema 폴더에 POJO 및 serialize , deserialize 코드 추가
4. Maven Packaing `.jar` file

### Using ReadTopic
`src/main/java/examples/ReadTopics`

- Change target topic serialization

### Packaging

Change in `pom.xml` file

```xml
<mainClass>{ target main classpath }</mainClass>
```

### Source Code Tree

```bash
.
├── README.md
├── data
├── pom.xml
├── src
   ├── main
   │   ├── java
   │   │   └── examples
   │   │       ├── JoinTopics.java
   │   │       ├── ReadTopic.java
   │   │       ├── model
   │   │       │   ├── AddressInfo.java
   │   │       │   ├── AddressTopic.java
   │   │       │   ├── GetPersonNameTopic.java
   │   │       │   ├── JoinTopic.java
   │   │       │   └── PersonTopic.java
   │   │       └── schema
   │   │           ├── AddressTopicDeserializationSchema.java
   │   │           ├── GetPersonNameSerializationSchema.java
   │   │           ├── JoinTopicSerializationSchema.java
   │   │           └── PersonTopicDeserializationSchema.java
   │   └── resources
   │       └── log4j.properties
   └── test

```

- model 디렉토리
    - Topic의 POJO 코드
    - Log 데이터 포맷에 맞게 코드 수정 및 추가
- schema
    - Serialization / Deserialization 을 위한 코드

## Run

**Run flink cluster**

- Flink Dashboard
    - [http://localhost:8081/](http://localhost:8081/)

```bash
./bin/start-cluster.sh
```

**********Stop flink cluster**********

```bash
./bin/stop-cluster.sh
```

**Submit Job**

- Check Running Job List on Dashboard

```java
./bin/flink run <job name>
```
