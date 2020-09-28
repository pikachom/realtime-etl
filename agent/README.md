## Message Queue & Log Agent

# Kafka 다운로드
http://apache.mirror.cdnetworks.com/kafka/2.6.0/kafka_2.13-2.6.0.tgz

# Kafka 압축 해제 및 폴더 진입
$tar -xzf kafka_2.13-2.6.0.tgz
$cd kafka_2.13-2.6.0

# ZooKeeper 및 Kafka Broker 실행 (session 2개 필요)
$ bin/zookeeper-server-start.sh config/zookeeper.properties
$ bin/kafka-server-start.sh config/server.properties

# 'test-topic-1' Topic 추가 (새 session 필요)
$ bin/kafka-topics.sh --create --topic test-topic-1 --bootstrap-server localhost:9092


# IntelliJ 에서 Project import
1. RandomProObjectLogProducer
    - ProObject Log 를 랜덤으로 생성하여 Producing 합니다.
    - Program Arguments 로 1회 line 수, interval(ms), iteration 수 를 입력합니다.
     eg. 100 100 1000
    
2. TestConsumer
    - Producer 가 제대로 동작하는지 Test 합니다.
    - 설정값이 code 내부에 포함되어 있어, args 없이 실행합니다.


# Kafka 종료
ZooKeeper 및 Kafka Broker session 에서 Ctrl-C
