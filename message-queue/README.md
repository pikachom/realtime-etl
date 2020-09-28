## Message Queue

# 다운로드 및 설치
http://apache.mirror.cdnetworks.com/kafka/2.6.0/kafka_2.13-2.6.0.tgz

# 압축 해제 및 폴더 진
$tar -xzf kafka_2.13-2.6.0.tgz
$cd kafka_2.13-2.6.0

# ZooKeeper 및 Kafka Broker 실행 (session 2개 필요)
$ bin/zookeeper-server-start.sh config/zookeeper.properties
$ bin/kafka-server-start.sh config/server.properties

# 'test-topic-1' Topic 추가 (새 session 필요)
$ bin/kafka-topics.sh --create --topic test-topic-1 --bootstrap-server localhost:9092

# 종료
ZooKeeper 및 Kafka Broker session 에서 Ctrl-C
