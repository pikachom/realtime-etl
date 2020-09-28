## Agent

1. Kafka Producer
    - ProObject Log 를 랜덤으로 생성하여 Producing 하는 프로그램입니다.
    - Program Arguments 로 1회 line 수, interval(ms), iteration 수 를 입력합니다.
     eg. 100 100 1000
    
2. Kafka Consumer for Test
    - Producer 가 제대로 동작하는지 Test 하는 프로그램입니다.
    - 설정값이 code 내부에 포함되어 있어, args 없이 실행합니다.