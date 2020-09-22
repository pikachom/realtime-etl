# Realtime-ETL


본 프로젝트는 대규모 서비스에서 발생하는 데이터를 실시간으로 분산 수집 및 처리하기 위한 프로젝트입니다.
프로젝트는 `Agent` , `MessageQueue` , `Spark-Transform` , `Storage` 모듈로 구성됩니다.


## Agent

1. 설명...
    - 설명
2. 설명...
    - 설명

## Message Queue

1. 설명...
    - 설명
2. 설명...
    - 설명

## Spark-Transform

1. 목표
    - `MessgaeQueue` 로부터 data를 consume 합니다.
    - consume해온 data를 rule에 따라 변환합니다.
        - rule은 customize할 수 있도록 config파일을 통해 주입받습니다.
    - `Storage` 에 변환한 데이터를 저장합니다.
    - 
2. 설명...
    - 설명

## Storage

1. 설명...
    - 설명
2. 설명...
    - 설명
        

