@echo off
cd C:\kafka

.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic transport_events --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic air_quality --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic air_quality --partitions 3 --replication-factor 1
pause