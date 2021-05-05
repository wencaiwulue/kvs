.PHONY: all
all: make node1 node2 node3

.PHONY: make
make:
	mvn clean package

.PHONY:	node1
node1:
	port=8000 java -jar target/kvs.jar

.PHONY: node2
node2:
	port=8001 java -jar target/kvs.jar

.PHONY: node3
node3:
	port=8001 java -jar target/kvs.jar
