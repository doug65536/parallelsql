all: test

clean:

.PHONY: all clean

test: main.cc
	g++ -g \
		-I/usr/include/mysql \
		-o $@ $^ -lmysqlclient
