cc=g++
CFLAGS=-O3 -g -std=c++11 -c
INCLUDES=-I/usr/local/include/ -I. -I./include/
LDFLAGS=-g -lprofiler -lpthread -lstdc++
SOURCES=cache/cache.cc db/cuckoodb.cc util/logger.cc util/status.cc util/coding.cc util/crc32c.cc util/endian.cc util/xxhash.c
SOURCES_MAIN=test/cuckoodb_test.cc
# SOURCES_TEST=test/cache_test.cc
OBJECTS=$(SOURCES:.cc=.o)
OBJECTS_MAIN=$(SOURCES_MAIN:.cc=.o)
OBJECTS_TEST=$(SOURCES_TEST:.cc=.o)
EXECUTABLE=cuckoodb_test
# EXECUTABLE_TEST=cache_test

all: $(SOURCES) $(EXECUTABLE) $(EXECUTABLE_TEST)

$(EXECUTABLE): $(OBJECTS) $(OBJECTS_MAIN) 
	$(CC) $(LDFLAGS) $(OBJECTS) $(OBJECTS_MAIN) -o $@

# $(EXECUTABLE_TEST): $(OBJECTS) $(OBJECTS_TEST)
# 	$(CC) $(LDFLAGS) $(OBJECTS) $(OBJECTS_TEST) -o $@

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	rm -f *~ .*~ *.o  cache/*.o db/*.o storage_engine/*.o util/*.o $(EXECUTABLE)
