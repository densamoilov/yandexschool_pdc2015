prog := phil
prog_objs := phil.o

CXX := g++
CXXFLAGS := -std=c++11 -Wall -O2
LDFLAGS := -pthread

.PHONY: all clean

all: $(prog)

$(prog): $(prog_objs)
	$(CXX) -o $@ $^ $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

phil.o: phil.cpp

clean:
	@rm -rf *.o $(prog)
