CXX := g++
CXXFLAGS := -std=c++11 -Wall -O2
LDFLAGS := -fopenmp

prog := kmeans.cpp
prog_obj := kmeans.o

prog_name := kmeans

all: $(prog_name)

$(prog_name): $(prog_obj)
	$(CXX) $(LDFLAGS) $(prog_obj) -o $(prog_name)

$(prog_obj): $(prog)
	$(CXX) $(LDFLAGS) $(CXXFLAGS) -c $(prog)  

clean:
	rm -f *.o $(prog_name)

