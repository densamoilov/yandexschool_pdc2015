CXX := mpicc
CXXFLAGS := -std=c11 -Wall -O2
LDFLAGS :=

prog := life.c

prog_obj := life.o

prog_name := life

all: $(prog_name)

$(prog_name): $(prog_obj)
	$(CXX) $(prog_obj) -o $(prog_name)

$(prog_obj): $(prog)
	$(CXX) $(CXXFLAGS) -c $(prog)  

clean:
	rm -f *.o $(prog_name) 

