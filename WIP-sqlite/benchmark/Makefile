CXXFLAGS=-std=c++20 -O3 -mtune=native -flto -lsqlite3 -fopenmp

# Check if macOS
ifeq ($(shell uname), Darwin)
	CXX=g++-15
	CXXFLAGS += -mcpu=apple-m1
else
	CXX=g++
	CXXFLAGS += -march=native
endif

TARGETS=schema1 schema4 schema7 schema10 pragmas parallel
TARGETS := $(addprefix bin/, $(TARGETS))

all: $(TARGETS)

bin/%: cpp/%.cpp cpp/shared.hpp
	@mkdir -p bin
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm -rf bin reports benchmark.sqlite

.PHONY: all clean

run: $(TARGETS)
	@echo "Running benchmarks..."
	@for target in $(TARGETS); do \
		echo "Running $$target..."; \
		./$$target; \
	done