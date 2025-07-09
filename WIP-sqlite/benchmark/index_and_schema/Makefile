CXXFLAGS=-std=c++20 -O3 -mtune=native -flto

# Check if macOS
ifeq ($(shell uname), Darwin)
	CXX=g++-15
	CXXFLAGS += -mcpu=apple-m1
else
	CXX=g++
	CXXFLAGS += -march=native
endif

TARGETS=schema1 schema4 schema7 schema10
TARGETS := $(addprefix bin/, $(TARGETS))

all: $(TARGETS)

bin/%: %.cpp shared.hpp
	@mkdir -p bin
	$(CXX) $(CXXFLAGS) -o $@ $< shared.hpp -lsqlite3

clean:
	rm -rf bin reports benchmark.sqlite

.PHONY: all clean

run: $(TARGETS)
	@echo "Running benchmarks..."
	@for target in $(TARGETS); do \
		echo "Running $$target..."; \
		./$$target; \
	done