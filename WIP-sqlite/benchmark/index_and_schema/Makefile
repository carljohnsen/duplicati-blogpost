CXXFLAGS=-std=c++20 -O3 -mtune=native -flto

# Check if macOS
ifeq ($(shell uname), Darwin)
	CXX=g++-15
	CXXFLAGS += -mcpu=apple-m1
else
	CXX=g++
	CXXFLAGS += -march=native
endif

TARGETS=schema1 schema4
TARGETS := $(addprefix bin/, $(TARGETS))

all: $(TARGETS)

bin/%: %.cpp shared.hpp
	mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -o $@ $< shared.hpp -lsqlite3