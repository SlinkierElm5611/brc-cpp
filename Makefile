all: build

required_flags:= -march=native --std=c++11 -Wall

build:
	g++ $(required_flags) -o brc -Ofast -g main.cpp

asm:
	g++ $(required_flags) -S -fverbose-asm -Ofast -g main.cpp

format:
	clang-format -i *.cpp

clean:
	rm -rf brc *.o *.s
