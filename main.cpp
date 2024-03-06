#include <iostream>
#include <thread>
#include <unordered_map>
#include <string>
#include <fstream>

void print_hello() {
    std::cout << "Hello from thread" << std::endl;
}

int main() {
    std::ifstream file = std::ifstream("measurements.txt");
    std::unordered_map<std::string, int> map;
    map["one"] = 1;
    std::thread t(print_hello);
    t.join();
}
