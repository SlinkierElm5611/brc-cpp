#include <fstream>
#include <iostream>
#include <streambuf>
#include <string>
#include <unordered_map>

#define CHAR_BUFFER_SIZE 1000000

struct City {
  int sum;
  int count;
  int max;
  int min;
};

char get_number_from_char(char c) { return c - '0'; }

int main() {
  std::unordered_map<std::string, City> cities;
  std::ifstream file("measurements.txt");
  std::streambuf *file_buffer = file.rdbuf();
  char buffer[CHAR_BUFFER_SIZE];
  char working_city_buffer[100];
  char working_temp_buffer[4];
  int city_counter = 0;
  int temp_counter = 0;
  bool passed_semicolon = false;
  while (file_buffer->in_avail() > 0) {
    long stream_size = file_buffer->sgetn(buffer, CHAR_BUFFER_SIZE);
    for (int i = 0; i < stream_size; i++) {
      if (buffer[i] == '\n') {
        int temp = 0;
        if (temp_counter == 4) {
          temp = -1 * (100 * get_number_from_char(working_temp_buffer[1]) +
                       (10 * get_number_from_char(working_temp_buffer[2]) +
                        get_number_from_char(working_temp_buffer[3])));
        } else if (temp_counter == 3) {
          if (working_temp_buffer[0] == '-') {
            temp = -1 * (10 * get_number_from_char(working_temp_buffer[1]) +
                         get_number_from_char(working_temp_buffer[2]));
          } else {
            temp = 100 * get_number_from_char(working_temp_buffer[0]) +
                   (10 * get_number_from_char(working_temp_buffer[1]) +
                    get_number_from_char(working_temp_buffer[2]));
          }
        } else {
          temp = 10 * get_number_from_char(working_temp_buffer[0]) +
                 get_number_from_char(working_temp_buffer[1]);
        }
        passed_semicolon = false;
        std::string city_name(working_city_buffer, city_counter);
        auto city = cities.find(city_name);
        if (city == cities.end()) {
          City new_city;
          new_city.sum = temp;
          new_city.count = 1;
          new_city.max = temp;
          new_city.min = temp;
          cities[city_name] = new_city;
        } else {
          City *found_city = &city->second;
          found_city->sum += temp;
          found_city->count++;
          if (temp > found_city->max) {
            found_city->max = temp;
          }
          if (temp < found_city->min) {
            found_city->min = temp;
          }
        }
        city_counter = 0;
        temp_counter = 0;
      } else if (buffer[i] == ';') {
        passed_semicolon = true;
      } else if (!passed_semicolon) {
        working_city_buffer[city_counter] = buffer[i];
        city_counter++;
      } else if (buffer[i] != '.') {
        working_temp_buffer[temp_counter] = buffer[i];
        temp_counter++;
      }
    }
  }
  for (auto const &x : cities) {
    std::cout << "City: " << x.first << std::endl;
    std::cout << "Average: " << x.second.sum / x.second.count << std::endl;
    std::cout << "Max: " << x.second.max << std::endl;
    std::cout << "Min: " << x.second.min << std::endl;
  }
  return 0;
}
