#include <fstream>
#include <iostream>
#include <streambuf>
#include <string>
#include <unordered_map>
#include <thread>

#define CHAR_BUFFER_SIZE 1000000
#define THREADS 32

struct City {
  int sum;
  int count;
  int max;
  int min;
};

char get_number_from_char(char c) { return c - '0'; }

void thread_worker(std::unordered_map<std::string, City>& cities,
                   long read_start, long read_end,
                   long *compute_start, short thread_id) {
  std::ifstream file("measurements.txt");
  file.seekg(read_start);
  std::streambuf *file_buffer = file.rdbuf();
  char working_city_buffer[100];
  char working_temp_buffer[4];
	char buffer[CHAR_BUFFER_SIZE];
  short city_counter = 0;
  short temp_counter = 0;
  bool passed_semicolon = false;
  bool first_newline_found = false;
  long buffer_index = read_start;
  while (buffer_index < read_end) {
    long stream_size =
        file_buffer->sgetn(buffer, CHAR_BUFFER_SIZE);
    for (long i = 0; i < stream_size; i++) {
      if (buffer[i] == '\n') {
        if (!first_newline_found and thread_id > 0) {
          first_newline_found = true;
          compute_start[thread_id] = i + buffer_index;
        } else {
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
        }
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
    buffer_index += stream_size;
  }
}

int main() {
  std::unordered_map<std::string, City> cities_threads[THREADS];
	std::thread threads[THREADS];
	long compute_start[THREADS];
  std::unordered_map<std::string, City> cities;
  std::ifstream file("measurements.txt");
  file.seekg(0, std::ios::end);
  long file_size = file.tellg();
  file.seekg(0, std::ios::beg);
	for (int i = 0; i < THREADS; i++) {
		threads[i] = std::thread(thread_worker, std::ref(cities_threads[i]), i * (file_size / THREADS), (i + 1) * (file_size / THREADS), compute_start, i);
	}
	for (int i = 0; i < THREADS; i++) {
		threads[i].join();
	}
	std::cout << "Finished Computation and Reading" << std::endl;
	for (int i = 0; i< THREADS; i++) {
		std::cout<<cities_threads[i].size()<<std::endl;
	}
  return 0;
}
