#include <cstdio>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <thread>
#include <unordered_map>
#include <unistd.h>
#include <fcntl.h>

#define CHAR_BUFFER_SIZE 1000000
#define MAX_CITY_NAME_SIZE 100
#define MAX_NUM_KEYS 10000

struct City {
  int sum;
  int count;
  int max;
  int min;
};

char get_number_from_char(char c) { return c - '0'; }

long get_next_read_size(long current_index, long compute_end) {
  long next_read_size = compute_end - current_index;
  if (next_read_size > CHAR_BUFFER_SIZE) {
    return CHAR_BUFFER_SIZE;
  }
  return next_read_size;
}

class HashFn {
public:
  size_t operator()(const std::string &key) const {
    unsigned long hash = 5381;
    for (int i = 0; i < MAX_CITY_NAME_SIZE; i++) {
      if (i >= key.size()) {
        break;
      }
      hash = ((hash << 5) + hash) + key[i];
    }
    return hash;
  }
};
void thread_worker(std::unordered_map<std::string, City, HashFn> &cities,
                   long read_start, long *compute_end, short thread_id, void* map_ptr) {
  char working_city_buffer[MAX_CITY_NAME_SIZE];
  char working_temp_buffer[4];
	char *buffer = (char*)map_ptr;
  short city_counter = 0;
  short temp_counter = 0;
  bool passed_semicolon = false;
  bool first_newline_found = false;
	for (long i = read_start; i < compute_end[thread_id]; i++) {
		if (buffer[i] == '\n') {
			if (!first_newline_found and thread_id > 0) {
				first_newline_found = true;
				compute_end[thread_id - 1] = i;
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
			}
			city_counter = 0;
			temp_counter = 0;
			passed_semicolon = false;
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

int main() {
	const unsigned int THREADS = std::thread::hardware_concurrency();
  std::unordered_map<std::string, City, HashFn> cities_threads[THREADS];
  for (int i = 0; i < THREADS; i++) {
    cities_threads[i].reserve(MAX_NUM_KEYS);
  }
  std::thread* threads = new std::thread[THREADS];
  long* compute_end=new long[THREADS];
	int fd = open("measurements.txt", O_RDONLY);
	size_t file_size=lseek(fd,0,SEEK_END);
	void* map_ptr;
	map_ptr=mmap(NULL,file_size,PROT_READ,MAP_SHARED,fd,0);
  for (int i = 0; i < THREADS; i++) {
    compute_end[i] = (i + 1) * (file_size / THREADS);
    threads[i] = std::thread(thread_worker, std::ref(cities_threads[i]),
                             i * (file_size / THREADS), compute_end, i, map_ptr);
  }
  for (int i = 0; i < THREADS; i++) {
    threads[i].join();
  }
  std::cout << "Finished Computation and Reading" << std::endl;
  for (int i = 1; i < THREADS; i++) {
    for (auto &city : cities_threads[i]) {
      City *found_city = &cities_threads[0][city.first];
      found_city->sum += city.second.sum;
      found_city->count += city.second.count;
      if (city.second.max > found_city->max) {
        found_city->max = city.second.max;
      }
      if (city.second.min < found_city->min) {
        found_city->min = city.second.min;
      }
    }
  }
  for (auto &city : cities_threads[0]) {
    std::cout << city.first << " " << city.second.sum << " "
              << city.second.count << " " << city.second.max << " "
              << city.second.min << std::endl;
  }
  std::cout << "Finished Merging" << std::endl;
	munmap(map_ptr,file_size);
	close(fd);
	delete[] threads;
	delete[] compute_end;
  return 0;
}
