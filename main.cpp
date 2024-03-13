#include <emmintrin.h>
#include <fstream>
#include <immintrin.h>
#include <iostream>
#include <streambuf>
#include <string>
#include <thread>
#include <unordered_map>

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

int get_number_from_chars(const char *c, char size) {
  __m128i numbers = _mm_setr_epi32(c[0], c[1], c[2], c[3]);
  __m128i operations_vector = _mm_setr_epi32('0','0','0','0');
	numbers = _mm_sub_epi32(numbers, operations_vector);
	if (size == 4) {
		__m128i result = _mm_mullo_epi32(numbers, _mm_setr_epi32(0, -100, -10, -1));
		const int *results = (int *)&result;
		return results[1] + results[2] + results[3];
	}else if (size == 3) {
		if (c[0] == '-') {
			__m128i result = _mm_mullo_epi32(numbers, _mm_setr_epi32(0, -10, -1, 0));
			const int *results = (int *)&result;
			return results[1] + results[2];
		} else {
			__m128i result = _mm_mullo_epi32(numbers, _mm_setr_epi32(100, 10, 1, 0));
			const int *results = (int *)&result;
			return results[0] + results[1] + results[2];
		}
	}
	__m128i result = _mm_mullo_epi32(numbers, _mm_setr_epi32(10, 1, 0, 0));
	const int *results = (int *)&result;
	return results[0] + results[1];
}

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
                   long read_start, long *compute_end, short thread_id) {
  std::ifstream file("measurements.txt");
  file.seekg(read_start);
  std::streambuf *file_buffer = file.rdbuf();
  char working_city_buffer[MAX_CITY_NAME_SIZE];
  char working_temp_buffer[4];
  char buffer[CHAR_BUFFER_SIZE];
  char city_counter = 0;
  char temp_counter = 0;
  bool passed_semicolon = false;
  bool first_newline_found = false;
  long buffer_index = read_start;
  while (buffer_index < compute_end[thread_id]) {
    long stream_size = file_buffer->sgetn(
        buffer, get_next_read_size(buffer_index, compute_end[thread_id]));
    for (long i = 0; i < stream_size; i++) {
      if (buffer[i] == '\n') {
        if (!first_newline_found and thread_id > 0) {
          first_newline_found = true;
          compute_end[thread_id - 1] = i + buffer_index;
        } else {
          int temp = get_number_from_chars(working_temp_buffer, temp_counter);
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
    buffer_index += stream_size;
  }
}

int main() {
  const unsigned int THREADS = std::thread::hardware_concurrency();
  std::unordered_map<std::string, City, HashFn> cities_threads[THREADS];
  for (int i = 0; i < THREADS; i++) {
    cities_threads[i].reserve(MAX_NUM_KEYS);
  }
  std::thread *threads = new std::thread[THREADS];
  long *compute_end = new long[THREADS];
  std::ifstream file("measurements.txt");
  file.seekg(0, std::ios::end);
  long file_size = file.tellg();
  file.seekg(0, std::ios::beg);
  for (int i = 0; i < THREADS; i++) {
    compute_end[i] = (i + 1) * (file_size / THREADS);
    threads[i] = std::thread(thread_worker, std::ref(cities_threads[i]),
                             i * (file_size / THREADS), compute_end, i);
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
  delete[] threads;
  delete[] compute_end;
  return 0;
}
