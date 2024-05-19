#include <cstdio>
#include <unistd.h>
#ifdef __x86_64__
#include <immintrin.h>
#else
#include "sse2neon.h"
#endif
#include <fcntl.h>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <thread>
#include <unordered_map>

#define MAX_CITY_NAME_SIZE 100
#define MAX_NUM_KEYS 10000

struct City {
  int32_t sum;
  int32_t count;
  int32_t max;
  int32_t min;
};

struct CityString {
  char name[MAX_CITY_NAME_SIZE];
  int32_t size;
  bool operator==(const CityString &other) const {
    if (size != other.size) {
      return false;
    }
#ifdef __x86_64__
    for (int32_t i = 0; i < size / 32; i++) {
      __m256i a = _mm256_loadu_si256((__m256i *)&name[i * 32]);
      __m256i b = _mm256_loadu_si256((__m256i *)&other.name[i * 32]);
      __m256i result = _mm256_cmpeq_epi8(a, b);
      int16_t bit_shift = (size - i * 32 > 32) ? 0 : 32 - size + i * 32;
      int32_t mask = _mm256_movemask_epi8(result) >> bit_shift;
      if (mask != ((2 << (32 - bit_shift)) - 1)) {
        return false;
      }
    }
#else
    for (int32_t i = 0; i < size / 16; i++) {
      __m128i a = _mm_loadu_si128((__m128i *)&name[i * 16]);
      __m128i b = _mm_loadu_si128((__m128i *)&other.name[i * 16]);
      __m128i result = _mm_cmpeq_epi8(a, b);
      int16_t bit_shift = (size - i * 16 > 16) ? 0 : 16 - size + i * 16;
      int32_t mask = _mm_movemask_epi8(result) >> bit_shift;
      if (mask != ((2 << (16 - bit_shift)) - 1)) {
        return false;
      }
    }
#endif
    return true;
  }
};

class CityStringHashFn {
public:
  size_t operator()(const CityString &key) const {
    uint64_t hash = 5381;
    for (int32_t i = 0; i < MAX_CITY_NAME_SIZE; i++) {
      if (i >= key.size) {
        break;
      }
      hash = ((hash << 5) + hash) + key.name[i];
    }
    return hash;
  }
};

int32_t get_number_from_chars(const char *c, char size) {
  __m128i numbers = _mm_setr_epi32(c[0], c[1], c[2], c[3]);
  __m128i operations_vector = _mm_setr_epi32('0', '0', '0', '0');
  numbers = _mm_sub_epi32(numbers, operations_vector);
  if (size == 4) {
    __m128i result = _mm_mullo_epi32(numbers, _mm_setr_epi32(0, -100, -10, -1));
    const int32_t *results = (int32_t *)&result;
    return results[1] + results[2] + results[3];
  } else if (size == 3) {
    if (c[0] == '-') {
      __m128i result = _mm_mullo_epi32(numbers, _mm_setr_epi32(0, -10, -1, 0));
      const int32_t *results = (int32_t *)&result;
      return results[1] + results[2];
    } else {
      __m128i result = _mm_mullo_epi32(numbers, _mm_setr_epi32(100, 10, 1, 0));
      const int32_t *results = (int32_t *)&result;
      return results[0] + results[1] + results[2];
    }
  }
  __m128i result = _mm_mullo_epi32(numbers, _mm_setr_epi32(10, 1, 0, 0));
  const int32_t *results = (int32_t *)&result;
  return results[0] + results[1];
}

void thread_worker(
    std::unordered_map<CityString, City, CityStringHashFn> &cities,
    uint64_t read_start, uint64_t *compute_end, int16_t thread_id, char *buffer) {
  char working_city_buffer[MAX_CITY_NAME_SIZE];
  char working_temp_buffer[4];
  char city_counter = 0;
  char temp_counter = 0;
  bool passed_semicolon = false;
  bool first_newline_found = false;
  uint64_t buffer_index = read_start;
  while (buffer_index < compute_end[thread_id]) {
    if (buffer[buffer_index] == '\n') {
      if (!first_newline_found and thread_id > 0) {
        first_newline_found = true;
        compute_end[thread_id - 1] = buffer_index;
      } else {
        int32_t temp = get_number_from_chars(working_temp_buffer, temp_counter);
        passed_semicolon = false;
        CityString city_name;
        city_name.size = city_counter;
        for (int32_t j = 0; j < city_counter; j++) {
          city_name.name[j] = working_city_buffer[j];
        }
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
    } else if (buffer[buffer_index] == ';') {
      passed_semicolon = true;
    } else if (!passed_semicolon) {
      working_city_buffer[city_counter] = buffer[buffer_index];
      city_counter++;
    } else if (buffer[buffer_index] != '.') {
      working_temp_buffer[temp_counter] = buffer[buffer_index];
      temp_counter++;
    }
    buffer_index++;
  }
}

int32_t main() {
  const uint32_t THREADS = std::thread::hardware_concurrency();
  std::unordered_map<CityString, City, CityStringHashFn>
      cities_threads[THREADS];
  for (int32_t i = 0; i < THREADS; i++) {
    cities_threads[i].reserve(MAX_NUM_KEYS);
  }
  std::thread *threads = new std::thread[THREADS];
  int32_t fd = open("measurements.txt", O_RDONLY);
  uint64_t file_size = lseek(fd, 0L, SEEK_END);
  void *data_ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
  char *char_ptr = reinterpret_cast<char *>(data_ptr);
  uint64_t *compute_end = new uint64_t[THREADS];
  for (int32_t i = 0; i < THREADS; i++) {
    compute_end[i] = (i + 1) * (file_size / THREADS);
    threads[i] =
        std::thread(thread_worker, std::ref(cities_threads[i]),
                    i * (file_size / THREADS), compute_end, i, char_ptr);
  }
  for (int32_t i = 0; i < THREADS; i++) {
    threads[i].join();
  }
  std::cout << "Finished Computation and Reading" << std::endl;
  for (int32_t i = 1; i < THREADS; i++) {
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
    std::cout << std::string(city.first.name, city.first.size) << " "
              << city.second.sum << " " << city.second.count << " "
              << city.second.max << " " << city.second.min << std::endl;
  }
  std::cout << "Finished Merging" << std::endl;
  delete[] threads;
  delete[] compute_end;
  munmap(data_ptr, file_size);
  close(fd);
  return 0;
}
