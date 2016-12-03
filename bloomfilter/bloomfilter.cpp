#include <iostream>
#include <vector>
#include <cmath>
#include <cstring>
#include <string>

inline uint32_t decode_fixed32(const char* ptr) {
  // Load the raw bytes
  uint32_t result;
  memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
  return result;
}

uint32_t hash_func(const char* data, size_t n, uint32_t seed) {
  // Similar to murmur hash
  const uint32_t m = 0xc6a4a793;
  const uint32_t r = 24;
  const char* limit = data + n;
  uint32_t h = seed ^ (n * m);

  // Pick up four bytes at a time
  while (data + 4 <= limit) {
    uint32_t w = decode_fixed32(data);
    data += 4;
    h += w;
    h *= m;
    h ^= (h >> 16);
  }

  // Pick up remaining bytes
  switch (limit - data) {
    case 3:
      h += static_cast<unsigned char>(data[2]) << 16;
    case 2:
      h += static_cast<unsigned char>(data[1]) << 8;
    case 1:
      h += static_cast<unsigned char>(data[0]);
      h *= m;
      h ^= (h >> r);
      break;
  }
  return h;
}

class Key
{
 public:
  Key(const int32_t key);
  uint32_t hash(uint32_t seed) const;
 private:
  int32_t key_;
};

Key::Key(const int32_t key)
  : key_(key)
{
}

uint32_t Key::hash(uint32_t seed) const
{
	return hash_func(reinterpret_cast<const char *>(&key_), sizeof(key_), seed);
}

class PerfKey
{
 public:
  PerfKey(const std::string &key);
  uint32_t hash(uint32_t seed) const;
 private:
  std::string key_;
};

PerfKey::PerfKey(const std::string &key)
  : key_(key)
{
}

uint32_t PerfKey::hash(uint32_t seed) const
{
  return hash_func(key_.data(), key_.size(), seed);
}

template<typename T>
class BloomFilter
{
 public:
  BloomFilter(const int32_t n, const double false_positive_p);
  void insert(const T &key);
  void insert2(const T &key);
  bool key_may_match(const T &key);
  bool key_may_match2(const T &key);

 private:
  std::vector<char> bits_;
  int32_t k_;
  int32_t m_;
  int32_t n_;
  double p_;
};

template<typename T>
BloomFilter<T>::BloomFilter(const int32_t n, const double false_positive_p)
  : bits_(), k_(0), m_(0), n_(n), p_(false_positive_p)
{
  k_ = static_cast<int32_t>(-std::log(p_) / std::log(2));
  m_ = static_cast<int32_t>(k_ * n * 1.0 / std::log(2));
  bits_.resize((m_ + 7) / 8, 0);
}

template<typename T>
void BloomFilter<T>::insert(const T &key)
{
	uint32_t hash_val = 0xbc9f1d34;
	for (int i = 0; i < k_; ++i) {
		hash_val = key.hash(hash_val);
		const uint32_t bit_pos = hash_val % m_;
    bits_[bit_pos/8] |= 1 << (bit_pos % 8);
	}
}

template<typename T>
void BloomFilter<T>::insert2(const T &key)
{
	uint32_t hash_val = key.hash(0xbc9f1d34);
  const uint32_t delta = (hash_val >> 17) | (hash_val << 15);
	for (int i = 0; i < k_; ++i) {
		const uint32_t bit_pos = hash_val % m_;
    bits_[bit_pos/8] |= 1 << (bit_pos % 8);
    hash_val += delta;
	}
}

template<typename T>
bool BloomFilter<T>::key_may_match(const T &key)
{
	uint32_t hash_val = 0xbc9f1d34;
  for (int i = 0; i < k_; ++i) {
		hash_val = key.hash(hash_val);
		const uint32_t bit_pos = hash_val % m_;
    if ((bits_[bit_pos/8] & (1 << (bit_pos % 8))) == 0) {
      return false;
    }
  }
  return true;
}

template<typename T>
bool BloomFilter<T>::key_may_match2(const T &key)
{
	uint32_t hash_val = key.hash(0xbc9f1d34);
  const uint32_t delta = (hash_val >> 17) | (hash_val << 15);
  for (int i = 0; i < k_; ++i) {
    const uint32_t bit_pos = hash_val % m_;
    if ((bits_[bit_pos/8] & (1 << (bit_pos % 8))) == 0) {
      return false;
    }
    hash_val += delta;
  }
  return true;
}

class TestSuite
{
 public:
  TestSuite(const int32_t key_nums, const double expected_false_positive_p);
  void run(bool with_opt);
 private:
  void gen_keys(bool with_opt);
  void test_false_positive_rates(bool with_opt);
  int32_t key_nums_;
  double expected_p_;
  BloomFilter<Key> bloom_;
};

TestSuite::TestSuite(const int32_t key_nums, const double expected_false_positive_p)
  : key_nums_(key_nums), expected_p_(expected_false_positive_p), bloom_(key_nums, expected_p_)
{
}

void TestSuite::run(bool with_opt)
{
  gen_keys(with_opt);
  test_false_positive_rates(with_opt);
}

void TestSuite::gen_keys(bool with_opt)
{
  for (int i = 1; i <= key_nums_; ++i) {
    const Key key(i);
    if (with_opt) {
      bloom_.insert2(key);
    } else {
      bloom_.insert(key);
    }
  }
}

void TestSuite::test_false_positive_rates(bool with_opt)
{
  int matches = 0;
  for (int i = 0; i < 10000; i++) {
    const Key key(i + 10000000);
    if (with_opt) {
      if (bloom_.key_may_match2(key)) {
        matches++;
      }
    } else {
      if (bloom_.key_may_match(key)) {
        matches++;
      }
    }
  }
  std::cout << "key_nums_=" << key_nums_ << " expected false positive rate=" << expected_p_ << " real false positive rate="
    << matches * 1.0 / 10000 << std::endl;
}

void test_false_positive_rates(void)
{
  std::cout << "before_opt" << std::endl;
  for (int i = 0; i < 10; ++i) {
    TestSuite test(10000 + i * 10000, 0.1);
    test.run(false);
  }
  std::cout << "after_opt" << std::endl;
  for (int i = 0; i < 10; ++i) {
    TestSuite test(10000 + i * 10000, 0.1);
    test.run(true);
  }
}

std::string gen_random_string(const int len)
{
  std::string s;
  s.reserve(len);
  static const char alphanum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

  for (int i = 0; i < len; ++i) {
    s.push_back(alphanum[rand() % (sizeof(alphanum) - 1)]);
  }
  return s;
}

int gen_random_int(const int low, const int high)
{
  return rand() % (high - low) + low;
}

class PerfTestSuite
{
 public:
  PerfTestSuite(const int32_t key_nums);
  void run(bool with_opt);
 private:
  void gen_keys(bool with_opt);
  int32_t key_nums_;
  BloomFilter<PerfKey> bloom_;
};

PerfTestSuite::PerfTestSuite(const int32_t key_nums)
  : key_nums_(key_nums), bloom_(key_nums, 0.01)
{
}

void PerfTestSuite::run(bool with_opt)
{
  gen_keys(with_opt);
}

void PerfTestSuite::gen_keys(bool with_opt)
{
  for (int i = 0; i < key_nums_; ++i) {
    const int len = gen_random_int(128, 256);
    const std::string key_str = gen_random_string(len);
    PerfKey key(key_str);
    if (with_opt) {
      bloom_.insert2(key);
    } else {
      bloom_.insert(key);
    }
  }
}

void test_perf(void)
{
  PerfTestSuite test(100000000);
  test.run(true);
}

int main(void)
{
  test_false_positive_rates();
  //test_perf();
}
