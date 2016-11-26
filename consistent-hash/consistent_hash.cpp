#include <algorithm>
#include <map>
#include <unordered_map>
#include <string>
#include <iostream>

class Node
{
 public:
  Node();
  Node(const std::string &name, const int replicas);
  std::string get_name(void) const;
  int get_replicas(void) const;
 private:
  std::string name_;
  int replicas_;
};

Node::Node(void)
  : name_(), replicas_(0)
{
}

Node::Node(const std::string &name, const int replicas)
  : name_(name), replicas_(replicas)
{
}

std::string Node::get_name(void) const
{
  return name_;
}

int Node::get_replicas(void) const
{
  return replicas_;
}

bool operator==(const Node& lhs, const Node& rhs)
{
  return lhs.get_name() == rhs.get_name() && lhs.get_replicas() == rhs.get_replicas();
}

namespace std
{
template<> struct hash<Node>
{
  typedef Node argument_type;
  typedef std::size_t result_type;
  result_type operator()(argument_type const& n) const
  {
    result_type const h1 (std::hash<std::string>{}(n.get_name() + std::to_string(n.get_replicas())));
    return h1;
  }
};
}

class ConsistentHash
{
 public:
  ConsistentHash(void);
  void add_node(const Node &node);
  void remove_node(const Node &node);
  const Node get_node(const std::string &key);
  void reset(void);
 private:
  std::map<int64_t, Node> hash_ring_;
};

ConsistentHash::ConsistentHash(void)
  : hash_ring_()
{
}

void ConsistentHash::add_node(const Node &node)
{
  std::string node_name = node.get_name();
  for (int i = 0; i < node.get_replicas(); ++i) {
    std::string vnode_name = node_name + std::to_string(i);
    std::size_t hash_value = std::hash<std::string>{}(vnode_name);
    hash_ring_[hash_value] = node;
  }
}

void ConsistentHash::remove_node(const Node &node)
{
  std::string node_name = node.get_name();
  for (int i = 0; i < node.get_replicas(); ++i) {
    std::string vnode_name = node_name + std::to_string(i);
    std::size_t hash_value = std::hash<std::string>{}(vnode_name);
    hash_ring_.erase(hash_value);
  }
}

const Node ConsistentHash::get_node(const std::string &key)
{
  std::size_t hash_value = std::hash<std::string>{}(key);
  Node node("", 0);
  if (hash_ring_.empty()) {
    return node;
  } else {
    auto iter = hash_ring_.upper_bound(hash_value);
    if (iter == hash_ring_.end()) {
      iter = hash_ring_.begin();
    }
    return iter->second;
  }
}

void ConsistentHash::reset(void)
{
  hash_ring_.clear();
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

class TestSuite
{
 public:
  TestSuite(void);
  void gen_nodes(const int count, const int replicas);
  void test(const int key_nums);
  void print_result(void);
  void reset(void);
 private:
  ConsistentHash consistent_hash_;
  std::unordered_map<Node, int> count_map_;
};

TestSuite::TestSuite(void)
  : consistent_hash_(), count_map_()
{
}

void TestSuite::gen_nodes(const int count, const int replicas)
{
  std::string prefix("Node");
  for (int i = 0; i < count; i++) {
    std::string name = prefix + std::to_string(i);
    Node node(name, replicas);
    consistent_hash_.add_node(node);
  }
}

void TestSuite::test(const int key_nums)
{
  for (int i = 0; i < key_nums; i++) {
    const int len = gen_random_int(10, 20);
    const std::string s = gen_random_string(len);
    const Node node = consistent_hash_.get_node(s);
    count_map_[node]++;
  }
}

void TestSuite::print_result(void)
{
  std::cout << "----------------------Test Result------------------------------" << std::endl;
  for (auto iter = count_map_.cbegin(); iter != count_map_.cend(); ++iter) {
    std::cout << iter->second << std::endl;
  }
}

void TestSuite::reset(void)
{
  consistent_hash_.reset();
  count_map_.clear();
}

void test_vnodes(void)
{
  std::vector<int> replicas = {1, 10, 50, 100, 200, 400, 800};
  for (int i = 0; i < replicas.size(); i++) {
    TestSuite test_suite;
    test_suite.gen_nodes(10, replicas[i]);
    test_suite.test(10000);
    test_suite.print_result();
  }
}

int main(void)
{
  time_t t;
  srand((unsigned) time(&t));
  test_vnodes();
  return 0;
}
