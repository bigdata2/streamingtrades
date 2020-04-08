#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <stdexcept>

namespace streamingtrade {

class Statistics {
public: 
    Statistics(const uint64_t &ts, const int &qty, const int &price) : 
                       lastTimestamp_(ts), 
		       maxTradePrice_(price), 
    		       totalVolume_(qty) 
		       {weightedAvgPrice_ = totalVolume_ * maxTradePrice_;}

    Statistics(const Statistics &other) = delete;
    Statistics& operator=(Statistics other) = delete;

    uint64_t lastTimestamp_ {0};
    uint64_t maxTimeGap_ {0};
    int maxTradePrice_ {0}; 
    int totalVolume_ {0};
    uint64_t weightedAvgPrice_ {0};
};

class TradeEntry {
public:
    TradeEntry(uint64_t ts, std::string &&sym, int qty, int price) : 
                        timestamp_(ts), symbol_(std::move(sym)), 
                        price_(price), quantity_(qty) {}

    TradeEntry(const TradeEntry &other) = delete;
    TradeEntry& operator=(TradeEntry other) = delete;

    uint64_t timestamp_;
    std::string symbol_;
    int price_;
    int quantity_;
};

template<typename key, typename val>
class Storage {
public: 
    std::unordered_map<key, val> hashmap_;
    inline void clear() {hashmap_.clear();}
};

template<typename derived, typename tradeentry>
class TradeEntryBase {
public:
    void process(const tradeentry &entry) {
        return static_cast<derived*>(this)->addTradeEntry(entry);
    }
    std::vector<std::string> cleanup() {
        return static_cast<derived*>(this)->cleanup();
    }
protected:
    TradeEntryBase() {}
};

class StatisticsCalculator: public TradeEntryBase<StatisticsCalculator, TradeEntry> {
public:
    StatisticsCalculator(Storage<std::string, std::unique_ptr<Statistics>> &storage) : storage_(storage) {}

    void addTradeEntry(const TradeEntry &entry) const {
        uint64_t ts = entry.timestamp_;
        std::string sym = entry.symbol_;
        int qty = entry.quantity_;
        int price = entry.price_;
        if(storage_.hashmap_.count(sym) == 0) {
            storage_.hashmap_[sym] = std::make_unique<Statistics>(ts, qty, price);
	    return;
        }
        storage_.hashmap_[sym]->totalVolume_ += qty;
        storage_.hashmap_[sym]->weightedAvgPrice_ += qty * price;
        storage_.hashmap_[sym]->maxTradePrice_ = 
                   std::max(price, storage_.hashmap_[sym]->maxTradePrice_);    
        uint64_t diff = ts - storage_.hashmap_[sym]->lastTimestamp_ ;    
        storage_.hashmap_[sym]->lastTimestamp_ = ts;
        storage_.hashmap_[sym]->maxTimeGap_ = std::max(storage_.hashmap_[sym]->maxTimeGap_, diff);
    }

    std::vector<std::string> cleanup() const {
        std::vector<std::string> retvec;
        for(const auto &v: storage_.hashmap_) {
            std::string str;
            str.reserve(75);
	    str += v.first; 
            str += "," ; 
            str += std::to_string(v.second->maxTimeGap_) ;
            str += "," ; 
            str += std::to_string(v.second->totalVolume_) ; 
            str += "," ;
            str += std::to_string(v.second->weightedAvgPrice_ / v.second->totalVolume_) ;
            str += ","; 
            str += std::to_string(v.second->maxTradePrice_); 
            retvec.push_back(std::move(str));
        }
        storage_.clear();
        return retvec;
    }
private:
    Storage<std::string, std::unique_ptr<Statistics>> &storage_;
};

template <typename derived, typename tradeentry>
class Parser {
public:
   Parser(TradeEntryBase<derived, tradeentry> &base) : base_(base) {} 

   void parseFile(std::istream &infile, int num_entries = 4) const {
       std::string line;
       while(getline(infile, line)) {
           line.erase(remove_if(line.begin(), line.end(), 
           static_cast<int(&)(int)>(std::isspace)), line.end());
           std::vector<std::string> tokens;
           tokens.reserve(num_entries);
           std::stringstream linestream(line);
           std::string tokenstr;
           while(getline(linestream, tokenstr, ',')) {
               tokens.push_back(std::move(tokenstr));
           }
           checkInput(tokens, num_entries);
           tradeentry te(std::move(std::stoull(tokens[0])), std::move(tokens[1]), 
                         stoi(tokens[2]), stoi(tokens[3])); 
           base_.process(te);
       }
   }
private:
   TradeEntryBase<derived, tradeentry> &base_;
   inline void checkInput(const std::vector<std::string> &input, const int &num_entries) const {
       if(input.size() < num_entries) {
           throw std::invalid_argument("number of enteries in the trade are less than " + 
				       std::to_string(num_entries));
       }
       if(stoi(input[2]) <= 0 || stoi(input[3]) <= 0) {
           throw std::invalid_argument("price or quantity is less than or equal to 0 "); 
       }
   }
}; 

class Outputter {
public:
    void save(std::ostream &outfile, std::vector<std::string> &vec, int sym_len = 3) const { 
        sort(vec.begin(), vec.end(), 
            [&sym_len](std::string &a, std::string &b) 
            {return a.substr(0,sym_len) < b.substr(0,sym_len);});
        for(auto &v: vec) outfile << v << std::endl;
    }
};

}

int main() {
    using namespace streamingtrade;
    Storage<std::string, std::unique_ptr<Statistics>> storage;
    Outputter outputter;
    StatisticsCalculator calculator(storage);
    Parser<StatisticsCalculator, TradeEntry> p(calculator);
    //std::ifstream infile = std::ifstream("test5.csv");
    std::ifstream infile = std::ifstream("input.csv");
    p.parseFile(infile);
    auto vec = calculator.cleanup();
    std::ofstream outfile = std::ofstream("output.csv");
    outputter.save(outfile,vec);
}
