#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <chrono>
#include <mutex>
#include <map>
#include <thread>
#include <cmath>
#include <algorithm>
extern "C" {
    #include "latency_log.h"
}

struct Trade {
    long long Trade_Id;
    long long ts_exchange_ms;
    long long ts_received_ms;
    long long delay;
    double price;
    double volume;
};

long long current_time_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

void sleep_until_next_minute() {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto next_minute = time_point_cast<minutes>(now) + minutes(1);
    std::this_thread::sleep_until(next_minute);
}

// One mutex per symbol
std::map<std::string, std::mutex> symbol_mutexes;

// Trades storage
std::map<std::string, std::vector<Trade>> trades_map;
// Global storage για moving averages
std::map<std::string, std::vector<std::pair<long long,double>>> moving_avgs;

std::mutex moving_avgs_mutex;
void load_trades(const std::string& filename, const std::string& symbol,
                 std::chrono::steady_clock::time_point start_time,
                 std::chrono::hours runtime_limit) 
{
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Error: cannot open file " << filename << "\n";
        return;
    }

    // Skip header
    std::string line;
    if (!std::getline(file, line)) return;

    long long last_trade_id = -1; // track processed trades

    while (std::chrono::steady_clock::now() - start_time < runtime_limit) {
        std::streampos pos_before = file.tellg();
        if (!std::getline(file, line)) {
            // No new data: clear eof and wait a bit
            file.clear();
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            file.seekg(pos_before);
            continue;
        }

        if (line.empty()) continue;

        std::stringstream ss(line);
        std::string item;
        Trade t;

        try {
            std::getline(ss, item, ',');
            if (item.empty()) continue;
            t.Trade_Id = std::stoll(item);
            if (t.Trade_Id <= last_trade_id) continue;

            std::getline(ss, item, ',');
            if (item.empty()) continue;
            t.ts_exchange_ms = std::stoll(item);

            std::getline(ss, item, ',');
            if (item.empty()) continue;
            t.ts_received_ms = std::stoll(item);

            std::getline(ss, item, ',');
            if (item.empty()) continue;
            t.delay = std::stoll(item);

            std::getline(ss, item, ',');
            if (item.empty()) continue;
            t.price = std::stod(item);

            std::getline(ss, item, ',');
            if (item.empty()) continue;
            t.volume = std::stod(item);
        }
        catch (const std::exception& e) {
            std::cerr << "Bad row in " << filename << " skipped: " << line << "\n";
            continue;
        }

        {
            std::lock_guard<std::mutex> lock(symbol_mutexes[symbol]);
            trades_map[symbol].push_back(t);

            // Keep only last 30 minutes in memory
            const long long cutoff = current_time_ms() - (30LL * 60 * 1000);
            trades_map[symbol].erase(
                std::remove_if(trades_map[symbol].begin(), trades_map[symbol].end(),
                               [&](const Trade& r){ return r.ts_exchange_ms < cutoff; }),
                trades_map[symbol].end()
            );
        }

        last_trade_id = t.Trade_Id;
    }
}


void calculate_metrics(const std::string& symbol,
                       std::chrono::steady_clock::time_point start_time,
                       std::chrono::hours runtime_limit) {

    std::string out_file = "all_metrics.csv";
    // write CSV header once 
    static std::once_flag header_flag;
    std::call_once(header_flag, [&]() {
        std::ofstream f(out_file, std::ios::app);
        f << "timestamp,symbol,moving_avg,volume_sum\n";
    });
    // Υπολογίζουμε άθροισμα τιμών και όγκου για τα trades εντός 15 λεπτών
    while(true){
          // check if 48h passed
        if (std::chrono::steady_clock::now() - start_time >= runtime_limit) break;
        struct timespec deadline_ts;
        clock_gettime(CLOCK_MONOTONIC, &deadline_ts);
        deadline_ts.tv_sec = (deadline_ts.tv_sec / 60 ) * 60 + 60; // next minute
        deadline_ts.tv_nsec = 0;

        latency_log_set_deadline(deadline_ts);

        long long now = current_time_ms();
        long long window_start = now - 15 * 60 * 1000; // 15 λεπτά πριν
        auto now2 = std::chrono::system_clock::now();
        std::time_t now2_c = std::chrono::system_clock::to_time_t(now2);
        std::tm tm = *std::localtime(&now2_c);

        char buf[20];
        std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);

        double price_sum = 0.0;
        double volume_sum = 0.0;
        int count = 0;
        {
            std::lock_guard<std::mutex> lock(symbol_mutexes[symbol]);

            for (const auto& t : trades_map[symbol]) {
                if (t.ts_exchange_ms >= window_start && t.ts_exchange_ms <= now) {
                    price_sum += t.price;
                    volume_sum += t.volume;
                    count++;
                }
            }

            long long cutoff = current_time_ms() - (30LL * 60 * 1000); // keep last 30min
            trades_map[symbol].erase(
                std::remove_if(trades_map[symbol].begin(), trades_map[symbol].end(),
                            [&](const Trade& t){ return t.ts_exchange_ms < cutoff; }),
                trades_map[symbol].end()
    );
        }
        if (count > 0) {
            double moving_avg = price_sum / count;

            {
            std::lock_guard<std::mutex> lock(moving_avgs_mutex);

            moving_avgs[symbol].push_back({now, moving_avg});
            if (moving_avgs[symbol].size() > 1000) { // περιορίζουμε μέγεθος buffer
                moving_avgs[symbol].erase(moving_avgs[symbol].begin(),moving_avgs[symbol].begin() + 500);
                }
            }

            std::cout << symbol << " 15-min Moving Average Price: " << moving_avg << std::endl;
            std::cout << symbol << " 15-min Total Volume: " << volume_sum << std::endl;
             // Append to the same CSV file
            std::ofstream f(out_file, std::ios::app);
            f << buf << "," << symbol << "," << moving_avg << "," << volume_sum << "\n";
        } 
        else {
            std::cout << "No trades in the last 15 minutes." << std::endl;
        }
        struct timespec actual;
        clock_gettime(CLOCK_MONOTONIC, &actual);

        latency_log_write(symbol.c_str(), actual);

        sleep_until_next_minute();// Ενημέρωση κάθε 60 δευτερόλεπτα
        }
}

double pearson_corr(const std::vector<double>& x, const std::vector<double>& y) {
    int n = x.size();
    if (n != y.size() || n == 0) return 0.0;

    double mean_x = 0.0, mean_y = 0.0;
    for (int i = 0; i < n; i++) {
        mean_x += x[i];
        mean_y += y[i];
    }
    mean_x /= n;
    mean_y /= n;

    double num = 0.0, den_x = 0.0, den_y = 0.0;
    for (int i = 0; i < n; i++) {
        double dx = x[i] - mean_x;
        double dy = y[i] - mean_y;
        num += dx * dy;
        den_x += dx * dx;
        den_y += dy * dy;
    }

    if (den_x == 0 || den_y == 0) return 0.0;
    return num / (std::sqrt(den_x) * std::sqrt(den_y));
}
void calculate_correlation(const std::vector<std::string>& symbols, 
                           const std::string& out_file,
                           std::chrono::steady_clock::time_point start_time,
                           std::chrono::hours runtime_limit) {

    static std::once_flag header_flag;
    std::call_once(header_flag, [&]() {
        std::ofstream f(out_file, std::ios::app);
        f << "timestamp,symbol,best_symbol,best_time,best_corr\n";
    });

    while (true) {
        // check if 48h passed
        if (std::chrono::steady_clock::now() - start_time >= runtime_limit) break;

        sleep_until_next_minute();

        auto now = std::chrono::system_clock::now();
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);
        std::tm tm = *std::localtime(&now_c);

        char buf[20];
        std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);

        std::lock_guard<std::mutex> lock(moving_avgs_mutex);

        for (const auto& sym : symbols) {
            const auto& series_x = moving_avgs[sym];
            if (series_x.size() < 8) continue;

            // Παίρνουμε τα τελευταία 8 moving averages για το symbol
            std::vector<double> x;
            for (int i = series_x.size() - 8; i < series_x.size(); i++) {
                x.push_back(series_x[i].second);
            }

            double best_corr = -2.0;
            std::string best_symbol;
            long long best_time = 0;

            for (const auto& [other_sym, series_y] : moving_avgs) {
                if (other_sym == sym) continue;
                if (series_y.size() < 8) continue;

                // Κυλιόμενο παράθυρο μήκους 8 στο άλλο σύμβολο
                for (size_t i = 0; i + 8 <= series_y.size(); i++) {
                    std::vector<double> y_values;
                    for (size_t j = i; j < i + 8; j++) {
                        y_values.push_back(series_y[j].second); // μόνο τα moving_avg
                    }

                    double corr = pearson_corr(x, y_values);
                    if (corr > best_corr) {
                        best_corr = corr;
                        best_symbol = other_sym;
                        best_time = series_y[i].first; // timestamp αρχής παραθύρου
                    }
                }
            }

            // Αποθήκευση αποτελέσματος
            std::ofstream f(out_file, std::ios::app);
            f << buf << "," << sym << ","
              << best_symbol << "," << best_time << ","
              << best_corr << "\n";
        }
    }
}


int main() {
    
    latency_log_init("proof_log.csv");   
    auto start_time = std::chrono::steady_clock::now();
    std::chrono::hours runtime_limit(48);

    std::vector<std::string> symbols = {"BTC","ADA","ETH","DOGE","LTC","BNB","SOL","XRP"};
    for (const auto& s : symbols) {
    symbol_mutexes.try_emplace(s);  // C++17: constructs value in-place (default-constructed std::mutex)
}
    std::thread reader1(load_trades, "trades/BTC-USDT.csv", "BTC", start_time, runtime_limit);
    std::thread reader2(load_trades, "trades/ADA-USDT.csv", "ADA", start_time, runtime_limit);
    std::thread reader3(load_trades, "trades/ETH-USDT.csv", "ETH", start_time, runtime_limit);
    std::thread reader4(load_trades, "trades/DOGE-USDT.csv", "DOGE", start_time, runtime_limit);
    std::thread reader5(load_trades, "trades/LTC-USDT.csv", "LTC", start_time, runtime_limit);
    std::thread reader6(load_trades, "trades/BNB-USDT.csv", "BNB", start_time, runtime_limit);
    std::thread reader7(load_trades, "trades/SOL-USDT.csv", "SOL", start_time, runtime_limit);
    std::thread reader8(load_trades, "trades/XRP-USDT.csv", "XRP", start_time, runtime_limit);

    std::thread calculator1(calculate_metrics, "BTC", start_time, runtime_limit);
    std::thread calculator2(calculate_metrics, "ADA", start_time, runtime_limit);
    std::thread calculator3(calculate_metrics, "ETH", start_time, runtime_limit);
    std::thread calculator4(calculate_metrics, "DOGE", start_time, runtime_limit);
    std::thread calculator5(calculate_metrics, "LTC", start_time, runtime_limit);
    std::thread calculator6(calculate_metrics, "BNB", start_time, runtime_limit);
    std::thread calculator7(calculate_metrics, "SOL", start_time, runtime_limit);
    std::thread calculator8(calculate_metrics, "XRP", start_time, runtime_limit);
    

    std::thread correlation_thread(calculate_correlation, symbols, "correlations.csv", start_time, runtime_limit);

    reader1.join();
    reader2.join();
    reader3.join();
    reader4.join();
    reader5.join();
    reader6.join();
    reader7.join();
    reader8.join();
    calculator1.join();
    calculator2.join();
    calculator3.join();
    calculator4.join();
    calculator5.join();
    calculator6.join();
    calculator7.join();
    calculator8.join();
    correlation_thread.join();

    latency_log_close();
    
    return 0;
}

