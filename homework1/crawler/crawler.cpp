/*
* Denis Samoylov pdc_shad 2015
* crawler.cpp: web crawler
*/

#include <algorithm>
#include <cstdlib>
#include <string>
#include <limits>
#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <iterator>
#include <curl/curl.h>
#include <iostream>
#include <string>
#include <fstream>
#include <queue>
#include <map>

// Element of queue (URL and the current level of depth it)
class queue_element {
public:
    std::string url_;
    unsigned int level_;

    queue_element(const std::string &url, const unsigned int &level) : url_(url), level_(level) {}
    queue_element() {}

    queue_element& operator = (const queue_element &rhs)
    {
        if (this == &rhs) {
            return *this;
        }
        url_ = rhs.url_;
        level_ = rhs.level_;
        return *this;
    }
};

// For priority_queue
class compare {
public:
    bool operator()(const queue_element &a, const queue_element &b)
    {
        return a.level_ > b.level_;
    } 
};


// Thread safe priority queue
class queue_thread_safe {
private:
    std::priority_queue<queue_element, std::vector<queue_element>, compare> priority_queue_;
    std::mutex mutex_;
public:
    void put(const std::string &url, unsigned int level)
    {
        std::lock_guard<std::mutex> glock(mutex_);
        priority_queue_.push(queue_element(url, level));
    }

    queue_element take()
    {
        std::lock_guard<std::mutex> glock(mutex_);
        queue_element url = priority_queue_.top();
        priority_queue_.pop();
        return url;
    }

    bool empty()
    {
        std::lock_guard<std::mutex> glock(mutex_);
        return priority_queue_.empty();
    }
};

// Thread safe map for download content
class map_thread_safe {
private:
    std::map<std::string, std::string> map_;
    std::mutex mutex_;
public:
    void insert(const char name[], const std::string &content)
    {    
        std::string filename(name);
        std::lock_guard<std::mutex> glock(mutex_);
        map_.insert((std::pair<std::string, std::string> (filename, content)));
    }

    std::map<std::string, std::string> :: iterator begin() 
    {
        return map_.begin();
    }

    std::map<std::string, std::string> :: iterator end() 
    {
        return map_.end();
    }
};

// Web crawler
class web_crawler {
private:
    std::atomic<unsigned int> count_downloads_;
    const std::string download_path_;
    const unsigned int stop_depth_;
    const unsigned int stop_download_;
    queue_thread_safe url_queue_;
    map_thread_safe download_content_;
    std::string start_url_;    
    std::mutex mutex_;
private:
    // Support function for curl_read
    size_t static string_write(void *contents, size_t size, size_t nmemb, void *userp) 
    {
        ((std::string*)userp)->append((char*)contents, size * nmemb);
        return size * nmemb;
    }
    
    // Write to buffer content
    CURLcode curl_read(const std::string& url, std::string& buffer, long timeout = 30) 
    {
        CURLcode code(CURLE_FAILED_INIT);
        std::unique_lock<std::mutex> ulock(mutex_);
        CURL* curl = curl_easy_init();
        ulock.unlock();

        if (curl) {    
            if (CURLE_OK == (code = curl_easy_setopt(curl, CURLOPT_URL, url.c_str()))
                && CURLE_OK == (code = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, string_write))
                && CURLE_OK == (code = curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buffer))
                && CURLE_OK == (code = curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L))
                && CURLE_OK == (code = curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L))
                && CURLE_OK == (code = curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout))) 
            {    
                std::lock_guard<std::mutex> glock(mutex_);
                code = curl_easy_perform(curl);            
            }
            std::lock_guard<std::mutex> glock(mutex_);
            curl_easy_cleanup(curl);
        }
        return code;
    }
public:
    web_crawler(const std::string &download_path, const unsigned int &stop_depth, 
                const unsigned int &stop_download,  const std::string &start_url) 
                : count_downloads_(0), download_path_(download_path), stop_depth_(stop_depth), 
                stop_download_(stop_download), start_url_(start_url)
    {
        // start_url has 0 level
        url_queue_.put(start_url, 0); 
    }

    // For saving result on the disk
    std::map<std::string, std::string> :: iterator begin() 
    {
        return download_content_.begin();
    }

    std::map<std::string, std::string> :: iterator end() 
    {
        return download_content_.end();
    }

    void start_web_crawler()
    {
        std::mutex lock_write_name;
        std::string content;
        std::string buffer;
        queue_element url;
        CURLcode res; 
        
        size_t start_search_pos = 0;
        size_t found_pos = 0;
        unsigned int url_level = 0;
        char filename[256];
    
        while (count_downloads_ < stop_download_) {
            if (!url_queue_.empty()) {
                url = url_queue_.take();
                url_level = url.level_;

                if (url_level > stop_depth_) {
                    break;
                } else {
                    url_level++;
                }
                res = curl_read(url.url_, content);
                if (res == CURLE_OK) {                    
                    std::unique_lock<std::mutex> ulock(lock_write_name);
                    ++count_downloads_;                    
                    std::sprintf(filename, "%s%u.html", download_path_.c_str(), count_downloads_.load());
                    ulock.unlock();
                    download_content_.insert(filename, content);
                    while (1) {                
                        found_pos = content.find("href=\"", start_search_pos);
                        if (found_pos == std::string::npos) {
                            break;
                        }
                        // Skip href="
                        found_pos = found_pos + 6;
                        if (content[found_pos] == 'h' && content[found_pos + 1] == 't' &&
                            content[found_pos + 2] == 't' && content[found_pos + 3] == 'p') {
                            // While the current character not quote
                            while (content[found_pos] != '"') {
                                buffer.push_back(content[found_pos]);
                                ++found_pos;
                            }
                            // Skip last qoute
                            ++found_pos;
                            url_queue_.put(buffer, url_level);
                            buffer.clear();
                        }
                        start_search_pos = found_pos;
                    }
                } else {
                    url.url_.clear();
                       std::cerr << "ERROR: " << curl_easy_strerror(res) << "(url skipped)" << std::endl;
                      continue;
                }         
            } else {
                std::this_thread::yield();
            }
        }
    }
};


int main(int argc, char* argv[])
{
    if (argc != 5 && argc != 6) {
        std::cout << "Usage: " << argv[0] << " <start_url> <stop_depth> <stop_download> <download_path> <nthreads>\n";
        return EXIT_FAILURE;
    }

    std::string start_url(argv[1]);
    unsigned int stop_depth = atoi(argv[2]);
    unsigned int stop_download = atoi(argv[3]);
    std::string download_path(argv[4]);
    // Default nthreads
    unsigned int nthreads = 2;

    if (argc == 6) {
        nthreads = atoi(argv[5]);
    }

    web_crawler wc(download_path, stop_depth, stop_download - 1, start_url);

    std::thread threads[nthreads];
    for (unsigned int i = 0; i < nthreads; ++i) {
        threads[i] = std::thread(&web_crawler::start_web_crawler, &wc);
    }
    std::for_each(threads, threads + nthreads, std::mem_fn(&std::thread::join));

    // Save results to disk
    for_each(wc.begin(), wc.end(), [](const std::pair<std::string, std::string> &val) 
    {
        std::ofstream out(val.first);
        out << val.second;
    });

    return 0;
}
