/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "congestion_aware/CustomTopology.h"
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <queue>
#include <sstream>
#include <string>

using namespace NetworkAnalytical;
using namespace NetworkAnalyticalCongestionAware;

Bandwidth CustomTopology::parse_bandwidth_str(const std::string& bw_str) noexcept {
    double value = 0;
    std::string unit;

    std::size_t pos = 0;
    try {
        value = std::stod(bw_str, &pos);
    } catch (...) {
        std::cerr << "[Error] (CustomTopology) Cannot parse bandwidth: "
                  << bw_str << std::endl;
        std::exit(-1);
    }
    unit = bw_str.substr(pos);

    // Convert to GB/s (binary, where 1 GB = 2^30 bytes)
    // as expected by the Link constructor
    double bits_per_sec = 0;
    if (unit == "Gbps" || unit == "gbps") {
        bits_per_sec = value * 1e9;
    } else if (unit == "Mbps" || unit == "mbps") {
        bits_per_sec = value * 1e6;
    } else if (unit == "Tbps" || unit == "tbps") {
        bits_per_sec = value * 1e12;
    } else if (unit == "bps") {
        bits_per_sec = value;
    } else {
        std::cerr << "[Error] (CustomTopology) Unknown bandwidth unit: "
                  << unit << " in " << bw_str << std::endl;
        std::exit(-1);
    }

    double bytes_per_sec = bits_per_sec / 8.0;
    double gb_per_sec = bytes_per_sec / static_cast<double>(1ULL << 30);
    return gb_per_sec;
}

Latency CustomTopology::parse_latency_str(const std::string& lat_str) noexcept {
    double value = 0;
    std::string unit;

    std::size_t pos = 0;
    try {
        value = std::stod(lat_str, &pos);
    } catch (...) {
        std::cerr << "[Error] (CustomTopology) Cannot parse latency: "
                  << lat_str << std::endl;
        std::exit(-1);
    }
    unit = lat_str.substr(pos);

    // Convert to nanoseconds
    if (unit == "ms") {
        return value * 1e6;
    } else if (unit == "us") {
        return value * 1e3;
    } else if (unit == "ns") {
        return value;
    } else if (unit == "s") {
        return value * 1e9;
    } else {
        std::cerr << "[Error] (CustomTopology) Unknown latency unit: "
                  << unit << " in " << lat_str << std::endl;
        std::exit(-1);
    }
}

void CustomTopology::bfs(int src, std::vector<int>& parent) const noexcept {
    parent.assign(devices_count, -2);  // -2 = unvisited
    parent[src] = -1;                  // -1 = root

    std::queue<int> q;
    q.push(src);

    while (!q.empty()) {
        int cur = q.front();
        q.pop();

        for (int neighbor : adjacency[cur]) {
            if (parent[neighbor] == -2) {
                parent[neighbor] = cur;
                q.push(neighbor);
            }
        }
    }
}

void CustomTopology::build_routing_tables() noexcept {
    parent_table.resize(npus_count);
    for (int npu = 0; npu < npus_count; npu++) {
        bfs(npu, parent_table[npu]);
    }
}

CustomTopology::CustomTopology(const std::string& topology_file) noexcept
    : Topology() {

    std::ifstream infile(topology_file);
    if (!infile.is_open()) {
        std::cerr << "[Error] (CustomTopology) Cannot open topology file: "
                  << topology_file << std::endl;
        std::exit(-1);
    }

    // Line 1: total_nodes switch_count link_count
    int total_nodes = 0, switch_count = 0, link_count = 0;
    infile >> total_nodes >> switch_count >> link_count;

    // Line 2: switch IDs
    std::vector<int> switch_ids(switch_count);
    for (int i = 0; i < switch_count; i++) {
        infile >> switch_ids[i];
    }

    // NPU count = total_nodes - switch_count
    this->npus_count = total_nodes - switch_count;
    this->devices_count = total_nodes;
    this->dims_count = 1;
    this->npus_count_per_dim.push_back(this->npus_count);

    // Instantiate all devices (NPUs + switches)
    instantiate_devices();

    // Build adjacency list for BFS routing
    adjacency.resize(total_nodes);
    auto representative_bw = std::numeric_limits<Bandwidth>::infinity();

    // Lines 3+: src dst bandwidth latency error_rate
    for (int i = 0; i < link_count; i++) {
        int src_id = 0, dst_id = 0;
        std::string bw_str, lat_str;
        double error_rate = 0;

        infile >> src_id >> dst_id >> bw_str >> lat_str >> error_rate;

        if (infile.fail()) {
            std::cerr << "[Error] (CustomTopology) Failed to parse link "
                      << i << " in " << topology_file << std::endl;
            std::exit(-1);
        }

        Bandwidth bw = parse_bandwidth_str(bw_str);
        Latency lat = parse_latency_str(lat_str);
        representative_bw = std::min(representative_bw, bw);

        // Create bidirectional link
        connect(src_id, dst_id, bw, lat, true);

        // Add to adjacency list (bidirectional)
        adjacency[src_id].push_back(dst_id);
        adjacency[dst_id].push_back(src_id);
    }

    infile.close();

    if (!std::isfinite(representative_bw)) {
        std::cerr << "[Error] (CustomTopology) No valid link bandwidth found in "
                  << topology_file << std::endl;
        std::exit(-1);
    }
    this->bandwidth_per_dim.push_back(representative_bw);

    std::cout << "[CustomTopology] Loaded topology: "
              << npus_count << " NPUs, "
              << switch_count << " switches, "
              << link_count << " links (bidirectional)" << std::endl;

    // Pre-compute BFS routing tables from each NPU
    build_routing_tables();

    std::cout << "[CustomTopology] Routing tables built." << std::endl;
}

Route CustomTopology::route(DeviceId src, DeviceId dest) const noexcept {
    assert(0 <= src && src < npus_count);
    assert(0 <= dest && dest < npus_count);
    assert(src != dest);

    const auto& parent = parent_table[src];

    // Reconstruct path from dest back to src using parent pointers
    Route r;
    int current = dest;
    while (current != src) {
        assert(current >= 0 && current < devices_count);
        assert(parent[current] != -2);  // must be reachable
        r.push_front(devices[current]);
        current = parent[current];
    }
    r.push_front(devices[src]);

    return r;
}
