/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#pragma once

#include "common/Type.h"
#include "congestion_aware/Topology.h"
#include <string>
#include <vector>

using namespace NetworkAnalytical;

namespace NetworkAnalyticalCongestionAware {

/**
 * CustomTopology reads an arbitrary network graph from a topology file
 * (ns3 format) and builds the Device/Link structure with per-link
 * bandwidth and latency. Routing uses BFS shortest-path.
 *
 * Topology file format:
 *   Line 1:   total_nodes  switch_count  link_count
 *   Line 2:   switch_id_0  switch_id_1  ...  switch_id_N
 *   Line 3+:  src  dst  bandwidth  latency  error_rate
 *
 * Bandwidth is a string like "4800Gbps" or "200Gbps".
 * Latency is a string like "0.00015ms" or "0.0005ms".
 * Links are treated as bidirectional.
 */
class CustomTopology final : public Topology {
  public:
    /**
     * Constructor.
     *
     * @param topology_file path to the ns3-format topology file
     * @param representative_bw representative bandwidth (GB/s) for
     *        ASTRA-sim scheduling decisions
     */
    CustomTopology(const std::string& topology_file,
                   Bandwidth representative_bw) noexcept;

    /**
     * Implementation of route function in Topology.
     * Uses pre-computed BFS shortest-path routing.
     */
    [[nodiscard]] Route route(DeviceId src, DeviceId dest) const noexcept override;

  private:
    /// BFS parent table: parent_table[src_npu][node] = parent of node
    /// in the BFS tree rooted at src_npu. -1 means root (src itself).
    std::vector<std::vector<int>> parent_table;

    /// Adjacency list for BFS (bidirectional), stores neighbor IDs
    std::vector<std::vector<int>> adjacency;

    /**
     * Parse a bandwidth string (e.g. "4800Gbps") and return GB/s (binary).
     */
    static Bandwidth parse_bandwidth_str(const std::string& bw_str) noexcept;

    /**
     * Parse a latency string (e.g. "0.00015ms") and return nanoseconds.
     */
    static Latency parse_latency_str(const std::string& lat_str) noexcept;

    /**
     * Run BFS from a source node and fill the parent array.
     */
    void bfs(int src, std::vector<int>& parent) const noexcept;

    /**
     * Pre-compute routing tables for all NPU sources.
     */
    void build_routing_tables() noexcept;
};

}  // namespace NetworkAnalyticalCongestionAware
