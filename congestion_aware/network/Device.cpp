/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "congestion_aware/Device.h"
#include "congestion_aware/Chunk.h"
#include "congestion_aware/Link.h"
#include <algorithm>
#include <cassert>
#include <numeric>
#include <tuple>

using namespace NetworkAnalyticalCongestionAware;

Device::Device(const DeviceId id) noexcept : device_id(id) {
    assert(id >= 0);
}

DeviceId Device::get_id() const noexcept {
    assert(device_id >= 0);

    return device_id;
}

void Device::send(std::unique_ptr<Chunk> chunk) noexcept {
    // assert the validity of the chunk
    assert(chunk != nullptr);

    // assert this node is the current source of the chunk
    assert(chunk->current_device()->get_id() == device_id);

    // assert the chunk hasn't arrived its final destination yet
    assert(!chunk->arrived_dest());

    // get next dest
    const auto next_dest = chunk->next_device();
    const auto next_dest_id = next_dest->get_id();

    // assert the next dest is connected to this node
    assert(connected(next_dest_id));

    auto& attachments = links[next_dest_id];
    assert(!attachments.empty());

    if (attachments.size() == 1) {
        attachments.front().link->send(std::move(chunk));
        return;
    }

    const auto chunk_sizes =
        split_chunk_across_links(chunk->get_size(), attachments);

    std::vector<ChunkSize> nonzero_sizes;
    std::vector<size_t> nonzero_indices;
    for (size_t i = 0; i < chunk_sizes.size(); ++i) {
        if (chunk_sizes[i] == 0) {
            continue;
        }
        nonzero_sizes.push_back(chunk_sizes[i]);
        nonzero_indices.push_back(i);
    }

    assert(!nonzero_indices.empty());

    if (nonzero_indices.size() == 1) {
        attachments[nonzero_indices.front()].link->send(std::move(chunk));
        return;
    }

    auto child_chunks = chunk->split(nonzero_sizes);
    assert(child_chunks.size() == nonzero_indices.size());
    for (size_t i = 0; i < child_chunks.size(); ++i) {
        attachments[nonzero_indices[i]].link->send(std::move(child_chunks[i]));
    }
}

void Device::connect(const DeviceId id,
                     const Bandwidth bandwidth,
                     const Latency latency,
                     const uint32_t weight) noexcept {
    assert(id >= 0);
    assert(bandwidth > 0);
    assert(latency >= 0);
    assert(weight > 0);

    links[id].push_back({std::make_shared<Link>(bandwidth, latency), weight});
}

bool Device::connected(const DeviceId dest) const noexcept {
    assert(dest >= 0);

    // check whether the connection exists
    return links.find(dest) != links.end();
}

std::vector<ChunkSize> Device::split_chunk_across_links(
    const ChunkSize chunk_size,
    const std::vector<LinkAttachment>& attachments) const noexcept {
    assert(chunk_size > 0);
    assert(!attachments.empty());

    const auto total_weight = std::accumulate(
        attachments.begin(), attachments.end(), uint64_t{0},
        [](const uint64_t sum, const LinkAttachment& attachment) {
            return sum + attachment.weight;
        });
    assert(total_weight > 0);

    std::vector<ChunkSize> chunk_sizes(attachments.size(), 0);
    std::vector<std::tuple<uint64_t, size_t>> remainders;
    remainders.reserve(attachments.size());

    ChunkSize assigned = 0;
    for (size_t i = 0; i < attachments.size(); ++i) {
        const auto weighted_bytes =
            static_cast<unsigned __int128>(chunk_size) * attachments[i].weight;
        chunk_sizes[i] =
            static_cast<ChunkSize>(weighted_bytes / total_weight);
        assigned += chunk_sizes[i];
        remainders.emplace_back(
            static_cast<uint64_t>(weighted_bytes % total_weight), i);
    }

    const auto remaining = chunk_size - assigned;
    std::sort(remainders.begin(), remainders.end(),
              [](const auto& lhs, const auto& rhs) {
                  if (std::get<0>(lhs) != std::get<0>(rhs)) {
                      return std::get<0>(lhs) > std::get<0>(rhs);
                  }
                  return std::get<1>(lhs) < std::get<1>(rhs);
              });

    for (ChunkSize i = 0; i < remaining; ++i) {
        chunk_sizes[std::get<1>(remainders[static_cast<size_t>(i)])]++;
    }

    return chunk_sizes;
}
