#include "tools/cabana/streams/abstractstream.h"

#include <limits>
#include <utility>

#include <QApplication>
#include "common/timing.h"
#include "tools/cabana/settings.h"

static const int EVENT_NEXT_BUFFER_SIZE = 6 * 1024 * 1024;  // 6MB

AbstractStream *can = nullptr;

AbstractStream::AbstractStream(QObject *parent) : QObject(parent) {
  assert(parent != nullptr);
  event_buffer_ = std::make_unique<MonotonicBuffer>(EVENT_NEXT_BUFFER_SIZE);

  QObject::connect(this, &AbstractStream::privateUpdateLastMsgsSignal, this, &AbstractStream::updateLastMessages, Qt::QueuedConnection);
  QObject::connect(this, &AbstractStream::seekedTo, this, &AbstractStream::updateLastMsgsTo);
  QObject::connect(this, &AbstractStream::seeking, this, [this](double sec) { current_sec_ = sec; });
  auto dbc_changed = [this]() {
    rebuildDemuxMessages();
    updateMasks();
    emit msgsReceived(nullptr, true);
  };
  QObject::connect(dbc(), &DBCManager::DBCFileChanged, this, dbc_changed);
  QObject::connect(dbc(), &DBCManager::maskUpdated, this, dbc_changed);
}

void AbstractStream::updateMasks() {
  std::lock_guard lk(mutex_);
  masks_.clear();
  if (!settings.suppress_defined_signals)
    return;

  for (const auto s : sources) {
    for (const auto &[address, m] : dbc()->getMessages(s)) {
      masks_[{.source = (uint8_t)s, .address = address}] = m.mask;
    }
  }
  for (const auto &[id, source_id] : demux_source_ids_) {
    if (auto msg = dbc()->msg(id)) {
      masks_[id] = msg->mask;
    }
  }
  // clear bit change counts
  for (auto &[id, m] : messages_) {
    auto &mask = masks_[id];
    const int size = std::min(mask.size(), m.last_changes.size());
    for (int i = 0; i < size; ++i) {
      for (int j = 0; j < 8; ++j) {
        if (((mask[i] >> (7 - j)) & 1) != 0) m.bit_flip_counts[i][j] = 0;
      }
    }
  }
}

void AbstractStream::suppressDefinedSignals(bool suppress) {
  settings.suppress_defined_signals = suppress;
  updateMasks();
}

size_t AbstractStream::suppressHighlighted() {
  std::lock_guard lk(mutex_);
  size_t cnt = 0;
  for (auto &[_, m] : messages_) {
    for (auto &last_change : m.last_changes) {
      const double dt = current_sec_ - last_change.ts;
      if (dt < 2.0) {
        last_change.suppressed = true;
      }
      cnt += last_change.suppressed;
    }
    for (auto &flip_counts : m.bit_flip_counts) flip_counts.fill(0);
  }
  return cnt;
}

void AbstractStream::clearSuppressed() {
  std::lock_guard lk(mutex_);
  for (auto &[_, m] : messages_) {
    std::for_each(m.last_changes.begin(), m.last_changes.end(), [](auto &c) { c.suppressed = false; });
  }
}

void AbstractStream::updateLastMessages() {
  auto prev_src_size = sources.size();
  auto prev_msg_size = last_msgs.size();
  std::set<MessageId> msgs;

  {
    std::lock_guard lk(mutex_);
    for (const auto &id : new_msgs_) {
      const bool is_virtual = demux_source_ids_.count(id);
      const auto &can_data = is_virtual ? virtual_last_msgs_[id] : messages_[id];
      current_sec_ = std::max(current_sec_, can_data.ts);
      if (is_virtual) {
        display_last_msgs_[id] = can_data;
      } else {
        last_msgs[id] = can_data;
        if (demuxRepetition(id) <= 1) {
          display_last_msgs_[id] = can_data;
        } else {
          display_last_msgs_.erase(id);
        }
      }
      sources.insert(id.source);
    }
    msgs = std::move(new_msgs_);
  }

  if (time_range_ && (current_sec_ < time_range_->first || current_sec_ >= time_range_->second)) {
    seekTo(time_range_->first);
    return;
  }

  if (sources.size() != prev_src_size) {
    updateMasks();
    emit sourcesUpdated(sources);
  }
  emit msgsReceived(&msgs, prev_msg_size != last_msgs.size());
}

void AbstractStream::setTimeRange(const std::optional<std::pair<double, double>> &range) {
  time_range_ = range;
  if (time_range_ && (current_sec_ < time_range_->first || current_sec_ >= time_range_->second)) {
    seekTo(time_range_->first);
  }
  emit timeRangeChanged(time_range_);
}

void AbstractStream::updateEvent(const MessageId &id, double sec, const uint8_t *data, uint8_t size) {
  std::lock_guard lk(mutex_);
  messages_[id].compute(id, data, size, sec, getSpeed(), masks_[id]);
  updateDemuxForEvent(id, data, size, sec);
  new_msgs_.insert(id);
}

const std::vector<const CanEvent *> &AbstractStream::events(const MessageId &id) const {
  static std::vector<const CanEvent *> empty_events;
  auto virtual_it = virtual_events_.find(id);
  if (virtual_it != virtual_events_.end()) return virtual_it->second;
  auto it = events_.find(id);
  return it != events_.end() ? it->second : empty_events;
}

const CanData &AbstractStream::lastMessage(const MessageId &id) const {
  static CanData empty_data = {};
  auto virtual_it = virtual_last_msgs_.find(id);
  if (virtual_it != virtual_last_msgs_.end()) return virtual_it->second;
  auto it = last_msgs.find(id);
  return it != last_msgs.end() ? it->second : empty_data;
}

bool AbstractStream::isMessageActive(const MessageId &id) const {
  if (id.source == INVALID_SOURCE) {
    return false;
  }
  // Check if the message is active based on time difference and frequency
  const auto &m = lastMessage(id);
  float delta = currentSec() - m.ts;

  if (m.freq < std::numeric_limits<double>::epsilon()) {
    return delta < 1.5;
  }

  return delta < (5.0 / m.freq) + (1.0 / settings.fps);
}

void AbstractStream::updateLastMsgsTo(double sec) {
  uint64_t last_ts = toMonoTime(sec);
  std::unordered_map<MessageId, CanData> msgs;
  bool id_changed = false;

  {
    std::lock_guard lk(mutex_);
    current_sec_ = sec;
    msgs.reserve(events_.size());

    for (const auto &[id, ev] : events_) {
      auto it = std::upper_bound(ev.begin(), ev.end(), last_ts, CompareCanEvent());
      if (it != ev.begin()) {
        auto &m = msgs[id];
        double freq = 0;
        // Keep suppressed bits.
        if (auto old_m = messages_.find(id); old_m != messages_.end()) {
          freq = old_m->second.freq;
          m.last_changes.reserve(old_m->second.last_changes.size());
          std::transform(old_m->second.last_changes.cbegin(), old_m->second.last_changes.cend(),
                         std::back_inserter(m.last_changes),
                         [](const auto &change) { return CanData::ByteLastChange{.suppressed = change.suppressed}; });
        }

        auto prev = std::prev(it);
        m.compute(id, (*prev)->dat, (*prev)->size, toSeconds((*prev)->mono_time), getSpeed(), {}, freq);
        m.count = std::distance(ev.begin(), prev) + 1;
      }
    }

    new_msgs_.clear();
    messages_ = std::move(msgs);
    id_changed = messages_.size() != last_msgs.size() ||
                 std::any_of(messages_.cbegin(), messages_.cend(),
                             [this](const auto &m) { return !last_msgs.count(m.first); });
    last_msgs = messages_;
    rebuildDemuxMessagesLocked();
    seek_finished_ = true;
  }

  emit msgsReceived(nullptr, id_changed);
  seek_finished_cv_.notify_one();
}

void AbstractStream::waitForSeekFinshed() {
  std::unique_lock lock(mutex_);
  seek_finished_cv_.wait(lock, [this]() { return seek_finished_; });
  seek_finished_ = false;
}

const CanEvent *AbstractStream::newEvent(uint64_t mono_time, const cereal::CanData::Reader &c) {
  auto dat = c.getDat();
  CanEvent *e = (CanEvent *)event_buffer_->allocate(sizeof(CanEvent) + sizeof(uint8_t) * dat.size());
  e->src = c.getSrc();
  e->address = c.getAddress();
  e->mono_time = mono_time;
  e->size = dat.size();
  memcpy(e->dat, (uint8_t *)dat.begin(), e->size);
  return e;
}

void AbstractStream::mergeEvents(const std::vector<const CanEvent *> &events) {
  static MessageEventsMap msg_events;
  std::for_each(msg_events.begin(), msg_events.end(), [](auto &e) { e.second.clear(); });

  // Group events by message ID
  for (auto e : events) {
    msg_events[{.source = e->src, .address = e->address}].push_back(e);
  }

  if (!events.empty()) {
    {
      std::lock_guard lk(mutex_);
      for (const auto &[id, new_e] : msg_events) {
        if (!new_e.empty()) {
          auto &e = events_[id];
          auto pos = std::upper_bound(e.cbegin(), e.cend(), new_e.front()->mono_time, CompareCanEvent());
          e.insert(pos, new_e.cbegin(), new_e.cend());
        }
      }
      auto pos = std::upper_bound(all_events_.cbegin(), all_events_.cend(), events.front()->mono_time, CompareCanEvent());
      all_events_.insert(pos, events.cbegin(), events.cend());
      rebuildDemuxMessagesLocked();
    }
    emit eventsMerged(msg_events);
  }
}

std::pair<CanEventIter, CanEventIter> AbstractStream::eventsInRange(const MessageId &id, std::optional<std::pair<double, double>> time_range) const {
  const auto &events = can->events(id);
  if (!time_range) return {events.begin(), events.end()};

  auto first = std::lower_bound(events.begin(), events.end(), can->toMonoTime(time_range->first), CompareCanEvent());
  auto last = std::upper_bound(first, events.end(), can->toMonoTime(time_range->second), CompareCanEvent());
  return {first, last};
}

MessageId AbstractStream::demuxMessageId(const MessageId &id, int cycle_base) const {
  return {.source = id.source, .address = (id.address << 8) + static_cast<uint32_t>(cycle_base)};
}

MessageId AbstractStream::demuxSourceId(const MessageId &id) const {
  auto it = demux_source_ids_.find(id);
  return it != demux_source_ids_.end() ? it->second : id;
}

int AbstractStream::demuxCycleBase(const MessageId &id) const {
  auto it = demux_cycle_bases_.find(id);
  return it != demux_cycle_bases_.end() ? it->second : -1;
}

int AbstractStream::demuxRepetition(const MessageId &id) const {
  const MessageId source_id = demuxSourceId(id);
  auto it = demux_repetitions_.find(source_id);
  if (it != demux_repetitions_.end()) return it->second;
  return dbcDemuxRepetition(source_id);
}

void AbstractStream::setDemuxRepetition(const MessageId &id, int repetition) {
  repetition = std::max(1, repetition);
  {
    std::lock_guard lk(mutex_);
    const auto source_it = demux_source_ids_.find(id);
    const MessageId source_id = source_it != demux_source_ids_.end() ? source_it->second : id;
    if (source_id.source == INVALID_SOURCE) return;

    if (repetition <= 1) {
      demux_repetitions_.erase(source_id);
    } else {
      demux_repetitions_[source_id] = repetition;
    }
    rebuildDemuxMessagesLocked();
  }
  emit msgsReceived(nullptr, true);
}

void AbstractStream::updateDemuxForEvent(const MessageId &id, const uint8_t *data, uint8_t size, double sec) {
  const int repetition = demuxRepetition(id);
  if (repetition <= 1 || size == 0) return;

  const int cycle_base = data[0] % repetition;
  const MessageId virtual_id = demuxMessageId(id, cycle_base);
  virtual_last_msgs_[virtual_id].compute(virtual_id, data, size, sec, getSpeed(), masks_[virtual_id]);
  display_last_msgs_.erase(id);
  display_last_msgs_[virtual_id] = virtual_last_msgs_[virtual_id];
  new_msgs_.insert(virtual_id);
}

void AbstractStream::rebuildDemuxMessages() {
  std::lock_guard lk(mutex_);
  rebuildDemuxMessagesLocked();
}

void AbstractStream::rebuildDemuxMessagesLocked() {
  virtual_events_.clear();
  virtual_last_msgs_.clear();
  demux_source_ids_.clear();
  demux_cycle_bases_.clear();
  display_last_msgs_ = last_msgs;

  const uint64_t current_mono = toMonoTime(current_sec_);
  for (const auto &[source_id, repetition] : activeDemuxRepetitions()) {
    if (repetition <= 1 || source_id.source == INVALID_SOURCE) continue;

    display_last_msgs_.erase(source_id);
    for (int cycle_base = 0; cycle_base < repetition; ++cycle_base) {
      const MessageId virtual_id = demuxMessageId(source_id, cycle_base);
      demux_source_ids_[virtual_id] = source_id;
      demux_cycle_bases_[virtual_id] = cycle_base;
      virtual_events_[virtual_id];
    }

    const auto raw_events = events_.find(source_id);
    if (raw_events != events_.end()) {
      for (const CanEvent *e : raw_events->second) {
        if (!e || e->size == 0) continue;
        const MessageId virtual_id = demuxMessageId(source_id, e->dat[0] % repetition);
        virtual_events_[virtual_id].push_back(e);
      }
    }

    for (int cycle_base = 0; cycle_base < repetition; ++cycle_base) {
      const MessageId virtual_id = demuxMessageId(source_id, cycle_base);
      auto &evs = virtual_events_[virtual_id];
      auto end = std::upper_bound(evs.begin(), evs.end(), current_mono, CompareCanEvent());
      CanData data;
      for (auto it = evs.begin(); it != end; ++it) {
        const CanEvent *e = *it;
        data.compute(virtual_id, e->dat, e->size, toSeconds(e->mono_time), getSpeed(), masks_[virtual_id]);
      }
      if (!data.dat.empty()) {
        virtual_last_msgs_[virtual_id] = data;
        display_last_msgs_[virtual_id] = data;
      } else if (last_msgs.count(source_id)) {
        CanData placeholder = last_msgs[source_id];
        placeholder.count = 0;
        placeholder.freq = 0;
        placeholder.colors.assign(placeholder.dat.size(), QColor(0, 0, 0, 0));
        virtual_last_msgs_[virtual_id] = placeholder;
        display_last_msgs_[virtual_id] = placeholder;
      }
    }
  }
}

int AbstractStream::dbcDemuxRepetition(const MessageId &source_id) const {
  if (source_id.source == INVALID_SOURCE) return 1;

  auto dbc_file = dbc()->findDBCFile(source_id);
  auto msg = dbc_file ? dbc_file->msg(source_id) : nullptr;
  if (!msg || !msg->multiplexor || msg->multiplexor->size <= 0) return 1;

  constexpr int max_demux_bits = 5;
  return 1 << std::min(msg->multiplexor->size, max_demux_bits);
}

std::unordered_map<MessageId, int> AbstractStream::activeDemuxRepetitions() const {
  auto ret = demux_repetitions_;
  for (const auto &[source_id, _] : last_msgs) {
    if (!ret.count(source_id)) {
      int repetition = dbcDemuxRepetition(source_id);
      if (repetition > 1) {
        ret[source_id] = repetition;
      }
    }
  }
  return ret;
}

namespace {

enum Color { GREYISH_BLUE, CYAN, RED};
QColor getColor(int c) {
  constexpr int start_alpha = 128;
  static const QColor colors[] = {
      [GREYISH_BLUE] = QColor(102, 86, 169, start_alpha / 2),
      [CYAN] = QColor(0, 187, 255, start_alpha),
      [RED] = QColor(255, 0, 0, start_alpha),
  };
  return settings.theme == LIGHT_THEME ? colors[c] : colors[c].lighter(135);
}

inline QColor blend(const QColor &a, const QColor &b) {
  return QColor((a.red() + b.red()) / 2, (a.green() + b.green()) / 2, (a.blue() + b.blue()) / 2, (a.alpha() + b.alpha()) / 2);
}

// Calculate the frequency from the past one minute data
double calc_freq(const MessageId &msg_id, double current_sec) {
  auto [first, last] = can->eventsInRange(msg_id, std::make_pair(current_sec - 59, current_sec));
  int count = std::distance(first, last);
  if (count <= 1) return 0.0;

  double duration = ((*std::prev(last))->mono_time - (*first)->mono_time) / 1e9;
  return duration > std::numeric_limits<double>::epsilon() ? (count - 1) / duration : 0.0;
}

}  // namespace

void CanData::compute(const MessageId &msg_id, const uint8_t *can_data, const int size, double current_sec,
                      double playback_speed, const std::vector<uint8_t> &mask, double in_freq) {
  ts = current_sec;
  ++count;

  if (auto sec = seconds_since_boot(); (sec - last_freq_update_ts) >= 1) {
    last_freq_update_ts = sec;
    freq = !in_freq ? calc_freq(msg_id, ts) : in_freq;
  }

  if (dat.size() != size) {
    dat.assign(can_data, can_data + size);
    colors.assign(size, QColor(0, 0, 0, 0));
    last_changes.resize(size);
    bit_flip_counts.resize(size);
    std::for_each(last_changes.begin(), last_changes.end(), [current_sec](auto &c) { c.ts = current_sec; });
  } else {
    constexpr int periodic_threshold = 10;
    constexpr float fade_time = 2.0;
    const float alpha_delta = 1.0 / (freq + 1) / (fade_time * playback_speed);

    for (int i = 0; i < size; ++i) {
      auto &last_change = last_changes[i];

      uint8_t mask_byte = last_change.suppressed ? 0x00 : 0xFF;
      if (i < mask.size()) mask_byte &= ~(mask[i]);

      const uint8_t last = dat[i] & mask_byte;
      const uint8_t cur = can_data[i] & mask_byte;
      if (last != cur) {
        const int delta = cur - last;
        // Keep track if signal is changing randomly, or mostly moving in the same direction
        last_change.same_delta_counter += std::signbit(delta) == std::signbit(last_change.delta) ? 1 : -4;
        last_change.same_delta_counter = std::clamp(last_change.same_delta_counter, 0, 16);

        const double delta_t = ts - last_change.ts;
        // Mostly moves in the same direction, color based on delta up/down
        if (delta_t * freq > periodic_threshold || last_change.same_delta_counter > 8) {
          // Last change was while ago, choose color based on delta up or down
          colors[i] = getColor(cur > last ? CYAN : RED);
        } else {
          // Periodic changes
          colors[i] = blend(colors[i], getColor(GREYISH_BLUE));
        }

        // Track bit level changes
        auto &row_bit_flips = bit_flip_counts[i];
        const uint8_t diff = (cur ^ last);
        for (int bit = 0; bit < 8; bit++) {
          if (diff & (1u << bit)) {
            ++row_bit_flips[7 - bit];
          }
        }

        last_change.ts = ts;
        last_change.delta = delta;
      } else {
        // Fade out
        colors[i].setAlphaF(std::max(0.0, colors[i].alphaF() - alpha_delta));
      }
    }
  }
  memcpy(dat.data(), can_data, size);
}
