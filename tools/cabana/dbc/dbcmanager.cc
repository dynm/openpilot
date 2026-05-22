#include "tools/cabana/dbc/dbcmanager.h"

#include <algorithm>
#include <set>

#include "tools/cabana/streams/abstractstream.h"

bool DBCManager::open(const SourceSet &sources, const std::string &dbc_file_name, QString *error) {
  demux_msg_cache.clear();
  demux_msg_cache_repetition.clear();
  try {
    auto it = std::find_if(dbc_files.begin(), dbc_files.end(),
                           [&](auto &f) { return f.second && f.second->filename == dbc_file_name; });
    auto file = (it != dbc_files.end()) ? it->second : std::make_shared<DBCFile>(dbc_file_name);
    for (auto s : sources) {
      dbc_files[s] = file;
    }
  } catch (std::exception &e) {
    if (error) *error = e.what();
    return false;
  }

  emit DBCFileChanged();
  return true;
}

bool DBCManager::open(const SourceSet &sources, const std::string &name, const std::string &content, QString *error) {
  demux_msg_cache.clear();
  demux_msg_cache_repetition.clear();
  try {
    auto file = std::make_shared<DBCFile>(name, content);
    for (auto s : sources) {
      dbc_files[s] = file;
    }
  } catch (std::exception &e) {
    if (error) *error = e.what();
    return false;
  }

  emit DBCFileChanged();
  return true;
}

void DBCManager::close(const SourceSet &sources) {
  demux_msg_cache.clear();
  demux_msg_cache_repetition.clear();
  for (auto s : sources) {
    dbc_files[s] = nullptr;
  }
  emit DBCFileChanged();
}

void DBCManager::close(DBCFile *dbc_file) {
  demux_msg_cache.clear();
  demux_msg_cache_repetition.clear();
  for (auto &[_, f] : dbc_files) {
    if (f.get() == dbc_file) f = nullptr;
  }
  emit DBCFileChanged();
}

void DBCManager::closeAll() {
  demux_msg_cache.clear();
  demux_msg_cache_repetition.clear();
  dbc_files.clear();
  emit DBCFileChanged();
}

void DBCManager::addSignal(const MessageId &id, const cabana::Signal &sig) {
  demux_msg_cache.clear();
  demux_msg_cache_repetition.clear();
  auto demux = demuxSource(id);
  MessageId target_id = demux ? demux->first : id;
  const int repetition = demux ? can->demuxRepetition(id) : 1;
  cabana::Signal target_sig = demux ? demuxSignal(sig, demux->second, repetition) : sig;
  auto dbc_file = findDBCFile(target_id);
  if (auto m = dbc_file ? dbc_file->msg(target_id) : nullptr) {
    if (demux) ensureDemuxMultiplexor(m, repetition);
    if (auto s = m->addSignal(target_sig)) {
      emit signalAdded(target_id, s);
      emit msgUpdated(target_id);
      if (demux) {
        emit msgUpdated(id);
      }
      emit maskUpdated();
    }
  }
}

void DBCManager::updateSignal(const MessageId &id, const std::string &sig_name, const cabana::Signal &sig) {
  demux_msg_cache.clear();
  demux_msg_cache_repetition.clear();
  auto demux = demuxSource(id);
  MessageId target_id = demux ? demux->first : id;
  const int repetition = demux ? can->demuxRepetition(id) : 1;
  cabana::Signal target_sig = demux ? demuxSignal(sig, demux->second, repetition) : sig;
  auto dbc_file = findDBCFile(target_id);
  if (auto m = dbc_file ? dbc_file->msg(target_id) : nullptr) {
    if (demux) ensureDemuxMultiplexor(m, repetition);
    if (auto s = m->updateSignal(sig_name, target_sig)) {
      emit signalUpdated(s);
      emit msgUpdated(target_id);
      if (demux) {
        emit msgUpdated(id);
      }
      emit maskUpdated();
    }
  }
}

void DBCManager::removeSignal(const MessageId &id, const std::string &sig_name) {
  demux_msg_cache.clear();
  demux_msg_cache_repetition.clear();
  auto demux = demuxSource(id);
  MessageId target_id = demux ? demux->first : id;
  auto dbc_file = findDBCFile(target_id);
  if (auto m = dbc_file ? dbc_file->msg(target_id) : nullptr) {
    if (auto s = m->sig(sig_name)) {
      if (demux && (s->type != cabana::Signal::Type::Multiplexed || s->multiplex_value % can->demuxRepetition(id) != demux->second)) {
        return;
      }
      emit signalRemoved(s);
      m->removeSignal(sig_name);
      emit msgUpdated(target_id);
      if (demux) {
        emit msgUpdated(id);
      }
      emit maskUpdated();
    }
  }
}

void DBCManager::updateMsg(const MessageId &id, const std::string &name, uint32_t size, const std::string &node, const std::string &comment) {
  demux_msg_cache.clear();
  demux_msg_cache_repetition.clear();
  auto demux = demuxSource(id);
  MessageId target_id = demux ? demux->first : id;
  auto dbc_file = findDBCFile(target_id);
  assert(dbc_file);  // This should be impossible
  if (demux && dbc_file->msg(id)) {
    dbc_file->removeMsg(id);
  }
  dbc_file->updateMsg(target_id, name, size, node, comment);
  emit msgUpdated(target_id);
  if (demux) {
    emit msgUpdated(id);
    emit maskUpdated();
  }
}

void DBCManager::removeMsg(const MessageId &id) {
  demux_msg_cache.clear();
  demux_msg_cache_repetition.clear();
  auto demux = demuxSource(id);
  MessageId target_id = demux ? demux->first : id;
  auto dbc_file = findDBCFile(target_id);
  assert(dbc_file);  // This should be impossible
  if (demux && dbc_file->msg(id)) {
    dbc_file->removeMsg(id);
  }
  dbc_file->removeMsg(target_id);
  emit msgRemoved(target_id);
  if (demux) {
    emit msgRemoved(id);
  }
  emit maskUpdated();
}

std::string DBCManager::newMsgName(const MessageId &id) {
  char buf[64];
  snprintf(buf, sizeof(buf), "NEW_MSG_%X", id.address);
  return buf;
}

std::string DBCManager::newSignalName(const MessageId &id) {
  auto demux = demuxSource(id);
  auto m = demux ? msg(demux->first) : msg(id);
  return m ? m->newSignalName() : "";
}

const std::map<uint32_t, cabana::Msg> &DBCManager::getMessages(uint8_t source) {
  static std::map<uint32_t, cabana::Msg> empty_msgs;
  auto dbc_file = findDBCFile(source);
  return dbc_file ? dbc_file->getMessages() : empty_msgs;
}

cabana::Msg *DBCManager::msg(const MessageId &id) {
  auto dbc_file = findDBCFile(id);
  if (!dbc_file) return nullptr;

  auto demux = demuxSource(id);
  if (!demux) return dbc_file->msg(id);

  int repetition = can->demuxRepetition(id);
  auto cached_it = demux_msg_cache.find(id);
  auto cached_repetition = demux_msg_cache_repetition.find(id);
  if (cached_it != demux_msg_cache.end() && cached_repetition != demux_msg_cache_repetition.end() && cached_repetition->second == repetition) {
    return &cached_it->second;
  }

  auto source_msg = dbc_file->msg(demux->first);
  if (!source_msg) return nullptr;

  auto &cached = demux_msg_cache[id];
  demux_msg_cache_repetition[id] = repetition;
  cached = *source_msg;
  cached.address = id.address;
  for (auto it = cached.sigs.begin(); it != cached.sigs.end();) {
    const auto sig = *it;
    const bool keep = sig->type == cabana::Signal::Type::Multiplexor ||
                      (sig->type == cabana::Signal::Type::Multiplexed &&
                       (repetition <= 1 || (sig->multiplex_value % repetition) == demux->second));
    if (keep) {
      ++it;
    } else {
      delete sig;
      it = cached.sigs.erase(it);
    }
  }
  cached.update();
  return &cached;
}

cabana::Msg *DBCManager::msg(uint8_t source, const std::string &name) {
  auto dbc_file = findDBCFile(source);
  auto msg = dbc_file ? dbc_file->msg(name) : nullptr;
  if (msg && can && can->demuxCycleBase({.source = source, .address = msg->address}) >= 0) {
    return nullptr;
  }
  return msg;
}

std::vector<std::string> DBCManager::signalNames() {
  // Used for autocompletion
  std::set<std::string> names;
  for (auto &f : allDBCFiles()) {
    for (auto &[_, m] : f->getMessages()) {
      for (auto sig : m.getSignals()) {
        names.insert(sig->name);
      }
    }
  }
  std::vector<std::string> ret(names.begin(), names.end());
  std::sort(ret.begin(), ret.end());
  return ret;
}

std::optional<std::pair<MessageId, int>> DBCManager::demuxSource(const MessageId &id) const {
  if (!can) return std::nullopt;
  int cycle_base = can->demuxCycleBase(id);
  if (cycle_base < 0) return std::nullopt;
  return std::make_pair(can->demuxSourceId(id), cycle_base);
}

cabana::Signal *DBCManager::ensureDemuxMultiplexor(cabana::Msg *msg, int repetition) const {
  if (!msg) return nullptr;

  int bits = 1;
  while ((1 << bits) < std::max(2, repetition)) {
    ++bits;
  }

  if (msg->multiplexor) {
    if (msg->multiplexor->name.rfind("DEMUX_CYCLE_COUNT", 0) == 0) {
      msg->multiplexor->start_bit = 0;
      msg->multiplexor->size = bits;
      msg->multiplexor->is_little_endian = true;
      msg->multiplexor->is_signed = false;
      msg->multiplexor->min = 0;
      msg->multiplexor->max = (1 << bits) - 1;
      msg->update();
    }
    return msg->multiplexor;
  }

  cabana::Signal mux = {};
  mux.type = cabana::Signal::Type::Multiplexor;
  mux.name = "DEMUX_CYCLE_COUNT";
  for (int i = 1; msg->sig(mux.name) != nullptr; ++i) {
    mux.name = "DEMUX_CYCLE_COUNT_" + std::to_string(i);
  }
  mux.start_bit = 0;
  mux.size = bits;
  mux.is_little_endian = true;
  mux.is_signed = false;
  mux.min = 0;
  mux.max = (1 << bits) - 1;
  mux.receiver_name = DEFAULT_NODE_NAME;
  return msg->addSignal(mux);
}

cabana::Signal DBCManager::demuxSignal(const cabana::Signal &sig, int cycle_base, int repetition) const {
  cabana::Signal target_sig = sig;
  target_sig.type = cabana::Signal::Type::Multiplexed;
  if (repetition <= 1 || target_sig.multiplex_value % repetition != cycle_base) {
    target_sig.multiplex_value = cycle_base;
  }
  return target_sig;
}

int DBCManager::nonEmptyDBCCount() {
  auto files = allDBCFiles();
  return std::count_if(files.cbegin(), files.cend(), [](auto &f) { return !f->isEmpty(); });
}

DBCFile *DBCManager::findDBCFile(const uint8_t source) {
  // Find DBC file that matches id.source, fall back to SOURCE_ALL if no specific DBC is found
  auto it = dbc_files.count(source) ? dbc_files.find(source) : dbc_files.find(-1);
  return it != dbc_files.end() ? it->second.get() : nullptr;
}

std::set<DBCFile *> DBCManager::allDBCFiles() {
  std::set<DBCFile *> files;
  for (const auto &[_, f] : dbc_files) {
    if (f) files.insert(f.get());
  }
  return files;
}

const SourceSet DBCManager::sources(const DBCFile *dbc_file) const {
  SourceSet sources;
  for (auto &[s, f] : dbc_files) {
    if (f.get() == dbc_file) sources.insert(s);
  }
  return sources;
}

std::string toString(const SourceSet &ss) {
  std::string result;
  for (int source : ss) {
    if (!result.empty()) result += ", ";
    result += (source == -1) ? "all" : std::to_string(source);
  }
  return result;
}

DBCManager *dbc() {
  static DBCManager dbc_manager(nullptr);
  return &dbc_manager;
}
