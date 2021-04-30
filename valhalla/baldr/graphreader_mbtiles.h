// CARTOHACK
#pragma once

#include <string>
#include <tuple>
#include <vector>
#include <utility>

#include "baldr/graphreader.h"
#include "filesystem.h"
#include "midgard/encoded.h"
#include "midgard/logging.h"

#include <zlib.h>
#include <sqlite3pp.h>

using namespace valhalla::midgard;

namespace {
static bool inflate(const void* in_data, size_t in_size, std::vector<char>& out) {
  const unsigned char* in = reinterpret_cast<const unsigned char*>(in_data);

  std::vector<unsigned char> buf(16384);
  ::z_stream infstream;
  std::memset(&infstream, 0, sizeof(infstream));
  infstream.zalloc = NULL;
  infstream.zfree = NULL;
  infstream.opaque = NULL;
  int err = Z_OK;
  infstream.avail_in = static_cast<unsigned int>(in_size); // size of input
  infstream.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in_data)); // input char array
  infstream.avail_out = static_cast<unsigned int>(buf.size()); // size of output
  infstream.next_out = buf.data(); // output char array
  ::inflateInit2(&infstream, MAX_WBITS + 32);
  do {
    infstream.avail_out = static_cast<unsigned int>(buf.size()); // size of output
    infstream.next_out = buf.data(); // output char array
    err = ::inflate(&infstream, infstream.avail_in > 0 ? Z_NO_FLUSH : Z_FINISH);
    if (err != Z_OK && err != Z_STREAM_END) {
      break;
    }
    out.insert(out.end(), reinterpret_cast<char*>(buf.data()), reinterpret_cast<char*>(buf.data() + buf.size() - infstream.avail_out));
  } while (err != Z_STREAM_END);
  ::inflateEnd(&infstream);
  return err == Z_OK || err == Z_STREAM_END;
}

static bool inflate_raw(const void* in_data, std::size_t in_size, const void* dict, std::size_t dict_size, std::vector<char>& out) {
  const unsigned char* in = reinterpret_cast<const unsigned char*>(in_data);

  std::vector<unsigned char> buf(16384);
  ::z_stream infstream;
  std::memset(&infstream, 0, sizeof(infstream));
  infstream.zalloc = NULL;
  infstream.zfree = NULL;
  infstream.opaque = NULL;
  int err = Z_OK;
  infstream.avail_in = static_cast<unsigned int>(in_size); // size of input
  infstream.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in_data)); // input char array
  infstream.avail_out = static_cast<unsigned int>(buf.size()); // size of output
  infstream.next_out = buf.data(); // output char array
  ::inflateInit2(&infstream, -MAX_WBITS);
  if (dict) {
    ::inflateSetDictionary(&infstream, reinterpret_cast<const Bytef*>(dict), static_cast<unsigned int>(dict_size));
  }
  do {
    infstream.avail_out = static_cast<unsigned int>(buf.size()); // size of output
    infstream.next_out = buf.data(); // output char array
    err = ::inflate(&infstream, infstream.avail_in > 0 ? Z_NO_FLUSH : Z_FINISH);
    if (err != Z_OK && err != Z_STREAM_END) {
      break;
    }
    out.insert(out.end(), reinterpret_cast<char*>(buf.data()), reinterpret_cast<char*>(buf.data() + buf.size() - infstream.avail_out));
  } while (err != Z_STREAM_END);
  ::inflateEnd(&infstream);
  return err == Z_OK || err == Z_STREAM_END;
}
}

namespace valhalla {
namespace baldr {

struct GraphReader::mbtiles_db_t {
  mbtiles_db_t(const std::vector<std::shared_ptr<sqlite3pp::database>>& dbs) {
    for (auto& db : dbs) {
      std::shared_ptr<std::vector<unsigned char>> dict;
      bool valid = false;
      try {
        sqlite3pp::query query1(*db, "SELECT value FROM metadata WHERE name='format'");
        for (auto qit = query1.begin(); qit != query1.end(); qit++) {
          valid = std::string(qit->get<const char*>(0)) == "gph3";
        }

        sqlite3pp::query query2(*db, "SELECT value FROM metadata WHERE name='shared_zlib_dict'");
        for (auto qit = query2.begin(); qit != query2.end(); qit++) {
          const unsigned char* dataPtr = reinterpret_cast<const unsigned char*>(qit->get<const void*>(0));
          std::size_t dataSize = qit->column_bytes(0);
          dict.reset(new std::vector<unsigned char>(dataPtr, dataPtr + dataSize));
        }
      } catch (const std::exception& e) {
        LOG_ERROR(e.what());
      }

      if (valid) {
        mbt_dbs_.emplace_back(MBTDatabase{ db, dict });
      }
      else {
        LOG_WARN("Routing package does not contain 'gph3' tiles, ignoring");
      }
    }
  }

  std::unordered_set<GraphId> FindTiles(int level) const {
    std::unordered_set<GraphId> graphids;
    for (auto& mbt_db : mbt_dbs_) {
      try {
        sqlite3pp::query query(*mbt_db.database, "SELECT zoom_level, tile_column, tile_row FROM tiles WHERE zoom_level=:level OR :level<0");
        query.bind(":level", level);
        for (auto it = query.begin(); it != query.end(); it++) {
          int z = (*it).get<int>(0);
          int x = (*it).get<int>(1);
          int y = (*it).get<int>(2);
          graphids.insert(ToGraphId(std::make_tuple(z, x, y)));
        }
      } catch (const std::exception& e) {
        LOG_ERROR(e.what());
      }
    }
    return graphids;
  }

  bool DoesTileExist(const GraphId& graphid) const {
    for (auto& mbt_db : mbt_dbs_) {
      try {
        std::tuple<int, int, int> tile_coords = FromGraphId(graphid);
        sqlite3pp::query query(*mbt_db.database, "SELECT COUNT(*) FROM tiles WHERE zoom_level=:z AND tile_row=:y and tile_column=:y");
        query.bind(":z", std::get<0>(tile_coords));
        query.bind(":x", std::get<1>(tile_coords));
        query.bind(":y", std::get<2>(tile_coords));
        for (auto it = query.begin(); it != query.end(); it++) {
          if ((*it).get<int>(0) > 0) {
            return true;
          }
        }
      } catch (const std::exception& e) {
        LOG_ERROR(e.what());
      }
    }
    return false;
  }

  bool ReadTile(const GraphId& graphid, std::vector<char>& tile_data) const {
    for (auto& mbt_db : mbt_dbs_) {
      try {
        std::tuple<int, int, int> tile_coords = FromGraphId(graphid);
        sqlite3pp::query query(*mbt_db.database, "SELECT tile_data FROM tiles WHERE zoom_level=:z AND tile_row=:y and tile_column=:x");
        query.bind(":z", std::get<0>(tile_coords));
        query.bind(":x", std::get<1>(tile_coords));
        query.bind(":y", std::get<2>(tile_coords));
        for (auto it = query.begin(); it != query.end(); it++) {
          std::size_t data_size = (*it).column_bytes(0);
          const unsigned char* data_ptr = static_cast<const unsigned char*>((*it).get<const void*>(0));
          tile_data.clear();
          tile_data.reserve(data_size + 64);
          if (mbt_db.zdict) {
            return inflate_raw(data_ptr, data_size, mbt_db.zdict->data(), mbt_db.zdict->size(), tile_data);
          } else {
            return inflate(data_ptr, data_size, tile_data);
          }
        }
      } catch (const std::exception& e) {
        LOG_ERROR(e.what());
      }
    }
    return false;
  }

private:
  struct MBTDatabase {
    std::shared_ptr<sqlite3pp::database> database;
    std::shared_ptr<std::vector<unsigned char>> zdict;
  };

  std::vector<MBTDatabase> mbt_dbs_;

  static GraphId ToGraphId(const std::tuple<int, int, int>& tile_coords) {
    const auto& levels = TileHierarchy::levels();
    for (const auto& level : levels) {
      if (level.level != std::get<0>(tile_coords)) {
        continue;
      }
      uint32_t tileid = level.tiles.TileId(std::get<1>(tile_coords), std::get<2>(tile_coords));
      return GraphId(tileid, std::get<0>(tile_coords), 0);
    }
    return GraphId();
  }

  static std::tuple<int, int, int> FromGraphId(const GraphId& graphid) {
    const auto& levels = TileHierarchy::levels();
    for (const auto& level : levels) {
      if (level.level != graphid.level()) {
        continue;
      }
      auto coords = level.tiles.GetRowColumn(graphid.tileid());
      return std::tuple<int, int, int>(graphid.level(), coords.second, coords.first);
    }
    return std::tuple<int, int, int>();
  }
};

} // namespace baldr
} // namespace valhalla
