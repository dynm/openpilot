#include "tools/cabana/http/server.h"

#include <QHostAddress>
#include <QJsonArray>
#include <QJsonDocument>

#include "tools/cabana/chart/chartswidget.h"
#include "tools/cabana/dbc/dbc.h"
#include "tools/cabana/dbc/dbcmanager.h"
#include "tools/cabana/mainwin.h"
#include "tools/cabana/streams/abstractstream.h"

namespace {

QString statusText(int status) {
  switch (status) {
    case 200: return "OK";
    case 400: return "Bad Request";
    case 404: return "Not Found";
    case 405: return "Method Not Allowed";
    default: return "Error";
  }
}

QByteArray formatData(const std::vector<uint8_t> &dat) {
  QByteArray hex;
  for (auto b : dat) {
    hex += QByteArray::number(b, 16).rightJustified(2, '0');
  }
  return hex;
}

}  // namespace

CabanaHttpServer::CabanaHttpServer(MainWindow *main_window, QObject *parent) : QObject(parent), main_window_(main_window) {
  QObject::connect(&server_, &QTcpServer::newConnection, this, &CabanaHttpServer::handleConnection);
  bool ok = server_.listen(QHostAddress::LocalHost, qEnvironmentVariableIntValue("CABANA_HTTP_PORT") > 0 ?
                                                   qEnvironmentVariableIntValue("CABANA_HTTP_PORT") : 8989);
  qInfo() << (ok ? "Cabana HTTP API listening on" : "Cabana HTTP API failed") << server_.serverAddress() << server_.serverPort();
}

void CabanaHttpServer::handleConnection() {
  while (auto *socket = server_.nextPendingConnection()) {
    QObject::connect(socket, &QTcpSocket::readyRead, this, [this, socket]() { handleSocketData(socket); });
    QObject::connect(socket, &QTcpSocket::disconnected, socket, &QObject::deleteLater);
  }
}

void CabanaHttpServer::handleSocketData(QTcpSocket *socket) {
  HttpRequest req;
  if (!parseRequest(socket->readAll(), &req)) {
    sendJson(socket, 400, {{"ok", false}, {"error", "invalid http request"}});
    return;
  }

  int status = 200;
  QJsonObject body = handleRequest(req, &status);
  sendJson(socket, status, body);
}

bool CabanaHttpServer::parseRequest(const QByteArray &raw, HttpRequest *request) const {
  auto header_end = raw.indexOf("\r\n\r\n");
  if (header_end < 0) return false;

  QList<QByteArray> lines = raw.left(header_end).split('\n');
  if (lines.isEmpty()) return false;

  auto start_line = lines.takeFirst().trimmed().split(' ');
  if (start_line.size() < 2) return false;
  request->method = start_line[0];

  const QUrl url(start_line[1]);
  request->path = url.path();
  request->query = QUrlQuery(url);

  QByteArray body = raw.mid(header_end + 4);
  if (!body.trimmed().isEmpty()) {
    auto doc = QJsonDocument::fromJson(body);
    if (!doc.isObject()) return false;
    request->json_body = doc.object();
  }
  return true;
}

void CabanaHttpServer::sendJson(QTcpSocket *socket, int status, const QJsonObject &body) const {
  QByteArray payload = QJsonDocument(body).toJson(QJsonDocument::Compact);
  QByteArray response = "HTTP/1.1 " + QByteArray::number(status) + " " + statusText(status).toUtf8() + "\r\n";
  response += "Content-Type: application/json\r\n";
  response += "Connection: close\r\n";
  response += "Content-Length: " + QByteArray::number(payload.size()) + "\r\n\r\n";
  response += payload;
  socket->write(response);
  socket->disconnectFromHost();
}

QJsonObject CabanaHttpServer::handleRequest(const HttpRequest &request, int *status) const {
  if (request.method == "GET" && request.path == "/health") {
    return {{"ok", true}, {"route", can ? can->routeName() : ""}};
  }

  if (request.method == "GET" && request.path == "/api/messages") {
    bool ok = false;
    int source = request.query.queryItemValue("source").toInt(&ok);
    if (!ok) {
      *status = 400;
      return {{"ok", false}, {"error", "source query parameter is required"}};
    }
    return handleListMessages(source);
  }

  if (request.method == "GET" && request.path == "/api/signals/value") {
    return handleSignalValue(request, status);
  }

  if (request.method == "POST" && request.path == "/api/charts/show") {
    return handleChartShow(request, status);
  }

  if (request.method == "POST" && request.path == "/api/dbc/signal") {
    return handleSignalWrite(request, status);
  }

  *status = 404;
  return {{"ok", false}, {"error", "not found"}};
}

QJsonObject CabanaHttpServer::handleListMessages(uint8_t source) const {
  QJsonArray messages;
  for (const auto &[address, msg] : dbc()->getMessages(source)) {
    QJsonArray signals;
    for (auto *sig : msg.getSignals()) {
      signals.push_back(QJsonObject{{"name", sig->name},
                                    {"start_bit", sig->start_bit},
                                    {"size", sig->size},
                                    {"factor", sig->factor},
                                    {"offset", sig->offset},
                                    {"is_little_endian", sig->is_little_endian},
                                    {"is_signed", sig->is_signed}});
    }
    messages.push_back(QJsonObject{{"address", int(address)}, {"name", msg.name}, {"size", int(msg.size)}, {"signals", signals}});
  }
  return {{"ok", true}, {"messages", messages}};
}

QJsonObject CabanaHttpServer::handleSignalValue(const HttpRequest &request, int *status) const {
  bool source_ok = false;
  bool address_ok = false;
  int source = request.query.queryItemValue("source").toInt(&source_ok);
  int address = request.query.queryItemValue("address").toInt(&address_ok, 0);
  QString signal_name = request.query.queryItemValue("signal");
  if (!source_ok || !address_ok || signal_name.isEmpty()) {
    *status = 400;
    return {{"ok", false}, {"error", "source/address/signal are required"}};
  }

  MessageId id = {.source = uint8_t(source), .address = uint32_t(address)};
  auto *msg = dbc()->msg(id);
  if (!msg) {
    *status = 404;
    return {{"ok", false}, {"error", "message not found in current DBC"}};
  }
  auto *sig = msg->sig(signal_name);
  if (!sig) {
    *status = 404;
    return {{"ok", false}, {"error", "signal not found"}};
  }

  const auto &last = can->lastMessage(id);
  double value = 0;
  bool decoded = sig->getValue(last.dat.data(), last.dat.size(), &value);

  return {{"ok", decoded},
          {"decoded", decoded},
          {"value", value},
          {"formatted", sig->formatValue(value)},
          {"last_ts", last.ts},
          {"data_hex", QString::fromUtf8(formatData(last.dat))}};
}

QJsonObject CabanaHttpServer::handleChartShow(const HttpRequest &request, int *status) const {
  bool source_ok = false;
  bool address_ok = false;
  int source = request.json_body["source"].toInt(-1);
  source_ok = source >= 0;
  int address = request.json_body["address"].toInt(-1);
  address_ok = address >= 0;
  QString signal_name = request.json_body["signal"].toString();
  bool merge = request.json_body["merge"].toBool(true);

  if (!source_ok || !address_ok || signal_name.isEmpty()) {
    *status = 400;
    return {{"ok", false}, {"error", "source/address/signal are required"}};
  }

  MessageId id = {.source = uint8_t(source), .address = uint32_t(address)};
  auto *msg = dbc()->msg(id);
  auto *sig = msg ? msg->sig(signal_name) : nullptr;
  if (!sig) {
    *status = 404;
    return {{"ok", false}, {"error", "signal not found"}};
  }

  main_window_->charts_widget->showChart(id, sig, true, merge);
  return {{"ok", true}, {"chart", QString("%1/%2").arg(id.toString(), sig->name)}};
}

QJsonObject CabanaHttpServer::handleSignalWrite(const HttpRequest &request, int *status) const {
  bool source_ok = false;
  bool address_ok = false;
  int source = request.json_body["source"].toInt(-1);
  source_ok = source >= 0;
  int address = request.json_body["address"].toInt(-1);
  address_ok = address >= 0;
  QString name = request.json_body["name"].toString();
  if (!source_ok || !address_ok || name.isEmpty()) {
    *status = 400;
    return {{"ok", false}, {"error", "source/address/name are required"}};
  }

  MessageId id = {.source = uint8_t(source), .address = uint32_t(address)};
  cabana::Signal sig;
  sig.name = name;
  sig.start_bit = request.json_body["start_bit"].toInt();
  sig.size = std::max(1, request.json_body["size"].toInt(1));
  sig.is_little_endian = request.json_body["is_little_endian"].toBool(true);
  sig.is_signed = request.json_body["is_signed"].toBool(false);
  sig.factor = request.json_body["factor"].toDouble(1.0);
  sig.offset = request.json_body["offset"].toDouble(0.0);
  sig.unit = request.json_body["unit"].toString();
  sig.min = request.json_body["min"].toDouble(0.0);
  sig.max = request.json_body["max"].toDouble(0.0);
  sig.update();

  if (auto *msg = dbc()->msg(id); msg && msg->sig(name)) {
    dbc()->updateSignal(id, name, sig);
    return {{"ok", true}, {"updated", true}};
  }

  dbc()->addSignal(id, sig);
  return {{"ok", true}, {"updated", false}};
}
