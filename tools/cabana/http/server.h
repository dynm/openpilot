#pragma once

#include <QObject>
#include <QJsonObject>
#include <QTcpServer>
#include <QTcpSocket>
#include <QUrlQuery>
#include <QUrl>

class MainWindow;

class CabanaHttpServer : public QObject {
  Q_OBJECT

public:
  explicit CabanaHttpServer(MainWindow *main_window, QObject *parent = nullptr);

private:
  struct HttpRequest {
    QString method;
    QString path;
    QUrlQuery query;
    QJsonObject json_body;
  };

  MainWindow *main_window_ = nullptr;
  QTcpServer server_;

  void handleConnection();
  void handleSocketData(QTcpSocket *socket);
  bool parseRequest(const QByteArray &raw, HttpRequest *request) const;
  void sendJson(QTcpSocket *socket, int status, const QJsonObject &body) const;

  QJsonObject handleRequest(const HttpRequest &request, int *status) const;
  QJsonObject handleListMessages(uint8_t source) const;
  QJsonObject handleSignalValue(const HttpRequest &request, int *status) const;
  QJsonObject handleChartShow(const HttpRequest &request, int *status) const;
  QJsonObject handleSignalWrite(const HttpRequest &request, int *status) const;
};
