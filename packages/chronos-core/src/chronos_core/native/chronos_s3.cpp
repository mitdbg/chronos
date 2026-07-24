#include "chronos_s3.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <pybind11/stl.h>

namespace chronos::native {
namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

class ChronosLakeServer {
  public:
    ChronosLakeServer(std::string host, std::uint16_t port)
        : host_(std::move(host)), port_(port) {}

    ~ChronosLakeServer() { stop(); }

    void start() {
        if (running_.exchange(true)) {
            return;
        }
        thread_ = std::thread([this]() { serve(); });
        for (int attempt = 0; attempt < 100 && !ready_.load(); ++attempt) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (!ready_.load()) {
            stop();
            throw std::runtime_error("Chronos lake server failed to start");
        }
    }

    void stop() {
        if (!running_.exchange(false)) {
            return;
        }
        if (ready_.load()) {
            asio::io_context wake_context;
            tcp::socket wake_socket(wake_context);
            boost::system::error_code ignored;
            wake_socket.connect(
                tcp::endpoint(asio::ip::make_address(host_), port_),
                ignored
            );
            wake_socket.shutdown(tcp::socket::shutdown_both, ignored);
            wake_socket.close(ignored);
        }
        if (thread_.joinable()) {
            thread_.join();
        }
        ready_.store(false);
    }

    bool running() const { return running_.load(); }
    std::uint16_t port() const { return port_; }

  private:
    void serve() {
        try {
            asio::io_context context;
            auto address = asio::ip::make_address(host_);
            acceptor_ = std::make_unique<tcp::acceptor>(context, tcp::endpoint(address, port_));
            port_ = acceptor_->local_endpoint().port();
            ready_.store(true);

            while (running_.load()) {
                tcp::socket socket(context);
                boost::system::error_code error;
                acceptor_->accept(socket, error);
                if (error) {
                    if (running_.load()) {
                        last_error_ = error.message();
                    }
                    continue;
                }
                if (!running_.load()) {
                    break;
                }
                handle(std::move(socket));
            }
            boost::system::error_code ignored;
            acceptor_->close(ignored);
        } catch (const std::exception &error) {
            last_error_ = error.what();
            ready_.store(false);
            running_.store(false);
        }
    }

    static void handle(tcp::socket socket) {
        beast::flat_buffer buffer;
        http::request<http::string_body> request;
        boost::system::error_code error;
        http::read(socket, buffer, request, error);
        if (error) {
            return;
        }

        http::response<http::string_body> response;
        response.version(request.version());
        response.keep_alive(false);
        response.set(http::field::server, "chronos-lake");
        if (request.method() == http::verb::get && request.target() == "/healthz") {
            response.result(http::status::ok);
            response.set(http::field::content_type, "application/json");
            response.body() = R"({"status":"ok"})";
        } else {
            response.result(http::status::not_found);
            response.set(http::field::content_type, "application/json");
            response.body() = R"({"error":"not found"})";
        }
        response.prepare_payload();
        http::write(socket, response, error);
        socket.shutdown(tcp::socket::shutdown_send, error);
    }

    std::string host_;
    std::uint16_t port_;
    std::atomic<bool> running_{false};
    std::atomic<bool> ready_{false};
    std::unique_ptr<tcp::acceptor> acceptor_;
    std::thread thread_;
    std::string last_error_;
};

} // namespace

void bind_chronos_s3(pybind11::module_ &m) {
    pybind11::class_<ChronosLakeServer>(m, "NativeChronosLakeServer")
        .def(pybind11::init<std::string, std::uint16_t>(),
             pybind11::arg("host") = "127.0.0.1",
             pybind11::arg("port") = 0)
        .def("start", &ChronosLakeServer::start, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("stop", &ChronosLakeServer::stop, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def_property_readonly("running", &ChronosLakeServer::running)
        .def_property_readonly("port", &ChronosLakeServer::port);
}

} // namespace chronos::native
