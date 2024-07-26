import tornado.ioloop
import tornado.web
import tornado.websocket
import tornado.httpserver

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, this is an HTTP response.")

class WebSocketHandler(tornado.websocket.WebSocketHandler):
    def open(self):
        print("WebSocket opened")
        self.write_message("WebSocket connection established")

    def on_message(self, message):
        print(f"Received message: {message}")
        self.write_message(f"You said: {message}")

    def on_close(self):
        print("WebSocket closed")

    def check_origin(self, origin):
        return True  # Allow connections from any origin

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/websocket", WebSocketHandler),
    ])

if __name__ == "__main__":
    app = make_app()
    server = tornado.httpserver.HTTPServer(app)
    server.listen(8080)
    print("Server started on http://localhost:8080")
    tornado.ioloop.IOLoop.current().start()
