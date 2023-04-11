from flask import Flask, Response
import time

app = Flask(__name__)


# Example SSE endpoint
@app.route('/stream')
def stream():
    def event_stream():
        while True:
            # Generate some data to send to the client
            data = "data: {}\n\n".format(time.time())
            yield data

            # Wait for some time before sending the next event
            time.sleep(1)

    # Set the response headers to indicate that this is an SSE endpoint
    return Response(event_stream(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Access-Control-Allow-Origin": "*",
    })


if __name__ == '__main__':
    app.run(debug=True, port=5002)
