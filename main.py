from flask import Flask, Response
from datetime import datetime
import time

app = Flask(__name__)


# a generator with yield expression
def gen_date_time():
    while True:
        time.sleep(1)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # DO NOT forget the prefix and suffix
        yield 'data: %s\n\n' % now


@app.route('/sse_demo')
def sse_demo():
    return Response(
        gen_date_time(),  # gen_date_time() is an Iterable
        mimetype='text/event-stream'  # mark as a stream response
    )


HTML = '''<!DOCTYPE html>
<html>
<body>
    Server side clock: <span id="clock"></span>
    <script>
        var source = new EventSource("/sse_demo");
        source.onmessage = function (event) {
            document.getElementById("clock").innerHTML = event.data;
        };
    </script>
</body>
</html>'''


@app.route('/')
def index():
    return HTML


app.run()
