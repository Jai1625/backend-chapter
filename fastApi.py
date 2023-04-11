# from flask import Flask, Response
from datetime import datetime
import time

from fastapi import FastAPI, Response

app = FastAPI()


# a generator with yield expression
def gen_date_time():
    while True:
        time.sleep(1)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # DO NOT forget the prefix and suffix
        yield 'data: %s\n\n' % now


@app.get('/sse_demo')
def sse_demo():
    return Response(
        gen_date_time()
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


@app.get('/')
def index():
    return HTML


app.run()
