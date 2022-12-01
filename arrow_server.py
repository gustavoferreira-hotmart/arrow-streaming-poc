import os
import logging
import pyarrow

from flask import Flask, Response
from pandas import DataFrame

logging.basicConfig(level=logging.INFO)

logging.info('Generating test data...')
data = [
    {
        'numero': x,
        'texto': f"Lorem ipsum {x}"
    }
    for x in range(100)
]

df = DataFrame.from_dict(data)
batch = pyarrow.record_batch(df)

batches = 200
logging.info(f"Generating {batches} Arrow batches...")
sink = pyarrow.BufferOutputStream()
with pyarrow.ipc.new_stream(sink, batch.schema) as writer:
    for i in range(batches):
        writer.write_batch(batch)

buf = sink.getvalue()

logging.info('Done! The app is starting.')

# Flask app

app = Flask(__name__)


@app.route("/")
def streamed_proxy():
    return Response(buf.to_pybytes())


if __name__ == '__main__':
    port = int(os.environ.get('PORT', '3000'))
    app.run(port=port)
