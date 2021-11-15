# Disk Buffer
Imagine you have a very slow sink and a fast source, and you are billed per hourly usage of your source. Disk buffering is a reliable and simple way to make your process cheaper and slimmer. Could you do it by separating the job in two steps? yes. Could you increase parallelism on the sink? yes. Do I want to program in rust? yes.

## Features
- Work correctly DONE
    - yield None on no remanining records
    - correct ordering
    - Accept any rust serde type
- Save to memory buffer before flushing to disk
    - DONE on the read size
- Tests
- futures::streams interop
- Configurable buffer sizes
- Rolling files to avoid leaking storage DONE
- Save in a fast format(avro, protobuf, thrift)
    - How to delimit records?
    - Use a custom reader?
- Read and write in batches (ex: write half of the input memory buffer whenever it is full, read half of the output buffer whenever it is half-full)
