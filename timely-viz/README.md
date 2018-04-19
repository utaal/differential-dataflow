## Show the dataflow graph as a graphviz diagram

Run with:

```
cargo run --release -- {number of workers in the source computation} {listening address} | dot -Tpdf > graph.pdf
```

For example:

```
cargo run --release -- 4 127.0.0.1 | dot -Tpdf > graph.pdf
```
