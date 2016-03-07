# ReliableFileTransferUDP

Two versions:

Simple version uses a basic stop and wait reliable data transfer protocol.

Advanced version uses multiple cycling buffers and worker thread pools to optimise CPU usage. (Though the bottleneck is mostly the network)
