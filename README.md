# bulk-download

Technique 1 (Requires good core count):
`python multi_process_download.py`
~95 Mbps

Technique 2 (Requires good RAM):
`python async_io_download.py`
~500+ Mbps (Works pretty fast, ensure cache location is an ssd)
