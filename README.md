# dcache-sse

This project provides a complete, stand-alone sample client for the
[dCache SSE Event
interface](https://www.dcache.org/manuals/UserGuide-8.2/frontend.shtml#storage-events).

This project exists for a few reasons.

First, it provides a relatively simple dCache SSE client with which
someone new to this feature can "get started" and play with the
interface.  It should be relatively easy to "demo" dCache's SSE
support.

Second, the source code provides concrete example of how an SSE client
might work.  This may help clarify points that are (inadvertently)
left ambiguous in the documentation.

Finally, it provides a starting point for more sophisticated or
domain-specific clients.  The code contains some structures to support
enhancements.  In addition, the code base is provided with a liberal
copyright to encourage reuse in other contexts.