// Vantage live refresh (issue #1982).
//
// The page marks its live root with data-live-stream. The script reads that
// SSE stream with fetch. On each frame, it fetches the page again and swaps
// each [data-live-region] by its id. The server stays the only renderer.
//
// fetch, not EventSource: fetch sends Last-Event-ID on the first connect, and
// it reads frames of every event name. When the stream fails, a timed fetch
// keeps the page current.
(function () {
  "use strict";

  var root = document.querySelector("[data-live-stream]");
  if (!root || !window.fetch || !window.DOMParser || !window.TextDecoder) {
    return;
  }

  var streamUrl = root.getAttribute("data-live-stream");
  var selfUrl = root.getAttribute("data-live-self") || window.location.href;
  var minGap = Number(root.getAttribute("data-live-gap-ms")) || 2000;
  var pollEvery = Number(root.getAttribute("data-live-poll-ms")) || 10000;
  var lastEventId = root.getAttribute("data-live-last-event-id");
  // These HTTP statuses do not change on a retry. The page polls instead.
  var noRetry = { 401: true, 403: true, 404: true, 405: true };

  var stopped = false;
  var ending = false;
  var pending = false;
  var inFlight = false;
  var lastRefresh = 0;
  var gapTimer = null;
  var pollTimer = null;
  var retryTimer = null;
  var backoff = 1000;
  var controller = null;

  function setStatus(text) {
    root.hidden = false;
    root.textContent = text;
  }

  // An operator edit marks its form. A marked or focused region is not
  // swapped, so a refresh never removes typed text.
  document.addEventListener(
    "input",
    function (event) {
      var form = event.target && event.target.form;
      if (form) {
        form.setAttribute("data-live-dirty", "");
      }
    },
    true
  );

  function busy(region) {
    var active = document.activeElement;
    if (
      active &&
      region.contains(active) &&
      /^(INPUT|TEXTAREA|SELECT|BUTTON)$/.test(active.tagName)
    ) {
      return true;
    }
    return region.querySelector("[data-live-dirty]") !== null;
  }

  // Returns true when a busy region kept its old content.
  function swap(doc) {
    var kept = false;
    var regions = document.querySelectorAll("[data-live-region]");
    for (var i = 0; i < regions.length; i++) {
      var region = regions[i];
      var fresh = region.id ? doc.getElementById(region.id) : null;
      if (!fresh) {
        continue;
      }
      if (busy(region)) {
        kept = true;
        continue;
      }
      var open = [];
      var details = region.querySelectorAll("details");
      for (var j = 0; j < details.length; j++) {
        if (details[j].open) {
          open.push(j);
        }
      }
      region.innerHTML = fresh.innerHTML;
      details = region.querySelectorAll("details");
      for (var k = 0; k < open.length; k++) {
        if (details[open[k]]) {
          details[open[k]].open = true;
        }
      }
    }
    // A page with no live root has nothing more to show, for example a
    // run that has ended.
    if (!doc.querySelector("[data-live-stream]")) {
      stop("Run ended. Live updates are off.");
    }
    return kept;
  }

  function refresh() {
    if (stopped) {
      return;
    }
    if (document.hidden) {
      pending = true;
      return;
    }
    var wait = lastRefresh + minGap - Date.now();
    if (inFlight || wait > 0) {
      pending = true;
      if (!gapTimer && !inFlight) {
        gapTimer = setTimeout(function () {
          gapTimer = null;
          if (pending) {
            pending = false;
            refresh();
          }
        }, Math.max(wait, 50));
      }
      return;
    }
    inFlight = true;
    lastRefresh = Date.now();
    fetch(selfUrl, {
      credentials: "same-origin",
      headers: { Accept: "text/html" },
    })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("page " + response.status);
        }
        return response.text();
      })
      .then(function (html) {
        var doc = new DOMParser().parseFromString(html, "text/html");
        if (swap(doc)) {
          setStatus("Live. Updates wait while you edit a form.");
        } else if (!stopped) {
          setStatus("Live. Updated " + new Date().toLocaleTimeString() + ".");
        }
      })
      .catch(function () {
        setStatus("Live update failed. The page tries again.");
      })
      .then(function () {
        inFlight = false;
        if (pending) {
          pending = false;
          refresh();
        }
      });
  }

  function startPolling() {
    if (!pollTimer && !stopped) {
      pollTimer = setInterval(refresh, pollEvery);
    }
  }

  function stopPolling() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  function stop(text) {
    stopped = true;
    stopPolling();
    if (retryTimer) {
      clearTimeout(retryTimer);
      retryTimer = null;
    }
    if (controller) {
      controller.abort();
    }
    setStatus(text);
  }

  function onFrame(frame) {
    var name = "message";
    var hasData = false;
    var lines = frame.split("\n");
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i];
      if (line.indexOf("event:") === 0) {
        name = line.slice(6).trim();
      } else if (line.indexOf("id:") === 0) {
        lastEventId = line.slice(3).trim();
      } else if (line.indexOf("data:") === 0) {
        hasData = true;
      }
    }
    // A frame with no data is a keep-alive comment.
    if (!hasData) {
      return;
    }
    if (name === "stream-end") {
      ending = true;
    }
    if (name === "stream-error" || name === "error") {
      throw new Error("stream " + name);
    }
    refresh();
  }

  function reconnectLater() {
    if (stopped || ending || retryTimer) {
      return;
    }
    retryTimer = setTimeout(function () {
      retryTimer = null;
      connect();
    }, backoff);
    backoff = Math.min(backoff * 2, 60000);
  }

  function connect() {
    if (stopped) {
      return;
    }
    var headers = { Accept: "text/event-stream" };
    if (lastEventId) {
      headers["Last-Event-ID"] = lastEventId;
    }
    controller = window.AbortController ? new AbortController() : null;
    var retry = true;
    fetch(streamUrl, {
      credentials: "same-origin",
      headers: headers,
      signal: controller ? controller.signal : undefined,
    })
      .then(function (response) {
        if (!response.ok || !response.body) {
          retry = !noRetry[response.status];
          throw new Error("stream " + response.status);
        }
        stopPolling();
        backoff = 1000;
        setStatus("Live.");
        var reader = response.body.getReader();
        var decoder = new TextDecoder();
        var buffer = "";
        function pump() {
          return reader.read().then(function (chunk) {
            if (chunk.done) {
              throw new Error("stream closed");
            }
            buffer += decoder.decode(chunk.value, { stream: true });
            buffer = buffer.replace(/\r\n?/g, "\n");
            var end = buffer.indexOf("\n\n");
            while (end >= 0) {
              onFrame(buffer.slice(0, end));
              buffer = buffer.slice(end + 2);
              end = buffer.indexOf("\n\n");
            }
            return pump();
          });
        }
        return pump();
      })
      .catch(function () {
        if (stopped) {
          return;
        }
        if (ending) {
          // The run ended. One last fetch shows the final state.
          refresh();
          return;
        }
        setStatus("Live stream is not available. The page checks for changes.");
        startPolling();
        if (retry) {
          reconnectLater();
        }
      });
  }

  document.addEventListener("visibilitychange", function () {
    if (!document.hidden && pending) {
      pending = false;
      refresh();
    }
  });

  connect();
})();
