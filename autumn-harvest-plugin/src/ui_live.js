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
  // A live stream can stop without an error, for example on a half-open TCP
  // connection. A slow timed fetch covers that case.
  var safetyEvery = 60000;
  // These HTTP statuses do not change on a retry. The page polls instead.
  var noRetry = { 401: true, 403: true, 404: true, 405: true };

  var stopped = false;
  var ending = false;
  var pending = false;
  var inFlight = false;
  var lastRefresh = 0;
  var gapTimer = null;
  var pollTimer = null;
  var pollMs = 0;
  var retryTimer = null;
  var keptTimer = null;
  var keptDoc = null;
  var backoff = 1000;
  var controller = null;
  var connected = false;
  var connectedOnce = false;
  // The last HTML that the script put in each region, by id.
  var applied = {};

  var regions = document.querySelectorAll("[data-live-region]");
  for (var r = 0; r < regions.length; r++) {
    applied[regions[r].id] = regions[r].innerHTML;
  }

  function setStatus(text) {
    root.hidden = false;
    root.textContent = text;
  }

  // A form with a changed field holds operator input.
  function edited(region) {
    var fields = region.querySelectorAll("input, textarea, select");
    for (var i = 0; i < fields.length; i++) {
      var field = fields[i];
      if (field.type === "checkbox" || field.type === "radio") {
        if (field.checked !== field.defaultChecked) {
          return true;
        }
      } else if (field.tagName === "SELECT") {
        for (var j = 0; j < field.options.length; j++) {
          if (field.options[j].selected !== field.options[j].defaultSelected) {
            return true;
          }
        }
      } else if (field.type !== "hidden" && field.value !== field.defaultValue) {
        return true;
      }
    }
    return false;
  }

  // A region with focus or with operator input keeps its content, so a
  // refresh never moves the focus or removes typed text.
  function busy(region) {
    var active = document.activeElement;
    if (active && active !== document.body && region.contains(active)) {
      return true;
    }
    return edited(region);
  }

  // Returns "swapped", "kept" (a busy region kept old content) or "foreign"
  // (the document is not this page, for example a login page).
  function swap(doc) {
    var found = 0;
    var kept = false;
    var current = document.querySelectorAll("[data-live-region]");
    for (var i = 0; i < current.length; i++) {
      var region = current[i];
      var fresh = region.id ? doc.getElementById(region.id) : null;
      if (!fresh) {
        continue;
      }
      found++;
      var html = fresh.innerHTML;
      if (applied[region.id] === html) {
        continue;
      }
      if (busy(region)) {
        kept = true;
        continue;
      }
      var open = [];
      var details = region.querySelectorAll("details");
      var before = details.length;
      for (var j = 0; j < details.length; j++) {
        if (details[j].open) {
          open.push(j);
        }
      }
      region.innerHTML = html;
      applied[region.id] = html;
      details = region.querySelectorAll("details");
      // An index names the same panel only when the count is the same.
      if (details.length === before) {
        for (var k = 0; k < open.length; k++) {
          if (details[open[k]]) {
            details[open[k]].open = true;
          }
        }
      }
    }
    if (found === 0) {
      return "foreign";
    }
    var freshRoot = doc.querySelector("[data-live-stream]");
    if (!freshRoot) {
      // The page has nothing more to show, for example a run that ended.
      stop("Live updates are off. The run ended.");
    } else if (freshRoot.getAttribute("data-live-last-event-id")) {
      // The fetched page is current, so the stream can resume after it.
      lastEventId = freshRoot.getAttribute("data-live-last-event-id");
    }
    return kept ? "kept" : "swapped";
  }

  // Tries a kept document again until each region takes it.
  function retryKept() {
    keptTimer = null;
    if (!keptDoc || stopped) {
      return;
    }
    if (swap(keptDoc) === "kept") {
      keptTimer = setTimeout(retryKept, minGap);
    } else {
      keptDoc = null;
    }
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
        var type = response.headers.get("content-type") || "";
        if (!response.ok || response.redirected || type.indexOf("text/html") !== 0) {
          throw new Error("page " + response.status);
        }
        return response.text();
      })
      .then(function (html) {
        var doc = new DOMParser().parseFromString(html, "text/html");
        var result = swap(doc);
        if (result === "foreign") {
          throw new Error("not this page");
        }
        if (stopped) {
          return;
        }
        if (result === "kept") {
          keptDoc = doc;
          if (!keptTimer) {
            keptTimer = setTimeout(retryKept, minGap);
          }
          setStatus("Live. Part of the page waits while you use it.");
        } else {
          keptDoc = null;
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

  function startPolling(every) {
    if (stopped || (pollTimer && pollMs === every)) {
      return;
    }
    stopPolling();
    pollMs = every;
    pollTimer = setInterval(refresh, every);
  }

  function stopPolling() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  function disconnect() {
    connected = false;
    if (controller) {
      controller.abort();
      controller = null;
    }
  }

  function stop(text) {
    stopped = true;
    stopPolling();
    if (retryTimer) {
      clearTimeout(retryTimer);
      retryTimer = null;
    }
    disconnect();
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
    if (name === "stream-error" || name === "error") {
      throw new Error("stream " + name);
    }
    // A real frame proves that the stream works.
    backoff = 1000;
    if (name === "stream-end") {
      ending = true;
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
    if (stopped || connected || document.hidden) {
      return;
    }
    var headers = { Accept: "text/event-stream" };
    if (lastEventId) {
      headers["Last-Event-ID"] = lastEventId;
    }
    var own = window.AbortController ? new AbortController() : null;
    controller = own;
    connected = true;
    var retry = true;
    fetch(streamUrl, {
      credentials: "same-origin",
      headers: headers,
      signal: own ? own.signal : undefined,
    })
      .then(function (response) {
        var type = response.headers.get("content-type") || "";
        if (!response.ok || !response.body || type.indexOf("text/event-stream") !== 0) {
          retry = !noRetry[response.status];
          throw new Error("stream " + response.status);
        }
        startPolling(safetyEvery);
        setStatus("Live.");
        // A page with no resume cursor, or a reconnect, can miss a change
        // from before the stream opened. One fetch covers that time.
        if (connectedOnce || !lastEventId) {
          refresh();
        }
        connectedOnce = true;
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
        if (own) {
          own.abort();
        }
        if (controller !== own) {
          // A newer connection, or a hidden tab, replaced this one.
          return;
        }
        connected = false;
        controller = null;
        if (stopped) {
          return;
        }
        // The page polls until the stream is back. After the run ends, the
        // polls fetch the final state.
        startPolling(pollEvery);
        if (ending) {
          refresh();
          return;
        }
        setStatus("Live stream is not available. The page checks for changes.");
        if (retry) {
          reconnectLater();
        }
      });
  }

  // A hidden tab holds no stream, so it holds no database connection.
  document.addEventListener("visibilitychange", function () {
    if (stopped) {
      return;
    }
    if (document.hidden) {
      disconnect();
      if (retryTimer) {
        clearTimeout(retryTimer);
        retryTimer = null;
      }
      return;
    }
    if (ending) {
      refresh();
      return;
    }
    // A successful connect fetches the page once.
    connect();
    if (pending) {
      pending = false;
      refresh();
    }
  });

  connect();
})();
