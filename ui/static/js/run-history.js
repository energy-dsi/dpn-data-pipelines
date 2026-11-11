// AJAX-ifies /run-history's three filters — "Currently viewing" (role),
// "Displaying results for" (limit), and the Product column's per-row link —
// so clicking any of them swaps #dpn-history-results in place instead of a
// full page reload, and shows .dpn-history-loader (see layout.css) while
// the fetch is in flight. Browser history still works: every click
// pushState()s the clicked URL, and back/forward re-fetches via popstate,
// so the address bar and reload both still reflect exactly what's on
// screen — nothing here is only client-side state.
//
// Server side, ui/app.py's history() route returns the same
// _run_history_results.html fragment this swaps in (tagged via the
// X-DPN-History-Request header below) for both the first full-page load
// and every one of these fetches, so there is only one place that ever
// renders this markup.
(function () {
  var ROOT_ID = "dpn-history-results";
  var CONTENT_ID = "dpn-history-results-content";

  function root() {
    return document.getElementById(ROOT_ID);
  }

  // The fetched fragment replaces just this inner div's innerHTML — never
  // #dpn-history-results itself, which also holds .dpn-history-loader
  // (see history.html): overwriting the outer element would wipe that
  // loader out along with everything else on the very first swap, leaving
  // nothing to show on every click after that.
  function content() {
    return document.getElementById(CONTENT_ID);
  }

  // Only intercept links that stay on this same page's filter state (role/
  // limit/product_type query params on /run-history itself) — not the
  // sidenav's "Run History" tab or anything else that happens to live
  // outside the swapped container.
  function isHistoryFilterLink(scope, anchor) {
    if (!anchor || anchor.tagName !== "A" || !scope.contains(anchor)) return false;
    var url;
    try {
      url = new URL(anchor.href, window.location.origin);
    } catch (e) {
      return false;
    }
    return url.pathname === "/run-history";
  }

  // Guards against a second click landing before the first one's response
  // comes back: without this, a slower response for an earlier click could
  // resolve after a faster one for a later click and overwrite its result —
  // e.g. click Producer, then Consumer, and if Producer's fetch is slower
  // it finishes last and leaves Consumer's click showing Producer's data
  // with "Currently viewing"/"Displaying results for" both stuck on
  // whatever the earlier click asked for. Each load() bumps this and only
  // the call that still holds the latest id is allowed to touch the DOM.
  var latestRequestId = 0;

  function load(url, push) {
    var rootEl = root();
    var contentEl = content();
    if (!rootEl || !contentEl) return;
    var requestId = ++latestRequestId;
    rootEl.classList.add("is-loading");

    fetch(url, { headers: { "X-Requested-With": "fetch", "X-DPN-History-Request": "1" } })
      .then(function (response) {
        if (!response.ok) throw new Error("run-history fetch failed: " + response.status);
        return response.text();
      })
      .then(function (html) {
        if (requestId !== latestRequestId) return; // superseded by a later click
        contentEl.innerHTML = html;
        if (push) window.history.pushState({ dpnHistoryUrl: url }, "", url);
        if (window.DPN && window.DPN.initTablePagination) window.DPN.initTablePagination(contentEl);
      })
      .catch(function () {
        if (requestId !== latestRequestId) return;
        // Fail soft: land the user where the link actually pointed rather
        // than leaving the view stuck mid-fetch with a spinner forever.
        window.location.href = url;
      })
      .then(function () {
        if (requestId === latestRequestId) rootEl.classList.remove("is-loading");
      });
  }

  document.addEventListener("click", function (evt) {
    var contentEl = content();
    if (!contentEl) return;
    var anchor = evt.target.closest ? evt.target.closest("a") : null;
    if (!isHistoryFilterLink(contentEl, anchor)) return;
    evt.preventDefault();
    load(anchor.href, true);
  });

  // Covers browser Back/Forward across the pushState() entries above —
  // including back past the very first one, which lands on the plain
  // GET /run-history URL the page originally loaded from.
  window.addEventListener("popstate", function () {
    if (window.location.pathname === "/run-history" && content()) {
      load(window.location.href, false);
    }
  });
})();
