// Drives ui/templates/_toast.html's success flash message (see
// layout.css's "Success toast" section for the slide/fade-in animation):
// auto-dismisses it after a few seconds, lets the close button dismiss it
// early, and — either way — strips the ?created=... query param
// index()/confirm_data_product() (ui/app.py) used to trigger it, so a
// refresh of "/producer-pipelines" doesn't show the same toast again.
(function () {
  var AUTO_DISMISS_MS = 6000;

  var toast = document.getElementById("dpn-toast");
  if (!toast) return;

  function stripToastParam() {
    var url = new URL(window.location.href);
    if (!url.searchParams.has("created")) return;
    url.searchParams.delete("created");
    window.history.replaceState({}, "", url.pathname + url.search + url.hash);
  }

  function dismiss() {
    if (toast.classList.contains("is-dismissing")) return;
    toast.classList.add("is-dismissing");
    toast.addEventListener("animationend", function () { toast.remove(); }, { once: true });
  }

  var closeBtn = document.getElementById("dpn-toast-close");
  if (closeBtn) closeBtn.addEventListener("click", dismiss);

  setTimeout(dismiss, AUTO_DISMISS_MS);
  stripToastParam();
})();
