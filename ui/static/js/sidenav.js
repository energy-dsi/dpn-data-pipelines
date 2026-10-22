// Collapsible side navigation: animates the sidenav between its full width
// and an icon-only rail (see .dpn-sidenav / .is-collapsed in layout.css),
// and remembers the choice per-browser across page loads/reloads.
(function () {
  var STORAGE_KEY = "dpn-sidenav-collapsed";
  var sidenav = document.getElementById("dpn-sidenav");
  var toggle = document.getElementById("dpn-sidenav-toggle");
  var label = document.getElementById("dpn-sidenav-toggle-label");
  if (!sidenav || !toggle) return;

  function applyState(collapsed) {
    sidenav.classList.toggle("is-collapsed", collapsed);
    toggle.setAttribute("aria-expanded", String(!collapsed));
    if (label) label.textContent = collapsed ? "Expand side panel" : "Collapse side panel";
  }

  var stored = null;
  try { stored = window.localStorage.getItem(STORAGE_KEY); } catch (err) { /* storage disabled */ }
  applyState(stored === "true");

  toggle.addEventListener("click", function () {
    var collapsed = !sidenav.classList.contains("is-collapsed");
    applyState(collapsed);
    try { window.localStorage.setItem(STORAGE_KEY, String(collapsed)); } catch (err) { /* storage disabled */ }
  });
})();
