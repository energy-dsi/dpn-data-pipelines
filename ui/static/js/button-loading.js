// Loading state for every .dpn-btn that submits a form: wraps each button's
// existing content in one .dpn-btn__label span (so the label can be hidden
// without collapsing the button's width), then on submit adds
// `.is-loading` + `disabled` to whichever button triggered it (see
// layout.css's "Button loading state" section for the paired CSS).
//
// Loaded after form-validation.js in base.html on purpose: both scripts
// attach their submit listener directly to the same <form> elements, so
// this one always runs second and can check `evt.defaultPrevented` to skip
// the loading state entirely when client-side validation (or an inline
// onsubmit confirm() cancel) already blocked the submit.
(function () {
  function wrapLabel(btn) {
    if (btn.querySelector(".dpn-btn__label")) return;
    var label = document.createElement("span");
    label.className = "dpn-btn__label";
    while (btn.firstChild) label.appendChild(btn.firstChild);
    btn.appendChild(label);
  }

  // Exposed as window.DPN.initButtonLoading(root) so ui/static/js/inline-
  // review.js can wire up buttons/forms injected into the page after load
  // (the AJAX-swapped review panel) the same way as this file's own
  // page-load pass below, instead of duplicating this logic. `root`
  // defaults to the whole document; pass a specific container to avoid
  // re-wiring forms/buttons outside it.
  function initButtonLoading(root) {
    root = root || document;

    root.querySelectorAll(".dpn-btn").forEach(wrapLabel);

    root.querySelectorAll("form").forEach(function (form) {
      if (form.dataset.loadingWired) return;
      form.dataset.loadingWired = "1";

      form.addEventListener("submit", function (evt) {
        if (evt.defaultPrevented) return;

        var submitter = evt.submitter;
        if (!submitter) {
          submitter = form.querySelector("button[type='submit']:not([disabled])");
        }
        if (!submitter || !submitter.classList.contains("dpn-btn")) return;

        submitter.classList.add("is-loading");
        submitter.disabled = true;
      });
    });
  }

  initButtonLoading(document);

  window.DPN = window.DPN || {};
  window.DPN.initButtonLoading = initButtonLoading;
})();
