// Keeps the whole New Data Product flow on a single page: submitting Step
// 1's form (action="/data-products/preview") normally navigates the
// browser to a full /data-products/preview page. Here it instead POSTs in
// the background and swaps the result into #review-panel (see new.html) —
// the right-hand column stays on /create-new-data-product the whole time, matching the Figma
// two-column layout (both steps visible together) instead of the old
// page-per-step flow. Step 2's own form (action="/data-products/confirm",
// injected into #review-panel by this script) is wired up the same way
// once it exists.
//
// ui/app.py recognises the X-DPN-Panel-Request header this script sends
// and, in response, either renders _review_panel.html alone (no base.html
// chrome — see ui/templates/_review_panel.html) or leaves the response as
// the ordinary full new.html page, telling us which one via the
// X-DPN-Panel response header:
//   "review" — Step 1 succeeded (or Step 2 was resubmitted and re-shown
//              with a collision error) — the body is the panel fragment,
//              swapped straight into #review-panel.
//   "form"   — Step 1 itself was rejected (bad product name, a duplicate
//              caught only at confirm time) — the body is the *full*
//              new.html page; we don't inject that whole document, we lift
//              its error banner/field states out and merge them onto the
//              form already on screen.
//
// Loaded after form-validation.js/button-loading.js (see base.html /
// new.html's {% block scripts %}) so window.DPN.initFormValidation /
// window.DPN.initButtonLoading are already defined.
(function () {
  var PANEL_ID = "review-panel";

  function clearLoading(form) {
    var btn = form.querySelector(".dpn-btn.is-loading");
    if (btn) {
      btn.classList.remove("is-loading");
      btn.disabled = false;
    }
  }

  function activateStepTwo() {
    document.querySelectorAll('[data-step="2"]').forEach(function (el) {
      el.classList.add("is-active");
    });
  }

  function wireInjectedForm(root) {
    var form = root.querySelector("form.dpn-validate");
    // initButtonLoading must be wired before wireAjaxForm below: both
    // attach their own "submit" listener to the same form, and
    // button-loading.js's bails out early on `evt.defaultPrevented`
    // (see its own comment) so it only shows the spinner if it's
    // registered — and therefore runs — first. This mirrors base.html's
    // own script order (button-loading.js loads before inline-review.js),
    // which is what makes Step 1's "Next: Review Names" spinner work; get
    // this backwards here and every button in the injected review panel
    // (e.g. "Create & Launch") silently skips the loading state.
    if (window.DPN && window.DPN.initButtonLoading) window.DPN.initButtonLoading(root);
    if (form && window.DPN) {
      if (window.DPN.initFormValidation) window.DPN.initFormValidation(form);
      wireAjaxForm(form);
    }
    // Kicks off the review step's live per-field name checks immediately
    // (see ui/static/js/review-live-check.js) instead of only finding out
    // about a collision after "Create & Launch" is clicked.
    if (window.DPN && window.DPN.initReviewLiveCheck) window.DPN.initReviewLiveCheck(root);
  }

  // Whenever Step 2 renders successfully, Step 1 itself must have been
  // accepted — clear any error state left over from an earlier rejected
  // Step 1 submission (e.g. the user fixed an invalid product name and
  // resubmitted) so it doesn't linger next to the now-valid form.
  function clearStep1Errors() {
    var step1Form = document.querySelector('form[action="/data-products/preview"]');
    if (!step1Form) return;
    var column = step1Form.closest(".dpn-step-column");
    var alert = column && column.querySelector(".dpn-alert--error");
    if (alert) alert.remove();
    step1Form.querySelectorAll(".dpn-field.has-error").forEach(function (field) {
      field.classList.remove("has-error");
    });
  }

  // Step 1's own "Next: Review Names" button is hidden only once Step 2's
  // panel has actually rendered (see injectReviewPanel below) — Step 2 is
  // meant to be the only next action once a preview is on screen. (A
  // duplicate product_type never gets this far any more — it's caught
  // back on Step 1's own field, see ui/static/js/review-live-check.js's
  // initProductNameCheck(), which hides this same button itself while the
  // name is unavailable.) Up until injection the button keeps its normal
  // button-loading.js spinner (see wireAjaxForm's submit handler), so the
  // user sees the request in flight instead of the button just vanishing.
  // It only ever comes back if the user changes something in Step 1
  // (wireStep1ReshowOnEdit below) — never automatically just because a new
  // response came back — so there's always exactly one enabled "next"
  // action on screen at a time.
  function hideStep1SubmitButton() {
    var step1Form = document.querySelector('form[action="/data-products/preview"]');
    var btn = step1Form && step1Form.querySelector(".dpn-step-actions button[type='submit']");
    if (btn) btn.hidden = true;
  }

  function showStep1SubmitButton() {
    var step1Form = document.querySelector('form[action="/data-products/preview"]');
    var btn = step1Form && step1Form.querySelector(".dpn-step-actions button[type='submit']");
    if (btn) btn.hidden = false;
  }

  // Wired once on Step 1's form: any edit there (product name, pathway,
  // execution, schedule) brings its "Next: Review Names" button back so
  // the user can regenerate the preview — this is the only thing that
  // un-hides it once a submit has hidden it.
  function wireStep1ReshowOnEdit(step1Form) {
    if (step1Form.dataset.reshowWired) return;
    step1Form.dataset.reshowWired = "1";
    step1Form.addEventListener("input", showStep1SubmitButton);
    step1Form.addEventListener("change", showStep1SubmitButton);
  }

  // Step 1 succeeded (or Step 2 was resubmitted): replace #review-panel's
  // placeholder/previous content with the freshly rendered review fields.
  function injectReviewPanel(html) {
    var panel = document.getElementById(PANEL_ID);
    if (!panel) return;
    var doc = new DOMParser().parseFromString(html, "text/html");
    panel.className = "dpn-step-column";
    panel.innerHTML = doc.body.innerHTML;
    wireInjectedForm(panel);
    activateStepTwo();
    clearStep1Errors();
    // Only now — once the panel is actually showing — does "Next: Review
    // Names" disappear (see hideStep1SubmitButton's own comment above).
    hideStep1SubmitButton();
    panel.scrollIntoView({ behavior: "smooth", block: "nearest", inline: "nearest" });
  }

  // Step 1 (or the confirm step's duplicate-product race) was rejected —
  // the response is the ordinary full new.html page. Lift its page-level
  // alert and per-field error state out and merge them onto the form
  // that's already on screen, instead of throwing that whole document at
  // innerHTML.
  function mergeFormErrors(form, html) {
    var doc = new DOMParser().parseFromString(html, "text/html");
    var column = form.closest(".dpn-step-column");

    var newAlert = doc.querySelector(".dpn-alert--error");
    var existingAlert = column.querySelector(".dpn-alert--error");
    if (newAlert) {
      if (existingAlert) {
        existingAlert.textContent = newAlert.textContent;
      } else {
        existingAlert = document.createElement("div");
        existingAlert.className = "dpn-alert--error";
        existingAlert.textContent = newAlert.textContent;
        column.insertBefore(existingAlert, column.querySelector(".dpn-info-box"));
      }
    } else if (existingAlert) {
      existingAlert.remove();
    }

    form.querySelectorAll(".dpn-field[data-field]").forEach(function (field) {
      var name = field.getAttribute("data-field");
      var newField = doc.querySelector('.dpn-field[data-field="' + name + '"]');
      var hasError = !!(newField && newField.classList.contains("has-error"));
      field.classList.toggle("has-error", hasError);
      if (hasError) {
        var newMsg = newField.querySelector(".dpn-field__error-text");
        var msgEl = field.querySelector(".dpn-field__error-text");
        if (msgEl) msgEl.textContent = newMsg ? newMsg.textContent : "";
      }
    });

    // A rejected product_type (e.g. the confirm-time duplicate race —
    // see ui/app.py's confirm_data_product()) is the one case that can
    // reach here with "Next: Review Names" already hidden by the live
    // check (see review-live-check.js's initProductNameCheck) based on a
    // now-stale "available" result. The server always renders this
    // button's `hidden` state correctly for the error it just produced, so
    // copy it over rather than leaving the stale client-side state up.
    var newSubmitBtn = doc.querySelector(".dpn-step-actions button[type='submit']");
    var submitBtn = form.querySelector(".dpn-step-actions button[type='submit']");
    if (submitBtn && newSubmitBtn) submitBtn.hidden = newSubmitBtn.hidden;
  }

  function handleAjaxResult(form, response, html) {
    clearLoading(form);
    if (response.headers.get("X-DPN-Panel") === "review") {
      injectReviewPanel(html);
    } else {
      mergeFormErrors(form, html);
    }
  }

  function wireAjaxForm(form) {
    if (form.dataset.ajaxWired) return;
    form.dataset.ajaxWired = "1";

    form.addEventListener("submit", function (evt) {
      // A prior listener (form-validation.js) already blocked this submit
      // client-side — nothing to send.
      if (evt.defaultPrevented) return;
      evt.preventDefault();

      fetch(form.action, {
        method: "POST",
        body: new FormData(form),
        headers: { "X-DPN-Panel-Request": "1" },
      })
        .then(function (response) {
          // Only /data-products/confirm ever redirects (on success, to
          // "/producer-pipelines") — follow it for real instead of trying
          // to inject a redirect response's body.
          if (response.redirected) {
            window.location.href = response.url;
            return null;
          }
          return response.text().then(function (html) {
            return { response: response, html: html };
          });
        })
        .then(function (result) {
          if (result) handleAjaxResult(form, result.response, result.html);
        })
        .catch(function () {
          // Network/parse failure — fall back to a real submit so the
          // user isn't stuck on a dead button; nothing was lost since the
          // AJAX attempt never got a response to act on.
          delete form.dataset.ajaxWired;
          form.submit();
        });
    });
  }

  var step1Form = document.querySelector('form[action="/data-products/preview"]');
  if (step1Form) {
    wireAjaxForm(step1Form);
    wireStep1ReshowOnEdit(step1Form);
  }
})();
