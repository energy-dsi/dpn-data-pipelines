// Generic inline validation for any <form class="dpn-validate">: on submit,
// walks every .dpn-field inside the form and flags required-but-empty (or
// otherwise HTML5-invalid) controls with the Carbon-style error state
// (.dpn-field.has-error — see layout.css). Clears a field's error as soon
// as the visitor changes it. Server-rendered errors (e.g. a name collision
// found only after a round-trip) use the same .has-error markup directly
// from Jinja — this script only ever adds/removes that one class, so the
// two sources of truth never fight each other.
(function () {
  function errorTextEl(field) {
    return field.querySelector(".dpn-field__error-text");
  }

  function showError(field, message) {
    field.classList.add("has-error");
    var msgEl = errorTextEl(field);
    if (msgEl) msgEl.textContent = message;
  }

  function clearError(field) {
    field.classList.remove("has-error");
  }

  function primaryControl(field) {
    return field.querySelector("input[required], select[required], input[type='radio']");
  }

  function validateField(field) {
    var control = primaryControl(field);
    if (!control) return true;

    if (control.type === "radio") {
      var group = field.querySelectorAll("input[type='radio'][name='" + control.name + "']");
      var checked = Array.prototype.some.call(group, function (r) { return r.checked; });
      if (!checked) {
        showError(field, field.dataset.requiredMessage || "Select an option.");
        return false;
      }
      clearError(field);
      return true;
    }

    if (!control.value || !control.value.trim()) {
      showError(field, field.dataset.requiredMessage || "This field is required.");
      return false;
    }
    if (control.validity && !control.validity.valid) {
      showError(field, control.validationMessage);
      return false;
    }
    clearError(field);
    return true;
  }

  // Exposed as window.DPN.initFormValidation so a form injected into the
  // page later (see ui/static/js/inline-review.js's AJAX-swapped review
  // panel) can be wired up the same way as the ones present at page load,
  // instead of duplicating this logic.
  function initFormValidation(form) {
    if (form.dataset.validationWired) return;
    form.dataset.validationWired = "1";

    var fields = form.querySelectorAll(".dpn-field");

    fields.forEach(function (field) {
      field.addEventListener("input", function () { clearError(field); }, true);
      field.addEventListener("change", function () { clearError(field); }, true);
    });

    form.addEventListener("submit", function (evt) {
      var allValid = true;
      fields.forEach(function (field) {
        if (!validateField(field)) allValid = false;
      });
      if (!allValid) evt.preventDefault();
    });
  }

  document.querySelectorAll("form.dpn-validate").forEach(initFormValidation);

  window.DPN = window.DPN || {};
  window.DPN.initFormValidation = initFormValidation;
})();
