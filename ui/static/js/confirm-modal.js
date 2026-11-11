// Drives ui/templates/_confirm_modal.html's "are you sure?" dialog (see
// layout.css's "Confirm modal" section for the fade/scale-in animation):
// intercepts submit on every form.dpn-confirm-form, shows the dialog with
// that form's data-confirm-title/data-confirm-message, and only actually
// submits the form if the user clicks the dialog's own Delete button.
(function () {
  var overlay = document.getElementById("dpn-confirm-overlay");
  if (!overlay) return;

  var dialog = overlay.querySelector(".dpn-modal");
  var titleEl = document.getElementById("dpn-confirm-title");
  var messageEl = document.getElementById("dpn-confirm-message");
  var okBtn = document.getElementById("dpn-confirm-ok");
  var cancelBtn = document.getElementById("dpn-confirm-cancel");

  var pendingForm = null;

  function open(form) {
    pendingForm = form;
    titleEl.textContent = form.dataset.confirmTitle || "Delete permanently?";
    if (form.dataset.confirmMessage) {
      messageEl.textContent = form.dataset.confirmMessage;
      messageEl.hidden = false;
    } else {
      messageEl.hidden = true;
    }
    overlay.hidden = false;
    overlay.classList.remove("is-dismissing");
    okBtn.focus();
  }

  // button-loading.js's own submit listener on this same form runs before
  // this one (registered first, in base.html), so it's already put the
  // clicked button into its .is-loading/disabled state by the time we get
  // here and preventDefault() the submit. Undo that now — otherwise
  // Cancel leaves the button spinning and unclickable.
  function resetLoadingState(form) {
    var btn = form.querySelector(".dpn-btn.is-loading");
    if (!btn) return;
    btn.classList.remove("is-loading");
    btn.disabled = false;
  }

  function close() {
    if (overlay.hidden) return;
    overlay.classList.add("is-dismissing");
    dialog.addEventListener("animationend", function () {
      overlay.hidden = true;
      overlay.classList.remove("is-dismissing");
    }, { once: true });
    pendingForm = null;
  }

  document.querySelectorAll("form.dpn-confirm-form").forEach(function (form) {
    form.addEventListener("submit", function (event) {
      if (form.dataset.confirmed === "true") return;
      event.preventDefault();
      resetLoadingState(form);
      open(form);
    });
  });

  okBtn.addEventListener("click", function () {
    var form = pendingForm;
    close();
    if (form) {
      form.dataset.confirmed = "true";
      // requestSubmit() (not submit()) so the submit event actually fires
      // this time — button-loading.js's listener needs it to add the
      // Delete button's spinner for the real, in-flight request.
      form.requestSubmit();
    }
  });

  cancelBtn.addEventListener("click", close);
  overlay.addEventListener("click", function (event) {
    if (event.target === overlay) close();
  });
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") close();
  });
})();
