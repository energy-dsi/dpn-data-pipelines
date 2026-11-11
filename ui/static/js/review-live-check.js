// Live per-field validation for the review step (Source Container, Target
// Container, Target Notification Topic / Source Topic, Target Topic): as
// soon as the review panel is on screen, and again (debounced) on every
// edit, each field's current value is checked two ways —
//   1. against its own siblings on this same pathway (findSiblingDuplicate
//      below): none of these names may equal another one, even if neither
//      exists on the platform yet.
//   2. against the platform's real Kafka broker/storage account, via POST
//      /data-products/check-name.
// — instead of only finding out about either kind of collision after
// "Create & Launch" is clicked. See ui/app.py's check_name_available() for
// the endpoint and layout.css's "Review step's live... indicator" section
// for the three visual states this drives: is-verifying (request in
// flight), has-error (duplicate — reuses the same class/markup contract
// the dpn-form-validation skill already uses), is-available (checked,
// free).
//
// Neither check here is itself the final word — /data-products/confirm
// always re-runs the authoritative _find_collision() (sibling duplicates
// via _find_duplicate_field_values(), then platform existence via
// _find_name_collision()) server-side before it creates anything, so
// nothing here needs to be trusted.
//
// Exposed as window.DPN.initReviewLiveCheck(root) so
// ui/static/js/inline-review.js can wire up the review form injected into
// #review-panel the same way this file wires up review.html's own copy at
// load, instead of duplicating this logic.
(function () {
  var DEBOUNCE_MS = 500;

  // Which fields get a live check, per pathway — must match
  // ui/app.py's _name_checks(). product_type isn't here: the review
  // panel no longer has an editable Data Product Name field (see
  // _review_panel.html) — a duplicate product_type is checked separately,
  // directly on Step 1's own field, by initProductNameCheck() below.
  var LIVE_CHECK_FIELDS = {
    file: ["source_container_name", "target_container_name", "target_topic_name"],
    topic: ["src_topic_name", "target_topic_name"],
  };

  // Human labels for this same set, used only for the client-side sibling-
  // duplicate message below — must read naturally in "X must be different
  // from Y". Matches the labels ui/app.py's _name_checks() uses server-side
  // (see _find_duplicate_field_values(), the authoritative version of this
  // same check, re-run at preview/confirm time regardless of what this
  // shows).
  var FIELD_LABELS = {
    file: {
      source_container_name: "Source Container",
      target_container_name: "Target Container",
      target_topic_name: "Target Notification Topic",
    },
    topic: {
      src_topic_name: "Source Topic",
      target_topic_name: "Target Topic",
    },
  };

  function setStatus(field, status, message) {
    field.classList.remove("has-error", "is-verifying", "is-available");
    field.dataset.checkStatus = status;

    if (status === "verifying") {
      field.classList.add("is-verifying");
    } else if (status === "error") {
      field.classList.add("has-error");
      var errText = field.querySelector(".dpn-field__error-text");
      if (errText) errText.textContent = message || "Duplicated name";
    } else if (status === "available") {
      field.classList.add("is-available");
    }
    // "empty" leaves all three classes off — no icon, nothing to check yet.

    updateSubmitState(field);
  }

  function updateSubmitState(field) {
    var form = field.closest("form");
    if (!form) return;
    var submitBtn = form.querySelector(".dpn-step-actions button[type='submit']");
    if (!submitBtn) return;

    var liveFields = form.querySelectorAll('.dpn-field[data-live-check="1"]');
    var blocked = Array.prototype.some.call(liveFields, function (f) {
      return f.dataset.checkStatus !== "available";
    });
    submitBtn.disabled = blocked;
  }

  // This pathway's own proposed names must also be distinct from each
  // other — Source Container, Target Container and Target Notification
  // Topic (or Source/Target Topic) can't share a value even if none of
  // them individually exists on the platform yet (e.g. both left as the
  // same override). Returns the other field's label if `value` collides
  // with one of them, else null. Mirrors ui/app.py's
  // _find_duplicate_field_values() so both sides agree on which pair gets
  // reported.
  function findSiblingDuplicate(field, pipelineType, value) {
    var form = field.closest("form");
    var name = field.getAttribute("data-field");
    var liveFieldNames = LIVE_CHECK_FIELDS[pipelineType] || [];
    var labels = FIELD_LABELS[pipelineType] || {};
    var match = null;
    liveFieldNames.some(function (otherName) {
      if (otherName === name) return false;
      var otherField = form.querySelector('.dpn-field[data-field="' + otherName + '"]');
      var otherInput = otherField && otherField.querySelector(".dpn-input");
      if (otherInput && otherInput.value.trim() === value) {
        match = labels[otherName] || otherName;
        return true;
      }
      return false;
    });
    return match;
  }

  function checkField(field, pipelineType) {
    var input = field.querySelector(".dpn-input");
    var name = field.getAttribute("data-field");
    var value = input.value.trim();

    if (!value) {
      setStatus(field, "empty");
      return;
    }

    var siblingLabel = findSiblingDuplicate(field, pipelineType, value);
    if (siblingLabel) {
      // Invalidates any in-flight request for this field so it can't
      // overwrite this result once it resolves.
      field._dpnCheckToken = (field._dpnCheckToken || 0) + 1;
      setStatus(field, "error", "Must be different from " + siblingLabel + " — both are set to '" + value + "'.");
      return;
    }

    setStatus(field, "verifying");

    // Guards against a slow earlier request resolving after a newer one
    // for the same field (e.g. fast typing) and clobbering its result.
    var token = (field._dpnCheckToken = (field._dpnCheckToken || 0) + 1);

    fetch("/data-products/check-name", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ pipeline_type: pipelineType, field: name, value: value }),
    })
      .then(function (response) { return response.json(); })
      .then(function (data) {
        if (field._dpnCheckToken !== token) return;
        if (data.status === "duplicate") {
          setStatus(field, "error", "Duplicated name");
        } else {
          setStatus(field, "available");
        }
      })
      .catch(function () {
        if (field._dpnCheckToken !== token) return;
        // Network failure — fail soft like precheck.py itself does: don't
        // block the user on a connectivity blip, the confirm step
        // re-checks for real regardless.
        setStatus(field, "available");
      });
  }

  function debounce(fn, ms) {
    var timer;
    return function () {
      var args = arguments;
      var ctx = this;
      clearTimeout(timer);
      timer = setTimeout(function () { fn.apply(ctx, args); }, ms);
    };
  }

  function initReviewLiveCheck(root) {
    root = root || document;
    var form = root.querySelector('form[action="/data-products/confirm"]');
    if (!form || form.dataset.liveCheckWired) return;
    form.dataset.liveCheckWired = "1";

    var pipelineInput = form.querySelector('input[name="pipeline_type"]');
    var pipelineType = pipelineInput ? pipelineInput.value : null;
    var liveFieldNames = LIVE_CHECK_FIELDS[pipelineType];
    if (!liveFieldNames) return;

    var submitBtn = form.querySelector(".dpn-step-actions button[type='submit']");
    if (submitBtn) submitBtn.disabled = true;

    var liveFields = [];
    form.querySelectorAll(".dpn-field[data-field]").forEach(function (field) {
      var name = field.getAttribute("data-field");
      if (liveFieldNames.indexOf(name) === -1) return;
      field.setAttribute("data-live-check", "1");
      liveFields.push(field);
    });

    // Re-validates every live-check field, not just the one just edited:
    // a sibling-duplicate result (see findSiblingDuplicate above) is only
    // ever shown on the field being typed into, so if that edit just
    // resolved an earlier collision (e.g. Target Container no longer
    // matches a since-changed Source Container), the other field's stale
    // "error" state needs this same re-check to clear it.
    var recheckAll = debounce(function () {
      liveFields.forEach(function (f) { checkField(f, pipelineType); });
    }, DEBOUNCE_MS);

    liveFields.forEach(function (field) {
      var input = field.querySelector(".dpn-input");
      input.addEventListener("input", recheckAll);
      checkField(field, pipelineType); // validate the proposed/current value immediately on mount
    });
  }

  // Step 1's own live check: the Data Product Name field on new.html
  // (data-field="product_type") isn't one of LIVE_CHECK_FIELDS' pathway
  // fields — it's checked unconditionally, before a pathway is even
  // relevant, via the same POST /data-products/check-name endpoint (see
  // ui/app.py's check_name_available(), which special-cases
  // field=="product_type" against the registry regardless of
  // pipeline_type). A duplicate here is shown inline below the field
  // itself (see new.html) and keeps "Next: Review Names" hidden — there is
  // nothing to preview yet until the name is free — instead of the old
  // behavior of previewing anyway and locking every field on the review
  // panel.
  function initProductNameCheck(root) {
    root = root || document;
    var form = root.querySelector('form[action="/data-products/preview"]');
    if (!form || form.dataset.nameCheckWired) return;
    form.dataset.nameCheckWired = "1";

    var field = form.querySelector('.dpn-field[data-field="product_type"]');
    if (!field) return;
    var input = field.querySelector(".dpn-input");
    var submitBtn = form.querySelector(".dpn-step-actions button[type='submit']");

    function setSubmitVisible(visible) {
      if (submitBtn) submitBtn.hidden = !visible;
    }

    function runCheck() {
      var value = input.value.trim();
      if (!value) {
        setStatus(field, "empty");
        setSubmitVisible(false);
        return;
      }

      setStatus(field, "verifying");
      setSubmitVisible(false);

      var token = (field._dpnCheckToken = (field._dpnCheckToken || 0) + 1);
      var pipelineInput = form.querySelector('input[name="pipeline_type"]:checked');

      fetch("/data-products/check-name", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          pipeline_type: pipelineInput ? pipelineInput.value : "file",
          field: "product_type",
          value: value,
        }),
      })
        .then(function (response) { return response.json(); })
        .then(function (data) {
          if (field._dpnCheckToken !== token) return;
          if (data.status === "duplicate") {
            setStatus(field, "error", "Data product '" + value + "' already exists.");
            setSubmitVisible(false);
          } else {
            setStatus(field, "available");
            setSubmitVisible(true);
          }
        })
        .catch(function () {
          if (field._dpnCheckToken !== token) return;
          // Network failure — fail soft like the review step's own check
          // does: don't block the user on a connectivity blip, the preview
          // step re-checks for real regardless.
          setStatus(field, "available");
          setSubmitVisible(true);
        });
    }

    input.addEventListener("input", debounce(runCheck, DEBOUNCE_MS));
    runCheck(); // validate the current value (e.g. after a 409 round-trip) immediately on mount
  }

  initReviewLiveCheck(document);
  initProductNameCheck(document);

  window.DPN = window.DPN || {};
  window.DPN.initReviewLiveCheck = initReviewLiveCheck;
})();
