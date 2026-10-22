// Opt any table into scroll + client-side pagination by adding
// data-paginate="<page size>" to the <table> element, e.g.:
//   <table class="dpn-table" data-paginate="20">
// Requires /static/css/pagination.css for the scroll/pagination-bar styling.
// Runs against tables already fully rendered server-side — no fetch, no
// backend pagination support needed.
(function () {
  function isPlaceholderRow(row) {
    return !!row.querySelector('td[colspan]');
  }

  function setupTable(table) {
    var pageSize = parseInt(table.getAttribute('data-paginate'), 10) || 20;
    var tbody = table.querySelector('tbody');
    if (!tbody) return;

    var allRows = Array.prototype.slice.call(tbody.children).filter(function (el) {
      return el.tagName === 'TR';
    });
    var dataRows = allRows.filter(function (row) {
      return !isPlaceholderRow(row);
    });

    if (!table.parentNode.classList || !table.parentNode.classList.contains('table-scroll')) {
      var wrapper = document.createElement('div');
      wrapper.className = 'table-scroll';
      table.parentNode.insertBefore(wrapper, table);
      wrapper.appendChild(table);
    }

    if (dataRows.length === 0) return;

    var bar = document.createElement('div');
    bar.className = 'pagination-bar';

    var summary = document.createElement('span');
    summary.className = 'pagination-summary';

    var controls = document.createElement('div');
    controls.className = 'pagination-controls';

    var pages = document.createElement('span');
    pages.className = 'pagination-pages';

    var prevBtn = document.createElement('button');
    prevBtn.type = 'button';
    prevBtn.className = 'pagination-btn';
    prevBtn.setAttribute('aria-label', 'Previous page');
    prevBtn.innerHTML = '<img src="/static/img/pagination-chevron-left.svg" alt="">';

    var nextBtn = document.createElement('button');
    nextBtn.type = 'button';
    nextBtn.className = 'pagination-btn';
    nextBtn.setAttribute('aria-label', 'Next page');
    nextBtn.innerHTML = '<img src="/static/img/pagination-chevron-right.svg" alt="">';

    function divider() {
      var el = document.createElement('div');
      el.className = 'pagination-divider';
      return el;
    }

    controls.appendChild(pages);
    controls.appendChild(divider());
    controls.appendChild(prevBtn);
    controls.appendChild(divider());
    controls.appendChild(nextBtn);
    bar.appendChild(summary);
    bar.appendChild(controls);
    table.parentNode.parentNode.insertBefore(bar, table.parentNode.nextSibling);

    var total = dataRows.length;
    var totalPages = Math.max(1, Math.ceil(total / pageSize));
    var currentPage = 1;

    function render() {
      var start = (currentPage - 1) * pageSize;
      var end = Math.min(start + pageSize, total);
      dataRows.forEach(function (row, i) {
        row.style.display = (i >= start && i < end) ? '' : 'none';
      });
      summary.textContent = (start + 1) + ' – ' + end + ' of ' + total + ' item(s)';
      pages.textContent = currentPage + ' of ' + totalPages + ' pages';
      prevBtn.disabled = currentPage <= 1;
      nextBtn.disabled = currentPage >= totalPages;
    }

    prevBtn.addEventListener('click', function () {
      if (currentPage > 1) { currentPage -= 1; render(); }
    });
    nextBtn.addEventListener('click', function () {
      if (currentPage < totalPages) { currentPage += 1; render(); }
    });

    render();
  }

  // Exposed so run-history.js can re-run this against a table it just
  // fetched in via AJAX (replacing #dpn-history-results' innerHTML drops
  // the .table-scroll wrapper and pagination bar this module built around
  // the old one, so the fresh <table data-paginate> needs its own pass).
  function initTablePagination(root) {
    root = root || document;
    Array.prototype.forEach.call(root.querySelectorAll('table[data-paginate]'), setupTable);
  }

  document.addEventListener('DOMContentLoaded', function () {
    initTablePagination(document);
  });

  window.DPN = window.DPN || {};
  window.DPN.initTablePagination = initTablePagination;
})();
