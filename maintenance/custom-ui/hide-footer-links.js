(function () {
  var HIDDEN_ATTR = 'data-aifxo-footer-hidden';
  var DOC_TEXT = '\u6587\u6863';
  var REDEEM_LINK_ID = 'aifxo-redeem-code-link';
  var REDEEM_HINT_TEXT = '\u5151\u6362\u7801\u533a\u5206\u5927\u5c0f\u5199';
  var REDEEM_LINK_TEXT = '\u9700\u8981\u5151\u6362\u7801\uff1f\u70b9\u51fb\u8d2d\u4e70\u5151\u6362\u7801 \u2197';
  var REDEEM_LINK_HREF = '/go/redeem-code';
  var CUSTOM_LOGO_SRC = '/logo.svg?v=aifxo-20260722';
  var CUSTOM_FAVICON_SRC = '/favicon.ico?v=aifxo-20260722';

  function setHidden(link, hidden) {
    if (hidden) {
      link.style.display = 'none';
      link.setAttribute(HIDDEN_ATTR, '1');
    } else if (link.getAttribute(HIDDEN_ATTR) === '1') {
      link.style.display = '';
      link.removeAttribute(HIDDEN_ATTR);
    }
  }

  function isHomePage() {
    var path = window.location.pathname.replace(/\/+$/, '') || '/';
    return path === '/' || path === '/home';
  }

  function updateFooterLinks() {
    document.querySelectorAll('a[' + HIDDEN_ATTR + '="1"]').forEach(function (link) {
      setHidden(link, false);
    });

    if (!isHomePage()) return;

    document.querySelectorAll('footer a').forEach(function (link) {
      var text = (link.textContent || '').trim();
      var match = text === DOC_TEXT || text === 'GitHub';
      if (match) {
        setHidden(link, true);
      }
    });
  }

  function findRedeemHintElement() {
    var walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, {
      acceptNode: function (node) {
        if ((node.nodeValue || '').trim() === REDEEM_HINT_TEXT) {
          return NodeFilter.FILTER_ACCEPT;
        }
        return NodeFilter.FILTER_REJECT;
      }
    });

    var node = walker.nextNode();
    return node && node.parentElement;
  }

  function updateRedeemLink() {
    if (!document.body) return;

    var hint = findRedeemHintElement();
    if (!hint) return;

    var link = document.getElementById(REDEEM_LINK_ID);
    if (!link) {
      link = document.createElement('a');
      link.id = REDEEM_LINK_ID;
      link.href = REDEEM_LINK_HREF;
      link.target = '_blank';
      link.rel = 'noopener noreferrer';
      link.textContent = REDEEM_LINK_TEXT;
      link.style.marginLeft = '24px';
      link.style.color = '#14b8a6';
      link.style.fontWeight = '500';
      link.style.textDecoration = 'none';
      link.style.whiteSpace = 'nowrap';
    }

    if (link.parentElement !== hint) {
      hint.appendChild(link);
    }
  }

  function updateLogoAssets() {
    document.querySelectorAll('img[src="/logo.svg"]').forEach(function (image) {
      image.setAttribute('src', CUSTOM_LOGO_SRC);
    });

    document.querySelectorAll('link[rel~="icon"][href="/logo.svg"]').forEach(function (link) {
      link.setAttribute('href', CUSTOM_FAVICON_SRC);
      link.setAttribute('type', 'image/x-icon');
    });
  }

  function updateCustomizations() {
    updateFooterLinks();
    updateRedeemLink();
    updateLogoAssets();
  }

  updateCustomizations();

  if (document.body) {
    new MutationObserver(updateCustomizations).observe(document.body, {
      childList: true,
      subtree: true
    });
  }
})();
