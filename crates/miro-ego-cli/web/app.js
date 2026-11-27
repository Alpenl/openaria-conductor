// Miro-Ego Device Console (Device API v1 - /api/v1)
// Self-contained ES module with embedded VDOM runtime (CSP script-src 'self', style-src 'self').

export const API_PREFIX = "/api/v1";

// ---------------------------------------------------------------------------
// Minimal Embedded Preact-Compatible VDOM + Hooks Runtime (CSP-safe CSSOM)
// ---------------------------------------------------------------------------

var sliceProp,
  vdomOptions,
  vnodeIdCounter,
  rerenderQueue,
  prevDebounce,
  deferEnqueue,
  sortComponents,
  randSuffix,
  eventClockProp,
  handlerStampProp,
  captureRegex,
  eventClock,
  bubbleProxy,
  captureProxy,
  EMPTY_OBJ = {},
  EMPTY_ARR = [],
  NON_DIMENSIONAL = /acit|ex(?:s|g|n|p|$)|rph|grid|ows|mnc|ntw|ine[ch]|zoo|^ord|itera/i,
  isArr = Array.isArray;

function assign(target, props) {
  for (var k in props) target[k] = props[k];
  return target;
}

function removeNode(node) {
  if (node && node.parentNode) node.parentNode.removeChild(node);
}

function createElement(type, props, children) {
  var key,
    ref,
    k,
    normalized = {};
  for (k in props) {
    if (k === "key") key = props[k];
    else if (k === "ref") ref = props[k];
    else normalized[k] = props[k];
  }
  if (arguments.length > 2) {
    normalized.children = arguments.length > 3 ? sliceProp.call(arguments, 2) : children;
  }
  if (typeof type === "function" && type.defaultProps != null) {
    for (k in type.defaultProps) {
      if (normalized[k] === undefined) normalized[k] = type.defaultProps[k];
    }
  }
  return createVNode(type, normalized, key, ref, null);
}

function createVNode(type, props, key, ref, original) {
  var vnode = {
    type: type,
    props: props,
    key: key,
    ref: ref,
    __k: null,
    __: null,
    __b: 0,
    __e: null,
    __c: null,
    constructor: undefined,
    __v: original ?? ++vnodeIdCounter,
    __i: -1,
    __u: 0,
  };
  if (original == null && vdomOptions.vnode != null) vdomOptions.vnode(vnode);
  return vnode;
}

function Fragment(props) {
  return props.children;
}

function BaseComponent(props, context) {
  this.props = props;
  this.context = context;
}

function getDomSibling(vnode, childIndex) {
  if (childIndex == null) {
    return vnode.__ ? getDomSibling(vnode.__, vnode.__i + 1) : null;
  }
  var sibling;
  for (; childIndex < vnode.__k.length; childIndex++) {
    sibling = vnode.__k[childIndex];
    if (sibling != null && sibling.__e != null) return sibling.__e;
  }
  return typeof vnode.type === "function" ? getDomSibling(vnode) : null;
}

function renderComponent(component) {
  if (component.__P && component.__d) {
    var vnode = component.__v,
      oldDom = vnode.__e,
      commitQueue = [],
      refQueue = [],
      newVNode = assign({}, vnode);
    newVNode.__v = vnode.__v + 1;
    if (vdomOptions.vnode) vdomOptions.vnode(newVNode);
    diff(
      component.__P,
      newVNode,
      vnode,
      component.__n,
      component.__P.namespaceURI,
      32 & vnode.__u ? [oldDom] : null,
      commitQueue,
      oldDom ?? getDomSibling(vnode),
      !!(32 & vnode.__u),
      refQueue
    );
    newVNode.__v = vnode.__v;
    newVNode.__.__k[newVNode.__i] = newVNode;
    commitRoot(commitQueue, newVNode, refQueue);
    vnode.__e = vnode.__ = null;
    if (newVNode.__e !== oldDom) updateParentDomPointers(newVNode);
  }
}

function updateParentDomPointers(vnode) {
  if ((vnode = vnode.__) != null && vnode.__c != null) {
    vnode.__e = vnode.__c.base = null;
    vnode.__k.some(function (child) {
      if (child != null && child.__e != null) {
        return (vnode.__e = vnode.__c.base = child.__e);
      }
    });
    return updateParentDomPointers(vnode);
  }
}

function enqueueRender(c) {
  if (
    (!c.__d && (c.__d = true) && rerenderQueue.push(c) && !processQueue.__r++) ||
    prevDebounce !== vdomOptions.debounceRendering
  ) {
    ((prevDebounce = vdomOptions.debounceRendering) || deferEnqueue)(processQueue);
  }
}

function processQueue() {
  try {
    var c,
      len = 1;
    while (rerenderQueue.length) {
      if (rerenderQueue.length > len) rerenderQueue.sort(sortComponents);
      c = rerenderQueue.shift();
      len = rerenderQueue.length;
      renderComponent(c);
    }
  } finally {
    rerenderQueue.length = processQueue.__r = 0;
  }
}

function diffChildren(
  parentDom,
  renderResult,
  newParentVNode,
  oldParentVNode,
  globalContext,
  namespace,
  excessDomChildren,
  commitQueue,
  oldDom,
  isHydrating,
  refQueue
) {
  var i,
    oldVNode,
    childVNode,
    newDom,
    firstChildDom,
    sibDom,
    oldChildren = (oldParentVNode && oldParentVNode.__k) || EMPTY_ARR,
    newChildrenLength = renderResult.length;
  oldDom = constructNewChildrenArray(newParentVNode, renderResult, oldChildren, oldDom, newChildrenLength);
  for (i = 0; i < newChildrenLength; i++) {
    childVNode = newParentVNode.__k[i];
    if (childVNode != null) {
      oldVNode = childVNode.__i !== -1 ? oldChildren[childVNode.__i] || EMPTY_OBJ : EMPTY_OBJ;
      childVNode.__i = i;
      sibDom = diff(
        parentDom,
        childVNode,
        oldVNode,
        globalContext,
        namespace,
        excessDomChildren,
        commitQueue,
        oldDom,
        isHydrating,
        refQueue
      );
      newDom = childVNode.__e;
      if (childVNode.ref && oldVNode.ref !== childVNode.ref) {
        if (oldVNode.ref) applyRef(oldVNode.ref, null, childVNode);
        refQueue.push(childVNode.ref, childVNode.__c || newDom, childVNode);
      }
      if (firstChildDom == null && newDom != null) firstChildDom = newDom;
      if (4 & childVNode.__u) {
        oldDom = insertChild(childVNode, oldDom, parentDom);
        if (oldVNode.__e) oldVNode.__e = null;
      } else if (typeof childVNode.type === "function" && sibDom !== undefined) {
        oldDom = sibDom;
      } else if (newDom) {
        oldDom = newDom.nextSibling;
      }
      childVNode.__u &= -7;
    }
  }
  newParentVNode.__e = firstChildDom;
  return oldDom;
}

function constructNewChildrenArray(newParentVNode, renderResult, oldChildren, oldDom, newChildrenLength) {
  var i,
    childVNode,
    oldVNode,
    skewedIndex,
    matchingIndex,
    oldChildrenLength = oldChildren.length,
    remainingOldChildren = oldChildrenLength,
    skew = 0;
  newParentVNode.__k = new Array(newChildrenLength);
  for (i = 0; i < newChildrenLength; i++) {
    childVNode = renderResult[i];
    if (childVNode != null && typeof childVNode !== "boolean" && typeof childVNode !== "function") {
      if (
        typeof childVNode === "string" ||
        typeof childVNode === "number" ||
        typeof childVNode === "bigint" ||
        childVNode.constructor === String
      ) {
        childVNode = newParentVNode.__k[i] = createVNode(null, childVNode, null, null, null);
      } else if (isArr(childVNode)) {
        childVNode = newParentVNode.__k[i] = createVNode(Fragment, { children: childVNode }, null, null, null);
      } else if (childVNode.constructor === undefined && childVNode.__b > 0) {
        childVNode = newParentVNode.__k[i] = createVNode(
          childVNode.type,
          childVNode.props,
          childVNode.key,
          childVNode.ref ? childVNode.ref : null,
          childVNode.__v
        );
      } else {
        newParentVNode.__k[i] = childVNode;
      }
      skewedIndex = i + skew;
      childVNode.__ = newParentVNode;
      childVNode.__b = newParentVNode.__b + 1;
      oldVNode = null;
      matchingIndex = childVNode.__i = findMatchingIndex(childVNode, oldChildren, skewedIndex, remainingOldChildren);
      if (matchingIndex !== -1) {
        remainingOldChildren--;
        oldVNode = oldChildren[matchingIndex];
        if (oldVNode) oldVNode.__u |= 2;
      }
      if (oldVNode == null || oldVNode.__v == null) {
        if (matchingIndex === -1) {
          if (newChildrenLength > oldChildrenLength) skew--;
          else if (newChildrenLength < oldChildrenLength) skew++;
        }
        if (typeof childVNode.type !== "function") childVNode.__u |= 4;
      } else if (matchingIndex !== skewedIndex) {
        if (matchingIndex === skewedIndex - 1) skew--;
        else if (matchingIndex === skewedIndex + 1) skew++;
        else {
          if (matchingIndex > skewedIndex) skew--;
          else skew++;
          childVNode.__u |= 4;
        }
      }
    } else {
      newParentVNode.__k[i] = null;
    }
  }
  if (remainingOldChildren) {
    for (i = 0; i < oldChildrenLength; i++) {
      oldVNode = oldChildren[i];
      if (oldVNode != null && (2 & oldVNode.__u) === 0) {
        if (oldVNode.__e === oldDom) oldDom = getDomSibling(oldVNode);
        unmount(oldVNode, oldVNode);
      }
    }
  }
  return oldDom;
}

function insertChild(vnode, oldDom, parentDom) {
  var children, i;
  if (typeof vnode.type === "function") {
    children = vnode.__k;
    for (i = 0; children && i < children.length; i++) {
      if (children[i]) {
        children[i].__ = vnode;
        oldDom = insertChild(children[i], oldDom, parentDom);
      }
    }
    return oldDom;
  }
  if (vnode.__e !== oldDom) {
    if (oldDom && vnode.type && !oldDom.parentNode) oldDom = getDomSibling(vnode);
    oldDom = parentDom.insertBefore(vnode.__e, oldDom || null);
  }
  do {
    oldDom = oldDom && oldDom.nextSibling;
  } while (oldDom != null && oldDom.nodeType === 8);
  return oldDom;
}

function findMatchingIndex(childVNode, oldChildren, skewedIndex, remainingOldChildren) {
  var x,
    y,
    idx,
    key = childVNode.key,
    type = childVNode.type,
    oldVNode = oldChildren[skewedIndex],
    shouldSearch = oldVNode != null && (2 & oldVNode.__u) === 0;
  if ((oldVNode === null && key == null) || (shouldSearch && key === oldVNode.key && type === oldVNode.type)) {
    return skewedIndex;
  }
  if (remainingOldChildren > (shouldSearch ? 1 : 0)) {
    x = skewedIndex - 1;
    y = skewedIndex + 1;
    while (x >= 0 || y < oldChildren.length) {
      idx = x >= 0 ? x-- : y++;
      oldVNode = oldChildren[idx];
      if (oldVNode != null && (2 & oldVNode.__u) === 0 && key === oldVNode.key && type === oldVNode.type) {
        return idx;
      }
    }
  }
  return -1;
}

function setStyleProp(styleObj, name, value) {
  if (name[0] === "-") {
    styleObj.setProperty(name, value ?? "");
  } else {
    styleObj[name] =
      value == null ? "" : typeof value !== "number" || NON_DIMENSIONAL.test(name) ? value : value + "px";
  }
}

function setProperty(dom, name, value, oldValue, namespace) {
  var useCapture, lower;
  if (name === "style") {
    // Strictly object-only CSSOM property updates (never inline style attribute strings)
    if (oldValue && typeof oldValue === "object") {
      for (var prevKey in oldValue) {
        if (!value || !(prevKey in value)) setStyleProp(dom.style, prevKey, "");
      }
    }
    if (value && typeof value === "object") {
      for (var nextKey in value) {
        if (!oldValue || value[nextKey] !== oldValue[nextKey]) {
          setStyleProp(dom.style, nextKey, value[nextKey]);
        }
      }
    }
  } else if (name[0] === "o" && name[1] === "n") {
    useCapture = name !== (name = name.replace(captureRegex, "$1"));
    lower = name.toLowerCase();
    name = lower in dom || name === "onFocusOut" || name === "onFocusIn" ? lower.slice(2) : name.slice(2);
    if (!dom.l) dom.l = {};
    dom.l[name + useCapture] = value;
    if (value) {
      if (!oldValue) {
        value[handlerStampProp] = eventClock;
        dom.addEventListener(name, useCapture ? captureProxy : bubbleProxy, useCapture);
      } else {
        value[handlerStampProp] = oldValue[handlerStampProp];
      }
    } else {
      dom.removeEventListener(name, useCapture ? captureProxy : bubbleProxy, useCapture);
    }
  } else {
    if (namespace === "http://www.w3.org/2000/svg") {
      name = name.replace(/xlink(H|:h)/, "h").replace(/sName$/, "s");
    } else if (
      name !== "width" &&
      name !== "height" &&
      name !== "href" &&
      name !== "list" &&
      name !== "form" &&
      name !== "tabIndex" &&
      name !== "download" &&
      name !== "rowSpan" &&
      name !== "colSpan" &&
      name !== "role" &&
      name !== "popover" &&
      name in dom
    ) {
      try {
        dom[name] = value ?? "";
        return;
      } catch {}
    }
    if (typeof value !== "function") {
      if (value == null || (value === false && name[4] !== "-")) {
        dom.removeAttribute(name);
      } else {
        dom.setAttribute(name, name === "popover" && value === true ? "" : value);
      }
    }
  }
}

function createEventProxy(useCapture) {
  return function (e) {
    if (this.l) {
      var handler = this.l[e.type + useCapture];
      if (e[eventClockProp] == null) e[eventClockProp] = eventClock++;
      else if (e[eventClockProp] < handler[handlerStampProp]) return;
      return handler(vdomOptions.event ? vdomOptions.event(e) : e);
    }
  };
}

function diff(
  parentDom,
  newVNode,
  oldVNode,
  globalContext,
  namespace,
  excessDomChildren,
  commitQueue,
  oldDom,
  isHydrating,
  refQueue
) {
  var tmp,
    commitLen,
    c,
    isNew,
    oldProps,
    oldState,
    snapshot,
    clearProcessingException,
    newProps,
    isClassComponent,
    provider,
    componentContext,
    renderHook,
    loopCount,
    rendered,
    idx,
    newType = newVNode.type;
  if (newVNode.constructor !== undefined) return null;
  if (128 & oldVNode.__u) {
    isHydrating = !!(32 & oldVNode.__u);
    excessDomChildren = [(oldDom = newVNode.__e = oldVNode.__e)];
  }
  if ((tmp = vdomOptions.__b)) tmp(newVNode);
  outer: if (typeof newType === "function") {
    commitLen = commitQueue.length;
    try {
      newProps = newVNode.props;
      isClassComponent = newType.prototype && newType.prototype.render;
      tmp = newType.contextType;
      provider = tmp && globalContext[tmp.__c];
      componentContext = tmp ? (provider ? provider.props.value : tmp.__) : globalContext;
      if (oldVNode.__c) {
        clearProcessingException = (c = newVNode.__c = oldVNode.__c).__ = c.__E;
      } else {
        if (isClassComponent) {
          newVNode.__c = c = new newType(newProps, componentContext);
        } else {
          newVNode.__c = c = new BaseComponent(newProps, componentContext);
          c.constructor = newType;
          c.render = doRender;
        }
        if (provider) provider.sub(c);
        c.props = newProps;
        if (!c.state) c.state = {};
        c.context = componentContext;
        c.__n = globalContext;
        isNew = c.__d = true;
        c.__h = [];
        c._sb = [];
      }
      if (isClassComponent && c.__s == null) c.__s = c.state;
      if (isClassComponent && newType.getDerivedStateFromProps != null) {
        if (c.__s === c.state) c.__s = assign({}, c.__s);
        assign(c.__s, newType.getDerivedStateFromProps(newProps, c.__s));
      }
      oldProps = c.props;
      oldState = c.state;
      c.__v = newVNode;
      if (isNew) {
        if (isClassComponent && newType.getDerivedStateFromProps == null && c.componentWillMount != null) {
          c.componentWillMount();
        }
        if (isClassComponent && c.componentDidMount != null) c.__h.push(c.componentDidMount);
      } else {
        if (
          isClassComponent &&
          newType.getDerivedStateFromProps == null &&
          newProps !== oldProps &&
          c.componentWillReceiveProps != null
        ) {
          c.componentWillReceiveProps(newProps, componentContext);
        }
        if (
          newVNode.__v === oldVNode.__v ||
          (!c.__e &&
            c.shouldComponentUpdate != null &&
            c.shouldComponentUpdate(newProps, c.__s, componentContext) === false)
        ) {
          if (newVNode.__v !== oldVNode.__v) {
            c.props = newProps;
            c.state = c.__s;
            c.__d = false;
          }
          newVNode.__e = oldVNode.__e;
          newVNode.__k = oldVNode.__k;
          newVNode.__k.some(function (v) {
            if (v) v.__ = newVNode;
          });
          EMPTY_ARR.push.apply(c.__h, c._sb);
          c._sb = [];
          if (c.__h.length) commitQueue.push(c);
          oldDom = getDomSibling(oldVNode);
          break outer;
        }
        if (c.componentWillUpdate != null) c.componentWillUpdate(newProps, c.__s, componentContext);
        if (isClassComponent && c.componentDidUpdate != null) {
          c.__h.push(function () {
            c.componentDidUpdate(oldProps, oldState, snapshot);
          });
        }
      }
      c.context = componentContext;
      c.props = newProps;
      c.__P = parentDom;
      c.__e = false;
      renderHook = vdomOptions.__r;
      loopCount = 0;
      if (isClassComponent) {
        c.state = c.__s;
        c.__d = false;
        if (renderHook) renderHook(newVNode);
        tmp = c.render(c.props, c.state, c.context);
        EMPTY_ARR.push.apply(c.__h, c._sb);
        c._sb = [];
      } else {
        do {
          c.__d = false;
          if (renderHook) renderHook(newVNode);
          tmp = c.render(c.props, c.state, c.context);
          c.state = c.__s;
        } while (c.__d && ++loopCount < 25);
      }
      c.state = c.__s;
      if (c.getChildContext != null) {
        globalContext = assign(assign({}, globalContext), c.getChildContext());
      }
      if (isClassComponent && !isNew && c.getSnapshotBeforeUpdate != null) {
        snapshot = c.getSnapshotBeforeUpdate(oldProps, oldState);
      }
      rendered =
        tmp != null && tmp.type === Fragment && tmp.key == null ? cloneNodeIfNeeded(tmp.props.children) : tmp;
      oldDom = diffChildren(
        parentDom,
        isArr(rendered) ? rendered : [rendered],
        newVNode,
        oldVNode,
        globalContext,
        namespace,
        excessDomChildren,
        commitQueue,
        oldDom,
        isHydrating,
        refQueue
      );
      c.base = newVNode.__e;
      newVNode.__u &= -161;
      if (c.__h.length) commitQueue.push(c);
      if (clearProcessingException) c.__E = c.__ = null;
    } catch (err) {
      commitQueue.length = commitLen;
      newVNode.__v = null;
      if (isHydrating || excessDomChildren != null) {
        if (err.then) {
          newVNode.__u |= isHydrating ? 160 : 128;
          while (oldDom && oldDom.nodeType === 8 && oldDom.nextSibling) oldDom = oldDom.nextSibling;
          if (excessDomChildren != null) excessDomChildren[excessDomChildren.indexOf(oldDom)] = null;
          newVNode.__e = oldDom;
        } else if (excessDomChildren != null) {
          for (idx = excessDomChildren.length; idx--; ) removeNode(excessDomChildren[idx]);
        }
      } else {
        newVNode.__e = oldVNode.__e;
      }
      if (newVNode.__k == null) newVNode.__k = oldVNode.__k || [];
      if (!err.then) markErrored(newVNode);
      vdomOptions.__e(err, newVNode, oldVNode);
    }
  } else if (excessDomChildren == null && newVNode.__v === oldVNode.__v) {
    newVNode.__k = oldVNode.__k;
    newVNode.__e = oldVNode.__e;
  } else {
    oldDom = newVNode.__e = diffElementNodes(
      oldVNode.__e,
      newVNode,
      oldVNode,
      globalContext,
      namespace,
      excessDomChildren,
      commitQueue,
      isHydrating,
      refQueue
    );
  }
  if ((tmp = vdomOptions.diffed)) tmp(newVNode);
  return 128 & newVNode.__u ? undefined : oldDom;
}

function markErrored(vnode) {
  if (vnode) {
    if (vnode.__c) vnode.__c.__e = true;
    if (vnode.__k) vnode.__k.some(markErrored);
  }
}

function commitRoot(commitQueue, root, refQueue) {
  for (var i = 0; i < refQueue.length; i++) {
    applyRef(refQueue[i], refQueue[++i], refQueue[++i]);
  }
  if (vdomOptions.__c) vdomOptions.__c(root, commitQueue);
  commitQueue.some(function (c) {
    try {
      commitQueue = c.__h;
      c.__h = [];
      commitQueue.some(function (cb) {
        cb.call(c);
      });
    } catch (err) {
      vdomOptions.__e(err, c.__v);
    }
  });
}

function cloneNodeIfNeeded(vnode) {
  if (typeof vnode !== "object" || vnode == null || vnode.__b > 0) return vnode;
  if (isArr(vnode)) return vnode.map(cloneNodeIfNeeded);
  if (vnode.constructor !== undefined) return null;
  return assign({}, vnode);
}

function diffElementNodes(
  dom,
  newVNode,
  oldVNode,
  globalContext,
  namespace,
  excessDomChildren,
  commitQueue,
  isHydrating,
  refQueue
) {
  var i,
    newHtml,
    oldHtml,
    newChildren,
    propVal,
    valueProp,
    checkedProp,
    oldProps = oldVNode.props || EMPTY_OBJ,
    newProps = newVNode.props,
    nodeType = newVNode.type;
  if (nodeType === "svg") namespace = "http://www.w3.org/2000/svg";
  else if (nodeType === "math") namespace = "http://www.w3.org/1998/Math/MathML";
  else if (!namespace) namespace = "http://www.w3.org/1999/xhtml";

  if (excessDomChildren != null) {
    for (i = 0; i < excessDomChildren.length; i++) {
      propVal = excessDomChildren[i];
      if (
        propVal &&
        "setAttribute" in propVal === !!nodeType &&
        (nodeType ? propVal.localName === nodeType : propVal.nodeType === 3)
      ) {
        dom = propVal;
        excessDomChildren[i] = null;
        break;
      }
    }
  }

  if (dom == null) {
    if (nodeType == null) return document.createTextNode(newProps);
    dom = document.createElementNS(namespace, nodeType, newProps.is && newProps);
    if (isHydrating) {
      if (vdomOptions.__m) vdomOptions.__m(newVNode, excessDomChildren);
      isHydrating = false;
    }
    excessDomChildren = null;
  }

  if (nodeType == null) {
    if (oldProps !== newProps && (!isHydrating || dom.data !== newProps)) dom.data = newProps;
  } else {
    excessDomChildren =
      nodeType === "textarea" && newProps.defaultValue != null
        ? null
        : excessDomChildren && sliceProp.call(dom.childNodes);
    if (!isHydrating && excessDomChildren != null) {
      oldProps = {};
      for (i = 0; i < dom.attributes.length; i++) {
        propVal = dom.attributes[i];
        oldProps[propVal.name] = propVal.value;
      }
    }
    for (i in oldProps) {
      propVal = oldProps[i];
      if (i === "dangerouslySetInnerHTML") oldHtml = propVal;
      else if (
        i !== "children" &&
        !(i in newProps) &&
        !(i === "value" && "defaultValue" in newProps) &&
        !(i === "checked" && "defaultChecked" in newProps)
      ) {
        setProperty(dom, i, null, propVal, namespace);
      }
    }
    for (i in newProps) {
      propVal = newProps[i];
      if (i === "children") newChildren = propVal;
      else if (i === "dangerouslySetInnerHTML") newHtml = propVal;
      else if (i === "value") valueProp = propVal;
      else if (i === "checked") checkedProp = propVal;
      else if ((!isHydrating || typeof propVal === "function") && oldProps[i] !== propVal) {
        setProperty(dom, i, propVal, oldProps[i], namespace);
      }
    }
    if (newHtml) {
      if (!isHydrating && (!oldHtml || (newHtml.__html !== oldHtml.__html && newHtml.__html !== dom.innerHTML))) {
        dom.innerHTML = newHtml.__html;
      }
      newVNode.__k = [];
    } else {
      if (oldHtml) dom.innerHTML = "";
      diffChildren(
        newVNode.type === "template" ? dom.content : dom,
        isArr(newChildren) ? newChildren : [newChildren],
        newVNode,
        oldVNode,
        globalContext,
        nodeType === "foreignObject" ? "http://www.w3.org/1999/xhtml" : namespace,
        excessDomChildren,
        commitQueue,
        excessDomChildren ? excessDomChildren[0] : oldVNode.__k && getDomSibling(oldVNode, 0),
        isHydrating,
        refQueue
      );
      if (excessDomChildren != null) {
        for (i = excessDomChildren.length; i--; ) removeNode(excessDomChildren[i]);
      }
    }
    if (!isHydrating || nodeType === "textarea") {
      if (nodeType === "progress" && valueProp == null) {
        dom.removeAttribute("value");
      } else if (
        valueProp != null &&
        (valueProp !== dom.value || (nodeType === "progress" && !valueProp) || (nodeType === "option" && valueProp !== oldProps.value))
      ) {
        setProperty(dom, "value", valueProp, oldProps.value, namespace);
      }
      if (checkedProp != null && checkedProp !== dom.checked) {
        setProperty(dom, "checked", checkedProp, oldProps.checked, namespace);
      }
    }
  }
  return dom;
}

function applyRef(ref, value, vnode) {
  try {
    if (typeof ref === "function") {
      var hasCleanup = typeof ref.__u === "function";
      if (hasCleanup) ref.__u();
      if (!hasCleanup || value != null) ref.__u = ref(value);
    } else {
      ref.current = value;
    }
  } catch (err) {
    vdomOptions.__e(err, vnode);
  }
}

function unmount(vnode, parentVNode, skipRemove) {
  var r, idx;
  if (vdomOptions.unmount) vdomOptions.unmount(vnode);
  if ((r = vnode.ref)) {
    if (!r.current || r.current === vnode.__e) applyRef(r, null, parentVNode);
  }
  if ((r = vnode.__c) != null) {
    if (r.componentWillUnmount) {
      try {
        r.componentWillUnmount();
      } catch (err) {
        vdomOptions.__e(err, parentVNode);
      }
    }
    r.base = r.__P = r.__n = null;
  }
  if ((r = vnode.__k)) {
    for (idx = 0; idx < r.length; idx++) {
      if (r[idx]) unmount(r[idx], parentVNode, skipRemove || typeof vnode.type !== "function");
    }
  }
  if (!skipRemove) removeNode(vnode.__e);
  vnode.__c = vnode.__ = vnode.__e = undefined;
}

function doRender(props, state, context) {
  return this.constructor(props, context);
}

function renderRoot(vnode, parentDom) {
  if (parentDom === document) parentDom = document.documentElement;
  if (vdomOptions.__) vdomOptions.__(vnode, parentDom);
  var oldVNode = parentDom.__k,
    commitQueue = [],
    refQueue = [];
  vnode = parentDom.__k = createElement(Fragment, null, [vnode]);
  diff(
    parentDom,
    vnode,
    oldVNode || EMPTY_OBJ,
    EMPTY_OBJ,
    parentDom.namespaceURI,
    oldVNode ? null : parentDom.firstChild ? sliceProp.call(parentDom.childNodes) : null,
    commitQueue,
    oldVNode ? oldVNode.__e : parentDom.firstChild,
    false,
    refQueue
  );
  commitRoot(commitQueue, vnode, refQueue);
  vnode.props.children = null;
}

sliceProp = EMPTY_ARR.slice;
vdomOptions = {
  __e: function (err, vnode, oldVNode, errorInfo) {
    var component, ctor, handled;
    while ((vnode = vnode.__)) {
      if ((component = vnode.__c) && !component.__) {
        try {
          ctor = component.constructor;
          if (ctor && ctor.getDerivedStateFromError != null) {
            component.setState(ctor.getDerivedStateFromError(err));
            handled = component.__d;
          }
          if (component.componentDidCatch != null) {
            component.componentDidCatch(err, errorInfo || {});
            handled = component.__d;
          }
          if (handled) return (component.__E = component);
        } catch (nextErr) {
          err = nextErr;
        }
      }
    }
    throw err;
  },
};
vnodeIdCounter = 0;
BaseComponent.prototype.setState = function (update, callback) {
  var s =
    this.__s != null && this.__s !== this.state ? this.__s : (this.__s = assign({}, this.state));
  if (typeof update === "function") update = update(assign({}, s), this.props);
  if (update) assign(s, update);
  if (update != null && this.__v) {
    if (callback) this._sb.push(callback);
    enqueueRender(this);
  }
};
BaseComponent.prototype.forceUpdate = function (callback) {
  if (this.__v) {
    this.__e = true;
    if (callback) this.__h.push(callback);
    enqueueRender(this);
  }
};
BaseComponent.prototype.render = Fragment;
rerenderQueue = [];
deferEnqueue =
  typeof Promise === "function" ? Promise.prototype.then.bind(Promise.resolve()) : setTimeout;
sortComponents = function (a, b) {
  return a.__v.__b - b.__v.__b;
};
processQueue.__r = 0;
randSuffix = Math.random().toString(8);
eventClockProp = "__d" + randSuffix;
handlerStampProp = "__a" + randSuffix;
captureRegex = /(PointerCapture)$|Capture$/i;
eventClock = 0;
bubbleProxy = createEventProxy(false);
captureProxy = createEventProxy(true);

var jsxIdCounter = 0;
function h(type, props, key) {
  props = props || {};
  var ref,
    k,
    normalized = props;
  if ("ref" in normalized) {
    normalized = {};
    for (k in props) {
      if (k === "ref") ref = props[k];
      else normalized[k] = props[k];
    }
  }
  var vnode = {
    type: type,
    props: normalized,
    key: key,
    ref: ref,
    __k: null,
    __: null,
    __b: 0,
    __e: null,
    __c: null,
    constructor: undefined,
    __v: --jsxIdCounter,
    __i: -1,
    __u: 0,
  };
  if (typeof type === "function" && (ref = type.defaultProps)) {
    for (k in ref) if (normalized[k] === undefined) normalized[k] = ref[k];
  }
  if (vdomOptions.vnode) vdomOptions.vnode(vnode);
  return vnode;
}

// Hooks
var currentHookIdx,
  currentComponent,
  previousComponent,
  rafCallback,
  currentHookType = 0,
  afterPaintEffects = [],
  optsRef = vdomOptions,
  oldBeforeDiff = optsRef.__b,
  oldBeforeRender = optsRef.__r,
  oldAfterDiff = optsRef.diffed,
  oldCommit = optsRef.__c,
  oldUnmount = optsRef.unmount,
  oldRoot = optsRef.__;

function getHookState(index, type) {
  if (optsRef.__h) optsRef.__h(currentComponent, index, currentHookType || type);
  currentHookType = 0;
  var hooks = currentComponent.__H || (currentComponent.__H = { __: [], __h: [] });
  if (index >= hooks.__.length) hooks.__.push({});
  return hooks.__[index];
}

function useState(initialState) {
  currentHookType = 1;
  return useReducer(invokeOrReturn, initialState);
}

function useReducer(reducer, initialState) {
  var hookState = getHookState(currentHookIdx++, 2);
  hookState.t = reducer;
  if (!hookState.__c) {
    hookState.__ = [
      invokeOrReturn(undefined, initialState),
      function (action) {
        var currentValue = hookState.__N ? hookState.__N[0] : hookState.__[0];
        var nextValue = hookState.t(currentValue, action);
        if (currentValue !== nextValue) {
          hookState.__N = [nextValue, hookState.__[1]];
          hookState.__c.setState({});
        }
      },
    ];
    hookState.__c = currentComponent;
    if (!currentComponent.__f) {
      var shouldUpdate = function (p, s, c) {
        if (!hookState.__c.__H) return true;
        var hasPending = false;
        var changed = hookState.__c.props !== p;
        hookState.__c.__H.__.some(function (item) {
          if (item.__N) {
            hasPending = true;
            var prev = item.__[0];
            item.__ = item.__N;
            item.__N = undefined;
            if (prev !== item.__[0]) changed = true;
          }
        });
        if (origShouldUpdate) {
          var res = origShouldUpdate.call(this, p, s, c);
          return hasPending ? res || changed : res;
        }
        return !hasPending || changed;
      };
      currentComponent.__f = true;
      var origShouldUpdate = currentComponent.shouldComponentUpdate;
      var origWillUpdate = currentComponent.componentWillUpdate;
      currentComponent.componentWillUpdate = function (p, s, c) {
        if (this.__e) {
          var tmp = origShouldUpdate;
          origShouldUpdate = undefined;
          shouldUpdate(p, s, c);
          origShouldUpdate = tmp;
        }
        if (origWillUpdate) origWillUpdate.call(this, p, s, c);
      };
      currentComponent.shouldComponentUpdate = shouldUpdate;
    }
  }
  return hookState.__N || hookState.__;
}

function useEffect(callback, args) {
  var state = getHookState(currentHookIdx++, 3);
  if (!optsRef.__s && argsChanged(state.__H, args)) {
    state.__ = callback;
    state.u = args;
    currentComponent.__H.__h.push(state);
  }
}

function useLayoutEffect(callback, args) {
  var state = getHookState(currentHookIdx++, 4);
  if (!optsRef.__s && argsChanged(state.__H, args)) {
    state.__ = callback;
    state.u = args;
    currentComponent.__h.push(state);
  }
}

function useRef(initialValue) {
  currentHookType = 5;
  return useMemo(function () {
    return { current: initialValue };
  }, []);
}

function useMemo(factory, args) {
  var state = getHookState(currentHookIdx++, 7);
  if (argsChanged(state.__H, args)) {
    state.__ = factory();
    state.__H = args;
    state.__h = factory;
  }
  return state.__;
}

function flushAfterPaintEffects() {
  var c;
  while ((c = afterPaintEffects.shift())) {
    var hooks = c.__H;
    if (c.__P && hooks) {
      try {
        hooks.__h.some(invokeCleanup);
        hooks.__h.some(invokeEffect);
        hooks.__h = [];
      } catch (err) {
        hooks.__h = [];
        optsRef.__e(err, c.__v);
      }
    }
  }
}

optsRef.__b = function (vnode) {
  currentComponent = null;
  if (oldBeforeDiff) oldBeforeDiff(vnode);
};
optsRef.__ = function (vnode, parentDom) {
  if (vnode && parentDom.__k && parentDom.__k.__m) vnode.__m = parentDom.__k.__m;
  if (oldRoot) oldRoot(vnode, parentDom);
};
optsRef.__r = function (vnode) {
  if (oldBeforeRender) oldBeforeRender(vnode);
  currentHookIdx = 0;
  var hooks = (currentComponent = vnode.__c).__H;
  if (hooks) {
    if (previousComponent === currentComponent) {
      hooks.__h = [];
      currentComponent.__h = [];
      hooks.__.some(function (item) {
        if (item.__N) item.__ = item.__N;
        item.u = item.__N = undefined;
      });
    } else {
      hooks.__h.some(invokeCleanup);
      hooks.__h.some(invokeEffect);
      hooks.__h = [];
      currentHookIdx = 0;
    }
  }
  previousComponent = currentComponent;
};
optsRef.diffed = function (vnode) {
  if (oldAfterDiff) oldAfterDiff(vnode);
  var c = vnode.__c;
  if (c && c.__H) {
    if (c.__H.__h.length) {
      if (afterPaintEffects.push(c) === 1 || rafCallback !== optsRef.requestAnimationFrame) {
        ((rafCallback = optsRef.requestAnimationFrame) || scheduleNextFrame)(flushAfterPaintEffects);
      }
    }
    c.__H.__.some(function (item) {
      if (item.u) {
        item.__H = item.u;
        item.u = undefined;
      }
    });
  }
  previousComponent = currentComponent = null;
};
optsRef.__c = function (vnode, commitQueue) {
  commitQueue.some(function (c) {
    try {
      c.__h.some(invokeCleanup);
      c.__h = c.__h.filter(function (cb) {
        return !cb.__ || invokeEffect(cb);
      });
    } catch (err) {
      commitQueue.some(function (item) {
        if (item.__h) item.__h = [];
      });
      commitQueue = [];
      optsRef.__e(err, c.__v);
    }
  });
  if (oldCommit) oldCommit(vnode, commitQueue);
};
optsRef.unmount = function (vnode) {
  if (oldUnmount) oldUnmount(vnode);
  var err,
    c = vnode.__c;
  if (c && c.__H) {
    c.__H.__.some(function (s) {
      try {
        invokeCleanup(s);
      } catch (e) {
        err = e;
      }
    });
    c.__H = undefined;
    if (err) optsRef.__e(err, c.__v);
  }
};

var hasRaf = typeof requestAnimationFrame === "function";
function scheduleNextFrame(cb) {
  var rafId,
    done = function () {
      clearTimeout(timeoutId);
      if (hasRaf) cancelAnimationFrame(rafId);
      setTimeout(cb);
    },
    timeoutId = setTimeout(done, 35);
  if (hasRaf) rafId = requestAnimationFrame(done);
}
function invokeCleanup(hook) {
  var prev = currentComponent,
    cleanup = hook.__c;
  if (typeof cleanup === "function") {
    hook.__c = undefined;
    cleanup();
  }
  currentComponent = prev;
}
function invokeEffect(hook) {
  var prev = currentComponent;
  hook.__c = hook.__();
  currentComponent = prev;
}
function argsChanged(oldArgs, newArgs) {
  return (
    !oldArgs ||
    oldArgs.length !== newArgs.length ||
    newArgs.some(function (arg, idx) {
      return arg !== oldArgs[idx];
    })
  );
}
function invokeOrReturn(arg, fn) {
  return typeof fn === "function" ? fn(arg) : fn;
}

// ---------------------------------------------------------------------------
// Miro-Ego Device API v1 Contract Validators & Constants
// ---------------------------------------------------------------------------

const SUPPORTED_DEVICE_API = Object.freeze({
  supported_device_api_majors: [1],
});

const STORAGE_KEYS = Object.freeze({
  accessToken: "miro-ego.access-token",
  accessTokenAlt: "miro-ego-access-token",
  csrfToken: "miro-ego.csrf-token",
  previewTransport: "miro-ego.preview-transport",
  focusPeaking: "miro-ego.focus-peaking",
  focusPeakingThreshold: "miro-ego.focus-peaking-threshold",
  inspectMode: "miro-ego.inspect-mode",
});

const UUID_V4_CI_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const UUID_V7_CI_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const UUID_V4_LOWER_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const UUID_V7_LOWER_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const SHA256_LOWER_RE = /^[0-9a-f]{64}$/;
const SHA256_PREFIXED_RE = /^sha256:[0-9a-f]{64}$/;
const DEVICE_LABEL_RE = /^Miro-Ego-[0-9A-F]{8}$/;
const IFACE_NAME_RE = /^[A-Za-z0-9_.:-]+$/;
const MDNS_HOST_RE = /^[A-Za-z0-9_.-]{1,122}\.local$/;
const MDNS_SVC_RE = /^[A-Za-z0-9_.-]{1,128}$/;
const CRED_REF_RE = /^cred-[A-Za-z0-9_.:-]+$/;
const LOG_NAME_RE = /^[A-Za-z0-9][A-Za-z0-9._-]*(?:\/[A-Za-z0-9][A-Za-z0-9._-]*)*$/;

const NET_MODES = new Set(["hotspot", "wifi-client", "ethernet-dhcp", "ethernet-static"]);
const WIFI_SECURITIES = new Set(["open", "wpa2-personal", "wpa3-personal", "wpa2-wpa3-personal"]);
const IFACE_STATES = new Set([
  "disabled",
  "disconnected",
  "starting",
  "connecting",
  "connected",
  "active",
  "degraded",
  "failed",
  "unavailable",
]);
const IFACE_UP_STATES = new Set(["connected", "active", "degraded"]);
const NET_TX_OPS = new Set(["apply", "retry", "forget"]);
const NET_TX_STATUSES = new Set(["accepted", "running", "committed", "rescued", "failed"]);
const NET_TX_STAGES = new Set([
  "accepted",
  "prepared",
  "ap_ready",
  "activating",
  "verifying",
  "committed",
  "falling_back",
  "rescued",
  "failed",
  "forgetting",
  "forgotten",
]);
const NET_RECOVERY_ACTIONS = new Set([
  "await_device",
  "reconnect_target_lan",
  "reconnect_rescue_ap",
  "retry",
  "service_required",
  "none",
]);
const NET_TX_ERROR_CODES = new Set([
  "rescue_ap_unavailable",
  "credential_rejected",
  "dhcp_timeout",
  "route_lost",
  "network_manager_unavailable",
  "concurrency_unsupported",
]);
const NET_MUTATION_DISABLED_REASONS = new Set([
  "not_enabled",
  "auth_profile_unavailable",
  "controller_unavailable",
  "network_manager_unavailable",
  "rescue_ap_not_validated",
  "capture_active",
  "recovery_required",
  "maintenance_window_closed",
  "unsupported_concurrency",
]);

function isPlainObject(v) {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

function hasExactKeysArray(obj, keys) {
  const actual = Object.keys(obj);
  return actual.length === keys.length && actual.every((k) => keys.includes(k));
}

function hasExactKeySet(obj, keySet) {
  return (
    isPlainObject(obj) &&
    Object.keys(obj).length === keySet.size &&
    Object.keys(obj).every((k) => keySet.has(k))
  );
}

function isSafeIntAtLeast(v, min = Number.MIN_SAFE_INTEGER) {
  return Number.isSafeInteger(v) && v >= min;
}

function isParseableDate(v) {
  return typeof v === "string" && v.length > 0 && !Number.isNaN(Date.parse(v));
}

function isIsoDateTime(v) {
  return typeof v === "string" && v.includes("T") && Number.isFinite(Date.parse(v));
}

function isBoundedString(v, min, max) {
  return typeof v === "string" && v.length >= min && v.length <= max;
}

function utf8ByteLength(v) {
  return new TextEncoder().encode(v).byteLength;
}

function isValidSsid(v) {
  return typeof v === "string" && v.length > 0 && utf8ByteLength(v) <= 32;
}

function isValidIpv4(v) {
  if (typeof v !== "string") return false;
  const parts = v.split(".");
  return (
    parts.length === 4 &&
    parts.every(
      (p) =>
        /^\d{1,3}$/.test(p) &&
        String(Number(p)) === p &&
        Number(p) >= 0 &&
        Number(p) <= 255
    )
  );
}

function isValidStaticIpv4(v) {
  if (
    !isPlainObject(v) ||
    !hasExactKeysArray(v, ["address", "prefix_length", "gateway", "dns"]) ||
    !isValidIpv4(v.address) ||
    !isSafeIntAtLeast(v.prefix_length, 1) ||
    v.prefix_length > 32 ||
    (v.gateway !== null && !isValidIpv4(v.gateway)) ||
    !Array.isArray(v.dns) ||
    v.dns.length > 3 ||
    !v.dns.every(isValidIpv4)
  ) {
    return false;
  }
  return new Set(v.dns).size === v.dns.length;
}

function isValidDesiredEthernet(v) {
  if (!isPlainObject(v) || !hasExactKeysArray(v, ["addressing", "static_ipv4"])) return false;
  return (
    (v.addressing === "dhcp" && v.static_ipv4 === null) ||
    (v.addressing === "static" && isValidStaticIpv4(v.static_ipv4))
  );
}

function isValidDesiredWifiClient(v) {
  return (
    isPlainObject(v) &&
    hasExactKeysArray(v, ["ssid", "security", "credential_state"]) &&
    isValidSsid(v.ssid) &&
    WIFI_SECURITIES.has(v.security) &&
    new Set(["absent", "pending_input", "stored"]).has(String(v.credential_state))
  );
}

function isValidNetworkDesiredState(v) {
  if (
    !isPlainObject(v) ||
    !hasExactKeysArray(v, ["mode", "wifi_client", "ethernet"]) ||
    !NET_MODES.has(String(v.mode)) ||
    (v.wifi_client !== null && !isValidDesiredWifiClient(v.wifi_client)) ||
    (v.ethernet !== null && !isValidDesiredEthernet(v.ethernet))
  ) {
    return false;
  }
  return !(
    (v.mode === "wifi-client" && v.wifi_client === null) ||
    (v.mode === "ethernet-static" && v.ethernet === null)
  );
}

function isValidNetworkApplyDesiredState(v) {
  if (
    !isPlainObject(v) ||
    !hasExactKeysArray(v, ["mode", "wifi_client", "ethernet"]) ||
    !NET_MODES.has(String(v.mode)) ||
    (v.ethernet !== null && !isValidDesiredEthernet(v.ethernet))
  ) {
    return false;
  }
  if (v.mode !== "wifi-client") {
    if (v.wifi_client !== null) return false;
    if (v.mode === "hotspot") return v.ethernet === null;
    if (v.mode === "ethernet-dhcp") return v.ethernet === null || v.ethernet.addressing === "dhcp";
    if (v.mode === "ethernet-static") return v.ethernet !== null && v.ethernet.addressing === "static";
    return false;
  }
  if (v.ethernet !== null) return false;
  const wf = v.wifi_client;
  if (!isPlainObject(wf) || !isValidSsid(wf.ssid) || !WIFI_SECURITIES.has(wf.security)) return false;
  const expectedKeys = wf.security === "open" ? ["ssid", "security"] : ["ssid", "security", "credential_ref"];
  return (
    hasExactKeysArray(wf, expectedKeys) &&
    (wf.security === "open" || (isBoundedString(wf.credential_ref, 1, 128) && CRED_REF_RE.test(wf.credential_ref)))
  );
}

function isValidInterfaceStatus(v) {
  if (
    !isPlainObject(v) ||
    !hasExactKeysArray(v, ["state", "interface", "addresses", "peer_or_ssid"]) ||
    !IFACE_STATES.has(String(v.state)) ||
    (v.interface !== null && (!isBoundedString(v.interface, 1, 64) || !IFACE_NAME_RE.test(v.interface))) ||
    !Array.isArray(v.addresses) ||
    !v.addresses.every((a) => isBoundedString(a, 1, 64)) ||
    new Set(v.addresses).size !== v.addresses.length ||
    (v.peer_or_ssid !== null && !isBoundedString(v.peer_or_ssid, 1, 128))
  ) {
    return false;
  }
  return !(IFACE_UP_STATES.has(String(v.state)) && (v.interface === null || v.addresses.length === 0));
}

function isValidNetworkTransaction(v) {
  if (
    !isPlainObject(v) ||
    !hasExactKeysArray(v, [
      "schema",
      "authority_epoch",
      "source_revision",
      "transaction_id",
      "operation",
      "status",
      "stage",
      "desired",
      "accepted_at",
      "updated_at",
      "deadline",
      "recovery_action",
      "rescue",
      "error",
    ]) ||
    v.schema !== "miro-ego.network-transaction.v1" ||
    typeof v.authority_epoch !== "string" ||
    !UUID_V4_CI_RE.test(v.authority_epoch) ||
    !isSafeIntAtLeast(v.source_revision, 0) ||
    typeof v.transaction_id !== "string" ||
    !UUID_V7_CI_RE.test(v.transaction_id) ||
    !NET_TX_OPS.has(String(v.operation)) ||
    !NET_TX_STATUSES.has(String(v.status)) ||
    !NET_TX_STAGES.has(String(v.stage)) ||
    !isValidNetworkDesiredState(v.desired) ||
    !isParseableDate(v.accepted_at) ||
    !isParseableDate(v.updated_at) ||
    !NET_RECOVERY_ACTIONS.has(String(v.recovery_action)) ||
    (v.deadline !== null &&
      (!isPlainObject(v.deadline) ||
        !hasExactKeysArray(v.deadline, ["time_base", "deadline_ns", "remaining_seconds"]) ||
        v.deadline.time_base !== "device_monotonic" ||
        !isSafeIntAtLeast(v.deadline.deadline_ns, 0) ||
        typeof v.deadline.remaining_seconds !== "number" ||
        !Number.isFinite(v.deadline.remaining_seconds) ||
        v.deadline.remaining_seconds < 0 ||
        v.deadline.remaining_seconds > 10))
  ) {
    return false;
  }
  const rescue = v.rescue;
  if (
    !isPlainObject(rescue) ||
    !hasExactKeysArray(rescue, ["ap_validated", "fallback_mode", "failure_trigger_seconds"]) ||
    typeof rescue.ap_validated !== "boolean" ||
    rescue.fallback_mode !== "hotspot" ||
    rescue.failure_trigger_seconds !== 10
  ) {
    return false;
  }
  const err = v.error;
  if (
    !(
      err === null ||
      (isPlainObject(err) &&
        hasExactKeysArray(err, ["code", "message", "retryable"]) &&
        NET_TX_ERROR_CODES.has(String(err.code)) &&
        isBoundedString(err.message, 1, 512) &&
        typeof err.retryable === "boolean")
    )
  ) {
    return false;
  }
  const st = String(v.status);
  const sg = String(v.stage);
  const terminal = new Set(["committed", "rescued", "failed"]).has(st);
  return !(
    (st === "accepted" && sg !== "accepted") ||
    (st === "committed" && !new Set(["committed", "forgotten"]).has(sg)) ||
    (st === "rescued" && sg !== "rescued") ||
    (st === "failed" && sg !== "failed") ||
    (new Set(["accepted", "running", "committed"]).has(st) && err !== null) ||
    (new Set(["rescued", "failed"]).has(st) && err === null) ||
    (terminal && v.deadline !== null) ||
    (new Set(["activating", "verifying"]).has(sg) && v.deadline === null) ||
    (st === "rescued" && v.recovery_action !== "reconnect_rescue_ap")
  );
}

function isValidNetworkObserved(v) {
  if (
    !isPlainObject(v) ||
    !hasExactKeysArray(v, ["ap", "wifi_client", "wired", "default_route", "mdns", "devices"]) ||
    !isValidInterfaceStatus(v.ap) ||
    !isValidInterfaceStatus(v.wifi_client) ||
    !isValidInterfaceStatus(v.wired) ||
    !new Set(["wifi_client", "wired", "none"]).has(String(v.default_route))
  ) {
    return false;
  }
  const mdns = v.mdns;
  if (
    !isPlainObject(mdns) ||
    !hasExactKeysArray(mdns, ["hostname", "service", "aliases", "port"]) ||
    typeof mdns.hostname !== "string" ||
    !MDNS_HOST_RE.test(mdns.hostname) ||
    typeof mdns.service !== "string" ||
    !MDNS_SVC_RE.test(mdns.service) ||
    !Array.isArray(mdns.aliases) ||
    mdns.aliases.length > 16 ||
    !mdns.aliases.every((a) => typeof a === "string" && MDNS_SVC_RE.test(a)) ||
    new Set(mdns.aliases).size !== mdns.aliases.length ||
    !isSafeIntAtLeast(mdns.port, 1) ||
    mdns.port > 65535 ||
    !Array.isArray(v.devices) ||
    v.devices.length > 64
  ) {
    return false;
  }
  const seen = new Set();
  for (const dev of v.devices) {
    if (
      !isPlainObject(dev) ||
      !hasExactKeysArray(dev, ["interface", "type", "state"]) ||
      ![dev.interface, dev.type, dev.state].every(
        (f) => typeof f === "string" && f.length <= 64 && IFACE_NAME_RE.test(f)
      ) ||
      seen.has(String(dev.interface))
    ) {
      return false;
    }
    seen.add(String(dev.interface));
  }
  return true;
}

function isValidNetworkMutationCapability(v) {
  if (
    !isPlainObject(v) ||
    !hasExactKeysArray(v, [
      "enabled",
      "disabled_reason",
      "operations",
      "idempotency_key_required",
      "secret_handling",
      "active_state_policy",
    ]) ||
    typeof v.enabled !== "boolean" ||
    !Array.isArray(v.operations) ||
    v.operations.join("\0") !== ["apply", "retry", "forget"].join("\0") ||
    v.idempotency_key_required !== true ||
    v.secret_handling !== "opaque_credential_reference_only" ||
    v.active_state_policy !== "idle_only"
  ) {
    return false;
  }
  return v.enabled
    ? v.disabled_reason === null
    : typeof v.disabled_reason === "string" && NET_MUTATION_DISABLED_REASONS.has(v.disabled_reason);
}

function isValidNetworkConcurrencyCapability(v) {
  return (
    isPlainObject(v) &&
    hasExactKeysArray(v, [
      "rescue_ap_required",
      "same_phy_ap_sta",
      "exclusive_client_failure_timeout_seconds",
      "max_managed_interfaces",
      "max_ap_interfaces",
    ]) &&
    v.rescue_ap_required === true &&
    new Set(["supported", "unsupported", "unverified"]).has(String(v.same_phy_ap_sta)) &&
    v.exclusive_client_failure_timeout_seconds === 10 &&
    isSafeIntAtLeast(v.max_managed_interfaces, 0) &&
    v.max_managed_interfaces <= 8 &&
    isSafeIntAtLeast(v.max_ap_interfaces, 0) &&
    v.max_ap_interfaces <= 8
  );
}

function isValidNetworkStatus(v) {
  if (
    !isPlainObject(v) ||
    !hasExactKeysArray(v, [
      "schema",
      "authority_epoch",
      "source_revision",
      "observed_at",
      "saved",
      "verified",
      "desired",
      "observed",
      "transaction",
      "mutation_capability",
      "concurrency_capability",
    ]) ||
    v.schema !== "miro-ego.network-status.v1" ||
    typeof v.authority_epoch !== "string" ||
    !UUID_V4_CI_RE.test(v.authority_epoch) ||
    !isSafeIntAtLeast(v.source_revision, 0) ||
    !isParseableDate(v.observed_at) ||
    typeof v.saved !== "boolean" ||
    typeof v.verified !== "boolean" ||
    (v.verified && !v.saved) ||
    !isValidNetworkDesiredState(v.desired) ||
    !isValidNetworkObserved(v.observed) ||
    !isValidNetworkMutationCapability(v.mutation_capability) ||
    !isValidNetworkConcurrencyCapability(v.concurrency_capability)
  ) {
    return false;
  }
  const tx = v.transaction;
  if (!isPlainObject(tx) || !hasExactKeysArray(tx, ["current", "latest"])) return false;
  const rev = v.source_revision;
  return (
    [tx.current, tx.latest].every(
      (item) =>
        item === null ||
        (isValidNetworkTransaction(item) &&
          item.authority_epoch === v.authority_epoch &&
          item.source_revision <= rev)
    ) &&
    (tx.current === null ||
      (isValidNetworkTransaction(tx.current) && new Set(["accepted", "running"]).has(tx.current.status)))
  );
}

function isValidScanEntry(v) {
  if (
    !isPlainObject(v) ||
    !hasExactKeysArray(v, ["ssid", "hidden", "security", "signal_dbm", "credential_required"]) ||
    typeof v.hidden !== "boolean" ||
    !WIFI_SECURITIES.has(v.security) ||
    !isSafeIntAtLeast(v.signal_dbm, -127) ||
    v.signal_dbm > 0 ||
    typeof v.credential_required !== "boolean"
  ) {
    return false;
  }
  return (
    (v.hidden ? v.ssid === null : isValidSsid(v.ssid)) &&
    (v.security === "open" ? v.credential_required === false : v.credential_required === true)
  );
}

function isValidNetworkScan(v) {
  return (
    isPlainObject(v) &&
    hasExactKeysArray(v, ["schema", "authority_epoch", "source_revision", "scanned_at", "networks"]) &&
    v.schema === "miro-ego.network-scan.v1" &&
    typeof v.authority_epoch === "string" &&
    UUID_V4_CI_RE.test(v.authority_epoch) &&
    isSafeIntAtLeast(v.source_revision, 0) &&
    isParseableDate(v.scanned_at) &&
    Array.isArray(v.networks) &&
    v.networks.length <= 256 &&
    v.networks.every(isValidScanEntry)
  );
}

function isValidCredentialReceipt(v) {
  return (
    isPlainObject(v) &&
    hasExactKeysArray(v, [
      "schema",
      "credential_ref",
      "issued_at",
      "expires_at",
      "ttl_seconds",
      "single_use",
    ]) &&
    v.schema === "miro-ego.network-credential-receipt.v1" &&
    isBoundedString(v.credential_ref, 1, 128) &&
    CRED_REF_RE.test(v.credential_ref) &&
    isParseableDate(v.issued_at) &&
    isParseableDate(v.expires_at) &&
    isSafeIntAtLeast(v.ttl_seconds, 1) &&
    v.ttl_seconds <= 120 &&
    v.single_use === true
  );
}

function isValidNetworkTransactionReceipt(v) {
  return (
    isPlainObject(v) &&
    hasExactKeysArray(v, ["schema", "accepted_at", "transaction"]) &&
    v.schema === "miro-ego.network-transaction-receipt.v1" &&
    isParseableDate(v.accepted_at) &&
    isValidNetworkTransaction(v.transaction) &&
    v.transaction.status === "accepted" &&
    v.accepted_at === v.transaction.accepted_at
  );
}

function isValidNetworkEvent(v) {
  if (
    !isPlainObject(v) ||
    !hasExactKeysArray(v, [
      "schema",
      "sse_delivery_id",
      "authority_epoch",
      "source_revision",
      "occurred_at",
      "type",
      "transaction_id",
      "data",
    ]) ||
    v.schema !== "miro-ego.network-event.v1" ||
    typeof v.sse_delivery_id !== "string" ||
    !/^\d+$/.test(v.sse_delivery_id) ||
    typeof v.authority_epoch !== "string" ||
    !UUID_V4_CI_RE.test(v.authority_epoch) ||
    !isSafeIntAtLeast(v.source_revision, 0) ||
    !isParseableDate(v.occurred_at)
  ) {
    return false;
  }
  if (v.type === "snapshot") {
    return (
      v.transaction_id === null &&
      isValidNetworkStatus(v.data) &&
      v.data.authority_epoch === v.authority_epoch &&
      v.data.source_revision === v.source_revision
    );
  }
  return (
    v.type === "transaction" &&
    typeof v.transaction_id === "string" &&
    UUID_V7_CI_RE.test(v.transaction_id) &&
    isValidNetworkTransaction(v.data) &&
    v.data.transaction_id === v.transaction_id &&
    v.data.authority_epoch === v.authority_epoch &&
    v.data.source_revision === v.source_revision
  );
}

// Session list v1 validators
const SESSION_LIST_KEYS = new Set(["schema", "catalog_revision", "items", "diagnostics", "next_cursor"]);
const SESSION_SUMMARY_KEYS = new Set([
  "session_id",
  "producer_outcome",
  "take_id",
  "take_sequence",
  "continuation_of",
  "display_name",
  "device",
  "started_at",
  "ended_at",
  "duration_seconds",
  "total_bytes",
  "verification",
]);
const SESSION_SUMMARY_RECOVERED_KEYS = new Set([...SESSION_SUMMARY_KEYS, "recovered_prefix"]);
const DEVICE_ID_KEYS = new Set(["device_id", "device_label"]);
const VERIFICATION_KEYS = new Set([
  "actor",
  "validator",
  "manifest_sha256",
  "verified_at",
  "verdict",
  "diagnostics",
]);
const VALIDATOR_KEYS = new Set(["name", "version", "build_sha256"]);
const DISCOVERY_DIAG_KEYS = new Set(["quarantine_id", "code", "observed_at", "message"]);
const VERIF_DIAG_KEYS = new Set(["code", "summary"]);
const DISCOVERY_DIAG_CODES = new Set([
  "manifest_unreadable",
  "unsupported_schema",
  "manifest_invalid",
  "manifest_not_sealed",
]);
const VERIF_DIAG_CODES = new Set([
  "artifact_digest_mismatch",
  "artifact_invalid",
  "manifest_invalid",
  "verification_failed",
  "recording_interrupted",
]);

function isValidDeviceIdentity(v) {
  return (
    hasExactKeySet(v, DEVICE_ID_KEYS) &&
    typeof v.device_id === "string" &&
    UUID_V4_LOWER_RE.test(v.device_id) &&
    typeof v.device_label === "string" &&
    DEVICE_LABEL_RE.test(v.device_label)
  );
}

function isValidGatewayVerification(v) {
  if (!hasExactKeySet(v, VERIFICATION_KEYS) || !Array.isArray(v.diagnostics)) return false;
  const val = v.validator;
  const verdict = v.verdict;
  const baseOk =
    v.actor === "gateway" &&
    hasExactKeySet(val, VALIDATOR_KEYS) &&
    isBoundedString(val.name, 1, 128) &&
    isBoundedString(val.version, 1, 64) &&
    typeof val.build_sha256 === "string" &&
    SHA256_LOWER_RE.test(val.build_sha256) &&
    typeof v.manifest_sha256 === "string" &&
    SHA256_LOWER_RE.test(v.manifest_sha256) &&
    isIsoDateTime(v.verified_at) &&
    (verdict === "usable" || verdict === "unusable");
  if (!baseOk) return false;
  return (
    v.diagnostics.every(
      (d) =>
        hasExactKeySet(d, VERIF_DIAG_KEYS) &&
        typeof d.code === "string" &&
        VERIF_DIAG_CODES.has(d.code) &&
        isBoundedString(d.summary, 1, 512)
    ) && (verdict !== "unusable" || v.diagnostics.length > 0)
  );
}

function isValidSessionSummary(v) {
  const keysOk =
    hasExactKeySet(v, SESSION_SUMMARY_KEYS) ||
    (hasExactKeySet(v, SESSION_SUMMARY_RECOVERED_KEYS) && v.recovered_prefix === true);
  if (!keysOk) return false;
  return (
    typeof v.session_id === "string" &&
    UUID_V7_LOWER_RE.test(v.session_id) &&
    v.producer_outcome === "sealed" &&
    typeof v.take_id === "string" &&
    UUID_V7_LOWER_RE.test(v.take_id) &&
    Number.isSafeInteger(v.take_sequence) &&
    Number(v.take_sequence) >= 1 &&
    (v.continuation_of === null ||
      (typeof v.continuation_of === "string" && UUID_V7_LOWER_RE.test(v.continuation_of))) &&
    isBoundedString(v.display_name, 1, 160) &&
    isValidDeviceIdentity(v.device) &&
    isIsoDateTime(v.started_at) &&
    isIsoDateTime(v.ended_at) &&
    typeof v.duration_seconds === "number" &&
    Number.isFinite(v.duration_seconds) &&
    v.duration_seconds >= 0 &&
    Number.isSafeInteger(v.total_bytes) &&
    Number(v.total_bytes) >= 0 &&
    (v.verification === null || isValidGatewayVerification(v.verification))
  );
}

function isValidDiscoveryDiagnostic(v) {
  return (
    hasExactKeySet(v, DISCOVERY_DIAG_KEYS) &&
    typeof v.quarantine_id === "string" &&
    UUID_V4_LOWER_RE.test(v.quarantine_id) &&
    typeof v.code === "string" &&
    DISCOVERY_DIAG_CODES.has(v.code) &&
    isIsoDateTime(v.observed_at) &&
    isBoundedString(v.message, 1, 512)
  );
}

function isValidSessionList(v) {
  return (
    hasExactKeySet(v, SESSION_LIST_KEYS) &&
    v.schema === "miro-ego.session-list.v1" &&
    typeof v.catalog_revision === "string" &&
    SHA256_PREFIXED_RE.test(v.catalog_revision) &&
    Array.isArray(v.items) &&
    v.items.every(isValidSessionSummary) &&
    Array.isArray(v.diagnostics) &&
    v.diagnostics.every(isValidDiscoveryDiagnostic) &&
    (v.next_cursor === null || (typeof v.next_cursor === "string" && v.next_cursor.length > 0))
  );
}

// SafeSwapReceipt validator
const SAFE_SWAP_RECEIPT_KEYS = new Set([
  "schema",
  "session_id",
  "volume_id",
  "generation_id",
  "manifest_id",
  "manifest_sha256",
  "sealed_at",
  "released_at",
  "release_state",
  "open_handle_count",
]);

function isValidSafeSwapReceipt(v) {
  return (
    hasExactKeySet(v, SAFE_SWAP_RECEIPT_KEYS) &&
    v.schema === "miro-ego.safe-swap-receipt.v1" &&
    typeof v.session_id === "string" &&
    UUID_V7_CI_RE.test(v.session_id) &&
    typeof v.volume_id === "string" &&
    UUID_V4_CI_RE.test(v.volume_id) &&
    typeof v.generation_id === "string" &&
    UUID_V4_CI_RE.test(v.generation_id) &&
    typeof v.manifest_id === "string" &&
    UUID_V7_CI_RE.test(v.manifest_id) &&
    typeof v.manifest_sha256 === "string" &&
    SHA256_LOWER_RE.test(v.manifest_sha256) &&
    isParseableDate(v.sealed_at) &&
    isParseableDate(v.released_at) &&
    (v.release_state === "unmounted" || v.release_state === "device-released") &&
    v.open_handle_count === 0
  );
}

function validateSafeSwapResource(v) {
  if (
    !isPlainObject(v) ||
    !hasExactKeysArray(v, ["schema", "receipt"]) ||
    v.schema !== "miro-ego.safe-swap-receipt-resource.v1" ||
    !isValidSafeSwapReceipt(v.receipt)
  ) {
    throw new DeviceApiError("安全弹出回执不符合 v1 契约", 502, "unsupported_device_api_schema");
  }
  return v.receipt;
}

// LogList validator
const LOG_MEDIA_TYPES = new Set([
  "text/plain",
  "application/x-ndjson",
  "application/gzip",
  "application/octet-stream",
]);

function isValidLogItem(v) {
  if (!isPlainObject(v)) return false;
  const keys = Object.keys(v);
  if (!keys.every((k) => ["name", "bytes", "modified_at", "media_type", "sha256"].includes(k))) {
    return false;
  }
  return (
    isBoundedString(v.name, 1, 255) &&
    LOG_NAME_RE.test(v.name) &&
    isSafeIntAtLeast(v.bytes, 0) &&
    isParseableDate(v.modified_at) &&
    LOG_MEDIA_TYPES.has(String(v.media_type)) &&
    (v.sha256 === undefined || v.sha256 === null || (typeof v.sha256 === "string" && SHA256_LOWER_RE.test(v.sha256)))
  );
}

function validateLogList(v) {
  if (
    !isPlainObject(v) ||
    !hasExactKeysArray(v, ["schema", "volume_id", "observed_at", "items", "truncated"]) ||
    v.schema !== "miro-ego.log-list.v1" ||
    (v.volume_id !== null && (typeof v.volume_id !== "string" || !UUID_V4_CI_RE.test(v.volume_id))) ||
    !isParseableDate(v.observed_at) ||
    !Array.isArray(v.items) ||
    !v.items.every(isValidLogItem) ||
    typeof v.truncated !== "boolean"
  ) {
    throw new DeviceApiError("诊断日志列表不符合 v1 契约", 502, "unsupported_device_api_schema");
  }
  return v;
}

export function encodeLogPath(name) {
  return String(name)
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

// ---------------------------------------------------------------------------
// API Error, Headers, Token Storage & HTTP Client
// ---------------------------------------------------------------------------

class DeviceApiError extends Error {
  status;
  code;
  details;
  retryAfterSeconds;
  waitState;
  constructor(message, status, code = `http_${status}`, details = {}, retryAfterSeconds = null, waitState = null) {
    super(message);
    this.name = "DeviceApiError";
    this.status = status;
    this.code = code;
    this.details = details;
    this.retryAfterSeconds = retryAfterSeconds;
    this.waitState = waitState;
  }
}

let auditStatusSubscribers = new Set();
let latestAuditUnavailable = false;
function observeResponseAuditHeader(response) {
  const unavailable = response.headers.get("Miro-Ego-Audit-Status") === "unavailable";
  if (unavailable !== latestAuditUnavailable) {
    latestAuditUnavailable = unavailable;
    for (const cb of auditStatusSubscribers) cb(unavailable);
  }
}

function parseApiMajor(ver) {
  if (typeof ver !== "string") return null;
  const m = ver.match(/^(\d+)(?:\.|$)/);
  return m ? Number(m[1]) : null;
}

const CAPABILITY_KEYS = new Set([
  "capture",
  "preview",
  "preview_h264",
  "diagnostic_logs",
  "range_download",
  "network_mutation",
  "session_list",
  "session_detail",
  "artifact_download",
  "capture_status",
  "session_deletion",
  "calibration_capture",
]);
const CAL_CAP_KEYS = new Set(["supported", "enabled", "disabled_reason", "required_video_layout"]);
const CAL_DISABLED_REASONS = new Set([
  "capture_source_unsupported",
  "storage_unavailable",
  "hardware_unavailable",
  "maintenance_or_capture_busy",
]);

function validateDeviceCapabilities(doc) {
  const cap = doc.capabilities;
  const cal = hasExactKeySet(cap, CAPABILITY_KEYS) ? cap.calibration_capture : null;
  const calKeysOk = hasExactKeySet(cal, CAL_CAP_KEYS);
  const capOk =
    hasExactKeySet(cap, CAPABILITY_KEYS) &&
    typeof cap.capture === "boolean" &&
    typeof cap.preview === "boolean" &&
    typeof cap.preview_h264 === "boolean" &&
    typeof cap.diagnostic_logs === "boolean" &&
    cap.range_download === true &&
    typeof cap.network_mutation === "boolean" &&
    cap.session_list === true &&
    cap.session_detail === true &&
    cap.artifact_download === true &&
    cap.capture_status === true &&
    typeof cap.session_deletion === "boolean";
  const calOk =
    calKeysOk &&
    typeof cal.supported === "boolean" &&
    typeof cal.enabled === "boolean" &&
    cal.required_video_layout === "split-eyes" &&
    (cal.enabled === true
      ? cal.supported === true && cal.disabled_reason === null
      : typeof cal.disabled_reason === "string" && CAL_DISABLED_REASONS.has(cal.disabled_reason));
  if (!capOk || !calOk) {
    throw new DeviceApiError("Device API capabilities 不符合 v1 契约", 502, "unsupported_device_api_schema", {
      field: "capabilities",
    });
  }
}

function validateDeviceDescriptor(doc) {
  const major = parseApiMajor(doc?.api_version);
  if (
    !isPlainObject(doc) ||
    major === null ||
    !SUPPORTED_DEVICE_API.supported_device_api_majors.includes(major) ||
    doc.api_version !== "1.0" ||
    doc.schema !== "miro-ego.device.v1" ||
    !isValidDeviceIdentity(doc.device)
  ) {
    throw new DeviceApiError("不支持的 Device API 版本或身份描述", 426, "unsupported_device_api_major", {
      api_version: doc?.api_version ?? null,
      schema: doc?.schema ?? null,
      supported_device_api_majors: [...SUPPORTED_DEVICE_API.supported_device_api_majors],
    });
  }
  validateDeviceCapabilities(doc);
  return doc;
}

function validateCaptureStatus(doc) {
  if (
    doc &&
    (doc.schema !== "miro-ego.capture-status.v1" ||
      doc.snapshot?.schema !== "miro-ego.capture-snapshot-event.v1")
  ) {
    throw new DeviceApiError("不支持的 Device API capture status schema", 502, "unsupported_device_api_schema", {
      schema: doc.schema,
      snapshot_schema: doc.snapshot?.schema ?? null,
    });
  }
  return doc;
}

function validateNetworkStatusOrNull(doc) {
  if (doc !== null && !isValidNetworkStatus(doc)) {
    throw new DeviceApiError("Device API network status 不符合 v1 契约", 502, "unsupported_device_api_schema", {
      schema: doc?.schema ?? null,
    });
  }
  return doc;
}

function validateNetworkScan(doc) {
  if (!isValidNetworkScan(doc)) {
    throw new DeviceApiError("Device API network scan 不符合 v1 契约", 502, "unsupported_device_api_schema");
  }
  return doc;
}

function validateCredentialReceipt(doc) {
  if (!isValidCredentialReceipt(doc)) {
    throw new DeviceApiError(
      "Device API network credential receipt 不符合 v1 契约",
      502,
      "unsupported_device_api_schema"
    );
  }
  return doc;
}

function validateNetworkTxReceipt(doc) {
  if (!isValidNetworkTransactionReceipt(doc)) {
    throw new DeviceApiError(
      "Device API network transaction receipt 不符合 v1 契约",
      502,
      "unsupported_device_api_schema"
    );
  }
  return doc;
}

function validateSessionList(doc) {
  if (!isValidSessionList(doc)) {
    throw new DeviceApiError("Device API session list 不符合 v1 契约", 502, "unsupported_device_api_schema", {
      schema: typeof doc?.schema === "string" ? doc.schema : null,
    });
  }
  return doc;
}

const DELETE_RESULT_KEYS = new Set(["schema", "deleted_session_ids", "failed_sessions"]);
const FAILED_SESSION_KEYS = new Set(["session_id", "error"]);
function validateDeleteResult(doc) {
  if (
    !(
      hasExactKeySet(doc, DELETE_RESULT_KEYS) &&
      doc.schema === "miro-ego.session-delete-result.v1" &&
      Array.isArray(doc.deleted_session_ids) &&
      new Set(doc.deleted_session_ids).size === doc.deleted_session_ids.length &&
      doc.deleted_session_ids.every((id) => typeof id === "string") &&
      Array.isArray(doc.failed_sessions) &&
      doc.failed_sessions.every(
        (item) =>
          hasExactKeySet(item, FAILED_SESSION_KEYS) &&
          typeof item.session_id === "string" &&
          typeof item.error === "string"
      )
    )
  ) {
    throw new DeviceApiError("Device API session deletion result 不符合 v1 契约", 502, "unsupported_device_api_schema");
  }
  return doc;
}

async function parseErrorResponse(response) {
  observeResponseAuditHeader(response);
  let parsed = null;
  try {
    parsed = await response.json();
  } catch {}
  const errObj = parsed?.schema === "miro-ego.api-error.v1" ? parsed.error : null;
  const headerCode = response.headers.get("Miro-Ego-Error-Code")?.trim() || null;
  const waitState = response.headers.get("Miro-Ego-Wait-State")?.trim() || null;
  const retryRaw = response.headers.get("Retry-After")?.trim() || null;
  const retryAfter = retryRaw && /^\d+$/.test(retryRaw) ? Number(retryRaw) : null;
  const code =
    typeof errObj?.code === "string" ? errObj.code : headerCode || `http_${response.status}`;
  const message =
    typeof errObj?.message === "string" ? errObj.message : `设备接口返回 ${response.status}`;
  const details =
    errObj?.details && typeof errObj.details === "object" ? errObj.details : {};
  return new DeviceApiError(message, response.status, code, details, retryAfter, waitState);
}

function abortableDelay(ms, signal) {
  return new Promise((resolve) => {
    let timer = null;
    const finish = () => {
      if (timer !== null) window.clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    };
    if (signal.aborted) {
      finish();
      return;
    }
    timer = window.setTimeout(finish, ms);
    signal.addEventListener("abort", finish, { once: true });
  });
}

function bytesToHex(bytes) {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

function newIdempotencyKey() {
  const cryptoObj = globalThis.crypto;
  if (typeof cryptoObj?.randomUUID === "function") return cryptoObj.randomUUID().toLowerCase();
  const buf = new Uint8Array(16);
  if (typeof cryptoObj?.getRandomValues === "function") {
    cryptoObj.getRandomValues(buf);
  } else {
    const seed = `${Date.now()}-${Math.random()}-${performance.now()}`;
    for (let i = 0; i < buf.length; i += 1) {
      buf[i] = (seed.charCodeAt(i % seed.length) ^ Math.floor(Math.random() * 256)) & 255;
    }
  }
  buf[6] = ((buf[6] ?? 0) & 15) | 64;
  buf[8] = ((buf[8] ?? 0) & 63) | 128;
  const hex = bytesToHex(buf);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function readAccessToken() {
  try {
    const fromSession =
      sessionStorage.getItem(STORAGE_KEYS.accessToken)?.trim() ||
      sessionStorage.getItem(STORAGE_KEYS.accessTokenAlt)?.trim();
    if (fromSession) return fromSession;
  } catch {}
  try {
    return (
      localStorage.getItem(STORAGE_KEYS.accessToken)?.trim() ||
      localStorage.getItem(STORAGE_KEYS.accessTokenAlt)?.trim() ||
      null
    );
  } catch {
    return null;
  }
}

function readCsrfToken() {
  try {
    const fromSession = sessionStorage.getItem(STORAGE_KEYS.csrfToken)?.trim();
    if (fromSession) return fromSession;
  } catch {}
  try {
    return localStorage.getItem(STORAGE_KEYS.csrfToken)?.trim() || null;
  } catch {
    return null;
  }
}

function saveCredentials(accessToken, csrfToken = "") {
  try {
    const trimmed = accessToken.trim();
    if (trimmed) {
      sessionStorage.setItem(STORAGE_KEYS.accessToken, trimmed);
      sessionStorage.setItem(STORAGE_KEYS.accessTokenAlt, trimmed);
    }
    const trimmedCsrf = csrfToken.trim();
    if (trimmedCsrf) {
      sessionStorage.setItem(STORAGE_KEYS.csrfToken, trimmedCsrf);
    } else {
      sessionStorage.removeItem(STORAGE_KEYS.csrfToken);
      try {
        localStorage.removeItem(STORAGE_KEYS.csrfToken);
      } catch {}
    }
  } catch {}
}

function readPref(key, fallback) {
  try {
    const val = localStorage.getItem(key);
    return val !== null ? val : fallback;
  } catch {
    return fallback;
  }
}

function writePref(key, value) {
  try {
    localStorage.setItem(key, String(value));
  } catch {}
}

function buildHeaders(accept, extra, isMutation = false) {
  const headers = new Headers(extra);
  headers.set("Accept", accept);
  const bearer = readAccessToken();
  const explicitCsrf = readCsrfToken();
  if (bearer) {
    headers.set("Authorization", `Bearer ${bearer}`);
  }
  if (isMutation) {
    const csrf = explicitCsrf || bearer;
    if (csrf) headers.set("X-CSRF-Token", csrf);
  }
  return headers;
}

async function apiJson(path, init = {}) {
  const method = (init.method ?? "GET").toUpperCase();
  const response = await fetch(`${API_PREFIX}${path}`, {
    ...init,
    cache: "no-store",
    headers: buildHeaders("application/json", init.headers, method === "POST"),
  });
  observeResponseAuditHeader(response);
  if (!response.ok) throw await parseErrorResponse(response);
  return response.status === 204 ? null : await response.json();
}

async function apiJsonOptional(path, init = {}) {
  const response = await fetch(`${API_PREFIX}${path}`, {
    ...init,
    cache: "no-store",
    headers: buildHeaders("application/json", init.headers),
  });
  observeResponseAuditHeader(response);
  if (response.status === 404) return null;
  if (!response.ok) throw await parseErrorResponse(response);
  return await response.json();
}

function commandInitWithKey(bodyObj, key) {
  return {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": key,
    },
    body: JSON.stringify(bodyObj),
  };
}

function commandInit(bodyObj) {
  return commandInitWithKey(bodyObj, newIdempotencyKey());
}

async function triggerAuthenticatedDownload(url, suggestedFilename) {
  const response = await fetch(url, {
    cache: "no-store",
    headers: buildHeaders("*/*"),
  });
  observeResponseAuditHeader(response);
  if (!response.ok) throw await parseErrorResponse(response);
  const blob = await response.blob();
  const blobUrl = URL.createObjectURL(blob);
  try {
    const anchor = document.createElement("a");
    anchor.href = blobUrl;
    anchor.download = suggestedFilename || "download";
    anchor.rel = "noreferrer";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
  } finally {
    window.setTimeout(() => URL.revokeObjectURL(blobUrl), 10000);
  }
}

// ---------------------------------------------------------------------------
// Boot-Scoped Clock Synchronization (/clock, /clock/check, /clock/sync)
// ---------------------------------------------------------------------------

function validateClockStatus(doc) {
  if (
    !isPlainObject(doc) ||
    doc.schema !== "miro-ego.clock-status.v1" ||
    !Number.isSafeInteger(doc.unix_time_ms) ||
    doc.unix_time_ms < 0 ||
    typeof doc.applied !== "boolean" ||
    !["ntp", "client", "unsynchronized"].includes(doc.source) ||
    (doc.applied && doc.source !== "client") ||
    typeof doc.boot_id !== "string" ||
    doc.boot_id.length === 0
  ) {
    throw new DeviceApiError("设备校时响应无效", 502, "invalid_clock_status");
  }
  const hasChallenge =
    typeof doc.challenge === "string" &&
    /^[0-9a-f]{32}$/.test(doc.challenge) &&
    doc.expires_in_ms === 5000;
  const noChallenge = doc.challenge === null && doc.expires_in_ms === 0;
  if (!hasChallenge && !noChallenge) {
    throw new DeviceApiError("设备校时挑战令牌无效", 502, "invalid_clock_status");
  }
  const hasValidCalibrationMeta =
    Number.isSafeInteger(doc.synchronized_at_ms) &&
    Number(doc.synchronized_at_ms) >= 1704067200000 &&
    Number.isSafeInteger(doc.age_seconds) &&
    Number(doc.age_seconds) >= 0;
  const hasEmptyCalibrationMeta = doc.synchronized_at_ms === null && doc.age_seconds === null;
  if (doc.source === "unsynchronized") {
    if (!hasEmptyCalibrationMeta) {
      throw new DeviceApiError("未校时状态包含非法校准元数据", 502, "invalid_clock_status");
    }
  } else if (doc.source === "ntp") {
    if (!hasValidCalibrationMeta && !hasEmptyCalibrationMeta) {
      throw new DeviceApiError("已校时状态缺少有效校准元数据", 502, "invalid_clock_status");
    }
  } else if (!hasValidCalibrationMeta) {
    throw new DeviceApiError("已校时状态缺少有效校准元数据", 502, "invalid_clock_status");
  }
  return doc;
}

let clockStoreState = {
  status: null,
  observedAt: 0,
  error: "",
  pending: false,
  promptRequested: false,
};
const clockSubscribers = new Set();

function updateClockStore(patch) {
  clockStoreState = { ...clockStoreState, ...patch };
  for (const cb of clockSubscribers) cb(clockStoreState);
}

const ClockStore = {
  current: () => clockStoreState,
  requestPrompt: () => updateClockStore({ promptRequested: true }),
  clearPrompt: () => updateClockStore({ promptRequested: false }),
  subscribe: (cb) => {
    clockSubscribers.add(cb);
    return () => clockSubscribers.delete(cb);
  },
};

let inflightClockSync = null;

async function fetchClockInspect(signal) {
  let raw = await apiJsonOptional("/clock/check", { signal });
  if (raw === null) raw = await apiJsonOptional("/clock", { signal });
  return raw === null ? null : validateClockStatus(raw);
}

async function synchronizeDeviceClock(replaceExisting = false, readOnly = false) {
  if (inflightClockSync) {
    if (replaceExisting) throw new DeviceApiError("正在校时，请稍后重试", 409, "clock_busy");
    return inflightClockSync;
  }
  updateClockStore({ pending: true });
  inflightClockSync = (async () => {
    const controller = new AbortController();
    const timer = globalThis.setTimeout(() => controller.abort(), 5000);
    try {
      const startedPerf = performance.now();
      const inspected = await fetchClockInspect(controller.signal);
      updateClockStore({ status: inspected, observedAt: performance.now(), error: "" });
      if (!inspected || readOnly || (inspected.source !== "unsynchronized" && !replaceExisting)) {
        if (inspected && inspected.source !== "unsynchronized") {
          updateClockStore({ promptRequested: false });
        }
        return inspected;
      }
      if (!inspected.challenge) {
        throw new DeviceApiError("设备未返回校时挑战令牌，请重试", 409, "clock_challenge_missing");
      }
      if (performance.now() - startedPerf > 2000) {
        throw new DeviceApiError("连接延迟较高，请重试校准时间", 409, "clock_challenge_expired");
      }
      const clientNowMs = Date.now();
      if (clientNowMs < 1704067200000 || clientNowMs >= 4102444800000) {
        throw new DeviceApiError("请先校准这台手机或电脑的系统日期", 400, "client_clock_invalid");
      }
      const synced = validateClockStatus(
        await apiJson("/clock/sync", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          signal: controller.signal,
          body: JSON.stringify({
            schema: "miro-ego.clock-sync-request.v1",
            challenge: inspected.challenge,
            unix_time_ms: clientNowMs,
            replace: Boolean(replaceExisting),
          }),
        })
      );
      const verified = await fetchClockInspect(controller.signal);
      if (
        !verified ||
        verified.source === "unsynchronized" ||
        Math.abs(verified.unix_time_ms - Date.now()) > 2500 ||
        (synced.boot_id && synced.boot_id !== verified.boot_id)
      ) {
        throw new DeviceApiError("设备时间回读不一致，请检查本机日期并重试", 409, "clock_verify_failed");
      }
      updateClockStore({
        status: verified,
        observedAt: performance.now(),
        error: "",
        promptRequested: false,
      });
      return verified;
    } catch (err) {
      updateClockStore({
        error: err instanceof DeviceApiError ? err.message : "校时连接失败，请重试",
      });
      throw err;
    } finally {
      globalThis.clearTimeout(timer);
      updateClockStore({ pending: false });
    }
  })().finally(() => {
    inflightClockSync = null;
  });
  return inflightClockSync;
}

// ---------------------------------------------------------------------------
// Unified Miro-Ego v1 API Surface
// ---------------------------------------------------------------------------

const Api = Object.freeze({
  syncClock: synchronizeDeviceClock,
  readClock: () => synchronizeDeviceClock(false, true),
  resyncClock: () => synchronizeDeviceClock(true, false),
  getDevice: (signal) => apiJson("/device", { signal }).then(validateDeviceDescriptor),
  getCaptureStatus: (signal) => apiJson("/capture/status", { signal }).then(validateCaptureStatus),
  getSafeSwapReceipt: (signal) =>
    apiJsonOptional("/capture/safe-swap", { signal }).then((res) =>
      res === null ? null : validateSafeSwapResource(res)
    ),
  getCameraFocus: () => apiJsonOptional("/camera/focus"),
  getNetwork: () => apiJsonOptional("/network").then(validateNetworkStatusOrNull),
  scanNetworks: () => apiJson("/network/scan").then(validateNetworkScan),
  createNetworkCredential: (passphrase) =>
    apiJson("/network/credentials", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        schema: "miro-ego.network-credential-request.v1",
        passphrase,
      }),
    }).then(validateCredentialReceipt),
  applyNetwork: (desired, key = newIdempotencyKey()) =>
    isValidNetworkApplyDesiredState(desired)
      ? apiJson(
          "/network/apply",
          commandInitWithKey(
            {
              schema: "miro-ego.network-apply-request.v1",
              desired,
            },
            key
          )
        ).then(validateNetworkTxReceipt)
      : Promise.reject(
          new DeviceApiError("网络目标状态不符合 v1 契约", 400, "invalid_network_desired_state")
        ),
  retryNetwork: (transactionId, key = newIdempotencyKey()) =>
    apiJson(
      "/network/retry",
      commandInitWithKey(
        {
          schema: "miro-ego.network-retry-request.v1",
          transaction_id: transactionId,
        },
        key
      )
    ).then(validateNetworkTxReceipt),
  forgetNetwork: (key = newIdempotencyKey()) =>
    apiJson(
      "/network/forget",
      commandInitWithKey(
        {
          schema: "miro-ego.network-forget-request.v1",
        },
        key
      )
    ).then(validateNetworkTxReceipt),
  listSessions: ({ limit = 25, cursor = null } = {}) => {
    const params = new URLSearchParams({ limit: String(limit) });
    if (cursor) params.set("cursor", cursor);
    return apiJson(`/sessions?${params.toString()}`, {
      headers: { Prefer: "include-recovery" },
    }).then(validateSessionList);
  },
  getSession: async (sessionId) => {
    const response = await fetch(`${API_PREFIX}/sessions/${encodeURIComponent(sessionId)}`, {
      cache: "no-store",
      headers: buildHeaders("application/json"),
    });
    observeResponseAuditHeader(response);
    if (!response.ok) throw await parseErrorResponse(response);
    const detail = await response.json();
    if (detail.session_id !== sessionId) {
      throw new DeviceApiError("设备返回了不同录制的详情", 502, "invalid_session_identity");
    }
    const shaHeader = response.headers.get("Miro-Ego-Manifest-SHA256");
    const etag = response.headers.get("ETag");
    const manifestSha256 =
      shaHeader && SHA256_LOWER_RE.test(shaHeader) && (!etag || etag === `"${shaHeader}"`)
        ? shaHeader
        : null;
    return { detail, manifestSha256 };
  },
  deleteSessions: (sessions, key = newIdempotencyKey()) =>
    apiJson(
      "/sessions/delete",
      commandInitWithKey(
        {
          schema: "miro-ego.session-delete-request.v1",
          sessions,
        },
        key
      )
    ).then(validateDeleteResult),
  getUnsuccessfulOutcome: (sessionId) =>
    apiJsonOptional(`/sessions/${encodeURIComponent(sessionId)}/unsuccessful-outcome`),
  artifactUrl: (sessionId, artifactId) =>
    `${API_PREFIX}/sessions/${encodeURIComponent(sessionId)}/artifacts/${encodeURIComponent(artifactId)}`,
  listLogs: ({ prefix = "", limit = 200 } = {}, signal) => {
    const params = new URLSearchParams({ limit: String(limit) });
    const trimmed = prefix.trim();
    if (trimmed) params.set("prefix", trimmed);
    return apiJson(`/logs?${params.toString()}`, { signal }).then(validateLogList);
  },
  logUrl: (logName) => `${API_PREFIX}/logs/${encodeLogPath(logName)}`,
  startCapture: async (displayName, mode = "production", continuationOf = null) => {
    await synchronizeDeviceClock();
    const trimmed = displayName?.trim();
    const take = continuationOf
      ? { kind: "continue", continuation_of: continuationOf }
      : { kind: "new" };
    return apiJson(
      "/capture/start",
      commandInit({
        schema: "miro-ego.capture-start.v1",
        mode,
        ...(trimmed ? { display_name: trimmed } : {}),
        take,
      })
    ).then(validateCaptureStatus);
  },
  stopCapture: (reason = "user") =>
    apiJson(
      "/capture/stop",
      commandInit({
        schema: "miro-ego.capture-stop.v1",
        reason,
      })
    ).then((res) => (res === null ? null : validateCaptureStatus(res))),
  setCameraFocus: (focusPatch) =>
    apiJson(
      "/camera/focus",
      commandInit({
        schema: "miro-ego.camera-focus-set.v1",
        ...focusPatch,
      })
    ),
});

async function fetchJpegPreviewFrame(signal) {
  const response = await fetch(`${API_PREFIX}/preview`, {
    cache: "no-store",
    headers: buildHeaders("image/jpeg"),
    signal,
  });
  observeResponseAuditHeader(response);
  if (!response.ok) throw await parseErrorResponse(response);
  if (response.headers.get("Content-Type")?.split(";", 1)[0] !== "image/jpeg") {
    throw new Error("设备预览不是 JPEG");
  }
  return response.blob();
}

function validateTimerStatus(doc) {
  const validDuration = (n) => typeof n === "number" && Number.isInteger(n) && n >= 1 && n <= 86400;
  if (
    !doc ||
    doc.schema !== "miro-ego.capture-timer.v1" ||
    !doc.settings ||
    typeof doc.settings.enabled !== "boolean" ||
    !validDuration(doc.settings.duration_seconds) ||
    (doc.active !== null &&
      (!doc.active ||
        typeof doc.active.session_id !== "string" ||
        !validDuration(doc.active.duration_seconds) ||
        typeof doc.active.remaining_seconds !== "number" ||
        !Number.isFinite(doc.active.remaining_seconds) ||
        doc.active.remaining_seconds < 0 ||
        doc.active.remaining_seconds > doc.active.duration_seconds))
  ) {
    throw new DeviceApiError("设备定时状态无效", 502, "invalid_timer_status");
  }
  return doc;
}

const TimerApi = {
  get: (signal) => apiJson("/capture/timer", { signal }).then(validateTimerStatus),
  set: (enabled, durationSeconds) =>
    apiJson("/capture/timer", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        schema: "miro-ego.capture-timer-set.v1",
        enabled,
        duration_seconds: durationSeconds,
      }),
    }).then(validateTimerStatus),
};

function validateVoiceStatus(doc) {
  if (
    !doc ||
    doc.schema !== "miro-ego.voice.v1" ||
    typeof doc.enabled !== "boolean" ||
    !Number.isInteger(doc.volume) ||
    doc.volume < 0 ||
    doc.volume > 100
  ) {
    throw new DeviceApiError("设备提示音状态无效", 502, "invalid_voice_status");
  }
  return doc;
}

const VoiceApi = {
  get: (signal) => apiJson("/voice", { signal }).then(validateVoiceStatus),
  set: (volume) =>
    apiJson("/voice", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        schema: "miro-ego.voice-set.v1",
        volume,
      }),
    }).then(validateVoiceStatus),
};

async function fetchCaptureReadiness(signal) {
  const doc = await apiJson("/capture/readiness", { signal });
  if (
    !doc ||
    doc.schema !== "miro-ego.capture-readiness.v1" ||
    typeof doc.allowed !== "boolean" ||
    typeof doc.code !== "string" ||
    typeof doc.message !== "string" ||
    !doc.message ||
    doc.allowed !== (doc.code === "ready") ||
    !doc.storage ||
    !["recording", "system", "backing"].every((slot) => {
      const budget = doc.storage[slot];
      return (
        budget === null ||
        (budget &&
          [budget.available_bytes, budget.required_bytes].every(
            (n) => Number.isSafeInteger(n) && n >= 0
          ))
      );
    })
  ) {
    throw new DeviceApiError("录制准入状态无效", 502, "invalid_readiness");
  }
  return doc;
}

// ---------------------------------------------------------------------------
// SSE Event Streams (/api/v1/capture/events & /api/v1/network/events)
// ---------------------------------------------------------------------------

const CAPTURE_EVENTS_URL = `${API_PREFIX}/capture/events`;
const NETWORK_EVENTS_URL = `${API_PREFIX}/network/events`;
const SSE_RETRY_MS = 1000;
const SSE_DISCONNECT_THRESHOLD_MS = 5000;

function parseSseFrame(rawBlock, expectedSchema, mismatchMessage, validator) {
  let id = null;
  let eventName = "message";
  const dataLines = [];
  for (const line of rawBlock.split(/\r?\n/)) {
    if (!line || line.startsWith(":")) continue;
    const idx = line.indexOf(":");
    const field = idx === -1 ? line : line.slice(0, idx);
    const rest = idx === -1 ? "" : line.slice(idx + 1);
    const val = rest.startsWith(" ") ? rest.slice(1) : rest;
    if (field === "id") id = val;
    else if (field === "event") eventName = val;
    else if (field === "data") dataLines.push(val);
  }
  if (!id || dataLines.length === 0) return null;
  const parsed = JSON.parse(dataLines.join("\n"));
  if (
    !parsed ||
    parsed.schema !== expectedSchema ||
    parsed.sse_delivery_id !== id ||
    parsed.type !== eventName ||
    (validator !== undefined && !validator(parsed))
  ) {
    throw new Error(mismatchMessage);
  }
  return { id, payload: parsed };
}

async function consumeSseStream(url, expectedSchema, mismatchMessage, options) {
  let lastEventId = null;
  let disconnectSince = null;
  while (!options.signal.aborted) {
    try {
      const headers = buildHeaders("text/event-stream");
      if (lastEventId) headers.set("Last-Event-ID", lastEventId);
      const response = await fetch(url, {
        cache: "no-store",
        headers,
        signal: options.signal,
      });
      observeResponseAuditHeader(response);
      if (!response.ok) throw await parseErrorResponse(response);
      const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
      if (!contentType.startsWith("text/event-stream")) {
        throw new Error("设备事件流 Content-Type 不是 text/event-stream");
      }
      if (!response.body) throw new Error("设备事件流没有响应正文");
      const reader = response.body.getReader();
      try {
        const decoder = new TextDecoder();
        let buffer = "";
        let connectedNotified = false;
        const markConnected = () => {
          if (!connectedNotified) {
            disconnectSince = null;
            options.onConnection?.("connected");
            connectedNotified = true;
          }
        };
        if (lastEventId !== null) {
          markConnected();
        }
        while (!options.signal.aborted) {
          const { done, value } = await reader.read();
          if (value && value.byteLength > 0) {
            markConnected();
          }
          buffer += decoder.decode(value, { stream: !done });
          let boundaryIdx = buffer.search(/\r?\n\r?\n/);
          while (boundaryIdx !== -1) {
            const frame = buffer.slice(0, boundaryIdx);
            const sep = buffer.slice(boundaryIdx).match(/^\r?\n\r?\n/)?.[0] ?? "\n\n";
            buffer = buffer.slice(boundaryIdx + sep.length);
            markConnected();
            const parsed = parseSseFrame(frame, expectedSchema, mismatchMessage, options.validatePayload);
            if (parsed) {
              await options.onEvent(parsed.payload);
              lastEventId = parsed.id;
            }
            boundaryIdx = buffer.search(/\r?\n\r?\n/);
          }
          if (done) break;
        }
      } finally {
        try {
          await reader.cancel();
        } catch {}
        reader.releaseLock();
      }
    } catch (err) {
      if (options.signal.aborted) return;
      if (err instanceof DeviceApiError && err.status === 401) {
        options.onConnection?.("disconnected");
        options.onUnauthorized(err);
        return;
      }
      console.warn(err);
    }
    disconnectSince ??= Date.now();
    options.onConnection?.(
      Date.now() - disconnectSince >= SSE_DISCONNECT_THRESHOLD_MS ? "disconnected" : "connecting"
    );
    await abortableDelay(SSE_RETRY_MS, options.signal);
  }
}

function streamCaptureEvents(options) {
  return consumeSseStream(
    CAPTURE_EVENTS_URL,
    "miro-ego.capture-event.v1",
    "设备事件与 SSE envelope 不一致",
    options
  );
}

function streamNetworkEvents(options) {
  return consumeSseStream(
    NETWORK_EVENTS_URL,
    "miro-ego.network-event.v1",
    "网络事件与 SSE envelope 不一致",
    {
      ...options,
      validatePayload: isValidNetworkEvent,
    }
  );
}

// ---------------------------------------------------------------------------
// Central State Store & Async Workflows
// ---------------------------------------------------------------------------

function clampThreshold(v) {
  return Number.isFinite(v) ? Math.min(255, Math.max(0, Math.round(v))) : 96;
}

const INITIAL_PREVIEW_TRANSPORT = (() => {
  const saved = readPref(STORAGE_KEYS.previewTransport, "auto");
  return ["auto", "h264", "mjpeg"].includes(saved) ? saved : "auto";
})();

const INITIAL_PEAKING_ENABLED = readPref(STORAGE_KEYS.focusPeaking, "false") === "true";
const INITIAL_PEAKING_THRESHOLD = clampThreshold(
  Number(readPref(STORAGE_KEYS.focusPeakingThreshold, "96"))
);

const INITIAL_STATE = {
  connection: "connecting",
  auditUnavailable: false,
  device: null,
  capture: null,
  phaseProgress: null,
  safeSwapReceipt: null,
  networkStatus: null,
  networkScan: null,
  networkScanPending: false,
  networkCommand: {
    operation: null,
    phase: "idle",
    transactionId: null,
  },
  sessions: {
    items: [],
    diagnostics: [],
    wireSchema: null,
    catalogRevision: null,
    nextCursor: null,
    loading: false,
    loadedOnce: false,
    error: null,
    query: "",
    filter: "all",
  },
  logs: {
    items: [],
    volumeId: null,
    observedAt: null,
    truncated: false,
    loading: false,
    loadedOnce: false,
    error: null,
    prefix: "",
  },
  selected: null,
  diagnostics: [],
  commandPending: false,
  focusPending: false,
  needsCredentials: false,
  error: null,
  inspect: "both",
  panel: "none",
  fullFrame: true,
  previewTransport: INITIAL_PREVIEW_TRANSPORT,
  focusPeaking: {
    enabled: INITIAL_PEAKING_ENABLED,
    threshold: INITIAL_PEAKING_THRESHOLD,
  },
};

function withUpdatedCameraFocus(runtime, focusPayload) {
  return { ...runtime, camera_focus: focusPayload };
}

function isOlderAuthorityRevision(current, incoming) {
  return (
    current !== null &&
    incoming.authority_epoch === current.authority_epoch &&
    incoming.source_revision < current.source_revision
  );
}

function isSameCatalogPageChain(sessionsState, incomingList) {
  if (!sessionsState.loadedOnce || sessionsState.wireSchema !== incomingList.schema) return false;
  return sessionsState.catalogRevision === incomingList.catalog_revision;
}

function resetSessionsCollection(sessionsState, loadedOnce) {
  return {
    ...sessionsState,
    items: [],
    diagnostics: [],
    wireSchema: null,
    catalogRevision: null,
    nextCursor: null,
    loading: false,
    loadedOnce,
    error: null,
  };
}

function reduceState(state, action) {
  switch (action.type) {
    case "audit.status":
      return { ...state, auditUnavailable: action.unavailable };
    case "device.loaded":
      return { ...state, device: action.payload };
    case "capture.snapshot": {
      if (isOlderAuthorityRevision(state.capture, action.payload)) return state;
      const devState = action.payload.snapshot.device_state;
      const keepProgress = ["recording", "finalizing", "encoding", "verifying"].includes(devState);
      return {
        ...state,
        capture: action.payload,
        phaseProgress: keepProgress ? state.phaseProgress : null,
      };
    }
    case "capture.state-projected": {
      if (!state.capture || isOlderAuthorityRevision(state.capture, action.event)) return state;
      const nextState = action.event.data.state;
      if (!["recording", "finalizing", "encoding", "verifying"].includes(nextState)) return state;
      const prevSnap = state.capture.snapshot;
      const prevActive = prevSnap.active_recording;
      return {
        ...state,
        capture: {
          ...state.capture,
          source_revision: Math.max(state.capture.source_revision, action.event.source_revision),
          snapshot: {
            ...prevSnap,
            device_state: nextState,
            active_recording: prevActive
              ? {
                  ...prevActive,
                  recording_state: {
                    ...prevActive.recording_state,
                    state: nextState,
                  },
                }
              : prevActive,
          },
        },
      };
    }
    case "capture.progress": {
      const p = action.payload;
      const prevCapture = state.capture;
      let nextCapture = prevCapture;
      if (
        prevCapture?.snapshot.active_recording &&
        typeof p.elapsed_seconds === "number" &&
        Number.isFinite(p.elapsed_seconds)
      ) {
        const prevRec = prevCapture.snapshot.active_recording;
        const prevProg = prevRec.recording_state.progress;
        nextCapture = {
          ...prevCapture,
          snapshot: {
            ...prevCapture.snapshot,
            device_state: p.phase || prevCapture.snapshot.device_state,
            active_recording: {
              ...prevRec,
              recording_state: {
                ...prevRec.recording_state,
                state: p.phase || prevRec.recording_state.state,
                progress: {
                  ...prevProg,
                  elapsed_seconds: Math.max(prevProg.elapsed_seconds ?? 0, p.elapsed_seconds),
                  ...(p.unit === "frames" && Number.isSafeInteger(p.completed_units)
                    ? { captured_frames: p.completed_units }
                    : {}),
                  ...(p.unit === "bytes" && Number.isSafeInteger(p.completed_units)
                    ? { bytes_written: p.completed_units }
                    : {}),
                },
              },
            },
          },
        };
      }
      return {
        ...state,
        capture: nextCapture,
        phaseProgress: p,
      };
    }
    case "safe-swap.loaded":
      return { ...state, safeSwapReceipt: action.payload };
    case "command.pending":
      return { ...state, commandPending: true, error: null };
    case "command.settled":
      return { ...state, commandPending: false };
    case "command.succeeded":
      return { ...state, error: null };
    case "command.failed":
      return { ...state, error: action.error };
    case "camera-focus.pending":
      return { ...state, focusPending: true, error: null };
    case "camera-focus.settled":
      return { ...state, focusPending: false };
    case "camera-focus.updated":
      return {
        ...state,
        error: null,
        device: state.device
          ? { ...state.device, runtime: withUpdatedCameraFocus(state.device.runtime, action.payload) }
          : state.device,
        capture: state.capture
          ? {
              ...state.capture,
              snapshot: {
                ...state.capture.snapshot,
                runtime: withUpdatedCameraFocus(state.capture.snapshot.runtime, action.payload),
              },
            }
          : state.capture,
      };
    case "network.loaded": {
      if (action.payload !== null && isOlderAuthorityRevision(state.networkStatus, action.payload)) {
        return state;
      }
      const txId = state.networkCommand.transactionId;
      const cur = action.payload?.transaction.current ?? null;
      const lat = action.payload?.transaction.latest ?? null;
      const matched = [cur, lat].find((tx) => tx?.transaction_id === txId);
      const nextCmd = matched
        ? {
            ...state.networkCommand,
            phase:
              matched.status === "committed" ||
              matched.status === "rescued" ||
              matched.status === "failed"
                ? "idle"
                : "accepted",
          }
        : state.networkCommand;
      return { ...state, networkStatus: action.payload, networkCommand: nextCmd };
    }
    case "network.scan.pending":
      return { ...state, networkScanPending: true, error: null };
    case "network.scan.loaded":
      return { ...state, networkScan: action.payload, networkScanPending: false, error: null };
    case "network.scan.failed":
      return { ...state, networkScanPending: false, error: action.error };
    case "network.command.pending":
      return {
        ...state,
        error: null,
        networkCommand: { operation: action.operation, phase: "submitting", transactionId: null },
      };
    case "network.command.accepted":
      return {
        ...state,
        error: null,
        networkCommand: {
          operation: action.operation,
          phase: "accepted",
          transactionId: action.payload.transaction.transaction_id,
        },
      };
    case "network.command.failed":
      return {
        ...state,
        error: action.error,
        networkCommand: { operation: action.operation, phase: "failed", transactionId: null },
      };
    case "error.cleared":
      return { ...state, error: null };
    case "sessions.pending":
      return { ...state, sessions: { ...state.sessions, loading: true } };
    case "sessions.loaded":
      if (action.append && !isSameCatalogPageChain(state.sessions, action.payload)) {
        return { ...state, sessions: resetSessionsCollection(state.sessions, false) };
      }
      return {
        ...state,
        sessions: {
          ...state.sessions,
          items: action.append
            ? [...state.sessions.items, ...action.payload.items]
            : action.payload.items,
          diagnostics: action.append
            ? [...state.sessions.diagnostics, ...action.payload.diagnostics]
            : action.payload.diagnostics,
          wireSchema: action.payload.schema,
          catalogRevision: action.payload.catalog_revision,
          nextCursor: action.payload.next_cursor,
          loading: false,
          loadedOnce: true,
          error: null,
        },
      };
    case "sessions.failed":
      return { ...state, sessions: { ...state.sessions, loading: false, error: action.error } };
    case "sessions.unavailable":
      return {
        ...state,
        sessions: { ...resetSessionsCollection(state.sessions, false), error: action.error },
      };
    case "sessions.query":
      return { ...state, sessions: { ...state.sessions, query: action.query } };
    case "sessions.filter":
      return { ...state, sessions: { ...state.sessions, filter: action.filter } };
    case "logs.pending":
      return { ...state, logs: { ...state.logs, loading: true, error: null } };
    case "logs.loaded":
      return {
        ...state,
        logs: {
          ...state.logs,
          items: action.payload.items,
          volumeId: action.payload.volume_id,
          observedAt: action.payload.observed_at,
          truncated: action.payload.truncated,
          loading: false,
          loadedOnce: true,
          error: null,
        },
      };
    case "logs.failed":
      return { ...state, logs: { ...state.logs, loading: false, error: action.error } };
    case "logs.prefix":
      return { ...state, logs: { ...state.logs, prefix: action.prefix } };
    case "session.opened":
      return {
        ...state,
        selected: {
          sessionId: action.sessionId,
          loading: true,
          detail: null,
          outcome: null,
          error: null,
        },
      };
    case "session.detail":
      if (state.selected?.sessionId !== action.sessionId) return state;
      return {
        ...state,
        selected: {
          ...state.selected,
          loading: false,
          detail: action.detail,
          manifestSha256: action.manifestSha256 ?? null,
          error: null,
        },
      };
    case "session.outcome":
      if (state.selected?.sessionId !== action.sessionId) return state;
      return {
        ...state,
        selected: { ...state.selected, loading: false, outcome: action.outcome },
      };
    case "session.failed":
      if (state.selected?.sessionId !== action.sessionId) return state;
      return {
        ...state,
        selected: { ...state.selected, loading: false, error: action.error },
      };
    case "session.closed":
      return { ...state, selected: null };
    case "diagnostic.received":
      return { ...state, diagnostics: [...state.diagnostics.slice(-3), action.payload] };
    case "connection.changed":
      return { ...state, connection: action.connection };
    case "connection.failed":
      return { ...state, connection: "disconnected", error: action.error };
    case "credentials.required":
      return { ...state, needsCredentials: true };
    case "credentials.cleared":
      return { ...state, needsCredentials: false };
    case "inspect.changed":
      writePref(STORAGE_KEYS.inspectMode, action.mode);
      return { ...state, inspect: action.mode, fullFrame: true };
    case "preview.transport":
      writePref(STORAGE_KEYS.previewTransport, action.transport);
      return { ...state, previewTransport: action.transport };
    case "panel.opened":
      return { ...state, panel: action.panel };
    case "panel.closed":
      return { ...state, panel: "none", selected: null };
    case "full-frame.toggled":
      return { ...state, fullFrame: !state.fullFrame };
    case "focus-peaking.toggled": {
      const nextEnabled = !state.focusPeaking.enabled;
      writePref(STORAGE_KEYS.focusPeaking, String(nextEnabled));
      return { ...state, focusPeaking: { ...state.focusPeaking, enabled: nextEnabled } };
    }
    case "focus-peaking.threshold": {
      const nextThreshold = clampThreshold(action.threshold);
      writePref(STORAGE_KEYS.focusPeakingThreshold, String(nextThreshold));
      return { ...state, focusPeaking: { ...state.focusPeaking, threshold: nextThreshold } };
    }
    default:
      return state;
  }
}

function shouldSurfaceRetainedDiagnostics(prevState, nextCapture) {
  const retained = nextCapture.snapshot.retained_unsuccessful;
  const prevActive = prevState.capture?.snapshot.active_recording;
  const wasBusy = !!(prevState.capture && prevState.capture.snapshot.device_state !== "idle");
  return !!(
    retained &&
    (wasBusy ||
      (prevActive &&
        prevActive.generation_id === retained.generation_id &&
        prevActive.recording_state.session_id === retained.recording_state.session_id))
  );
}

const SESSION_PAGE_SIZE = 25;
const SESSION_MAX_LIMIT = 200;
const STOP_POLL_DELAYS = [0, 150, 300, 600, 1000, 2000, 4000, 8000];
const SEALED_VISIBILITY_DELAYS = [0, 150, 300, 600, 1000, 1500, 2500, 4000, 6000];
const CAPTURE_STATE_EVENT_KEYS = new Set(["schema", "state", "volume_id", "generation_id"]);
const CAPTURE_STATE_VALUES = new Set([
  "recording",
  "finalizing",
  "encoding",
  "verifying",
  "recoverable",
  "failed",
  "abandoned",
]);

function sleepMs(ms) {
  return new Promise((resolve) => globalThis.setTimeout(resolve, ms));
}

function isCatalogChangedError(err) {
  return err instanceof DeviceApiError && err.status === 409 && err.code === "catalog_changed";
}

function makeCatalogChangedError() {
  return new DeviceApiError("会话目录已变化", 409, "catalog_changed");
}

function sessionWindowCount(listOrState) {
  return listOrState.items.length + listOrState.diagnostics.length;
}

function isStorageUnavailableForSessions(err) {
  return (
    err instanceof DeviceApiError &&
    ((err.status === 409 && err.code === "volume_not_mounted") ||
      (err.status === 503 && err.code === "storage_unavailable"))
  );
}

function detectNewlySealedSessionId(prevCapture, nextCapture) {
  const prevActive = prevCapture?.snapshot.active_recording;
  if (
    !prevActive ||
    !nextCapture ||
    prevCapture.authority_epoch !== nextCapture.authority_epoch ||
    nextCapture.snapshot.device_state !== "idle" ||
    nextCapture.snapshot.active_recording ||
    nextCapture.snapshot.retained_unsuccessful
  ) {
    return null;
  }
  return prevActive.recording_state.session_id;
}

function isValidCaptureStateEventData(v) {
  if (!isPlainObject(v)) return false;
  const keys = Object.keys(v);
  return (
    keys.length === CAPTURE_STATE_EVENT_KEYS.size &&
    keys.every((k) => CAPTURE_STATE_EVENT_KEYS.has(k)) &&
    v.schema === "miro-ego.capture-state-event.v1" &&
    typeof v.state === "string" &&
    CAPTURE_STATE_VALUES.has(v.state) &&
    typeof v.volume_id === "string" &&
    UUID_V4_CI_RE.test(v.volume_id) &&
    typeof v.generation_id === "string" &&
    UUID_V4_CI_RE.test(v.generation_id)
  );
}

function isValidCaptureProgressEventData(v) {
  return (
    isPlainObject(v) &&
    v.schema === "miro-ego.capture-progress-event.v1" &&
    ["recording", "finalizing", "encoding", "verifying"].includes(String(v.phase)) &&
    typeof v.elapsed_seconds === "number" &&
    Number.isFinite(v.elapsed_seconds) &&
    v.elapsed_seconds >= 0 &&
    Number.isSafeInteger(v.completed_units) &&
    v.completed_units >= 0 &&
    (v.total_units === null || (Number.isSafeInteger(v.total_units) && v.total_units >= 0)) &&
    ["frames", "bytes", "artifacts", "checks"].includes(String(v.unit))
  );
}

function toUiError(err) {
  if (err instanceof DeviceApiError) {
    return { code: err.code, message: err.message, details: err.details };
  }
  return {
    code: "command_failed",
    message: err instanceof Error ? err.message : "命令失败",
  };
}

class ConsoleStore {
  state = INITIAL_STATE;
  listeners = new Set();
  captureRefresh = null;
  deviceRefresh = null;
  safeSwapRefresh = null;
  relatedRefresh = null;
  sessionsRefresh = null;
  sessionsRefreshDirty = false;
  logsRefresh = null;
  networkRefresh = null;
  lastClockAttempt = -Infinity;
  lastClockAuthority = null;
  stoppedSessionRefreshes = new Map();
  sealedSessionRefreshes = new Map();

  constructor() {
    auditStatusSubscribers.add((unavailable) => {
      this.dispatch({ type: "audit.status", unavailable });
    });
  }

  getState = () => this.state;

  subscribe = (listener) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  dispatch = (action) => {
    const prev = this.state;
    this.state = reduceState(this.state, action);
    if (action.type === "capture.snapshot" && this.state !== prev) {
      const incomingDiags = [
        ...(action.payload.snapshot.active_recording?.recording_state.diagnostics ?? []),
        ...(shouldSurfaceRetainedDiagnostics(prev, action.payload)
          ? action.payload.snapshot.retained_unsuccessful?.recording_state.diagnostics ?? []
          : []),
      ];
      const existingKeys = new Set(this.state.diagnostics.map((d) => `${d.code}|${d.at}`));
      for (const diag of incomingDiags) {
        const key = `${diag.code}|${diag.at}`;
        if (!existingKeys.has(key)) {
          this.state = reduceState(this.state, { type: "diagnostic.received", payload: diag });
          existingKeys.add(key);
        }
      }
    }
    if (this.state !== prev) {
      for (const listener of this.listeners) listener();
    }
  };

  async loadInitialState() {
    try {
      const device = await Api.getDevice();
      const capture = await Api.getCaptureStatus();
      this.dispatch({ type: "device.loaded", payload: device });
      this.dispatch({ type: "capture.snapshot", payload: capture });
      this.dispatch({ type: "error.cleared" });
      this.dispatch({ type: "credentials.cleared" });

      if (capture.snapshot.device_state === "idle") {
        await this.synchronizeClock();
      } else {
        Api.readClock().catch(console.warn);
      }

      this.refreshSafeSwap().catch(console.warn);
      Api.getNetwork()
        .then((net) => this.dispatch({ type: "network.loaded", payload: net }))
        .catch((err) => console.warn(err));
      this.refreshSessions();
      return true;
    } catch (err) {
      if (err instanceof DeviceApiError && err.status === 401) {
        this.dispatch({ type: "credentials.required" });
      }
      this.dispatch({
        type: "connection.failed",
        error:
          err instanceof DeviceApiError
            ? toUiError(err)
            : { code: "connection_failed", message: "连接失败" },
      });
      return false;
    }
  }

  refreshCapture = async () => {
    if (!this.captureRefresh) {
      this.captureRefresh = Api.getCaptureStatus()
        .then((snap) => this.dispatch({ type: "capture.snapshot", payload: snap }))
        .finally(() => {
          this.captureRefresh = null;
        });
    }
    await this.captureRefresh;
  };

  refreshSafeSwap = async () => {
    if (!this.safeSwapRefresh) {
      this.safeSwapRefresh = Api.getSafeSwapReceipt()
        .then((receipt) => this.dispatch({ type: "safe-swap.loaded", payload: receipt }))
        .catch((err) => console.warn(err))
        .finally(() => {
          this.safeSwapRefresh = null;
        });
    }
    await this.safeSwapRefresh;
  };

  reconcileCapture = async () => {
    const prevCapture = this.state.capture;
    try {
      await this.refreshCapture();
    } catch (err) {
      console.warn(err);
      return;
    }
    const sealedId = detectNewlySealedSessionId(prevCapture, this.state.capture);
    if (
      this.state.capture?.snapshot.device_state === "idle" &&
      (this.lastClockAuthority !== this.state.capture.authority_epoch ||
        performance.now() - this.lastClockAttempt >= 30000)
    ) {
      this.synchronizeClock();
    }
    if (sealedId) {
      this.refreshRelatedResources();
      this.scheduleSealedSessionRefresh(sealedId);
    }
  };

  async synchronizeClock() {
    this.lastClockAttempt = performance.now();
    this.lastClockAuthority = this.state.capture?.authority_epoch ?? null;
    try {
      await Api.syncClock();
    } catch (err) {
      console.warn(err);
    }
  }

  refreshRelatedResources = async () => {
    if (!this.relatedRefresh) {
      this.relatedRefresh = Promise.allSettled([
        Api.getDevice().then((dev) => this.dispatch({ type: "device.loaded", payload: dev })),
        this.refreshSafeSwap(),
      ]).finally(() => {
        this.relatedRefresh = null;
      });
      this.refreshSessions();
    }
    await this.relatedRefresh;
  };

  async listSessions(cursor, limit = SESSION_PAGE_SIZE) {
    return Api.listSessions({ limit, cursor });
  }

  async readSessionWindow(targetCount) {
    const currentSessions = this.state.sessions;
    const headPage = await this.listSessions(null);
    if (
      isSameCatalogPageChain(currentSessions, headPage) &&
      (sessionWindowCount(currentSessions) >= targetCount || currentSessions.nextCursor === null)
    ) {
      return {
        ...headPage,
        items: currentSessions.items,
        diagnostics: currentSessions.diagnostics,
        next_cursor: currentSessions.nextCursor,
      };
    }
    const acc = headPage;
    while (acc.next_cursor && sessionWindowCount(acc) < targetCount) {
      const cursor = acc.next_cursor;
      const remaining = targetCount - sessionWindowCount(acc);
      const nextPage = await this.listSessions(cursor, Math.min(remaining, SESSION_MAX_LIMIT));
      if (
        nextPage.schema !== acc.schema ||
        nextPage.catalog_revision !== acc.catalog_revision
      ) {
        throw makeCatalogChangedError();
      }
      if (nextPage.next_cursor && (nextPage.next_cursor === cursor || sessionWindowCount(nextPage) === 0)) {
        throw new DeviceApiError("会话分页未前进", 502, "invalid_response");
      }
      acc.items.push(...nextPage.items);
      acc.diagnostics.push(...nextPage.diagnostics);
      acc.next_cursor = nextPage.next_cursor;
    }
    return acc;
  }

  async replaceSessionWindow(targetCount) {
    try {
      const windowData = await this.readSessionWindow(targetCount).catch((err) => {
        if (!isCatalogChangedError(err)) throw err;
        return this.readSessionWindow(targetCount);
      });
      this.dispatch({ type: "sessions.loaded", payload: windowData, append: false });
    } catch (err) {
      this.reportSessionError(err);
    }
  }

  reportSessionError(err) {
    if (isStorageUnavailableForSessions(err)) {
      this.dispatch({ type: "sessions.unavailable", error: toUiError(err) });
    } else {
      console.warn(err);
      this.dispatch({ type: "sessions.failed", error: toUiError(err) });
    }
  }

  refreshSessions = async () => {
    if (this.sessionsRefresh) {
      this.sessionsRefreshDirty = true;
      await this.sessionsRefresh;
      if (this.sessionsRefreshDirty) await this.refreshSessions();
      return;
    }
    this.sessionsRefresh = this.runSessionsRefreshLoop();
    await this.sessionsRefresh;
  };

  refreshLogs = async (prefixOverride = null) => {
    const prefix = prefixOverride !== null ? prefixOverride : this.state.logs.prefix;
    if (prefixOverride !== null && prefixOverride !== this.state.logs.prefix) {
      this.dispatch({ type: "logs.prefix", prefix: prefixOverride });
    }
    this.dispatch({ type: "logs.pending" });
    try {
      const list = await Api.listLogs({ prefix, limit: 200 });
      this.dispatch({ type: "logs.loaded", payload: list });
    } catch (err) {
      this.dispatch({ type: "logs.failed", error: toUiError(err) });
    }
  };

  refreshOpenPanel = async () => {
    if (this.state.panel === "sessions") {
      if (!this.sessionsRefresh) await this.refreshSessions();
      return;
    }
    if (this.state.panel === "device") {
      await Promise.allSettled([this.refreshDevice(), this.refreshSafeSwap()]);
      return;
    }
    if (this.state.panel === "logs") {
      await this.refreshLogs();
      return;
    }
    if (this.state.panel === "network") {
      await this.refreshNetwork();
    }
  };

  async runSessionsRefreshLoop() {
    try {
      do {
        this.sessionsRefreshDirty = false;
        this.dispatch({ type: "sessions.pending" });
        const target = Math.max(SESSION_PAGE_SIZE, sessionWindowCount(this.state.sessions));
        await this.replaceSessionWindow(target);
      } while (this.sessionsRefreshDirty);
    } finally {
      this.sessionsRefresh = null;
    }
  }

  async refreshSessionsUntilVisible(sessionId) {
    for (const delay of SEALED_VISIBILITY_DELAYS) {
      if (delay > 0) await sleepMs(delay);
      await this.refreshSessions();
      if (this.state.sessions.items.some((s) => s.session_id === sessionId)) return;
    }
  }

  scheduleSealedSessionRefresh(sessionId) {
    if (
      this.state.sessions.items.some((s) => s.session_id === sessionId) ||
      this.sealedSessionRefreshes.has(sessionId)
    ) {
      return;
    }
    const task = this.refreshSessionsUntilVisible(sessionId)
      .catch((err) => console.warn(err))
      .finally(() => this.sealedSessionRefreshes.delete(sessionId));
    this.sealedSessionRefreshes.set(sessionId, task);
  }

  loadMoreSessions = async () => {
    const { nextCursor, loading } = this.state.sessions;
    if (!nextCursor || loading || this.sessionsRefresh) return;
    const target = sessionWindowCount(this.state.sessions) + SESSION_PAGE_SIZE;
    this.dispatch({ type: "sessions.pending" });
    this.sessionsRefresh = this.listSessions(nextCursor)
      .then((page) => {
        if (!isSameCatalogPageChain(this.state.sessions, page)) throw makeCatalogChangedError();
        this.dispatch({ type: "sessions.loaded", payload: page, append: true });
      })
      .catch((err) => {
        if (isCatalogChangedError(err)) return this.replaceSessionWindow(target);
        this.reportSessionError(err);
      })
      .finally(() => {
        this.sessionsRefresh = null;
      });
    await this.sessionsRefresh;
    if (this.sessionsRefreshDirty) await this.refreshSessions();
  };

  setSessionQuery = (query) => this.dispatch({ type: "sessions.query", query });
  setSessionFilter = (filter) => this.dispatch({ type: "sessions.filter", filter });

  openSession = async (sessionId, producerOutcome) => {
    this.dispatch({ type: "session.opened", sessionId });
    if (producerOutcome !== "sealed") {
      try {
        const outcome = await Api.getUnsuccessfulOutcome(sessionId);
        this.dispatch({ type: "session.outcome", sessionId, outcome });
      } catch (err) {
        this.dispatch({ type: "session.failed", sessionId, error: toUiError(err) });
      }
      return;
    }
    try {
      const { detail, manifestSha256 } = await Api.getSession(sessionId);
      this.dispatch({ type: "session.detail", sessionId, detail, manifestSha256 });
    } catch (err) {
      this.dispatch({ type: "session.failed", sessionId, error: toUiError(err) });
    }
  };

  closeSession = () => this.dispatch({ type: "session.closed" });

  deleteSession = async (sessionId, manifestSha256) => {
    if (this.state.device?.capabilities.session_deletion !== true) {
      throw new DeviceApiError("当前设备不支持远程删除", 409, "session_deletion_unsupported");
    }
    const res = await Api.deleteSessions([
      {
        session_id: sessionId,
        manifest_sha256: manifestSha256,
      },
    ]);
    await this.refreshRelatedResources();
    await this.refreshSessions();
    return res;
  };

  startCapture = async (displayName, mode = "production", continuationOf = null) => {
    if (this.state.commandPending || this.state.connection !== "connected") return;
    this.dispatch({ type: "command.pending" });
    try {
      const snap = await Api.startCapture(displayName, mode, continuationOf);
      if (snap) this.dispatch({ type: "capture.snapshot", payload: snap });
      this.dispatch({ type: "safe-swap.loaded", payload: null });
      await this.refreshCapture();
      this.dispatch({ type: "command.succeeded" });
    } catch (err) {
      if (err instanceof DeviceApiError && err.code === "clock_unsynchronized") {
        ClockStore.requestPrompt();
      } else if (err instanceof DeviceApiError && err.status === 403 && err.code === "csrf_forbidden") {
        this.dispatch({ type: "credentials.required" });
      }
      this.dispatch({ type: "command.failed", error: toUiError(err) });
    } finally {
      this.dispatch({ type: "command.settled" });
    }
  };

  stopCapture = async (reason = "user") => {
    if (this.state.commandPending || this.state.connection !== "connected") return;
    const activeSessionId =
      this.state.capture?.snapshot.active_recording?.recording_state.session_id ?? null;
    this.dispatch({ type: "command.pending" });
    try {
      const before = this.state.capture;
      const snap = await Api.stopCapture(reason);
      if (snap) this.dispatch({ type: "capture.snapshot", payload: snap });
      await this.refreshCapture();
      await this.refreshRelatedResources();
      if (reason === "safe_swap") {
        await this.refreshSafeSwap();
      }
      if (activeSessionId) {
        this.scheduleStoppedSessionRefresh(before, activeSessionId);
      }
      this.dispatch({ type: "command.succeeded" });
    } catch (err) {
      this.dispatch({ type: "command.failed", error: toUiError(err) });
    } finally {
      this.dispatch({ type: "command.settled" });
    }
  };

  safeSwapStop = async () => {
    await this.stopCapture("safe_swap");
  };

  async refreshStoppedSession(prevCapture, sessionId) {
    for (const delay of STOP_POLL_DELAYS) {
      if (delay > 0) await sleepMs(delay);
      await this.refreshCapture();
      if (detectNewlySealedSessionId(prevCapture, this.state.capture) === sessionId) {
        await this.refreshRelatedResources();
        this.scheduleSealedSessionRefresh(sessionId);
        return;
      }
    }
  }

  scheduleStoppedSessionRefresh(prevCapture, sessionId) {
    if (this.stoppedSessionRefreshes.has(sessionId)) return;
    const task = this.refreshStoppedSession(prevCapture, sessionId)
      .catch((err) => console.warn(err))
      .finally(() => this.stoppedSessionRefreshes.delete(sessionId));
    this.stoppedSessionRefreshes.set(sessionId, task);
  }

  refreshDevice = async () => {
    if (!this.deviceRefresh) {
      this.deviceRefresh = Api.getDevice()
        .then((dev) => this.dispatch({ type: "device.loaded", payload: dev }))
        .catch((err) => console.warn(err))
        .finally(() => {
          this.deviceRefresh = null;
        });
    }
    await this.deviceRefresh;
  };

  refreshNetwork = async () => {
    if (!this.networkRefresh) {
      this.networkRefresh = this.refreshDevice()
        .then(async () => {
          const net = await Api.getNetwork();
          this.dispatch({ type: "network.loaded", payload: net });
        })
        .catch((err) => console.warn(err))
        .finally(() => {
          this.networkRefresh = null;
        });
    }
    await this.networkRefresh;
  };

  acceptNetworkEvent = async (ev) => {
    const current = this.state.networkStatus;
    if (
      current &&
      ev.authority_epoch === current.authority_epoch &&
      (ev.type === "snapshot"
        ? ev.source_revision < current.source_revision
        : ev.source_revision <= current.source_revision)
    ) {
      return;
    }
    if (ev.type === "snapshot" && ev.data?.schema === "miro-ego.network-status.v1") {
      this.dispatch({ type: "network.loaded", payload: ev.data });
      return;
    }
    await this.refreshNetwork();
  };

  scanNetworks = async () => {
    if (this.state.networkScanPending) return;
    this.dispatch({ type: "network.scan.pending" });
    try {
      const scan = await Api.scanNetworks();
      this.dispatch({ type: "network.scan.loaded", payload: scan });
    } catch (err) {
      this.dispatch({ type: "network.scan.failed", error: toUiError(err) });
    }
  };

  networkMutationBlocked() {
    return (
      this.state.networkCommand.phase === "submitting" ||
      this.state.networkCommand.phase === "accepted" ||
      this.state.networkStatus?.transaction.current !== null ||
      this.state.networkStatus?.mutation_capability.enabled !== true
    );
  }

  acceptNetworkReceipt(op, receipt) {
    this.dispatch({ type: "network.command.accepted", operation: op, payload: receipt });
    this.refreshNetwork();
  }

  rejectNetworkCommand(op, err) {
    if (!(err instanceof DeviceApiError)) {
      const fallback = {
        code: "network_command_disconnected",
        message: "设备未返回明确的网络命令结果；本次操作已失败，连接设备后可重新发起",
      };
      this.dispatch({ type: "network.command.failed", operation: op, error: fallback });
      this.dispatch({ type: "connection.failed", error: fallback });
      return;
    }
    this.dispatch({ type: "network.command.failed", operation: op, error: toUiError(err) });
  }

  applyWifiNetwork = async (params) => {
    if (this.networkMutationBlocked()) return;
    this.dispatch({ type: "network.command.pending", operation: "apply" });
    try {
      let credentialRef;
      if (params.security !== "open") {
        if (params.passphrase === undefined) {
          throw new DeviceApiError("受保护的 Wi-Fi 需要密码", 400, "invalid_network_desired_state");
        }
        credentialRef = (await Api.createNetworkCredential(params.passphrase)).credential_ref;
      }
      const desired = {
        mode: "wifi-client",
        wifi_client: {
          ssid: params.ssid,
          security: params.security,
          ...(credentialRef === undefined ? {} : { credential_ref: credentialRef }),
        },
        ethernet: null,
      };
      const receipt = await Api.applyNetwork(desired);
      this.acceptNetworkReceipt("apply", receipt);
    } catch (err) {
      this.rejectNetworkCommand("apply", err);
    }
  };

  applyHotspot = async () => {
    if (this.networkMutationBlocked()) return;
    this.dispatch({ type: "network.command.pending", operation: "apply" });
    try {
      const receipt = await Api.applyNetwork({
        mode: "hotspot",
        wifi_client: null,
        ethernet: null,
      });
      this.acceptNetworkReceipt("apply", receipt);
    } catch (err) {
      this.rejectNetworkCommand("apply", err);
    }
  };

  applyEthernet = async (ethernetDesired) => {
    if (this.networkMutationBlocked()) return;
    this.dispatch({ type: "network.command.pending", operation: "apply" });
    try {
      const receipt = await Api.applyNetwork(ethernetDesired);
      this.acceptNetworkReceipt("apply", receipt);
    } catch (err) {
      this.rejectNetworkCommand("apply", err);
    }
  };

  retryNetwork = async (transactionId) => {
    if (this.networkMutationBlocked()) return;
    this.dispatch({ type: "network.command.pending", operation: "retry" });
    try {
      const receipt = await Api.retryNetwork(transactionId);
      this.acceptNetworkReceipt("retry", receipt);
    } catch (err) {
      this.rejectNetworkCommand("retry", err);
    }
  };

  forgetNetwork = async () => {
    if (this.networkMutationBlocked()) return;
    this.dispatch({ type: "network.command.pending", operation: "forget" });
    try {
      const receipt = await Api.forgetNetwork();
      this.acceptNetworkReceipt("forget", receipt);
    } catch (err) {
      this.rejectNetworkCommand("forget", err);
    }
  };

  setCameraFocus = async (focusPatch) => {
    if (this.state.focusPending) return;
    this.dispatch({ type: "camera-focus.pending" });
    try {
      const updated = await Api.setCameraFocus(focusPatch);
      this.dispatch({ type: "camera-focus.updated", payload: updated });
    } catch (err) {
      this.dispatch({ type: "command.failed", error: toUiError(err) });
    } finally {
      this.dispatch({ type: "camera-focus.settled" });
    }
  };

  acceptCaptureEvent = async (ev) => {
    if (ev.type === "safe_swap") {
      if (isValidSafeSwapReceipt(ev.data)) {
        this.dispatch({ type: "safe-swap.loaded", payload: ev.data });
      }
      await this.refreshRelatedResources();
      return;
    }
    if (ev.type === "progress") {
      if (isValidCaptureProgressEventData(ev.data)) {
        this.dispatch({ type: "capture.progress", payload: ev.data });
      }
      await this.refreshCapture();
      return;
    }
    if (ev.type === "state") {
      if (isValidCaptureStateEventData(ev.data)) {
        this.dispatch({ type: "capture.state-projected", event: ev });
        const prev = this.state.capture;
        await this.refreshCapture();
        const sealedId = detectNewlySealedSessionId(prev, this.state.capture);
        if (sealedId) {
          await this.refreshRelatedResources();
          this.scheduleSealedSessionRefresh(sealedId);
        }
      }
      return;
    }
    const prev = this.state.capture;
    if (
      ev.type === "snapshot" &&
      ev.data?.schema === "miro-ego.capture-snapshot-event.v1" &&
      ((prev &&
        ev.authority_epoch === prev.authority_epoch &&
        ev.source_revision >= prev.source_revision) ||
        !prev)
    ) {
      const wrapped = {
        schema: "miro-ego.capture-status.v1",
        authority_epoch: ev.authority_epoch,
        source_revision: ev.source_revision,
        snapshot: ev.data,
      };
      this.dispatch({ type: "capture.snapshot", payload: wrapped });
      await this.refreshRelatedResources();
      const sealedId = detectNewlySealedSessionId(prev, wrapped);
      if (sealedId) this.scheduleSealedSessionRefresh(sealedId);
      return;
    }
    if (ev.type === "diagnostic" && ev.data?.schema === "miro-ego.capture-diagnostic-event.v1") {
      this.dispatch({ type: "diagnostic.received", payload: ev.data.diagnostic });
      return;
    }
    const beforeRefresh = this.state.capture;
    await this.refreshCapture();
    if (ev.type === "snapshot") {
      await this.refreshRelatedResources();
      const sealedId = detectNewlySealedSessionId(beforeRefresh, this.state.capture);
      if (sealedId) this.scheduleSealedSessionRefresh(sealedId);
    }
  };
}

const Store = new ConsoleStore();

function useConsoleState() {
  const [st, setSt] = useState(Store.getState());
  useEffect(() => Store.subscribe(() => setSt(Store.getState())), []);
  return st;
}

// ---------------------------------------------------------------------------
// Formatting Helpers & SVG Icons (Zero Middle-Dot Characters)
// ---------------------------------------------------------------------------

const BYTE_UNITS = ["B", "KiB", "MiB", "GiB", "TiB"];
function formatBytes(n) {
  if (typeof n !== "number" || !Number.isFinite(n)) return "--";
  let v = n;
  let u = 0;
  while (v >= 1024 && u < BYTE_UNITS.length - 1) {
    v /= 1024;
    u += 1;
  }
  return `${v.toFixed(1)} ${BYTE_UNITS[u]}`;
}

function formatGiB(n) {
  return typeof n === "number" && Number.isFinite(n) ? `${(n / 1024 ** 3).toFixed(1)} GiB` : "--";
}

function formatMiB(n) {
  return typeof n === "number" && Number.isFinite(n) ? `${(n / 1024 ** 2).toFixed(1)} MiB` : "--";
}

function formatSeconds(n) {
  return typeof n !== "number" || !Number.isFinite(n) ? "--" : `${n.toFixed(1)} 秒`;
}

function formatCountdown(seconds) {
  const ceil = Math.ceil(seconds);
  return `${Math.floor(ceil / 60)}:${String(ceil % 60).padStart(2, "0")}`;
}

function formatCount(n) {
  return typeof n === "number" && Number.isFinite(n) ? String(n) : "--";
}

function formatFixed3(n) {
  return typeof n === "number" && Number.isFinite(n) ? n.toFixed(3) : "--";
}

function formatTempC(n) {
  return typeof n === "number" && Number.isFinite(n) ? `${n.toFixed(1)} °C` : "--";
}

function formatImuVec(vec, unit) {
  return !vec || ![vec.x, vec.y, vec.z].every(Number.isFinite)
    ? "不可用"
    : `x ${formatFixed3(vec.x)}  y ${formatFixed3(vec.y)}  z ${formatFixed3(vec.z)} ${unit}`;
}

function formatDateTime(iso) {
  if (!iso) return "--";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString("zh-CN", { hour12: false });
}

const DEVICE_STATE_LABELS = {
  blocked: "受阻",
  encoding: "编码中",
  finalizing: "正在结束",
  idle: "待机",
  recording: "录制中",
  verifying: "正在校验封存",
};

const UNSUCCESSFUL_OUTCOME_LABELS = {
  abandoned: "已放弃",
  failed: "失败",
  media_lost: "录制存储不可用",
  recoverable: "未成功",
};

const CONNECTION_LABELS = {
  connected: "已连接",
  connecting: "正在连接",
  disconnected: "连接中断",
};

const DEFAULT_ROUTE_LABELS = {
  none: "无默认路由",
  wifi_client: "Wi-Fi",
  wired: "有线",
};

function formatDefaultRoute(route) {
  return DEFAULT_ROUTE_LABELS[route ?? "none"] ?? "不可用";
}

function formatDeviceState(st) {
  return st ? DEVICE_STATE_LABELS[st] ?? "状态未知" : "未连接";
}

const CONNECTION_METHOD_LABELS = {
  ethernet_direct: "直连网线",
  ethernet_lan: "局域网网线",
  offline: "离线",
  wifi_ap: "设备热点",
  wifi_client: "Wi-Fi",
};

function formatConnectionMethod(m) {
  return m ? CONNECTION_METHOD_LABELS[m] ?? m : "--";
}

const IFACE_STATE_LABELS = {
  active: "已启用",
  connected: "已连接",
  connecting: "连接中",
  degraded: "降级",
  disabled: "未启用",
  disconnected: "未连接",
  failed: "失败",
  starting: "启动中",
  unavailable: "不可用",
};

function formatInterfaceSummary(iface) {
  if (!iface) return "不可用";
  const parts = [IFACE_STATE_LABELS[iface.state] ?? iface.state];
  if (iface.peer_or_ssid) parts.push(iface.peer_or_ssid);
  if (iface.addresses.length > 0) parts.push(iface.addresses.join(", "));
  return parts.join(" / ");
}

const CLOCK_SOURCE_LABELS = {
  ntp: "NTP 网络校时 (ntp)",
  client: "浏览器已校准 (client)",
  unsynchronized: "尚未校时 (unsynchronized)",
};

function Icon({ size = 20, children, ...rest }) {
  return h("svg", {
    width: size,
    height: size,
    viewBox: "0 0 24 24",
    fill: "none",
    stroke: "currentColor",
    "stroke-width": "1.6",
    "stroke-linecap": "round",
    "stroke-linejoin": "round",
    "aria-hidden": "true",
    ...rest,
    children,
  });
}

const ListIcon = (props) =>
  h(Icon, { ...props, children: h("path", { d: "M4 6h16M4 12h16M4 18h10" }) });
const EditIcon = (props) =>
  h(Icon, { ...props, children: h("path", { d: "M14 5l5 5M4 20l5-1L20 8a2 2 0 00-5-5L4 15z" }) });
const TrashIcon = (props) =>
  h(Icon, {
    ...props,
    children: h("path", { d: "M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13M10 11v5M14 11v5" }),
  });
const DeviceIcon = (props) =>
  h(Icon, {
    ...props,
    children: [
      h("rect", { x: "3.5", y: "4.5", width: "17", height: "15" }),
      h("path", { d: "M7.5 9h9M7.5 12.5h5" }),
    ],
  });
const LogsIcon = (props) =>
  h(Icon, {
    ...props,
    children: [
      h("path", { d: "M6 3.5h9l4 4V20.5H6z" }),
      h("path", { d: "M14.5 3.5V8H19M9 12h6M9 15.5h6" }),
    ],
  });
const CloseIcon = (props) =>
  h(Icon, { ...props, children: h("path", { d: "M6 6l12 12M18 6L6 18" }) });
const BackIcon = (props) =>
  h(Icon, { ...props, children: h("path", { d: "M14 5l-7 7 7 7" }) });
const SearchIcon = (props) =>
  h(Icon, {
    ...props,
    children: [
      h("circle", { cx: "10.5", cy: "10.5", r: "6.5" }),
      h("path", { d: "M15.5 15.5L20 20" }),
    ],
  });
const RefreshIcon = (props) =>
  h(Icon, {
    ...props,
    children: [h("path", { d: "M19 8a7.5 7.5 0 10.5 7" }), h("path", { d: "M19 3v5h-5" })],
  });
const FullscreenIcon = (props) =>
  h(Icon, {
    ...props,
    children: h("path", { d: "M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5" }),
  });
const GridIcon = (props) =>
  h(Icon, {
    ...props,
    children: [
      h("rect", { x: "3.5", y: "6.5", width: "17", height: "11" }),
      h("path", { d: "M3.5 10h17M3.5 14h17M9 6.5v11M15 6.5v11" }),
    ],
  });
const DownloadIcon = (props) =>
  h(Icon, {
    ...props,
    children: [
      h("path", { d: "M12 4v10M8 10.5l4 4 4-4" }),
      h("path", { d: "M4.5 16v3.5h15V16" }),
    ],
  });

// ---------------------------------------------------------------------------
// Readiness, Timer, Clock Sync, Voice & Bottom Capture Deck
// ---------------------------------------------------------------------------

function useCaptureReadiness(connected) {
  const [status, setStatus] = useState(null);
  const [error, setError] = useState("");
  useEffect(() => {
    setStatus(null);
    setError("");
    if (!connected) return;
    const controller = new AbortController();
    let cancelled = false;
    let hadSuccess = false;
    let timerId;
    async function poll() {
      try {
        const res = await fetchCaptureReadiness(controller.signal);
        if (cancelled) return;
        hadSuccess = true;
        setStatus(res);
        setError("");
        if (res.code === "clock_unsynchronized") {
          ClockStore.requestPrompt();
        }
      } catch (err) {
        if (cancelled) return;
        if (!hadSuccess && err instanceof DeviceApiError && err.status === 404) {
          setStatus(null);
          setError("");
          return;
        }
        if (hadSuccess) {
          const msg = "暂时无法确认录制条件，正在重试";
          setError(msg);
          setStatus({
            schema: "miro-ego.capture-readiness.v1",
            allowed: false,
            code: "readiness_unavailable",
            message: msg,
            storage: { recording: null, system: null, backing: null },
          });
        }
      }
      if (!cancelled) timerId = setTimeout(poll, 2000);
    }
    poll();
    return () => {
      cancelled = true;
      controller.abort();
      clearTimeout(timerId);
    };
  }, [connected]);
  return { status, error };
}

function CaptureReadinessBar({ status, error, idle }) {
  if (!status) return null;
  const slotLabels = {
    recording: "录像盘",
    system: "系统盘",
    backing: "宿主盘",
  };
  return h("div", {
    class: "capture-readiness",
    role: "status",
    "aria-label": "当前录制条件",
    children: [
      h("span", {
        children:
          error ||
          (idle || status.code === "recovery_pending" || status.code === "volume_released"
            ? status.message
            : ""),
      }),
      Object.entries(status.storage).map(([slot, budget]) =>
        budget
          ? h(
              "span",
              {
                children: [
                  slotLabels[slot] || slot,
                  "余 ",
                  (budget.available_bytes / 1024 ** 3).toFixed(2),
                  " GiB",
                  budget.available_bytes < budget.required_bytes
                    ? `（至少需要 ${(budget.required_bytes / 1024 ** 3).toFixed(2)} GiB）`
                    : "",
                ],
              },
              slot
            )
          : null
      ),
    ],
  });
}

function CaptureTimerControl({ connected, idle, sessionId }) {
  const [timerDoc, setTimerDoc] = useState(null);
  const [unsupported, setUnsupported] = useState(false);
  const [loadErr, setLoadErr] = useState("");
  const [saveErr, setSaveErr] = useState("");
  const [enabledDraft, setEnabledDraft] = useState(false);
  const [minsDraft, setMinsDraft] = useState("10");
  const [secsDraft, setSecsDraft] = useState("0");
  const [saving, setSaving] = useState(false);
  const dialogRef = useRef(null);
  const epochRef = useRef(0);

  useEffect(() => {
    if (!connected) return;
    const controller = new AbortController();
    let timerId;
    let cancelled = false;
    const poll = async () => {
      const myEpoch = epochRef.current;
      try {
        const res = await TimerApi.get(controller.signal);
        if (!cancelled && myEpoch === epochRef.current) {
          setTimerDoc(res);
          setUnsupported(false);
          setLoadErr("");
        }
      } catch (err) {
        if (!cancelled && myEpoch === epochRef.current) {
          if (err instanceof DeviceApiError && err.status === 404) setUnsupported(true);
          else setLoadErr(err instanceof Error ? err.message : "读取定时设置失败");
        }
      } finally {
        if (!cancelled) timerId = setTimeout(poll, sessionId ? 1000 : 5000);
      }
    };
    poll();
    return () => {
      cancelled = true;
      controller.abort();
      clearTimeout(timerId);
    };
  }, [connected, sessionId]);

  const totalSeconds = Number(minsDraft) * 60 + Number(secsDraft);
  const validInput =
    /^\d+$/.test(minsDraft) &&
    /^\d+$/.test(secsDraft) &&
    Number(secsDraft) < 60 &&
    Number.isInteger(totalSeconds) &&
    totalSeconds >= 1 &&
    totalSeconds <= 86400;
  const canEdit = connected && idle && !saving;
  const activeCountdown =
    timerDoc?.active?.session_id === sessionId ? timerDoc?.active : null;

  async function saveTimer() {
    if (!canEdit || !validInput) return;
    epochRef.current += 1;
    setSaving(true);
    setSaveErr("");
    try {
      const updated = await TimerApi.set(enabledDraft, totalSeconds);
      epochRef.current += 1;
      setTimerDoc(updated);
      dialogRef.current?.close();
    } catch (err) {
      epochRef.current += 1;
      setSaveErr(`保存失败：${err instanceof Error ? err.message : "请重试"}`);
    } finally {
      setSaving(false);
    }
  }

  if (unsupported) return null;
  return h(Fragment, {
    children: [
      h("button", {
        type: "button",
        class: "capture-timer-trigger",
        "aria-label": "定时结束设置",
        disabled: !connected || saving,
        onClick: () => {
          setSaveErr("");
          setEnabledDraft(timerDoc?.settings.enabled ?? false);
          const dur = timerDoc?.settings.duration_seconds ?? 600;
          setMinsDraft(String(Math.floor(dur / 60)));
          setSecsDraft(String(dur % 60));
          dialogRef.current?.showModal();
        },
        children: loadErr
          ? "定时状态待确认"
          : timerDoc
            ? activeCountdown
              ? `剩余 ${formatCountdown(activeCountdown.remaining_seconds)}`
              : timerDoc.settings.enabled
                ? `定时 ${formatCountdown(timerDoc.settings.duration_seconds)}`
                : "定时结束：关"
            : "读取定时设置…",
      }),
      h("dialog", {
        ref: dialogRef,
        class: "capture-timer-dialog",
        "aria-labelledby": "capture-timer-title",
        onKeyDown: (ev) => {
          if (ev.key === "Enter") {
            ev.preventDefault();
            saveTimer();
          }
        },
        children: [
          h("h2", { id: "capture-timer-title", children: "定时结束采集" }),
          h("p", { children: "由设备自动停止并封存录像，关闭网页后仍然有效。" }),
          h("label", {
            class: "capture-timer-switch",
            children: [
              h("input", {
                type: "checkbox",
                checked: enabledDraft,
                disabled: !canEdit,
                onChange: (ev) => setEnabledDraft(ev.currentTarget.checked),
              }),
              "开启定时结束",
            ],
          }),
          h("div", {
            class: "capture-timer-duration",
            children: [
              h("label", {
                children: [
                  "分钟",
                  h("input", {
                    "aria-label": "定时时长（分钟）",
                    type: "number",
                    inputMode: "numeric",
                    min: "0",
                    max: "1440",
                    step: "1",
                    value: minsDraft,
                    disabled: !canEdit,
                    onInput: (ev) => setMinsDraft(ev.currentTarget.value),
                  }),
                ],
              }),
              h("label", {
                children: [
                  "秒",
                  h("input", {
                    "aria-label": "定时时长（秒）",
                    type: "number",
                    inputMode: "numeric",
                    min: "0",
                    max: "59",
                    step: "1",
                    value: secsDraft,
                    disabled: !canEdit,
                    onInput: (ev) => setSecsDraft(ev.currentTarget.value),
                  }),
                ],
              }),
            ],
          }),
          h("p", { children: "可设置 1 秒至 24 小时。设置会持久保留，用于后续网页或实体按钮录制。" }),
          !idle ? h("p", { children: "录制期间不能修改定时，可随时手动结束录制。" }) : null,
          !validInput ? h("p", { role: "alert", children: "请输入 1 秒至 24 小时的有效时长。" }) : null,
          saveErr || loadErr ? h("p", { role: "alert", children: saveErr || loadErr }) : null,
          h("div", {
            class: "capture-timer-actions",
            children: [
              h("button", {
                type: "button",
                disabled: saving,
                onClick: () => dialogRef.current?.close(),
                children: "关闭",
              }),
              h("button", {
                type: "button",
                disabled: !canEdit || !validInput,
                onClick: () => saveTimer(),
                children: saving ? "正在保存…" : "保存设置",
              }),
            ],
          }),
        ],
      }),
    ],
  });
}

function DeviceClockControl({ connected, idle, readinessBlockedByClock, startBlockedByClock }) {
  const [clockState, setClockState] = useState(ClockStore.current);
  const [nowPerf, setNowPerf] = useState(performance.now());
  const dialogRef = useRef(null);
  const [confirmReplace, setConfirmReplace] = useState(false);

  useEffect(() => ClockStore.subscribe(setClockState), []);
  useEffect(() => {
    const id = setInterval(() => setNowPerf(performance.now()), 1000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    if (clockState.promptRequested && connected && idle && dialogRef.current && !dialogRef.current.open) {
      dialogRef.current.showModal();
    }
  }, [clockState.promptRequested, connected, idle]);

  const status = clockState.status;
  const source = status?.source ?? "unsynchronized";
  const isSynchronized = !!status && source !== "unsynchronized" && !clockState.error;
  const liveDeviceMs = status
    ? status.unix_time_ms + Math.max(0, nowPerf - clockState.observedAt)
    : null;
  const driftMs = liveDeviceMs === null ? 0 : Math.abs(liveDeviceMs - Date.now());
  const fmtMs = (ms) => new Date(ms).toLocaleString("zh-CN", { hour12: false });
  const largeDrift = isSynchronized && driftMs > 60000;
  const canSync = connected && idle && !clockState.pending;
  const clockBlocked =
    readinessBlockedByClock ||
    startBlockedByClock ||
    (connected && status?.source === "unsynchronized");

  async function triggerClockSync(replace = false) {
    setConfirmReplace(false);
    try {
      await (replace ? Api.resyncClock() : Api.syncClock());
      Store.dispatch({ type: "error.cleared" });
      await Store.refreshCapture();
    } catch {}
  }

  if (!status && !clockState.error && !clockState.pending && !clockBlocked) return null;

  const sourceShort =
    source === "ntp" ? "NTP" : source === "client" ? "浏览器校时" : "未校时";

  return h("div", {
    class: "device-clock",
    children: [
      h("button", {
        type: "button",
        class: "capture-timer-trigger",
        "data-testid": "clock-status",
        disabled: !connected,
        onClick: () => {
          setConfirmReplace(false);
          ClockStore.clearPrompt();
          dialogRef.current?.showModal();
        },
        children: [
          connected
            ? clockState.error
              ? "校时失败 - 点击重试"
              : isSynchronized
                ? largeDrift
                  ? `时间偏差较大 (${sourceShort})`
                  : `时间已校准 (${sourceShort})`
                : "尚未校时 (unsynchronized)"
            : "设备时间待确认",
          connected && isSynchronized && liveDeviceMs !== null
            ? h("span", { children: [" | ", fmtMs(liveDeviceMs)] })
            : null,
        ],
      }),
      clockBlocked && canSync
        ? h("button", {
            type: "button",
            class: "capture-timer-trigger icon-label-gap",
            "data-testid": "clock-quick-sync",
            disabled: !canSync,
            onClick: () => triggerClockSync(false),
            children: clockState.pending ? "正在同步…" : "一键同步浏览器时间",
          })
        : null,
      h("dialog", {
        ref: dialogRef,
        class: "capture-timer-dialog",
        "aria-labelledby": "clock-title",
        onClose: () => ClockStore.clearPrompt(),
        children: [
          h("h2", { id: "clock-title", children: "设备时间同步 (Boot-Scoped)" }),
          h("p", {
            children: ["时钟源：", CLOCK_SOURCE_LABELS[source] ?? source],
          }),
          h("p", {
            children: ["设备时间：", liveDeviceMs === null ? "尚未读取" : fmtMs(liveDeviceMs)],
          }),
          h("p", {
            children: ["浏览器本机时间：", fmtMs(Date.now())],
          }),
          h("p", {
            children: [
              "本机时区：",
              Intl.DateTimeFormat().resolvedOptions().timeZone,
            ],
          }),
          status?.boot_id
            ? h("p", { children: ["当前启动周期 (boot_id)：", status.boot_id] })
            : null,
          status?.synchronized_at_ms != null
            ? h("p", {
                children: [
                  "最近校准：",
                  fmtMs(status.synchronized_at_ms),
                  status.age_seconds != null ? `（已运行 ${status.age_seconds} 秒）` : "",
                ],
              })
            : null,
          h("p", {
            children: isSynchronized
              ? source === "ntp"
                ? "设备时间由网络 NTP 权威同步。"
                : "设备时间已在当前启动周期内通过浏览器校准，断网后仍可独立录制。"
              : "当前启动周期尚未完成校时（clock_unsynchronized 会阻止开始录制）。点击下方按钮即可用当前浏览器时间完成校时。",
          }),
          largeDrift
            ? h("p", {
                role: "alert",
                children: "设备与当前浏览器时间相差超过一分钟，请确认本机日期正确后重新校准。",
              })
            : null,
          !connected
            ? h("p", { role: "alert", children: "设备已断开，显示的是上次读取的时间。" })
            : null,
          !idle
            ? h("p", { children: "录制或封存期间禁止修改系统时钟，请待机后操作。" })
            : null,
          clockState.error ? h("p", { role: "alert", children: clockState.error }) : null,
          confirmReplace
            ? h("p", {
                children: "确认用当前浏览器时间覆盖已有校准基准？仅影响之后的新录像，已封存录像保持不变。",
              })
            : null,
          h("div", {
            class: "capture-timer-actions",
            children: [
              h("button", {
                type: "button",
                onClick: () => {
                  ClockStore.clearPrompt();
                  dialogRef.current?.close();
                },
                children: "关闭",
              }),
              !isSynchronized
                ? h("button", {
                    type: "button",
                    disabled: !canSync,
                    onClick: () => triggerClockSync(false),
                    children: clockState.pending ? "正在同步…" : "立即同步浏览器时间",
                  })
                : null,
              isSynchronized && source !== "ntp"
                ? h("button", {
                    type: "button",
                    disabled: !canSync,
                    onClick: () => (confirmReplace ? triggerClockSync(true) : setConfirmReplace(true)),
                    children: confirmReplace ? "确认覆盖校准" : "使用本机时间重新校准",
                  })
                : null,
            ],
          }),
        ],
      }),
    ],
  });
}

function VoiceVolumeControl({ connected, idle }) {
  const [voiceDoc, setVoiceDoc] = useState(null);
  const [unsupported, setUnsupported] = useState(false);
  const [draftVolume, setDraftVolume] = useState(100);
  const [errMsg, setErrMsg] = useState("");
  const [saving, setSaving] = useState(false);
  const dialogRef = useRef(null);
  const epochRef = useRef(0);

  useEffect(() => {
    if (!connected) return;
    const controller = new AbortController();
    let cancelled = false;
    let timerId;
    async function poll() {
      const myEpoch = epochRef.current;
      try {
        const res = await VoiceApi.get(controller.signal);
        if (!cancelled && myEpoch === epochRef.current) {
          setVoiceDoc(res);
          setUnsupported(false);
          if (!dialogRef.current?.open) setErrMsg("");
        }
      } catch (err) {
        if (!cancelled && myEpoch === epochRef.current) {
          if (err instanceof DeviceApiError && err.status === 404) setUnsupported(true);
          else if (!dialogRef.current?.open) setErrMsg("提示音状态读取失败，请稍后重试");
        }
      } finally {
        if (!cancelled) timerId = setTimeout(poll, 5000);
      }
    }
    poll();
    return () => {
      cancelled = true;
      controller.abort();
      clearTimeout(timerId);
    };
  }, [connected]);

  const canSave = connected && idle && !saving && voiceDoc !== null;

  async function saveVolume() {
    if (!canSave) return;
    epochRef.current += 1;
    setSaving(true);
    setErrMsg("");
    try {
      const updated = await VoiceApi.set(draftVolume);
      epochRef.current += 1;
      setVoiceDoc(updated);
      dialogRef.current?.close();
    } catch (err) {
      epochRef.current += 1;
      setErrMsg(`保存失败：${err instanceof Error ? err.message : "请重试"}`);
    } finally {
      setSaving(false);
    }
  }

  if (unsupported) return null;
  return h(Fragment, {
    children: [
      h("button", {
        type: "button",
        class: "capture-timer-trigger",
        "aria-label": "提示音量设置",
        disabled: !connected || !voiceDoc || saving,
        onClick: () => {
          setDraftVolume(voiceDoc?.volume ?? 100);
          setErrMsg("");
          dialogRef.current?.showModal();
        },
        children:
          errMsg && !dialogRef.current?.open
            ? "提示音状态待确认"
            : voiceDoc
              ? voiceDoc.volume === 0
                ? "提示音：静音"
                : `提示音：${voiceDoc.volume}%`
              : "读取提示音设置…",
      }),
      h("dialog", {
        ref: dialogRef,
        class: "capture-timer-dialog",
        "aria-labelledby": "voice-volume-title",
        onKeyDown: (ev) => {
          if (ev.key === "Enter" && ev.target instanceof HTMLInputElement) {
            ev.preventDefault();
            saveVolume();
          }
        },
        children: [
          h("h2", { id: "voice-volume-title", children: "提示音量" }),
          h("p", { children: "调节设备的开始录制、封存和故障语音提示音量，不影响麦克风采集增益。" }),
          h("label", {
            class: "voice-volume-control",
            children: [
              "提示音量：",
              draftVolume === 0 ? "静音" : `${draftVolume}%`,
              h("input", {
                "aria-label": "提示音量",
                type: "range",
                min: "0",
                max: "100",
                step: "1",
                value: draftVolume,
                disabled: !canSave,
                onInput: (ev) => setDraftVolume(Number(ev.currentTarget.value)),
              }),
            ],
          }),
          h("p", { children: "0 为静音，保存后持久生效。" }),
          !idle ? h("p", { children: "请在录制结束后调整提示音量。" }) : null,
          !connected ? h("p", { role: "alert", children: "设备已断开，暂时无法保存。" }) : null,
          errMsg ? h("p", { role: "alert", children: errMsg }) : null,
          h("div", {
            class: "capture-timer-actions",
            children: [
              h("button", {
                type: "button",
                disabled: saving,
                onClick: () => dialogRef.current?.close(),
                children: "关闭",
              }),
              h("button", {
                type: "button",
                disabled: !canSave,
                onClick: () => saveVolume(),
                children: saving ? "正在保存…" : "保存音量",
              }),
            ],
          }),
        ],
      }),
    ],
  });
}

const CALIBRATION_DISABLED_LABELS = {
  capture_source_unsupported: "分眼录制链路不可用",
  storage_unavailable: "录制存储不可用",
  hardware_unavailable: "相机硬件不可用",
  maintenance_or_capture_busy: "设备正忙",
};

function useSmoothElapsedSeconds(reportedSeconds, sessionId, running) {
  const [displaySeconds, setDisplaySeconds] = useState(reportedSeconds);
  const trackerRef = useRef({
    sessionId,
    seconds: reportedSeconds,
    observedAt: performance.now(),
    running: false,
  });

  useEffect(() => {
    const now = performance.now();
    const prev = trackerRef.current;
    const sameActive = running && sessionId !== null && prev.running && prev.sessionId === sessionId;
    const projected = prev.seconds + Math.max(0, now - prev.observedAt) / 1000;
    const base = sameActive ? Math.max(reportedSeconds, projected) : reportedSeconds;
    trackerRef.current = {
      sessionId,
      seconds: base,
      observedAt: now,
      running,
    };
    setDisplaySeconds((cur) => (sameActive ? Math.max(cur, base) : base));
    if (!running || sessionId === null) return;
    const tick = () => {
      const cur = trackerRef.current;
      if (!cur.running || cur.sessionId !== sessionId) return;
      const val = cur.seconds + (performance.now() - cur.observedAt) / 1000;
      setDisplaySeconds((prevVal) => Math.max(prevVal, val));
    };
    const id = globalThis.setInterval(tick, 50);
    return () => globalThis.clearInterval(id);
  }, [reportedSeconds, running, sessionId]);

  return displaySeconds;
}

function BottomCommandDeck({ state }) {
  const [sessionName, setSessionName] = useState("");
  const [captureMode, setCaptureMode] = useState("record");
  const [namingOpen, setNamingOpen] = useState(false);
  const [confirmIdleEject, setConfirmIdleEject] = useState(false);
  const nameInputRef = useRef(null);

  const snapshot = state.capture?.snapshot;
  const deviceState = snapshot?.device_state ?? null;
  const activeRec = snapshot?.active_recording?.recording_state ?? null;
  const progress = activeRec?.progress ?? null;
  const verificationCounter = progress?.verification ?? null;

  useEffect(() => {
    if (namingOpen) {
      nameInputRef.current?.focus();
      nameInputRef.current?.select();
    }
  }, [namingOpen]);

  useEffect(() => {
    if (activeRec) {
      setNamingOpen(false);
      setConfirmIdleEject(false);
    }
  }, [activeRec?.session_id]);

  const connected = state.connection === "connected";
  const isRecording = deviceState === "recording";
  const isFinalizingChain = ["finalizing", "encoding", "verifying"].includes(deviceState ?? "");
  const elapsed = useSmoothElapsedSeconds(
    progress?.elapsed_seconds ?? 0,
    activeRec?.session_id ?? null,
    connected && isRecording
  );
  const readiness = useCaptureReadiness(connected);
  const readinessAllowed = readiness.status?.allowed ?? state.device?.capabilities.capture !== false;
  const cameraConnected =
    (snapshot?.runtime.camera ?? state.device?.runtime.camera)?.state === "connected";
  const storageWritable = readiness.status?.allowed ?? state.device?.storage.writable === true;
  const canStartProd =
    !!snapshot &&
    connected &&
    cameraConnected &&
    readinessAllowed &&
    storageWritable &&
    deviceState === "idle" &&
    !state.commandPending;
  const canStop = connected && isRecording && !state.commandPending;

  const calCap = state.device?.capabilities.calibration_capture;
  const calEnabled = calCap?.enabled === true;
  const canStartCal = canStartProd && calEnabled;
  const calReasonText = calEnabled
    ? "开始分眼标定录制"
    : calCap
      ? CALIBRATION_DISABLED_LABELS[calCap.disabled_reason] ?? "标定不可用"
      : "设备未报告标定录制能力";
  const isCalSelected = captureMode === "calibration" && calEnabled;
  const shutterEnabled = isRecording ? canStop : isCalSelected ? canStartCal : canStartProd;

  const shutterLabel = state.commandPending
    ? "正在发送"
    : isRecording
      ? "结束录制"
      : isFinalizingChain
        ? DEVICE_STATE_LABELS[deviceState] ?? "正在处理"
        : isCalSelected
          ? "开始标定录制"
          : "开始录制";

  const volumeAlreadyReleased =
    readiness.status?.code === "volume_released" || state.safeSwapReceipt !== null;
  const canIdleSafeEject =
    connected &&
    deviceState === "idle" &&
    !state.commandPending &&
    !volumeAlreadyReleased;

  const readinessClockBlocked = readiness.status?.code === "clock_unsynchronized";
  const startClockBlocked = state.error?.code === "clock_unsynchronized";

  const phaseProg = state.phaseProgress;
  const phaseTotal =
    verificationCounter?.total ??
    (phaseProg && typeof phaseProg.total_units === "number" ? phaseProg.total_units : null);
  const phaseCompleted =
    verificationCounter?.completed ??
    (phaseProg ? phaseProg.completed_units : null);

  return h("footer", {
    class: "bottombar",
    "data-active": String(!!activeRec),
    "data-naming": String(namingOpen),
    children: [
      h(DeviceClockControl, {
        connected,
        idle: deviceState === "idle" && !state.commandPending,
        readinessBlockedByClock: readinessClockBlocked,
        startBlockedByClock: startClockBlocked,
      }),
      h(CaptureReadinessBar, {
        status: readiness.status,
        error: readiness.error,
        idle: deviceState === "idle",
      }),
      h("form", {
        class: "command-form",
        onSubmit: (ev) => {
          ev.preventDefault();
          if (isRecording) {
            if (canStop) Store.stopCapture("user");
          } else if (isCalSelected) {
            if (canStartCal) Store.startCapture(sessionName, "calibration");
          } else if (canStartProd) {
            Store.startCapture(sessionName, "production");
          }
        },
        children: [
          h("button", {
            type: "button",
            class: "capture-shortcut capture-library",
            "aria-label": "打开录制列表",
            onClick: () => {
              Store.dispatch({ type: "panel.opened", panel: "sessions" });
              Store.refreshSessions();
            },
            children: [h(ListIcon, { size: 24 }), h("span", { children: "录制列表" })],
          }),
          h("div", {
            class: "shutter-stack",
            children: [
              h("div", {
                class: "mode-seg",
                role: "group",
                "aria-label": "录制模式",
                children: [
                  h("button", {
                    type: "button",
                    "aria-pressed": captureMode === "record",
                    disabled: isRecording || isFinalizingChain || state.commandPending,
                    onClick: () => setCaptureMode("record"),
                    children: "录制",
                  }),
                  h("button", {
                    type: "button",
                    "aria-pressed": captureMode === "calibration",
                    disabled: isRecording || isFinalizingChain || state.commandPending || !calEnabled,
                    title: calReasonText,
                    "aria-describedby": calEnabled ? undefined : "calibration-reason",
                    onClick: () => {
                      if (calEnabled) setCaptureMode("calibration");
                    },
                    children: [
                      h(GridIcon, { size: 13 }),
                      h("span", { class: "icon-label-gap", children: "标定" }),
                    ],
                  }),
                ],
              }),
              h("button", {
                type: "submit",
                class: "shutter",
                "data-recording": String(isRecording),
                "aria-label": shutterLabel,
                title: shutterLabel,
                disabled: !shutterEnabled,
                children: h("span", { class: "shutter-dot", "aria-hidden": "true" }),
              }),
              h("span", {
                class: "shutter-caption",
                "aria-hidden": "true",
                children: shutterLabel,
              }),
              h("div", {
                class: "capture-preferences",
                children: [
                  h(VoiceVolumeControl, {
                    connected,
                    idle: deviceState === "idle" && !state.commandPending,
                  }),
                  h(CaptureTimerControl, {
                    connected,
                    idle: deviceState === "idle" && !state.commandPending,
                    sessionId: isRecording ? activeRec?.session_id ?? null : null,
                  }),
                ],
              }),
            ],
          }),
          h("button", {
            type: "button",
            class: "capture-shortcut capture-name-shortcut",
            "aria-label": "命名",
            "aria-expanded": namingOpen,
            "aria-controls": "capture-name-editor",
            disabled:
              !!activeRec || !connected || !cameraConnected || state.commandPending || deviceState !== "idle",
            onClick: () => setNamingOpen(!namingOpen),
            children: [
              h(EditIcon, { size: 24 }),
              h("span", { title: sessionName, children: sessionName || "命名" }),
            ],
          }),
          h("div", {
            class: "command-body",
            id: "capture-name-editor",
            children: activeRec
              ? h(Fragment, {
                  children: [
                    h("span", {
                      class: "eyebrow",
                      children: isFinalizingChain
                        ? `当前阶段：${DEVICE_STATE_LABELS[deviceState] ?? deviceState}`
                        : "当前会话",
                    }),
                    h("span", {
                      class: "session-title",
                      "data-testid": "current-session-name",
                      children: activeRec.display_name,
                    }),
                    h("code", { class: "session-uuid", children: activeRec.session_id }),
                    isFinalizingChain
                      ? h("div", {
                          class: "phase-progress",
                          "data-testid": "finalizing-progress",
                          children: [
                            phaseTotal !== null && phaseTotal > 0
                              ? h("progress", { max: phaseTotal, value: phaseCompleted ?? 0 })
                              : h("progress", {}),
                            h("span", {
                              children:
                                phaseTotal !== null && phaseTotal > 0
                                  ? `${DEVICE_STATE_LABELS[deviceState] ?? deviceState} (${phaseCompleted ?? 0}/${phaseTotal})`
                                  : `${DEVICE_STATE_LABELS[deviceState] ?? deviceState}…`,
                            }),
                          ],
                        })
                      : null,
                  ],
                })
              : h(Fragment, {
                  children: [
                    h("label", {
                      class: "eyebrow",
                      for: "capture-name",
                      children: "录制名称（可选）",
                    }),
                    h("button", {
                      type: "button",
                      class: "name-done",
                      onClick: () => setNamingOpen(false),
                      children: "完成",
                    }),
                    h("div", {
                      class: "name-field",
                      children: [
                        h("input", {
                          ref: nameInputRef,
                          id: "capture-name",
                          class: "name-input",
                          type: "text",
                          maxLength: 160,
                          autocomplete: "off",
                          enterKeyHint: "done",
                          "aria-describedby": "capture-name-hint",
                          placeholder: "留空按时间命名",
                          value: sessionName,
                          disabled:
                            !connected || !cameraConnected || state.commandPending || deviceState !== "idle",
                          onInput: (ev) => setSessionName(ev.currentTarget.value),
                          onKeyDown: (ev) => {
                            if (ev.key === "Enter" && !ev.isComposing) {
                              ev.preventDefault();
                              nameInputRef.current?.blur();
                              setNamingOpen(false);
                            }
                          },
                        }),
                        sessionName
                          ? h("button", {
                              type: "button",
                              class: "clear-name",
                              "aria-label": "清空名称",
                              disabled:
                                !connected ||
                                !cameraConnected ||
                                state.commandPending ||
                                deviceState !== "idle",
                              onClick: () => {
                                setSessionName("");
                                nameInputRef.current?.focus();
                              },
                              children: h(CloseIcon, { size: 18 }),
                            })
                          : null,
                      ],
                    }),
                    h("p", {
                      id: "capture-name-hint",
                      class: "name-hint",
                      children: sessionName
                        ? "名称已就绪，点击快门开始录制"
                        : "可直接开始，设备会按录制时间自动命名",
                    }),
                    connected
                      ? cameraConnected
                        ? null
                        : h("p", { class: "command-lock", children: "相机未接入，录制已锁定" })
                      : h("p", {
                          class: "command-lock",
                          children:
                            state.connection === "connecting"
                              ? "等待权威事件，命令已封锁"
                              : "事件流断开，命令已封锁",
                        }),
                  ],
                }),
          }),
          h("dl", {
            class: "readout",
            children: [
              h("div", {
                children: [
                  h("dt", { children: "时长" }),
                  h("dd", {
                    "data-testid": "elapsed-seconds",
                    "data-idle": String(!progress),
                    children: formatSeconds(elapsed),
                  }),
                ],
              }),
              h("div", {
                children: [
                  h("dt", { children: "帧数" }),
                  h("dd", {
                    "data-testid": "captured-frames",
                    "data-idle": String(!progress),
                    children: formatCount(progress?.captured_frames ?? 0),
                  }),
                ],
              }),
              h("div", {
                children: [
                  h("dt", { children: "写入" }),
                  h("dd", {
                    "data-testid": "bytes-written",
                    "data-idle": String(!progress),
                    children: formatMiB(progress?.bytes_written ?? 0),
                  }),
                ],
              }),
            ],
          }),
          h("div", {
            class: "command-actions",
            children: isRecording
              ? [
                  h("button", {
                    type: "button",
                    class: "command-button",
                    "data-testid": "safe-swap-trigger",
                    disabled: !canStop,
                    title: "停止当前录制、完成零重读封存并安全释放录像盘用于拔卡",
                    onClick: () => Store.safeSwapStop(),
                    children: "封存并安全弹出",
                  }),
                ]
              : deviceState === "idle"
                ? confirmIdleEject
                  ? [
                      h("button", {
                        type: "button",
                        class: "command-button",
                        "data-tone": "danger",
                        "data-testid": "safe-swap-confirm",
                        disabled: !canIdleSafeEject,
                        onClick: () => {
                          setConfirmIdleEject(false);
                          Store.safeSwapStop();
                        },
                        children: "确认释放存储卷",
                      }),
                      h("button", {
                        type: "button",
                        class: "command-button",
                        onClick: () => setConfirmIdleEject(false),
                        children: "取消",
                      }),
                    ]
                  : [
                      h("button", {
                        type: "button",
                        class: "command-button",
                        "data-testid": "safe-swap-trigger",
                        disabled: !canIdleSafeEject,
                        title: volumeAlreadyReleased
                          ? "存储卷已安全释放"
                          : "安全释放 MIRO-EGO 录像存储卷 (Safe Swap / Safe Eject)",
                        onClick: () => setConfirmIdleEject(true),
                        children: volumeAlreadyReleased ? "存储卷已释放" : "安全弹出存储卷",
                      }),
                    ]
                : [],
          }),
        ],
      }),
      h("span", { id: "calibration-reason", class: "visually-hidden", children: calReasonText }),
    ],
  });
}

// ---------------------------------------------------------------------------
// Customer Authentication Dialog
// ---------------------------------------------------------------------------

function CredentialDialog({ onConnected }) {
  const [token, setToken] = useState("");
  const [csrfToken, setCsrfToken] = useState(() => readCsrfToken() || "");
  const [submitting, setSubmitting] = useState(false);

  return h("div", {
    class: "credential",
    role: "dialog",
    "aria-modal": "true",
    "aria-label": "设备认证",
    children: h("form", {
      onSubmit: (ev) => {
        ev.preventDefault();
        if (!token.trim() || submitting) return;
        setSubmitting(true);
        saveCredentials(token, csrfToken);
        Store.loadInitialState().then((ok) => {
          setSubmitting(false);
          if (ok) {
            setToken("");
            onConnected();
          }
        });
      },
      children: [
        h("span", { class: "eyebrow", children: "设备访问令牌" }),
        h("p", {
          class: "credential-note",
          children: "当前设备运行在需要认证的安全配置下，请提供有效的 Bearer 访问令牌。",
        }),
        h("input", {
          type: "password",
          autocomplete: "off",
          spellcheck: false,
          required: true,
          placeholder: "Bearer 访问令牌",
          value: token,
          "aria-label": "设备访问令牌",
          onInput: (ev) => setToken(ev.currentTarget.value),
        }),
        h("input", {
          type: "password",
          autocomplete: "off",
          spellcheck: false,
          placeholder: "可选独立 CSRF 令牌（默认复用访问令牌）",
          value: csrfToken,
          "aria-label": "CSRF 令牌（可选）",
          onInput: (ev) => setCsrfToken(ev.currentTarget.value),
        }),
        h("button", {
          type: "submit",
          class: "command-button",
          disabled: submitting || !token.trim(),
          children: submitting ? "正在连接" : "连接设备",
        }),
      ],
    }),
  });
}

// ---------------------------------------------------------------------------
// Device Panel, Optics, Storage Capacity & SafeSwap Receipt Card
// ---------------------------------------------------------------------------

function formatCameraFocusNote(focus) {
  const base = `手动焦距 ${focus.value}（${focus.minimum}-${focus.maximum}，步进 ${focus.step}）`;
  if (focus.auto_enabled === true) return "自动对焦已开启";
  return focus.auto_supported ? base : `${base}；当前相机未暴露 V4L2 focus_auto`;
}

function OpticsSection({ state }) {
  const runtime = state.capture?.snapshot.runtime ?? state.device?.runtime ?? null;
  const focus = runtime?.camera_focus;
  const [draftFocus, setDraftFocus] = useState(null);

  if (runtime?.camera.state !== "connected" || !focus) {
    const disconnected = runtime?.camera.state === "disconnected";
    return h("section", {
      class: "detail-section",
      children: [
        h("span", { class: "eyebrow", children: "OPTICS" }),
        h("dl", {
          class: "facts",
          children: h("div", {
            children: [
              h("dt", { children: "手动焦距" }),
              h("dd", {
                "data-testid": "camera-focus-value",
                "data-tone": "caution",
                children: "不可用",
              }),
            ],
          }),
        }),
        h("p", {
          id: "focus-status",
          class: "panel-note",
          children: disconnected ? "相机未接入" : "当前相机未暴露 V4L2 focus_absolute/focus_auto 控制",
        }),
      ],
    });
  }

  const effectiveVal = draftFocus ?? focus.value;
  return h("section", {
    class: "detail-section",
    children: [
      h("span", { class: "eyebrow", children: "OPTICS" }),
      h("dl", {
        class: "facts",
        children: h("div", {
          children: [
            h("dt", { children: "手动焦距" }),
            h("dd", { "data-testid": "camera-focus-value", children: effectiveVal }),
          ],
        }),
      }),
      h("input", {
        id: "camera-focus-range",
        name: "value",
        type: "range",
        min: focus.minimum,
        max: focus.maximum,
        step: focus.step,
        value: effectiveVal,
        disabled: state.focusPending || focus.auto_enabled === true,
        "aria-label": "手动焦距",
        onInput: (ev) => setDraftFocus(Number(ev.currentTarget.value)),
      }),
      h("p", { id: "focus-status", class: "panel-note", children: formatCameraFocusNote(focus) }),
      h("div", {
        class: "chips",
        children: [
          h("button", {
            type: "button",
            class: "chip",
            id: "focus-command",
            disabled: state.focusPending || draftFocus === null,
            onClick: () => {
              if (draftFocus !== null) {
                Store.setCameraFocus({ value: draftFocus });
                setDraftFocus(null);
              }
            },
            children: state.focusPending ? "正在应用" : "应用焦距",
          }),
          h("button", {
            type: "button",
            class: "chip",
            id: "camera-focus-auto",
            "aria-pressed": focus.auto_enabled === true,
            disabled: state.focusPending || !focus.auto_supported,
            onClick: () => Store.setCameraFocus({ auto_enabled: !focus.auto_enabled }),
            children: "自动对焦",
          }),
        ],
      }),
    ],
  });
}

function StorageCapacityMeter({ state }) {
  const storage = state.device?.storage;
  const total = storage?.total_bytes;
  const avail = storage?.available_bytes;
  if (
    !(
      typeof total === "number" &&
      Number.isFinite(total) &&
      total > 0 &&
      typeof avail === "number" &&
      Number.isFinite(avail)
    )
  ) {
    return h("p", {
      class: "panel-note",
      "data-testid": "storage-capacity",
      children: "容量不可用（存储卷未挂载或已释放）",
    });
  }
  const used = Math.max(0, total - avail);
  const pct = Math.min(100, Math.max(0, (used / total) * 100));
  return h("div", {
    "data-testid": "storage-capacity",
    children: [
      h("div", {
        class: "capacity-bar",
        role: "img",
        "aria-label": `已用 ${formatBytes(used)} / 共 ${formatBytes(total)}`,
        children: h("svg", {
          viewBox: "0 0 100 6",
          preserveAspectRatio: "none",
          "aria-hidden": "true",
          children: h("rect", { x: "0", y: "0", width: pct.toFixed(1), height: "6" }),
        }),
      }),
      h("div", {
        class: "capacity-caption",
        children: [
          h("span", { children: ["已用 ", formatBytes(used)] }),
          h("span", { children: ["可用 ", formatBytes(avail), " / ", formatBytes(total)] }),
        ],
      }),
    ],
  });
}

function SafeSwapReceiptCard({ receipt }) {
  if (!receipt) return null;
  return h("div", {
    class: "safe-swap-card",
    "data-testid": "safe-swap-receipt",
    children: [
      h("strong", {
        children: [
          "安全弹出回执已就绪 (",
          receipt.release_state,
          ")",
        ],
      }),
      h("dl", {
        class: "facts",
        children: [
          h("div", {
            children: [
              h("dt", { children: "释放状态" }),
              h("dd", { children: receipt.release_state }),
            ],
          }),
          h("div", {
            children: [
              h("dt", { children: "打开句柄数" }),
              h("dd", { children: String(receipt.open_handle_count) }),
            ],
          }),
          h("div", {
            children: [
              h("dt", { children: "释放时间" }),
              h("dd", { children: formatDateTime(receipt.released_at) }),
            ],
          }),
          h("div", {
            children: [
              h("dt", { children: "封存会话" }),
              h("dd", { class: "artifact-hash", children: receipt.session_id }),
            ],
          }),
          h("div", {
            children: [
              h("dt", { children: "清单摘要" }),
              h("dd", { class: "artifact-hash", children: receipt.manifest_sha256 }),
            ],
          }),
        ],
      }),
    ],
  });
}

function DevicePanel({ state }) {
  const dev = state.device;
  const runtime = state.capture?.snapshot.runtime ?? dev?.runtime ?? null;
  const deviceState = state.capture?.snapshot.device_state ?? null;
  const canSafeSwapNow =
    state.connection === "connected" &&
    !state.commandPending &&
    (deviceState === "recording" || (deviceState === "idle" && !state.safeSwapReceipt));

  return h("aside", {
    id: "device-panel",
    class: "panel",
    "aria-label": "设备与链路",
    children: [
      h("div", {
        class: "panel-head",
        children: [
          h("span", { class: "eyebrow", children: "DEVICE" }),
          h("span", { class: "panel-title", children: "设备与链路" }),
          h("span", { class: "panel-spacer" }),
          h("button", {
            type: "button",
            class: "icon-button",
            "aria-label": "关闭",
            onClick: () => {
              Store.dispatch({ type: "panel.closed" });
              window.requestAnimationFrame(() =>
                document.getElementById("device-panel-trigger")?.focus()
              );
            },
            children: h(CloseIcon, {}),
          }),
        ],
      }),
      h("div", {
        class: "panel-body",
        children: [
          h("section", {
            class: "detail-section",
            children: [
              h("span", { class: "eyebrow", children: "RUNTIME" }),
              h("dl", {
                class: "facts",
                children: [
                  h("div", {
                    children: [
                      h("dt", { children: "温度" }),
                      h("dd", { children: formatTempC(runtime?.temperature_celsius) }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "连接方式" }),
                      h("dd", { children: formatConnectionMethod(runtime?.connection_method) }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "相机" }),
                      h("dd", {
                        "data-testid": "camera-connection",
                        "data-tone":
                          runtime?.camera.state === "disconnected" ? "caution" : undefined,
                        children: runtime
                          ? runtime.camera.state === "connected"
                            ? "已连接"
                            : "未接入"
                          : "--",
                      }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "H.264 低带宽预览" }),
                      h("dd", {
                        children: dev?.capabilities.preview_h264 ? "已启用 (/preview/h264)" : "未启用 (使用 MJPEG)",
                      }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "观测于" }),
                      h("dd", { "data-tone": "muted", children: formatDateTime(runtime?.observed_at) }),
                    ],
                  }),
                ],
              }),
              dev?.capabilities.diagnostic_logs !== false
                ? h("div", {
                    class: "chips",
                    children: h("button", {
                      type: "button",
                      class: "chip",
                      onClick: () => {
                        Store.dispatch({ type: "panel.opened", panel: "logs" });
                        Store.refreshLogs();
                      },
                      children: "打开诊断日志面板 (/logs)",
                    }),
                  })
                : null,
            ],
          }),
          h(OpticsSection, { state }),
          h("section", {
            class: "detail-section",
            children: [
              h("span", { class: "eyebrow", children: "STORAGE (MIRO-EGO EXFAT)" }),
              h(StorageCapacityMeter, { state }),
              h("dl", {
                class: "facts",
                children: [
                  h("div", {
                    children: [
                      h("dt", { children: "可写" }),
                      h("dd", {
                        "data-tone": dev?.storage.writable ? undefined : "caution",
                        children: dev?.storage.writable ? "是" : "否",
                      }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "volume" }),
                      h("dd", { children: dev?.storage.volume_id ?? "--" }),
                    ],
                  }),
                ],
              }),
              h("div", {
                class: "storage-actions",
                children: [
                  h("button", {
                    type: "button",
                    class: "chip",
                    disabled: !canSafeSwapNow,
                    onClick: () => Store.safeSwapStop(),
                    children:
                      deviceState === "recording"
                        ? "封存并安全弹出存储卷"
                        : state.safeSwapReceipt
                          ? "存储卷已安全释放"
                          : "安全弹出 / 释放存储卷",
                  }),
                  h("button", {
                    type: "button",
                    class: "chip",
                    onClick: () => Store.refreshSafeSwap(),
                    children: "查询弹出回执",
                  }),
                ],
              }),
              h(SafeSwapReceiptCard, { receipt: state.safeSwapReceipt }),
            ],
          }),
          h("section", {
            class: "detail-section",
            children: [
              h("span", { class: "eyebrow", children: "BUILD & AUTHORITY" }),
              h("dl", {
                class: "facts",
                children: [
                  h("div", {
                    children: [
                      h("dt", { children: "软件包" }),
                      h("dd", { children: dev?.build?.package_version ?? "--" }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "Device API" }),
                      h("dd", { children: dev?.api_version ?? "--" }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "安全 profile" }),
                      h("dd", { children: dev?.security_profile ?? "--" }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "build id" }),
                      h("dd", { children: dev?.build?.build_id ?? "--" }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "commit" }),
                      h("dd", { class: "artifact-hash", children: dev?.build?.commit ?? "--" }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "硬件指纹" }),
                      h("dd", { class: "artifact-hash", children: dev?.hardware_fingerprint ?? "--" }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "authority epoch" }),
                      h("dd", {
                        class: "artifact-hash",
                        children: state.capture?.authority_epoch ?? "--",
                      }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "source revision" }),
                      h("dd", { children: state.capture?.source_revision ?? "--" }),
                    ],
                  }),
                ],
              }),
            ],
          }),
        ],
      }),
    ],
  });
}

// ---------------------------------------------------------------------------
// Diagnostic Logs Panel (/api/v1/logs & /api/v1/logs/{name})
// ---------------------------------------------------------------------------

function DiagnosticLogsPanel({ state }) {
  const { items, volumeId, observedAt, truncated, loading, loadedOnce, error, prefix } = state.logs;
  const [downloadErr, setDownloadErr] = useState("");
  const hasBearer = !!readAccessToken();

  return h("aside", {
    id: "logs-panel",
    class: "panel",
    "data-testid": "diagnostic-logs-panel",
    "aria-label": "诊断日志",
    children: [
      h("div", {
        class: "panel-head",
        children: [
          h("span", { class: "eyebrow", children: "LOGS" }),
          h("span", { class: "panel-title", children: "诊断日志" }),
          h("span", { class: "panel-spacer" }),
          h("button", {
            type: "button",
            class: "icon-button",
            "aria-label": "刷新日志",
            title: "刷新日志",
            disabled: loading,
            onClick: () => Store.refreshLogs(),
            children: h(RefreshIcon, {}),
          }),
          h("button", {
            type: "button",
            class: "icon-button",
            "aria-label": "关闭",
            onClick: () => {
              Store.dispatch({ type: "panel.closed" });
              window.requestAnimationFrame(() =>
                document.getElementById("logs-panel-trigger")?.focus()
              );
            },
            children: h(CloseIcon, {}),
          }),
        ],
      }),
      h("div", {
        class: "panel-tools",
        children: [
          h("form", {
            class: "search",
            onSubmit: (ev) => {
              ev.preventDefault();
              Store.refreshLogs(prefix);
            },
            children: [
              h(SearchIcon, { size: 15 }),
              h("input", {
                type: "search",
                value: prefix,
                placeholder: "按路径前缀筛选（如 wifi-watchdog）",
                "aria-label": "筛选诊断日志前缀",
                onInput: (ev) => Store.dispatch({ type: "logs.prefix", prefix: ev.currentTarget.value }),
              }),
            ],
          }),
          h("div", {
            class: "chips",
            children: [
              h("button", {
                type: "button",
                class: "chip",
                disabled: loading,
                onClick: () => Store.refreshLogs(prefix),
                children: loading ? "正在读取…" : "应用筛选",
              }),
              prefix
                ? h("button", {
                    type: "button",
                    class: "chip",
                    disabled: loading,
                    onClick: () => Store.refreshLogs(""),
                    children: "清除前缀",
                  })
                : null,
              h("span", {
                class: "chip chip-stat",
                "aria-hidden": "true",
                children: loadedOnce ? `共 ${items.length} 项${truncated ? "（已截断）" : ""}` : null,
              }),
            ],
          }),
          observedAt || volumeId
            ? h("p", {
                class: "panel-note",
                children: [
                  observedAt ? `观测于 ${formatDateTime(observedAt)}` : "",
                  volumeId ? ` | 卷 ${volumeId.slice(0, 8)}` : "",
                ],
              })
            : null,
        ],
      }),
      h("div", {
        class: "panel-body",
        children: [
          error
            ? h("div", {
                class: "alert",
                role: "alert",
                children: [h("code", { children: error.code }), h("span", { children: error.message })],
              })
            : null,
          downloadErr
            ? h("div", {
                class: "alert",
                role: "alert",
                children: [h("code", { children: "download_failed" }), h("span", { children: downloadErr })],
              })
            : null,
          items.map((item) => {
            const href = Api.logUrl(item.name);
            const filename = item.name.split("/").pop() || item.name;
            return h(
              "div",
              {
                class: "log-row",
                "data-testid": "diagnostic-log-item",
                children: [
                  h("div", {
                    class: "log-body",
                    children: [
                      h("span", { class: "log-name", children: item.name }),
                      h("span", {
                        class: "log-meta",
                        children: [
                          h("span", { children: formatBytes(item.bytes) }),
                          h("span", { children: item.media_type }),
                          h("span", { children: formatDateTime(item.modified_at) }),
                        ],
                      }),
                      item.sha256
                        ? h("span", { class: "artifact-hash", children: ["sha256:", item.sha256] })
                        : null,
                    ],
                  }),
                  h("a", {
                    class: "download",
                    href,
                    download: filename,
                    rel: "noreferrer",
                    onClick: hasBearer
                      ? (ev) => {
                          ev.preventDefault();
                          setDownloadErr("");
                          triggerAuthenticatedDownload(href, filename).catch((err) =>
                            setDownloadErr(err instanceof Error ? err.message : "下载日志失败")
                          );
                        }
                      : undefined,
                    children: [
                      h(DownloadIcon, { size: 15 }),
                      h("span", { class: "icon-label-gap", children: "下载" }),
                    ],
                  }),
                ],
              },
              item.name
            );
          }),
          items.length === 0 && loadedOnce && !loading && !error
            ? h("p", { class: "panel-empty", children: "暂无可导出的诊断日志文件" })
            : null,
          !loadedOnce && !error
            ? h("p", { class: "panel-empty", role: "status", children: "正在读取诊断日志列表…" })
            : null,
        ],
      }),
    ],
  });
}

// ---------------------------------------------------------------------------
// Stage Banners (Hazard, SafeSwap, Clock Prompt, Diagnostic Alerts)
// ---------------------------------------------------------------------------

function RetainedHazardBanner({ state }) {
  const retained = state.capture?.snapshot.retained_unsuccessful;
  if (!retained) return null;
  const st = retained.recording_state.state;
  const prefixSavedDiag = retained.recording_state.diagnostics.find(
    (d) => d.code === "recording_prefix_saved"
  );
  const sealedMatch = state.sessions.items.find(
    (s) => s.session_id === retained.recording_state.session_id && s.producer_outcome === "sealed"
  );
  return h("section", {
    class: "hazard",
    role: "status",
    "aria-label": "设备危险状态",
    children: [
      h("span", { class: "hazard-mark", "aria-hidden": "true" }),
      h("p", {
        children:
          prefixSavedDiag?.message ??
          (sealedMatch
            ? `录制已中断，已保存前 ${formatSeconds(sealedMatch.duration_seconds)}，可导出`
            : "上一次录制没有成功封存"),
      }),
      h("strong", {
        class: "hazard-outcome",
        "data-testid": "retained-outcome",
        children: sealedMatch || prefixSavedDiag ? "部分已保存" : UNSUCCESSFUL_OUTCOME_LABELS[st] ?? st,
      }),
      h("code", {
        class: "hazard-session",
        "data-testid": "retained-session",
        children: retained.recording_state.session_id,
      }),
      h("span", { class: "tag", children: retained.recording_state.display_name }),
      h("span", { children: "这是上次录制结果；当前能否开始请查看录制按钮旁的提示。" }),
    ],
  });
}

function SafeSwapStageBanner({ state }) {
  const receipt = state.safeSwapReceipt;
  if (!receipt) return null;
  return h("section", {
    class: "safe-swap-banner",
    role: "status",
    "aria-label": "存储卷安全弹出状态",
    children: [
      h("strong", {
        children: `存储卷已安全释放 (${receipt.release_state})`,
      }),
      h("span", { children: "打开句柄已归零，现在可安全拔出或更换 MIRO-EGO 存储介质。" }),
      h("code", { children: `volume:${receipt.volume_id.slice(0, 8)}` }),
      h("button", {
        type: "button",
        onClick: () => {
          Store.refreshSafeSwap();
          Store.refreshDevice();
          Store.refreshCapture();
        },
        children: "重新检查存储卷",
      }),
    ],
  });
}

function AlertsBanner({ state }) {
  if (state.diagnostics.length === 0 && !state.error) {
    return h("div", { class: "alerts" });
  }
  const isClockBlock = state.error?.code === "clock_unsynchronized";
  return h("div", {
    class: "alerts",
    children: [
      state.error
        ? h("div", {
            class: "alert",
            role: "alert",
            children: [
              h("code", { children: state.error.code }),
              h("span", { children: state.error.message }),
              isClockBlock
                ? h("button", {
                    type: "button",
                    class: "chip",
                    onClick: () => {
                      Api.syncClock()
                        .then(() => {
                          Store.dispatch({ type: "error.cleared" });
                          Store.refreshCapture();
                        })
                        .catch(() => ClockStore.requestPrompt());
                    },
                    children: "立即同步浏览器时间",
                  })
                : null,
            ],
          })
        : null,
      state.diagnostics.map((d) =>
        h(
          "div",
          {
            class: "alert",
            role: "alert",
            children: [
              h("code", { children: d.code }),
              h("span", { children: d.message }),
              d.details && Object.keys(d.details).length > 0
                ? h("pre", { children: JSON.stringify(d.details) })
                : null,
              h("time", { dateTime: d.at, children: d.at }),
            ],
          },
          `${d.code}|${d.at}`
        )
      ),
    ],
  });
}

// ---------------------------------------------------------------------------
// Network Control Panel (/network, /network/scan, /network/apply, /retry, /forget)
// ---------------------------------------------------------------------------

const WIFI_SECURITY_LABELS = {
  open: "开放网络",
  "wpa2-personal": "WPA2 Personal",
  "wpa3-personal": "WPA3 Personal",
  "wpa2-wpa3-personal": "WPA2 / WPA3",
};

const WIFI_SECURITY_SHORT = {
  open: "开放",
  "wpa2-personal": "WPA2",
  "wpa3-personal": "WPA3",
  "wpa2-wpa3-personal": "WPA2 / WPA3",
};

const NET_DISABLED_LABELS = {
  not_enabled: "网络修改未启用",
  auth_profile_unavailable: "当前配置不允许修改网络",
  controller_unavailable: "网络控制器不可用",
  network_manager_unavailable: "NetworkManager 不可用",
  rescue_ap_not_validated: "设备热点尚未验证",
  capture_active: "录制期间不可修改网络",
  recovery_required: "网络修改暂不可用",
  maintenance_window_closed: "网络维护窗口已关闭",
  unsupported_concurrency: "无线硬件不支持当前模式",
};

const NET_STAGE_LABELS = {
  accepted: "请求已接收",
  prepared: "正在准备",
  ap_ready: "设备热点已就绪",
  activating: "正在连接",
  verifying: "正在验证连接",
  committed: "连接完成",
  falling_back: "正在恢复网络",
  rescued: "已恢复到设备热点",
  failed: "连接失败",
  forgetting: "正在忘记网络",
  forgotten: "已忘记网络",
};

const NET_OP_LABELS = {
  apply: "切换网络",
  retry: "重试连接",
  forget: "忘记网络",
};

const NET_ERR_LABELS = {
  rescue_ap_unavailable: "设备热点不可用",
  credential_rejected: "密码可能不正确",
  dhcp_timeout: "未能获取网络地址",
  route_lost: "网络路由已断开",
  network_manager_unavailable: "NetworkManager 不可用",
  concurrency_unsupported: "无线硬件不支持同时切换",
};

const NET_STAGE_PROGRESS = {
  accepted: 1,
  prepared: 2,
  ap_ready: 3,
  activating: 4,
  verifying: 5,
  committed: 6,
  falling_back: 4,
  rescued: 6,
  failed: 6,
  forgetting: 4,
  forgotten: 6,
};

const WIFI_COLLAPSE_COUNT = 6;

function describeNetworkCapability(state) {
  const net = state.networkStatus;
  const cap = net?.mutation_capability;
  if (!cap) return "网络状态不可用";
  if (!cap.enabled) return NET_DISABLED_LABELS[cap.disabled_reason ?? "not_enabled"] ?? "网络修改不可用";
  const tx = net.transaction.current ?? net.transaction.latest;
  if (
    net.desired.mode === "hotspot" &&
    tx?.desired.mode === "hotspot" &&
    tx.status === "committed" &&
    tx.rescue.ap_validated
  ) {
    return "热点配置已验证";
  }
  return net.verified ? "配置已保存并验证" : "网络修改可用";
}

function getVisibleNetworkTransaction(state) {
  const cur = state.networkStatus?.transaction.current;
  if (cur) return cur;
  const lat = state.networkStatus?.transaction.latest;
  return lat && ["rescued", "failed"].includes(lat.status) ? lat : null;
}

function scanEntryKey(entry) {
  return JSON.stringify([entry.ssid, entry.security]);
}

function deduplicateVisibleWifiNetworks(state) {
  const byKey = new Map();
  for (const item of state.networkScan?.networks ?? []) {
    if (item.hidden || item.ssid === null) continue;
    const key = scanEntryKey(item);
    const existing = byKey.get(key);
    if (!existing || item.signal_dbm > existing.signal_dbm) byKey.set(key, item);
  }
  return [...byKey.values()].sort((a, b) => b.signal_dbm - a.signal_dbm);
}

function resolveScannedSelection(state, selection) {
  if (!selection.startsWith("scan:")) return null;
  const target = selection.slice(5);
  return deduplicateVisibleWifiNetworks(state).find((item) => scanEntryKey(item) === target) ?? null;
}

function signalStrengthMeta(dbm) {
  if (dbm >= -55) return { level: 4, label: "强" };
  if (dbm >= -67) return { level: 3, label: "良好" };
  if (dbm >= -75) return { level: 2, label: "一般" };
  return { level: 1, label: "弱" };
}

function WifiSignalBars({ signalDbm }) {
  const meta = signalStrengthMeta(signalDbm);
  return h("span", {
    class: "wifi-signal",
    role: "img",
    "aria-label": `信号${meta.label}`,
    children: [
      h("span", {
        class: "wifi-signal-bars",
        "aria-hidden": "true",
        children: [1, 2, 3, 4].map((bar) => h("i", { "data-active": bar <= meta.level }, bar)),
      }),
      h("span", { children: meta.label }),
    ],
  });
}

function summarizeCurrentConnection(state) {
  const runtime = state.capture?.snapshot.runtime ?? state.device?.runtime ?? null;
  const obs = state.networkStatus?.observed ?? runtime?.network ?? null;
  if (!obs) {
    return { name: "网络状态不可用", kind: "网络", address: null, connected: false };
  }
  const wifiUp = obs.wifi_client.state === "connected";
  const wiredUp = obs.wired.state === "connected";
  const apUp = ["active", "connected"].includes(obs.ap.state);
  const desiredWifi =
    state.networkStatus?.desired.mode === "wifi-client"
      ? state.networkStatus.desired.wifi_client
      : null;

  if (obs.default_route === "wifi_client" && wifiUp) {
    return {
      name: obs.wifi_client.peer_or_ssid ?? desiredWifi?.ssid ?? "Wi-Fi",
      kind: "Wi-Fi",
      address: obs.wifi_client.addresses[0] ?? null,
      connected: true,
    };
  }
  if (obs.default_route === "wired" && wiredUp) {
    return {
      name: "有线网络",
      kind: "以太网",
      address: obs.wired.addresses[0] ?? null,
      connected: true,
    };
  }
  if (wifiUp) {
    return {
      name: obs.wifi_client.peer_or_ssid ?? desiredWifi?.ssid ?? "Wi-Fi",
      kind: "Wi-Fi",
      address: obs.wifi_client.addresses[0] ?? null,
      connected: true,
    };
  }
  if (apUp) {
    return {
      name: obs.ap.peer_or_ssid ?? "设备热点",
      kind: "设备热点",
      address: obs.ap.addresses[0] ?? null,
      connected: true,
    };
  }
  if (wiredUp) {
    return {
      name: "有线网络",
      kind: "以太网",
      address: obs.wired.addresses[0] ?? null,
      connected: true,
    };
  }
  return { name: "未连接", kind: "网络", address: null, connected: false };
}

function isCurrentWifiNetwork(state, entry) {
  const obs = state.networkStatus?.observed;
  const desired = state.networkStatus?.desired;
  if (obs?.wifi_client.state !== "connected") return false;
  const activeSsid =
    obs.wifi_client.peer_or_ssid ?? (desired?.mode === "wifi-client" ? desired.wifi_client?.ssid : null);
  return activeSsid === entry.ssid;
}

function NetworkProvisioningSection({ state }) {
  const [selection, setSelection] = useState("");
  const [expanded, setExpanded] = useState(false);
  const [manualSsid, setManualSsid] = useState("");
  const [manualSecurity, setManualSecurity] = useState("wpa2-personal");
  const [passphrase, setPassphrase] = useState("");
  const [confirmKind, setConfirmKind] = useState(null);
  const [pendingTarget, setPendingTarget] = useState(null);

  const connectBtnRef = useRef(null);
  const hotspotBtnRef = useRef(null);
  const ethBtnRef = useRef(null);
  const forgetBtnRef = useRef(null);
  const cancelBtnRef = useRef(null);
  const returnFocusRef = useRef(null);

  useEffect(() => {
    if (state.networkScan === null) Store.scanNetworks();
  }, []);

  useEffect(() => {
    if (confirmKind === null) return;
    const raf = window.requestAnimationFrame(() => cancelBtnRef.current?.focus());
    return () => {
      window.cancelAnimationFrame(raf);
      const target = returnFocusRef.current;
      returnFocusRef.current = null;
      window.requestAnimationFrame(() => {
        if (target?.isConnected) target.focus();
      });
    };
  }, [confirmKind]);

  const networks = deduplicateVisibleWifiNetworks(state);
  const visibleNetworks = expanded ? networks : networks.slice(0, WIFI_COLLAPSE_COUNT);
  const scannedEntry = resolveScannedSelection(state, selection);
  const isManual = selection === "manual";
  const hasWifiSelection = scannedEntry !== null || isManual;
  const targetSsid = scannedEntry?.ssid ?? (isManual ? manualSsid : "");
  const targetSecurity = scannedEntry?.security ?? manualSecurity;
  const needsPassphrase = hasWifiSelection && targetSecurity !== "open";

  const cmdBusy = ["submitting", "accepted"].includes(state.networkCommand.phase);
  const canMutate = state.networkStatus?.mutation_capability?.enabled === true && !cmdBusy;
  const ssidValid = utf8ByteLength(targetSsid) >= 1 && utf8ByteLength(targetSsid) <= 32;
  const passBytes = utf8ByteLength(passphrase);
  const passValid = !needsPassphrase || (passBytes >= 8 && passBytes <= 63);
  const canSubmitWifi = canMutate && hasWifiSelection && ssidValid && passValid;

  const applyBusy = cmdBusy && state.networkCommand.operation === "apply";
  const wifiConnecting = applyBusy && pendingTarget === "wifi-client";
  const hotspotActivating = applyBusy && pendingTarget === "hotspot";
  const ethActivating = applyBusy && pendingTarget === "ethernet-dhcp";

  const canEnableHotspot = canMutate && state.networkStatus?.desired.mode !== "hotspot";
  const canEnableEthDhcp = canMutate && state.networkStatus?.desired.mode !== "ethernet-dhcp";
  const forgetBusy = cmdBusy && state.networkCommand.operation === "forget";
  const showForget = state.networkStatus?.saved === true || forgetBusy;

  const selectEntry = (key) => {
    setSelection(key);
    setPassphrase("");
  };

  const executeWifiApply = () => {
    if (!canSubmitWifi) return;
    const pass = needsPassphrase ? passphrase : undefined;
    setPassphrase("");
    setPendingTarget("wifi-client");
    setConfirmKind(null);
    Store.applyWifiNetwork({ ssid: targetSsid, security: targetSecurity, passphrase: pass });
  };

  const executeHotspotApply = () => {
    if (!canEnableHotspot) return;
    setPendingTarget("hotspot");
    setConfirmKind(null);
    Store.applyHotspot();
  };

  const executeEthernetDhcpApply = () => {
    if (!canEnableEthDhcp) return;
    setPendingTarget("ethernet-dhcp");
    setConfirmKind(null);
    Store.applyEthernet({
      mode: "ethernet-dhcp",
      wifi_client: null,
      ethernet: { addressing: "dhcp", static_ipv4: null },
    });
  };

  return h("section", {
    class: "network-provisioning",
    "aria-labelledby": "available-networks-title",
    children: [
      h("div", {
        class: "network-section-head",
        children: [
          h("div", {
            children: [
              h("h3", { id: "available-networks-title", children: "可用网络" }),
              h("span", {
                "aria-live": "polite",
                children: state.networkScanPending
                  ? "正在扫描"
                  : state.networkScan
                    ? `${networks.length} 个`
                    : "尚未扫描",
              }),
            ],
          }),
          h("button", {
            type: "button",
            class: "icon-button compact",
            "aria-label": "扫描 Wi-Fi",
            title: "扫描 Wi-Fi",
            disabled: state.networkScanPending || cmdBusy,
            onClick: () => Store.scanNetworks(),
            children: h(RefreshIcon, { size: 17 }),
          }),
        ],
      }),
      h("form", {
        class: "network-form",
        "aria-label": "网络设置",
        onSubmit: (ev) => {
          ev.preventDefault();
          if (canSubmitWifi) {
            returnFocusRef.current = connectBtnRef.current;
            setConfirmKind("apply");
          }
        },
        children: [
          h("div", {
            class: "wifi-network-list",
            "aria-label": "Wi-Fi 网络",
            "aria-busy": state.networkScanPending,
            children: [
              visibleNetworks.map((entry) => {
                const key = scanEntryKey(entry);
                const selected = selection === `scan:${key}`;
                const current = isCurrentWifiNetwork(state, entry);
                return h(
                  "button",
                  {
                    type: "button",
                    class: "wifi-network-option",
                    "data-testid": "wifi-network-option",
                    "data-current": current,
                    "aria-pressed": selected,
                    onClick: () => selectEntry(`scan:${key}`),
                    children: [
                      h("span", {
                        class: "wifi-network-identity",
                        children: [
                          h("strong", { children: entry.ssid }),
                          h("span", { children: WIFI_SECURITY_SHORT[entry.security] }),
                        ],
                      }),
                      h("span", {
                        class: "wifi-network-meta",
                        children: [
                          current ? h("span", { class: "wifi-current-tag", children: "当前" }) : null,
                          h(WifiSignalBars, { signalDbm: entry.signal_dbm }),
                        ],
                      }),
                    ],
                  },
                  key
                );
              }),
              state.networkScan && networks.length === 0
                ? h("p", { class: "wifi-network-empty", children: "未发现可用 Wi-Fi" })
                : null,
              h("button", {
                type: "button",
                class: "wifi-network-option wifi-network-manual",
                "aria-pressed": isManual,
                onClick: () => selectEntry("manual"),
                children: [
                  h("span", { "aria-hidden": "true", children: "+" }),
                  h("span", { children: "手动添加网络" }),
                ],
              }),
            ],
          }),
          networks.length > WIFI_COLLAPSE_COUNT
            ? h("button", {
                type: "button",
                class: "network-list-toggle",
                "aria-expanded": expanded,
                onClick: () => setExpanded((prev) => !prev),
                children: expanded ? "收起" : `显示其他 ${networks.length - WIFI_COLLAPSE_COUNT} 个网络`,
              })
            : null,
          hasWifiSelection
            ? h("div", {
                class: "network-selection-fields",
                children: [
                  isManual
                    ? h(Fragment, {
                        children: [
                          h("label", {
                            class: "field-stack",
                            children: [
                              h("span", { children: "网络名称" }),
                              h("input", {
                                type: "text",
                                name: "ssid",
                                "aria-label": "SSID",
                                maxLength: 32,
                                value: manualSsid,
                                disabled: !canMutate,
                                onInput: (ev) => setManualSsid(ev.currentTarget.value),
                              }),
                            ],
                          }),
                          h("label", {
                            class: "field-stack",
                            children: [
                              h("span", { children: "安全类型" }),
                              h("select", {
                                "aria-label": "安全类型",
                                value: manualSecurity,
                                disabled: !canMutate,
                                onChange: (ev) => {
                                  setManualSecurity(ev.currentTarget.value);
                                  setPassphrase("");
                                },
                                children: Object.entries(WIFI_SECURITY_LABELS).map(([val, label]) =>
                                  h("option", { value: val, children: label }, val)
                                ),
                              }),
                            ],
                          }),
                        ],
                      })
                    : null,
                  needsPassphrase
                    ? h("label", {
                        class: "field-stack",
                        children: [
                          h("span", { children: "Wi-Fi 密码" }),
                          h("input", {
                            type: "password",
                            name: "network-passphrase",
                            "aria-label": "Wi-Fi 密码",
                            autoComplete: "new-password",
                            minLength: 8,
                            maxLength: 63,
                            value: passphrase,
                            disabled: !canMutate,
                            onInput: (ev) => setPassphrase(ev.currentTarget.value),
                          }),
                        ],
                      })
                    : null,
                  h("button", {
                    ref: connectBtnRef,
                    type: "submit",
                    class: "panel-command",
                    disabled: !(canSubmitWifi || wifiConnecting),
                    "aria-disabled": !canSubmitWifi,
                    children: wifiConnecting ? "正在连接" : "连接",
                  }),
                ],
              })
            : null,
        ],
      }),
      confirmKind === "apply"
        ? h("div", {
            class: "network-confirm",
            role: "alertdialog",
            "aria-labelledby": "network-apply-confirm-title",
            "aria-describedby": "network-apply-confirm-description",
            onKeyDown: (ev) => {
              if (ev.key === "Escape") {
                ev.preventDefault();
                setConfirmKind(null);
              }
            },
            children: [
              h("strong", { id: "network-apply-confirm-title", children: ["切换到 ", targetSsid] }),
              h("p", {
                id: "network-apply-confirm-description",
                children: "当前管理连接可能短暂中断；若切换失败设备会在 10 秒内自动回到救援热点。",
              }),
              h("div", {
                class: "network-confirm-actions",
                children: [
                  h("button", {
                    type: "button",
                    class: "panel-command",
                    onClick: executeWifiApply,
                    children: "确认切换",
                  }),
                  h("button", {
                    ref: cancelBtnRef,
                    type: "button",
                    class: "panel-secondary",
                    onClick: () => setConfirmKind(null),
                    children: "取消",
                  }),
                ],
              }),
            ],
          })
        : null,
      h("div", {
        class: "network-actions",
        children: [
          h("button", {
            ref: hotspotBtnRef,
            type: "button",
            class: "panel-secondary",
            disabled: !(canEnableHotspot || hotspotActivating),
            "aria-disabled": !canEnableHotspot,
            onClick: () => {
              if (canEnableHotspot) {
                returnFocusRef.current = hotspotBtnRef.current;
                setConfirmKind("hotspot");
              }
            },
            children: hotspotActivating
              ? "正在启用热点"
              : state.networkStatus?.desired.mode === "hotspot"
                ? "设备热点已启用"
                : "启用设备热点",
          }),
          h("button", {
            ref: ethBtnRef,
            type: "button",
            class: "panel-secondary",
            disabled: !(canEnableEthDhcp || ethActivating),
            "aria-disabled": !canEnableEthDhcp,
            onClick: () => {
              if (canEnableEthDhcp) {
                returnFocusRef.current = ethBtnRef.current;
                setConfirmKind("ethernet-dhcp");
              }
            },
            children: ethActivating
              ? "正在切换有线"
              : state.networkStatus?.desired.mode === "ethernet-dhcp"
                ? "有线 DHCP 已启用"
                : "有线 DHCP",
          }),
          showForget
            ? h("button", {
                ref: forgetBtnRef,
                type: "button",
                class: "panel-danger",
                disabled: !(canMutate || forgetBusy),
                "aria-disabled": !canMutate,
                onClick: () => {
                  if (canMutate) {
                    returnFocusRef.current = forgetBtnRef.current;
                    setConfirmKind("forget");
                  }
                },
                children: forgetBusy ? "正在忘记" : "忘记 Wi-Fi",
              })
            : null,
        ],
      }),
      confirmKind === "hotspot"
        ? h("div", {
            class: "network-confirm",
            role: "alertdialog",
            "aria-labelledby": "network-hotspot-title",
            "aria-describedby": "network-hotspot-description",
            onKeyDown: (ev) => {
              if (ev.key === "Escape") {
                ev.preventDefault();
                setConfirmKind(null);
              }
            },
            children: [
              h("strong", { id: "network-hotspot-title", children: "启用设备热点" }),
              h("p", { id: "network-hotspot-description", children: "当前管理连接可能中断。" }),
              h("div", {
                class: "network-confirm-actions",
                children: [
                  h("button", {
                    type: "button",
                    class: "panel-command",
                    onClick: executeHotspotApply,
                    children: "确认切换",
                  }),
                  h("button", {
                    ref: cancelBtnRef,
                    type: "button",
                    class: "panel-secondary",
                    onClick: () => setConfirmKind(null),
                    children: "取消",
                  }),
                ],
              }),
            ],
          })
        : null,
      confirmKind === "ethernet-dhcp"
        ? h("div", {
            class: "network-confirm",
            role: "alertdialog",
            "aria-labelledby": "network-eth-title",
            "aria-describedby": "network-eth-description",
            onKeyDown: (ev) => {
              if (ev.key === "Escape") {
                ev.preventDefault();
                setConfirmKind(null);
              }
            },
            children: [
              h("strong", { id: "network-eth-title", children: "切换到有线网络 (DHCP)" }),
              h("p", { id: "network-eth-description", children: "设备将优先通过以太网获取地址，救援热点仍保持可用验证。" }),
              h("div", {
                class: "network-confirm-actions",
                children: [
                  h("button", {
                    type: "button",
                    class: "panel-command",
                    onClick: executeEthernetDhcpApply,
                    children: "确认切换",
                  }),
                  h("button", {
                    ref: cancelBtnRef,
                    type: "button",
                    class: "panel-secondary",
                    onClick: () => setConfirmKind(null),
                    children: "取消",
                  }),
                ],
              }),
            ],
          })
        : null,
      confirmKind === "forget"
        ? h("div", {
            class: "network-confirm danger",
            role: "alertdialog",
            "aria-labelledby": "network-forget-title",
            "aria-describedby": "network-forget-description",
            onKeyDown: (ev) => {
              if (ev.key === "Escape") {
                ev.preventDefault();
                setConfirmKind(null);
              }
            },
            children: [
              h("strong", { id: "network-forget-title", children: "忘记已保存的 Wi-Fi" }),
              h("p", { id: "network-forget-description", children: "设备将擦除已保存凭据并回到救援热点。" }),
              h("div", {
                class: "network-confirm-actions",
                children: [
                  h("button", {
                    type: "button",
                    class: "panel-danger",
                    onClick: () => {
                      setConfirmKind(null);
                      Store.forgetNetwork();
                    },
                    children: "确认忘记",
                  }),
                  h("button", {
                    ref: cancelBtnRef,
                    type: "button",
                    class: "panel-secondary",
                    onClick: () => setConfirmKind(null),
                    children: "取消",
                  }),
                ],
              }),
            ],
          })
        : null,
    ],
  });
}

function NetworkTransactionProgress({ state }) {
  const tx = getVisibleNetworkTransaction(state);
  if (!tx) return null;
  const cmdBusy = ["submitting", "accepted"].includes(state.networkCommand.phase);
  const canRetry =
    state.networkStatus?.mutation_capability.enabled === true &&
    !cmdBusy &&
    ["rescued", "failed"].includes(tx.status) &&
    tx.error?.retryable === true;

  return h("section", {
    class: "network-progress",
    "data-status": tx.status,
    "aria-live": "polite",
    children: [
      h("progress", { max: 6, value: NET_STAGE_PROGRESS[tx.stage] ?? 0 }),
      h("div", {
        children: [
          h("strong", {
            "data-testid": "network-transaction",
            children: NET_STAGE_LABELS[tx.stage] ?? tx.stage,
          }),
          h("span", { children: NET_OP_LABELS[tx.operation] ?? tx.operation }),
        ],
      }),
      tx.deadline
        ? h("span", { children: [tx.deadline.remaining_seconds.toFixed(1), " 秒"] })
        : null,
      tx.error
        ? h("p", {
            class: "network-progress-error",
            children: NET_ERR_LABELS[tx.error.code] ?? tx.error.message,
          })
        : null,
      canRetry
        ? h("button", {
            type: "button",
            class: "panel-secondary",
            onClick: () => Store.retryNetwork(tx.transaction_id),
            children: "重试连接",
          })
        : null,
    ],
  });
}

function NetworkView({ state }) {
  const runtime = state.capture?.snapshot.runtime ?? state.device?.runtime ?? null;
  const obs = state.networkStatus?.observed ?? runtime?.network ?? null;
  const mdns = state.networkStatus?.observed.mdns ?? null;
  const cap = state.networkStatus?.mutation_capability;
  const cur = summarizeCurrentConnection(state);

  return h("section", {
    class: "network-view",
    children: [
      h("header", {
        class: "network-current",
        "data-connected": cur.connected,
        children: [
          h("div", {
            class: "network-current-topline",
            children: [
              h("span", { children: "当前连接" }),
              h("span", { children: cur.connected ? "已连接" : "未连接" }),
            ],
          }),
          h("strong", { "data-testid": "network-current-name", children: cur.name }),
          h("div", {
            class: "network-current-meta",
            children: [
              h("span", { children: cur.kind }),
              cur.address
                ? h("span", { "data-testid": "network-current-address", children: cur.address })
                : null,
            ],
          }),
          h("p", {
            id: "network-status",
            "data-tone": cap?.enabled ? "normal" : "caution",
            children: describeNetworkCapability(state),
          }),
        ],
      }),
      cap?.enabled ? h(NetworkProvisioningSection, { state }) : null,
      h(NetworkTransactionProgress, { state }),
      h("details", {
        class: "network-details",
        children: [
          h("summary", { children: "网络详情" }),
          h("dl", {
            children: [
              h("div", {
                children: [
                  h("dt", { children: "默认连接" }),
                  h("dd", {
                    "data-testid": "network-default-route",
                    children: formatDefaultRoute(obs?.default_route),
                  }),
                ],
              }),
              h("div", {
                children: [
                  h("dt", { children: "Wi-Fi" }),
                  h("dd", {
                    "data-testid": "network-wifi",
                    children: formatInterfaceSummary(obs?.wifi_client),
                  }),
                ],
              }),
              h("div", {
                children: [
                  h("dt", { children: "设备热点" }),
                  h("dd", {
                    "data-testid": "network-ap",
                    children: formatInterfaceSummary(obs?.ap),
                  }),
                ],
              }),
              h("div", {
                children: [
                  h("dt", { children: "有线" }),
                  h("dd", {
                    "data-testid": "network-wired",
                    children: formatInterfaceSummary(obs?.wired),
                  }),
                ],
              }),
              h("div", {
                children: [
                  h("dt", { children: "mDNS 地址" }),
                  h("dd", {
                    "data-testid": "network-mdns",
                    "data-tone": mdns ? undefined : "caution",
                    children: mdns ? `${mdns.hostname}:${mdns.port}` : "不可用",
                  }),
                ],
              }),
            ],
          }),
        ],
      }),
    ],
  });
}

function NetworkPanel({ state }) {
  return h("aside", {
    id: "network-panel",
    class: "panel",
    "aria-label": "网络设置",
    children: [
      h("div", {
        class: "panel-head",
        children: [
          h("span", { class: "eyebrow", children: "NETWORK" }),
          h("span", { class: "panel-title", children: "网络设置" }),
          h("span", { class: "panel-spacer" }),
          h("button", {
            type: "button",
            class: "icon-button",
            "aria-label": "关闭",
            onClick: () => {
              Store.dispatch({ type: "panel.closed" });
              window.requestAnimationFrame(() =>
                document.getElementById("network-panel-trigger")?.focus()
              );
            },
            children: h(CloseIcon, {}),
          }),
        ],
      }),
      h("div", { class: "panel-body", children: h(NetworkView, { state }) }),
    ],
  });
}

// ---------------------------------------------------------------------------
// Sessions Ledger, Session Detail, Replay & Deletion
// ---------------------------------------------------------------------------

function collectSessionArtifacts(manifest) {
  const list = [];
  for (const seg of manifest.video?.segments ?? []) {
    if (seg.artifacts.left) list.push(seg.artifacts.left);
    if (seg.artifacts.right) list.push(seg.artifacts.right);
  }
  for (const seg of manifest.audio?.segments ?? []) {
    if (seg.artifact) list.push(seg.artifact);
  }
  if (manifest.frames?.artifact) list.push(manifest.frames.artifact);
  if (manifest.imu?.artifact) list.push(manifest.imu.artifact);
  for (const logEntry of manifest.logs ?? []) {
    if (logEntry.artifact) list.push(logEntry.artifact);
  }
  return list;
}

function SessionDetailHeader({ title, onBack }) {
  return h("div", {
    class: "panel-head",
    children: [
      h("button", {
        type: "button",
        class: "icon-button",
        "aria-label": "返回会话台账",
        onClick: onBack,
        children: h(BackIcon, {}),
      }),
      h("span", {
        class: "panel-title panel-title-ellipsis",
        children: title,
      }),
      h("span", { class: "panel-spacer" }),
      h("button", {
        type: "button",
        class: "icon-button",
        "aria-label": "关闭",
        onClick: () => Store.dispatch({ type: "panel.closed" }),
        children: h(CloseIcon, {}),
      }),
    ],
  });
}

function SessionReplaySection({ detail, enabled }) {
  const segments = (detail.video?.segments ?? []).filter(
    (s) => s.artifacts.left || s.artifacts.right
  );
  const [segIdx, setSegIdx] = useState(0);
  const leftVideoRef = useRef(null);
  const rightVideoRef = useRef(null);
  const syncingRef = useRef(false);
  const [playErr, setPlayErr] = useState(false);

  if (!segments.length) return null;
  const currentSeg = segments[segIdx] ?? segments[0];
  if (!currentSeg) return null;

  const audioArtifact = detail.audio?.segments?.find((a) => a.index === currentSeg.index)?.artifact;
  const urlFor = (art) => (art ? Api.artifactUrl(detail.session_id, art.artifact_id) : undefined);

  const syncPeer = (sourceEl, fn) => {
    if (syncingRef.current) return;
    const peer = sourceEl === leftVideoRef.current ? rightVideoRef.current : leftVideoRef.current;
    if (peer) {
      syncingRef.current = true;
      fn(peer);
      globalThis.setTimeout(() => {
        syncingRef.current = false;
      }, 0);
    }
  };

  return h("section", {
    class: "detail-section replay-section",
    "data-testid": "session-replay",
    children: [
      h("span", { class: "eyebrow", children: "REPLAY" }),
      h("div", {
        class: "replay-toolbar",
        children: [
          h("label", {
            children: [
              h("span", { children: "视频分段" }),
              h("select", {
                value: String(currentSeg.index),
                onChange: (ev) => {
                  const targetIndex = Number(ev.currentTarget.value);
                  const found = segments.findIndex((s) => s.index === targetIndex);
                  setPlayErr(false);
                  setSegIdx(found >= 0 ? found : 0);
                },
                "data-testid": "replay-segment",
                children: segments.map((s, idx) =>
                  h(
                    "option",
                    {
                      value: String(s.index),
                      children: [
                        "#",
                        s.index + 1,
                        s.start_time_seconds !== undefined && s.end_time_seconds !== undefined
                          ? ` - ${formatSeconds(s.end_time_seconds - s.start_time_seconds)}`
                          : "",
                        idx === 0 ? "（首段）" : "",
                      ],
                    },
                    s.index
                  )
                ),
              }),
            ],
          }),
          h("span", {
            class: "replay-hint",
            children: enabled ? "选择分段即可同步回放" : "设备未声明 Range 回放能力",
          }),
        ],
      }),
      h("div", {
        class: "replay-grid",
        children: ["left", "right"].map((eye) => {
          const art = currentSeg.artifacts[eye];
          return h(
            "div",
            {
              class: "replay-player",
              children: [
                h("span", { class: "artifact-role", children: eye === "left" ? "左目" : "右目" }),
                art && enabled
                  ? h("video", {
                      ref: eye === "left" ? leftVideoRef : rightVideoRef,
                      controls: true,
                      playsInline: true,
                      preload: "metadata",
                      src: urlFor(art),
                      "data-testid": `replay-${eye}`,
                      onError: () => setPlayErr(true),
                      onPlay: (ev) =>
                        syncPeer(ev.currentTarget, (peer) => {
                          peer.play().catch(() => {});
                        }),
                      onPause: (ev) => syncPeer(ev.currentTarget, (peer) => peer.pause()),
                      onSeeked: (ev) =>
                        syncPeer(ev.currentTarget, (peer) => {
                          peer.currentTime = ev.currentTarget.currentTime;
                        }),
                    })
                  : h("div", {
                      class: "replay-unavailable",
                      children: art ? "设备不支持 Range 回放" : "此分段无制品",
                    }),
              ],
            },
            eye
          );
        }),
      }),
      audioArtifact && enabled
        ? h("label", {
            class: "replay-audio",
            children: [
              h("span", { class: "artifact-role", children: "音频" }),
              h("audio", { controls: true, preload: "metadata", src: urlFor(audioArtifact) }),
            ],
          })
        : null,
      playErr
        ? h("p", {
            class: "panel-note",
            role: "alert",
            children:
              detail.video?.codec === "hevc"
                ? "H.265 回放需要浏览器和系统支持 HEVC。若此浏览器无法播放，请下载后使用支持 HEVC 的播放器打开。"
                : readAccessToken()
                  ? "当前处于受保护访问令牌模式，请下载制品文件到本地播放器查看。"
                  : "录像暂时无法播放（录制期间会返回 423 capture_busy），请待机后重试或下载播放。",
          })
        : null,
    ],
  });
}

function SessionDetailPanel({ state, requestDelete = false }) {
  const selected = state.selected;
  if (!selected) return null;
  const onBack = () => Store.closeSession();
  const summaryItem = state.sessions.items.find((s) => s.session_id === selected.sessionId);
  const title = summaryItem?.display_name ?? selected.sessionId;

  const [confirmDelete, setConfirmDelete] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [opError, setOpError] = useState(null);

  const deleteTriggerRef = useRef(null);
  const cancelDeleteRef = useRef(null);
  const dialogContainerRef = useRef(null);
  const returnFocusRef = useRef(null);
  const autoOpenedDeleteRef = useRef(false);

  useEffect(() => {
    if (requestDelete && selected.detail && !autoOpenedDeleteRef.current) {
      autoOpenedDeleteRef.current = true;
      const sha = selected.manifestSha256;
      if (
        state.device?.capabilities.session_deletion &&
        selected.detail.sealed &&
        sha &&
        SHA256_LOWER_RE.test(sha)
      ) {
        returnFocusRef.current = deleteTriggerRef.current;
        setConfirmDelete(true);
      }
    }
  }, [requestDelete, selected.detail, selected.manifestSha256]);

  useEffect(() => {
    if (!confirmDelete) return;
    const raf = window.requestAnimationFrame(() => cancelDeleteRef.current?.focus());
    return () => {
      window.cancelAnimationFrame(raf);
      const prev = returnFocusRef.current;
      returnFocusRef.current = null;
      window.requestAnimationFrame(() => {
        if (prev?.isConnected) prev.focus();
      });
    };
  }, [confirmDelete]);

  if (selected.loading) {
    return h("aside", {
      class: "panel",
      "aria-label": "会话详情",
      children: [
        h(SessionDetailHeader, { title, onBack }),
        h("div", {
          class: "panel-body",
          children: h("p", { class: "panel-empty", children: "正在读取不可变清单" }),
        }),
      ],
    });
  }

  if (selected.error) {
    return h("aside", {
      class: "panel",
      "aria-label": "会话详情",
      children: [
        h(SessionDetailHeader, { title, onBack }),
        h("div", {
          class: "panel-body",
          children: h("div", {
            class: "alert",
            children: [
              h("code", { children: selected.error.code }),
              h("span", { children: selected.error.message }),
            ],
          }),
        }),
      ],
    });
  }

  if (!selected.detail) {
    const outResource = selected.outcome;
    const recState = outResource?.outcome?.recording_state ?? null;
    const terminalState = recState?.state ?? summaryItem?.producer_outcome ?? "未知";
    return h("aside", {
      class: "panel",
      "aria-label": "未成功会话结果",
      children: [
        h(SessionDetailHeader, { title, onBack }),
        h("div", {
          class: "panel-body",
          children: [
            h("section", {
              class: "detail-section",
              children: [
                h("span", { class: "eyebrow", children: "RETAINED OUTCOME" }),
                h("dl", {
                  class: "facts",
                  children: [
                    h("div", {
                      children: [
                        h("dt", { children: "session" }),
                        h("dd", { children: selected.sessionId }),
                      ],
                    }),
                    h("div", {
                      children: [
                        h("dt", { children: "终态" }),
                        h("dd", {
                          "data-tone": "caution",
                          children: UNSUCCESSFUL_OUTCOME_LABELS[terminalState] ?? terminalState,
                        }),
                      ],
                    }),
                  ],
                }),
                h("p", {
                  class: "muted-note",
                  children: "这是只读结果查询（miro-ego.retained-unsuccessful-session-resource.v1），未成功会话不提供制品下载。",
                }),
              ],
            }),
            outResource
              ? h("section", {
                  class: "detail-section",
                  children: [
                    h("span", { class: "eyebrow", children: "RAW" }),
                    h("pre", {
                      class: "raw-json-pre",
                      children: JSON.stringify(outResource, null, 2),
                    }),
                  ],
                })
              : h("p", {
                  class: "panel-empty",
                  children: "设备没有为这个 session 保留可查询的结果。",
                }),
          ],
        }),
      ],
    });
  }

  const detail = selected.detail;
  const artifacts = collectSessionArtifacts(detail);
  const canDownload = detail.sealed && state.device?.capabilities.artifact_download === true;
  const deletionSupported = state.device?.capabilities.session_deletion === true;
  const manifestSha = selected.manifestSha256 ?? null;
  const validManifestSha = typeof manifestSha === "string" && /^[0-9a-f]{64}$/i.test(manifestSha);
  const canDelete = deletionSupported && detail.sealed && validManifestSha && !deleting;
  const missingFrames = detail.capture_audit?.camera?.source_missing_frames ?? 0;
  const hasBearer = !!readAccessToken();
  const isIdle =
    state.connection === "connected" &&
    state.capture?.snapshot.device_state === "idle" &&
    !state.commandPending;

  const executeDelete = async () => {
    if (!canDelete || manifestSha === null) return;
    setDeleting(true);
    dialogContainerRef.current?.focus();
    setOpError(null);
    try {
      const res = await Store.deleteSession(detail.session_id, manifestSha);
      if (!res.deleted_session_ids.includes(detail.session_id)) {
        const failed = res.failed_sessions.find((f) => f.session_id === detail.session_id);
        setOpError({
          code: failed?.error ?? "session_delete_failed",
          message: failed ? `删除失败：${failed.error}` : "设备没有确认删除此会话",
        });
        return;
      }
      setDeleting(false);
      setConfirmDelete(false);
      Store.closeSession();
    } catch (err) {
      setOpError(toUiError(err));
    } finally {
      setDeleting(false);
    }
  };

  return h("aside", {
    class: "panel",
    "aria-label": "会话详情",
    children: [
      h(SessionDetailHeader, { title: detail.display_name, onBack }),
      h("div", {
        class: "panel-body",
        children: [
          h("section", {
            class: "detail-section",
            children: [
              h("dl", {
                class: "facts",
                children: [
                  h("div", {
                    children: [
                      h("dt", { children: "状态" }),
                      h("dd", {
                        children: [
                          detail.sealed ? "已保存" : "保存未完成",
                          missingFrames > 0 ? " - 有源帧缺口" : "",
                        ],
                      }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "保存时间" }),
                      h("dd", { children: formatDateTime(detail.sealed_at) }),
                    ],
                  }),
                  manifestSha
                    ? h("div", {
                        children: [
                          h("dt", { children: "Manifest SHA-256" }),
                          h("dd", { class: "artifact-hash", children: manifestSha }),
                        ],
                      })
                    : null,
                ],
              }),
              detail.sealed && isIdle
                ? h("div", {
                    class: "chips",
                    children: h("button", {
                      type: "button",
                      class: "chip",
                      onClick: () => {
                        Store.dispatch({ type: "panel.closed" });
                        Store.startCapture(
                          `${detail.display_name} (续)`,
                          detail.capture_mode === "calibration" ? "calibration" : "production",
                          detail.session_id
                        );
                      },
                      children: "以此会话续录下一段 (continue)",
                    }),
                  })
                : null,
            ],
          }),
          detail.capture_audit?.schema === "miro-ego.capture-audit.v1"
            ? h("section", {
                class: "detail-section",
                "aria-label": "录制质量与时间戳",
                children: [
                  h("span", { class: "eyebrow", children: "录制质量与时间戳" }),
                  h("dl", {
                    class: "facts",
                    children: [
                      h("div", {
                        children: [
                          h("dt", { children: "编码" }),
                          h("dd", {
                            children: [
                              detail.video?.codec === "hevc" ? "H.265 / HEVC" : detail.video?.codec,
                              detail.video?.encoding?.bitrate_kbps
                                ? ` - 每眼 ${detail.video.encoding.bitrate_kbps / 1024}M`
                                : "",
                            ],
                          }),
                        ],
                      }),
                      h("div", {
                        children: [
                          h("dt", { children: "实际 / 标称帧率" }),
                          h("dd", {
                            children: [
                              detail.capture_audit.camera.actual_fps?.toFixed(3) ?? "--",
                              " / ",
                              detail.capture_audit.camera.nominal_fps,
                              " fps",
                            ],
                          }),
                        ],
                      }),
                      h("div", {
                        children: [
                          h("dt", { children: "最大帧间隔" }),
                          h("dd", {
                            children: [
                              (detail.capture_audit.camera.max_interval_ns / 1e6).toFixed(2),
                              " ms",
                            ],
                          }),
                        ],
                      }),
                      missingFrames > 0
                        ? h("div", {
                            children: [
                              h("dt", { children: "源序号缺口" }),
                              h("dd", { children: [missingFrames, " 个"] }),
                            ],
                          })
                        : null,
                      h("div", {
                        children: [
                          h("dt", { children: "IMU 实际交付" }),
                          h("dd", {
                            children: [
                              detail.capture_audit.imu.delivered_slots_hz?.toFixed(1) ?? "--",
                              " 样本槽/秒",
                            ],
                          }),
                        ],
                      }),
                      h("div", {
                        children: [
                          h("dt", { children: "相机-IMU 计数器核对" }),
                          h("dd", {
                            children: [
                              detail.capture_audit.camera.counter_matched_frames,
                              " / ",
                              detail.capture_audit.camera.frame_count,
                              " 帧",
                            ],
                          }),
                        ],
                      }),
                      h("div", {
                        children: [
                          h("dt", { children: "标称时间轴偏差" }),
                          h("dd", {
                            children: [
                              (detail.capture_audit.camera.nominal_timeline_error_seconds * 1e3).toFixed(2),
                              " ms",
                            ],
                          }),
                        ],
                      }),
                      h("div", {
                        children: [
                          h("dt", { children: "存储格式" }),
                          h("dd", {
                            children:
                              detail.schema === "miro-ego.device-session.v1"
                                ? "Miro-Ego v1 无损封存清单"
                                : "原始日志与音频",
                          }),
                        ],
                      }),
                    ],
                  }),
                  h("p", {
                    class: "panel-empty",
                    children: "导出使用逐帧真实单调时钟时间戳；零重读 SHA-256 校验保证落盘字节一致。",
                  }),
                ],
              })
            : null,
          h("section", {
            class: "detail-section",
            children: [
              h("span", { class: "eyebrow", children: "IDENTITY" }),
              h("dl", {
                class: "facts",
                children: [
                  h("div", {
                    children: [h("dt", { children: "session" }), h("dd", { children: detail.session_id })],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "take" }),
                      h("dd", { children: [detail.take.take_id, " - #", detail.take.sequence] }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "continuation_of" }),
                      h("dd", { "data-tone": "muted", children: detail.take.continuation_of ?? "无" }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "manifest" }),
                      h("dd", { children: detail.manifest_id }),
                    ],
                  }),
                  h("div", {
                    children: [h("dt", { children: "volume" }), h("dd", { children: detail.volume_id })],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "capture_mode" }),
                      h("dd", { children: detail.capture_mode }),
                    ],
                  }),
                ],
              }),
            ],
          }),
          h("section", {
            class: "detail-section",
            children: [
              h("span", { class: "eyebrow", children: "CAPTURE" }),
              h("dl", {
                class: "facts",
                children: [
                  h("div", {
                    children: [
                      h("dt", { children: "时长" }),
                      h("dd", { children: formatSeconds(detail.time.duration_seconds) }),
                    ],
                  }),
                  h("div", {
                    children: [
                      h("dt", { children: "起止" }),
                      h("dd", {
                        children: [
                          formatDateTime(detail.time.started_at),
                          " -> ",
                          formatDateTime(detail.time.ended_at),
                        ],
                      }),
                    ],
                  }),
                  detail.camera
                    ? h("div", {
                        children: [
                          h("dt", { children: "画面" }),
                          h("dd", {
                            children: [
                              detail.camera.width,
                              "x",
                              detail.camera.height,
                              " - ",
                              detail.camera.nominal_fps,
                              " fps",
                            ],
                          }),
                        ],
                      })
                    : null,
                  detail.frames
                    ? h("div", {
                        children: [
                          h("dt", { children: "帧数" }),
                          h("dd", { children: detail.frames.count }),
                        ],
                      })
                    : null,
                  detail.integrity
                    ? h("div", {
                        children: [
                          h("dt", { children: "丢帧" }),
                          h("dd", {
                            "data-tone": detail.integrity.dropped_frames > 0 ? "caution" : "permit",
                            children: detail.integrity.dropped_frames,
                          }),
                        ],
                      })
                    : null,
                  detail.video
                    ? h("div", {
                        children: [
                          h("dt", { children: "视频" }),
                          h("dd", {
                            children: [
                              detail.video.codec,
                              "/",
                              detail.video.container,
                              " - ",
                              detail.video.layout,
                            ],
                          }),
                        ],
                      })
                    : null,
                ],
              }),
            ],
          }),
          h("section", {
            class: "detail-section",
            children: [
              h("span", { class: "eyebrow", children: ["ARTIFACTS - ", artifacts.length] }),
              canDownload
                ? null
                : h("p", {
                    class: "caution-note",
                    children: detail.sealed ? "当前设备不支持下载。" : "录像保存完成后即可下载。",
                  }),
              artifacts.map((art) => {
                const href = canDownload ? Api.artifactUrl(detail.session_id, art.artifact_id) : undefined;
                const filename = art.path.split("/").pop() || art.artifact_id;
                return h(
                  "div",
                  {
                    class: "artifact",
                    children: [
                      h("div", {
                        class: "artifact-body",
                        children: [
                          h("span", { class: "artifact-role", children: art.role }),
                          h("span", { class: "artifact-path", children: art.path }),
                          h("span", { class: "artifact-hash", children: ["sha256:", art.sha256] }),
                        ],
                      }),
                      h("span", { class: "artifact-size", children: formatBytes(art.bytes) }),
                      h("a", {
                        class: "download",
                        "aria-disabled": !canDownload,
                        href,
                        download: filename,
                        rel: "noreferrer",
                        onClick:
                          canDownload && hasBearer && href
                            ? (ev) => {
                                ev.preventDefault();
                                setOpError(null);
                                triggerAuthenticatedDownload(href, filename).catch((err) =>
                                  setOpError(toUiError(err))
                                );
                              }
                            : undefined,
                        children: [
                          h(DownloadIcon, { size: 15 }),
                          h("span", { class: "icon-label-gap", children: "下载" }),
                        ],
                      }),
                    ],
                  },
                  art.artifact_id
                );
              }),
            ],
          }),
          h(SessionReplaySection, {
            detail,
            enabled: state.device?.capabilities.range_download === true,
          }),
        ],
      }),
      h("section", {
        class: "detail-section session-delete",
        "aria-label": "录制操作",
        children: [
          opError && !confirmDelete
            ? h("div", {
                class: "alert",
                role: "alert",
                children: [
                  h("code", { children: opError.code }),
                  h("span", { children: opError.message }),
                ],
              })
            : null,
          h("button", {
            ref: deleteTriggerRef,
            type: "button",
            class: "panel-danger",
            "data-testid": "delete-session",
            disabled: !canDelete,
            "aria-disabled": !canDelete,
            onClick: () => {
              if (canDelete) {
                returnFocusRef.current = deleteTriggerRef.current;
                setOpError(null);
                setConfirmDelete(true);
              }
            },
            children: deleting ? "正在删除" : "删除此会话",
          }),
          deletionSupported
            ? validManifestSha
              ? null
              : h("p", {
                  class: "panel-note",
                  children: "未能读取这条录像的 Miro-Ego-Manifest-SHA256 摘要，请刷新后重试。",
                })
            : h("p", { class: "panel-note", children: "当前固件未声明远程删除能力。" }),
          confirmDelete
            ? h("div", {
                class: "delete-backdrop",
                children: h("div", {
                  ref: dialogContainerRef,
                  class: "network-confirm danger session-delete-confirm",
                  role: "alertdialog",
                  tabIndex: -1,
                  "aria-modal": "true",
                  "aria-labelledby": "session-delete-title",
                  "aria-describedby": "session-delete-description",
                  "aria-busy": deleting,
                  onKeyDown: (ev) => {
                    if (ev.key === "Escape" && !deleting) {
                      ev.preventDefault();
                      setConfirmDelete(false);
                    }
                    if (ev.key === "Tab") {
                      const focusable = Array.from(
                        ev.currentTarget.querySelectorAll("button:not(:disabled)")
                      );
                      const first = focusable[0];
                      const last = focusable[focusable.length - 1];
                      if (!first) {
                        ev.preventDefault();
                      } else if (
                        ev.shiftKey &&
                        (document.activeElement === first || document.activeElement === ev.currentTarget)
                      ) {
                        ev.preventDefault();
                        last?.focus();
                      } else if (!ev.shiftKey && document.activeElement === last) {
                        ev.preventDefault();
                        first?.focus();
                      }
                    }
                  },
                  children: [
                    h("strong", {
                      id: "session-delete-title",
                      children: ["永久删除“", detail.display_name, "”？"],
                    }),
                    h("p", {
                      id: "session-delete-description",
                      children: "将删除设备上这次录制的全部文件，无法撤销。已下载的本地文件不受影响。",
                    }),
                    opError ? h("p", { role: "alert", children: opError.message }) : null,
                    h("div", {
                      class: "network-confirm-actions",
                      children: [
                        h("button", {
                          type: "button",
                          class: "panel-danger",
                          disabled: deleting,
                          onClick: () => executeDelete(),
                          children: deleting ? "正在删除" : "确认删除",
                        }),
                        h("button", {
                          ref: cancelDeleteRef,
                          type: "button",
                          class: "panel-secondary",
                          disabled: deleting,
                          onClick: () => setConfirmDelete(false),
                          children: "取消",
                        }),
                      ],
                    }),
                  ],
                }),
              })
            : null,
        ],
      }),
    ],
  });
}

const SESSION_FILTERS = [
  { id: "all", label: "全部" },
  { id: "saved", label: "已保存" },
  { id: "unsuccessful", label: "恢复/中断", tone: "caution" },
];

function isRecoveredPrefixSession(item, retainedPrefixSessionId) {
  return (
    item.recovered_prefix === true ||
    item.session_id === retainedPrefixSessionId ||
    (item.verification?.diagnostics.some(
      (d) => typeof d !== "string" && d.code === "recording_interrupted"
    ) ??
      false)
  );
}

function matchesSessionFilter(item, query, filter, retainedPrefixSessionId) {
  if (filter === "saved" && item.producer_outcome !== "sealed") return false;
  if (
    filter === "unsuccessful" &&
    item.producer_outcome === "sealed" &&
    !isRecoveredPrefixSession(item, retainedPrefixSessionId)
  ) {
    return false;
  }
  if (!query) return true;
  const q = query.trim().toLowerCase();
  return (
    item.display_name.toLowerCase().includes(q) || item.session_id.toLowerCase().includes(q)
  );
}

function SessionsPanel({ state }) {
  const [quickDeleteId, setQuickDeleteId] = useState(null);
  if (state.selected) {
    return h(
      SessionDetailPanel,
      {
        state,
        requestDelete: quickDeleteId === state.selected.sessionId,
      },
      state.selected.sessionId
    );
  }

  const { items, query, filter, loading, loadedOnce, nextCursor, diagnostics, error } =
    state.sessions;
  const retainedState = state.capture?.snapshot.retained_unsuccessful?.recording_state;
  const retainedPrefixId = retainedState?.diagnostics.some(
    (d) => d.code === "recording_prefix_saved"
  )
    ? retainedState.session_id
    : undefined;
  const filtered = items.filter((item) =>
    matchesSessionFilter(item, query, filter, retainedPrefixId)
  );

  return h("aside", {
    class: "panel",
    "aria-label": "会话台账",
    children: [
      h("div", {
        class: "panel-head",
        children: [
          h("span", { class: "eyebrow", children: "SESSIONS" }),
          h("span", { class: "panel-title", children: "录制列表" }),
          h("span", { class: "panel-spacer" }),
          h("button", {
            type: "button",
            class: "icon-button",
            "aria-label": "刷新会话",
            title: "刷新会话",
            disabled: loading,
            onClick: () => Store.refreshSessions(),
            children: h(RefreshIcon, {}),
          }),
          h("button", {
            type: "button",
            class: "icon-button",
            "aria-label": "关闭",
            onClick: () => Store.dispatch({ type: "panel.closed" }),
            children: h(CloseIcon, {}),
          }),
        ],
      }),
      h("div", {
        class: "panel-tools",
        children: [
          h("div", {
            class: "search",
            children: [
              h(SearchIcon, { size: 15 }),
              h("input", {
                type: "search",
                value: query,
                placeholder: "搜索会话名或 ID",
                "aria-label": "搜索会话",
                onInput: (ev) => Store.setSessionQuery(ev.currentTarget.value),
              }),
            ],
          }),
          h("div", {
            class: "chips",
            children: [
              SESSION_FILTERS.map((f) =>
                h(
                  "button",
                  {
                    type: "button",
                    class: "chip",
                    "data-tone": f.tone,
                    "aria-pressed": filter === f.id,
                    onClick: () => Store.setSessionFilter(f.id),
                    children: f.label,
                  },
                  f.id
                )
              ),
              h("span", {
                class: "chip chip-stat",
                "aria-hidden": "true",
                children: loadedOnce ? `已载入 ${items.length}` : null,
              }),
            ],
          }),
        ],
      }),
      h("div", {
        class: "panel-body",
        children: [
          error
            ? h("div", {
                class: "discovery-diagnostic",
                role: "alert",
                "data-testid": "session-list-error",
                children: [
                  h("strong", { children: "录像列表读取失败" }),
                  h("p", { children: error.message }),
                  items.length > 0
                    ? h("p", { children: "以下为上次成功读取的列表，可能不是最新状态。" })
                    : null,
                  h("button", {
                    type: "button",
                    class: "panel-more",
                    disabled: loading,
                    onClick: () => Store.refreshSessions(),
                    children: loading ? "正在重试" : "重试读取列表",
                  }),
                ],
              })
            : null,
          filtered.map((item) =>
            h(
              "div",
              {
                class: "session-entry",
                children: [
                  h(
                    "button",
                    {
                      type: "button",
                      class: "session-row",
                      "data-testid": "session-item",
                      "data-outcome":
                        item.producer_outcome === "sealed" ? "sealed" : "unsuccessful",
                      onClick: () => {
                        setQuickDeleteId(null);
                        Store.openSession(item.session_id, item.producer_outcome);
                      },
                      children: [
                        h("span", {
                          class: "session-head",
                          children: h("span", {
                            class: "session-name",
                            children: item.display_name,
                          }),
                        }),
                        h("span", {
                          class: "session-meta",
                          children: [
                            h("span", { children: formatSeconds(item.duration_seconds) }),
                            h("span", { children: formatGiB(item.total_bytes) }),
                            h("span", {
                              class: "producer",
                              children: isRecoveredPrefixSession(item, retainedPrefixId)
                                ? "录制中断 - 完整前段已保存"
                                : item.producer_outcome === "sealed"
                                  ? "已保存"
                                  : item.producer_outcome,
                            }),
                            h("code", { class: "session-uuid", children: item.session_id }),
                          ],
                        }),
                      ],
                    },
                    item.session_id
                  ),
                  state.device?.capabilities.session_deletion &&
                  item.producer_outcome === "sealed"
                    ? h("button", {
                        type: "button",
                        class: "session-quick-delete",
                        "aria-label": `删除录制：${item.display_name}`,
                        onClick: () => {
                          setQuickDeleteId(item.session_id);
                          Store.openSession(item.session_id, item.producer_outcome);
                        },
                        children: [h(TrashIcon, { size: 18 }), h("span", { children: "删除" })],
                      })
                    : null,
                ],
              },
              item.session_id
            )
          ),
          diagnostics.map((d) =>
            h(
              "p",
              { class: "discovery-diagnostic", title: d.code, children: d.message },
              `msg-${d.quarantine_id}`
            )
          ),
          filtered.length === 0 && loadedOnce && !loading && !error && diagnostics.length === 0
            ? h("p", {
                class: "panel-empty",
                children: items.length === 0 ? "暂无会话" : "没有匹配当前搜索或筛选的会话",
              })
            : null,
          !loadedOnce && !error
            ? h("p", { class: "panel-empty", role: "status", children: "正在读取录制列表" })
            : null,
          nextCursor
            ? h("button", {
                type: "button",
                class: "panel-more",
                disabled: loading,
                onClick: () => Store.loadMoreSessions(),
                children: loading ? "正在读取" : "加载更多",
              })
            : loadedOnce && items.length > 0 && !error
              ? h("p", { class: "panel-empty", children: "已到台账末尾" })
              : null,
        ],
      }),
    ],
  });
}

// ---------------------------------------------------------------------------
// Live Preview (Capability-Gated MSE H.264 fMP4 + MJPEG Fallback + Peaking)
// ---------------------------------------------------------------------------

const COMPACT_MEDIA_QUERY =
  "(max-width: 859px), (max-width: 1200px) and (max-height: 600px) and (pointer: coarse)";

function useCompactViewport() {
  const [compact, setCompact] = useState(() => window.matchMedia(COMPACT_MEDIA_QUERY).matches);
  useEffect(() => {
    const mql = window.matchMedia(COMPACT_MEDIA_QUERY);
    const update = () => setCompact(mql.matches);
    mql.addEventListener("change", update);
    update();
    return () => mql.removeEventListener("change", update);
  }, []);
  return compact;
}

function FullscreenToggle() {
  const [isFs, setIsFs] = useState(!!document.fullscreenElement);
  const [rotateHint, setRotateHint] = useState(false);

  useEffect(() => {
    const onFs = () => setIsFs(!!document.fullscreenElement);
    const onOrient = () => setRotateHint(false);
    const mql = window.matchMedia("(orientation: landscape)");
    document.addEventListener("fullscreenchange", onFs);
    mql.addEventListener("change", onOrient);
    return () => {
      document.removeEventListener("fullscreenchange", onFs);
      mql.removeEventListener("change", onOrient);
    };
  }, []);

  async function toggle() {
    if (document.fullscreenElement) {
      try {
        await document.exitFullscreen();
      } catch {}
      return;
    }
    try {
      await document.documentElement.requestFullscreen?.();
      const orient = screen.orientation;
      if (document.fullscreenElement) await orient?.lock?.("landscape");
    } catch {}
    setRotateHint(!window.matchMedia("(orientation: landscape)").matches);
  }

  return h(Fragment, {
    children: [
      h("button", {
        type: "button",
        class: "preview-view-toggle preview-tools-toggle icon-button",
        "aria-label": isFs ? "退出全屏取景" : "横屏全屏取景",
        onClick: () => toggle(),
        children: [
          h(FullscreenIcon, { size: 18 }),
          isFs ? h("span", { children: "退出全屏" }) : h("span", { children: "横屏取景" }),
        ],
      }),
      rotateHint
        ? h("span", {
            class: "rotate-hint",
            role: "status",
            children: "请将手机横放，画面会自动铺开",
          })
        : null,
    ],
  });
}

function getSourceDimensions(imgOrBitmap) {
  if (imgOrBitmap instanceof HTMLImageElement) {
    return { width: imgOrBitmap.naturalWidth, height: imgOrBitmap.naturalHeight };
  }
  if (imgOrBitmap instanceof HTMLVideoElement) {
    return { width: imgOrBitmap.videoWidth, height: imgOrBitmap.videoHeight };
  }
  return { width: imgOrBitmap.width, height: imgOrBitmap.height };
}

const decodedFrameCache = new Map();
function evictDecodedFrame(url) {
  const entry = decodedFrameCache.get(url);
  decodedFrameCache.delete(url);
  entry
    ?.then((bmp) => {
      if ("close" in bmp) bmp.close();
    })
    .catch(() => {});
}

function decodeFrameUrl(url, blobOpt) {
  const cached = decodedFrameCache.get(url);
  if (cached) return cached;
  const decodeViaImage = async () => {
    const img = new Image();
    img.decoding = "async";
    img.src = url;
    await img.decode();
    return img;
  };
  const promise =
    typeof createImageBitmap === "function"
      ? (blobOpt ? Promise.resolve(blobOpt) : fetch(url).then((r) => r.blob()))
          .then((b) => createImageBitmap(b))
          .catch(decodeViaImage)
      : decodeViaImage();
  decodedFrameCache.set(url, promise);
  while (decodedFrameCache.size > 4) {
    evictDecodedFrame(decodedFrameCache.keys().next().value);
  }
  return promise;
}

const H264_MSE_CODECS = [
  'video/mp4; codecs="avc1.640028"',
  'video/mp4; codecs="avc1.4d4028"',
  'video/mp4; codecs="avc1.42E01E"',
  'video/mp4; codecs="avc1.64001f"',
];

function selectSupportedH264Mime() {
  if (typeof MediaSource === "undefined" || typeof MediaSource.isTypeSupported !== "function") {
    return null;
  }
  for (const codec of H264_MSE_CODECS) {
    if (MediaSource.isTypeSupported(codec)) return codec;
  }
  return null;
}

// Streams H.264 fMP4 from /api/v1/preview/h264 into MSE <video> and pumps frames;
// returns false if unsupported or stream decode fails so caller falls back cleanly to MJPEG!
async function runH264MsePreviewLoop(options) {
  const mime = selectSupportedH264Mime();
  const videoEl = options.videoElement;
  if (!mime || !videoEl) return false;

  const response = await fetch(`${API_PREFIX}/preview/h264`, {
    cache: "no-store",
    headers: buildHeaders("video/mp4"),
    signal: options.signal,
  });
  observeResponseAuditHeader(response);
  if (response.status === 404) return false;
  if (!response.ok) {
    const err = await parseErrorResponse(response);
    if (err.code === "preview_h264_unsupported") return false;
    throw err;
  }
  if (!response.body) return false;

  const mediaSource = new MediaSource();
  let objectUrl = null;
  let rafId = null;
  let gotFirstDecodedFrame = false;
  const reader = response.body.getReader();

  try {
    try {
      if ("srcObject" in videoEl && "handle" in mediaSource && mediaSource.handle) {
        videoEl.srcObject = mediaSource.handle;
      } else {
        objectUrl = URL.createObjectURL(mediaSource);
        videoEl.src = objectUrl;
      }
    } catch {
      return false;
    }

    const sourceOpenOk = await new Promise((resolve) => {
      const timer = window.setTimeout(() => resolve(false), 2000);
      const onOpen = () => {
        window.clearTimeout(timer);
        resolve(true);
      };
      const onErr = () => {
        window.clearTimeout(timer);
        resolve(false);
      };
      mediaSource.addEventListener("sourceopen", onOpen, { once: true });
      videoEl.addEventListener("error", onErr, { once: true });
    });
    if (!sourceOpenOk || options.signal.aborted) return false;

    let sourceBuffer;
    try {
      sourceBuffer = mediaSource.addSourceBuffer(mime);
      sourceBuffer.mode = "segments";
    } catch {
      return false;
    }

    let failed = false;
    const queue = [];
    let appending = false;

    const pumpBuffer = () => {
      if (failed || appending || sourceBuffer.updating || queue.length === 0) return;
      if (mediaSource.readyState !== "open") return;
      appending = true;
      const chunk = queue.shift();
      try {
        sourceBuffer.appendBuffer(chunk);
      } catch {
        failed = true;
        appending = false;
      }
    };

    sourceBuffer.addEventListener("error", () => {
      failed = true;
    });
    videoEl.addEventListener("error", () => {
      failed = true;
    });
    sourceBuffer.addEventListener("updateend", () => {
      appending = false;
      try {
        if (sourceBuffer.buffered.length > 0) {
          const end = sourceBuffer.buffered.end(sourceBuffer.buffered.length - 1);
          const start = sourceBuffer.buffered.start(0);
          if (end - videoEl.currentTime > 0.6) {
            videoEl.currentTime = Math.max(start, end - 0.15);
          }
          if (end - start > 6 && !sourceBuffer.updating) {
            sourceBuffer.remove(start, end - 3);
            return;
          }
        }
      } catch {}
      pumpBuffer();
    });

    const renderTick = () => {
      if (options.signal.aborted || failed) return;
      if (videoEl.readyState >= 2 && videoEl.videoWidth > 0 && videoEl.videoHeight > 0) {
        gotFirstDecodedFrame = true;
        options.onVideoTick();
        options.onState("live");
      }
      rafId = window.requestAnimationFrame(renderTick);
    };
    rafId = window.requestAnimationFrame(renderTick);
    videoEl.play().catch(() => {});

    const firstFrameDeadline = performance.now() + 2500;
    try {
      while (!options.signal.aborted && !failed) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value && value.byteLength > 0) {
          queue.push(value);
          if (queue.length > 32) queue.splice(0, queue.length - 16);
          pumpBuffer();
        }
        if (!gotFirstDecodedFrame && performance.now() > firstFrameDeadline) {
          // Stream did not produce decodable fMP4 frames (e.g. mock synthetic header or CSP media block)
          return false;
        }
      }
    } catch (err) {
      if (options.signal.aborted) return false;
      if (!gotFirstDecodedFrame) throw err;
    }
    if (!options.signal.aborted && gotFirstDecodedFrame) {
      options.onState("unavailable");
      return true;
    }
    return false;
  } finally {
    if (rafId !== null) window.cancelAnimationFrame(rafId);
    try {
      await reader.cancel();
    } catch {}
    reader.releaseLock();
    try {
      videoEl.pause();
      videoEl.removeAttribute("src");
      videoEl.srcObject = null;
      videoEl.load();
    } catch {}
    if (objectUrl) URL.revokeObjectURL(objectUrl);
  }
}

async function runMjpegPreviewLoop(options) {
  let activeUrl = null;
  let prevUrl = null;
  let seq = 0;
  let displayedSeq = 0;
  let stopped = false;
  let pendingErr = null;
  const inflight = new Set();

  const releaseUrl = (url) => {
    URL.revokeObjectURL(url);
    evictDecodedFrame(url);
  };

  let rafId = null;
  let staged = null;

  const cancelStaged = () => {
    if (rafId !== null) cancelAnimationFrame(rafId);
    rafId = null;
    if (staged) {
      releaseUrl(staged.url);
      staged.done();
      staged = null;
    }
  };

  const stageFrame = (url, frameSeq) => {
    if (
      stopped ||
      options.signal.aborted ||
      frameSeq <= displayedSeq ||
      (staged && frameSeq < staged.sequence)
    ) {
      releaseUrl(url);
      return Promise.resolve();
    }
    if (staged) {
      releaseUrl(staged.url);
      staged.done();
    }
    return new Promise((done) => {
      staged = { url, sequence: frameSeq, done };
      rafId ??= requestAnimationFrame(() => {
        rafId = null;
        const item = staged;
        staged = null;
        if (stopped || options.signal.aborted || item.sequence <= displayedSeq) {
          releaseUrl(item.url);
        } else {
          if (prevUrl) releaseUrl(prevUrl);
          prevUrl = activeUrl;
          activeUrl = item.url;
          displayedSeq = item.sequence;
          options.onFrame(item.url);
          options.onState("live");
        }
        item.done();
      });
    });
  };

  const clearAll = () => {
    if (prevUrl) {
      releaseUrl(prevUrl);
      prevUrl = null;
    }
    if (activeUrl) {
      releaseUrl(activeUrl);
      activeUrl = null;
    }
    options.onFrame(null);
  };

  options.signal.addEventListener("abort", cancelStaged, { once: true });
  try {
    while (!options.signal.aborted) {
      try {
        if (inflight.size >= 2) await Promise.race(inflight);
        if (options.signal.aborted) return;
        if (pendingErr) {
          const err = pendingErr;
          pendingErr = null;
          throw err;
        }
        const t0 = performance.now();
        const blob = await fetchJpegPreviewFrame(options.signal);
        const url = URL.createObjectURL(blob);
        const mySeq = ++seq;
        const task = decodeFrameUrl(url, blob)
          .then(() => stageFrame(url, mySeq))
          .catch((err) => {
            releaseUrl(url);
            if (mySeq > displayedSeq) pendingErr = err;
          })
          .finally(() => {
            inflight.delete(task);
          });
        inflight.add(task);
        await abortableDelay(Math.max(0, 1000 / 25 - (performance.now() - t0)), options.signal);
      } catch (err) {
        if (options.signal.aborted) return;
        const previewUnavail =
          err instanceof DeviceApiError &&
          ((err.status === 503 && err.code === "preview_unavailable") || err.status === 409);
        const camDisconnected =
          err instanceof DeviceApiError && err.status === 503 && err.code === "camera_not_connected";
        if (camDisconnected) {
          displayedSeq = seq;
          cancelStaged();
          clearAll();
          options.onState("disconnected");
        } else {
          options.onState("unavailable");
        }
        if (!previewUnavail && !camDisconnected) console.warn(err);
        await abortableDelay(500, options.signal);
      }
    }
  } finally {
    stopped = true;
    cancelStaged();
    await Promise.all(inflight);
    options.signal.removeEventListener("abort", cancelStaged);
    clearAll();
  }
}

// Focus Peaking Worker (strictly blob: worker-src compliant; falls back to main thread)
const FOCUS_PEAKING_WORKER_SOURCE = `(function(){"use strict";function fit(w,h){const nw=Math.max(0,Math.floor(w)),nh=Math.max(0,Math.floor(h));if(!nw||!nh)return{width:nw,height:nh};const scale=Math.min(1,Math.sqrt(524288/(nw*nh)));return{width:Math.max(0,Math.floor(nw*scale)),height:Math.max(0,Math.floor(nh*scale))}}class Peaker{luminance=new Uint8Array(0);apply(px,w,h,thr){const n=w*h;if(this.luminance.length!==n)this.luminance=new Uint8Array(n);const lum=this.luminance;for(let i=0;i<n;i++){const o=i*4;lum[i]=(px[o]??0)*0.299+(px[o+1]??0)*0.587+(px[o+2]??0)*0.114}px.fill(0);for(let y=1;y<h-1;y++)for(let x=1;x<w-1;x++){const idx=y*w+x,gx=Math.abs((lum[idx-1]??0)-(lum[idx+1]??0)),gy=Math.abs((lum[idx-w]??0)-(lum[idx+w]??0));if(Math.max(gx,gy)>thr){const o=idx*4;px[o]=232;px[o+1]=88;px[o+2]=255;px[o+3]=230}}}}let canvas=null;const peaker=new Peaker();self.onmessage=ev=>{const{image,threshold}=ev.data;try{const{width,height}=fit(image.width,image.height);canvas??=new OffscreenCanvas(width,height);if(canvas.width!==width)canvas.width=width;if(canvas.height!==height)canvas.height=height;const ctx=canvas.getContext("2d",{willReadFrequently:true});if(!ctx)throw new Error("no ctx");ctx.drawImage(image,0,0,width,height);const imgData=ctx.getImageData(0,0,width,height);peaker.apply(imgData.data,width,height,threshold);ctx.putImageData(imgData,0,0);const mask=canvas.transferToImageBitmap();self.postMessage({mask},{transfer:[mask]})}catch{self.postMessage({error:true})}finally{image.close()}}})();`;

const PEAKING_WORKER_BLOB =
  typeof self !== "undefined" &&
  self.Blob &&
  new Blob(
    ["(self.URL || self.webkitURL).revokeObjectURL(self.location.href);", FOCUS_PEAKING_WORKER_SOURCE],
    { type: "application/javascript;charset=utf-8" }
  );

function spawnPeakingWorker() {
  if (!PEAKING_WORKER_BLOB) throw new Error("Blob worker unavailable");
  const blobUrl = (self.URL || self.webkitURL).createObjectURL(PEAKING_WORKER_BLOB);
  if (!blobUrl) throw new Error("ObjectURL unavailable");
  const worker = new Worker(blobUrl);
  worker.addEventListener("error", () => {
    (self.URL || self.webkitURL).revokeObjectURL(blobUrl);
  });
  return worker;
}

const MAX_PEAKING_PIXELS = 512 * 1024;
function computePeakingSize(w, h) {
  const nw = Math.max(0, Math.floor(w));
  const nh = Math.max(0, Math.floor(h));
  if (!nw || !nh) return { width: nw, height: nh };
  const scale = Math.min(1, Math.sqrt(MAX_PEAKING_PIXELS / (nw * nh)));
  return { width: Math.max(0, Math.floor(nw * scale)), height: Math.max(0, Math.floor(nh * scale)) };
}

class MainThreadPeaker {
  luminance = new Uint8Array(0);
  apply(px, w, h, thr) {
    const n = w * h;
    if (this.luminance.length !== n) this.luminance = new Uint8Array(n);
    const lum = this.luminance;
    for (let i = 0; i < n; i++) {
      const o = i * 4;
      lum[i] = (px[o] ?? 0) * 0.299 + (px[o + 1] ?? 0) * 0.587 + (px[o + 2] ?? 0) * 0.114;
    }
    px.fill(0);
    for (let y = 1; y < h - 1; y++) {
      for (let x = 1; x < w - 1; x++) {
        const idx = y * w + x;
        const gx = Math.abs((lum[idx - 1] ?? 0) - (lum[idx + 1] ?? 0));
        const gy = Math.abs((lum[idx - w] ?? 0) - (lum[idx + w] ?? 0));
        if (Math.max(gx, gy) > thr) {
          const o = idx * 4;
          px[o] = 232;
          px[o + 1] = 88;
          px[o + 2] = 255;
          px[o + 3] = 230;
        }
      }
    }
  }
}

function clearCanvas2d(canvas) {
  if (!canvas) return;
  canvas.getContext("2d", { willReadFrequently: true })?.clearRect(0, 0, canvas.width, canvas.height);
}

function renderFallbackPeaking(canvas, source, threshold, peaker) {
  const dims = getSourceDimensions(source);
  if (dims.width < 3 || dims.height < 3) {
    clearCanvas2d(canvas);
    return;
  }
  const { width, height } = computePeakingSize(dims.width, dims.height);
  if (canvas.width !== width) canvas.width = width;
  if (canvas.height !== height) canvas.height = height;
  const ctx = canvas.getContext("2d", { willReadFrequently: true });
  if (!ctx) return;
  ctx.clearRect(0, 0, width, height);
  ctx.drawImage(source, 0, 0, width, height);
  const imgData = ctx.getImageData(0, 0, width, height);
  peaker.apply(imgData.data, width, height, threshold);
  ctx.putImageData(imgData, 0, 0);
}

function FocusPeakingCanvas({ frameUrl, videoTick, videoRef, state }) {
  const canvasRef = useRef(null);
  const fallbackPeakerRef = useRef(null);
  const busyRef = useRef(false);
  const workerRef = useRef(null);
  const lastFallbackMsRef = useRef(0);
  const workerDisabledRef = useRef(false);
  const mountedRef = useRef(true);
  const abortWorkerJobRef = useRef(null);

  const activeRef = useRef(state.focusPeaking.enabled);
  const genRef = useRef(0);
  const queuedSourceRef = useRef(null);
  const thresholdRef = useRef(state.focusPeaking.threshold);

  useEffect(
    () => () => {
      mountedRef.current = false;
      genRef.current += 1;
      activeRef.current = false;
      abortWorkerJobRef.current?.();
      workerRef.current?.terminate();
      workerRef.current = null;
    },
    []
  );

  async function runWorkerPeaking(source, threshold) {
    if (
      workerDisabledRef.current ||
      typeof OffscreenCanvas === "undefined" ||
      typeof createImageBitmap === "undefined"
    ) {
      return null;
    }
    const dims = getSourceDimensions(source);
    const { width, height } = computePeakingSize(dims.width, dims.height);
    if (width < 3 || height < 3) return null;
    let bmp;
    try {
      bmp = await createImageBitmap(source, {
        resizeWidth: width,
        resizeHeight: height,
        resizeQuality: "low",
      });
    } catch {
      workerDisabledRef.current = true;
      return null;
    }
    if (!mountedRef.current) {
      bmp.close();
      return null;
    }
    try {
      workerRef.current ??= spawnPeakingWorker();
    } catch {
      bmp.close();
      workerDisabledRef.current = true;
      return null;
    }
    const worker = workerRef.current;
    return new Promise((resolve) => {
      const fail = () => {
        workerDisabledRef.current = true;
        worker.terminate();
        workerRef.current = null;
        abortWorkerJobRef.current = null;
        resolve(null);
      };
      abortWorkerJobRef.current = fail;
      worker.onerror = fail;
      worker.onmessage = (ev) => {
        abortWorkerJobRef.current = null;
        if (ev.data.error) fail();
        else resolve(ev.data.mask ?? null);
      };
      try {
        worker.postMessage({ image: bmp, threshold }, [bmp]);
      } catch {
        bmp.close();
        fail();
      }
    });
  }

  function resetPeaking() {
    genRef.current += 1;
    activeRef.current = false;
    queuedSourceRef.current = null;
    clearCanvas2d(canvasRef.current);
  }

  function enqueuePeaking(sourceDescriptor) {
    const myGen = genRef.current;
    activeRef.current = true;
    queuedSourceRef.current = sourceDescriptor;
    if (!busyRef.current) drainLoop(myGen);
  }

  async function drainLoop(myGen) {
    busyRef.current = true;
    try {
      while (activeRef.current && myGen === genRef.current) {
        const desc = queuedSourceRef.current;
        if (!desc) return;
        queuedSourceRef.current = null;
        try {
          const source =
            desc.kind === "url" ? await decodeFrameUrl(desc.url) : desc.video;
          if (!source || !activeRef.current || myGen !== genRef.current) return;
          const mask = await runWorkerPeaking(source, thresholdRef.current);
          try {
            const canvas = canvasRef.current;
            if (!canvas || !activeRef.current || myGen !== genRef.current) return;
            if (mask) {
              if (canvas.width !== mask.width) canvas.width = mask.width;
              if (canvas.height !== mask.height) canvas.height = mask.height;
              const ctx = canvas.getContext("2d");
              ctx?.clearRect(0, 0, canvas.width, canvas.height);
              ctx?.drawImage(mask, 0, 0);
              canvas.dataset.renderer = "worker";
            } else if (performance.now() - lastFallbackMsRef.current >= 100) {
              lastFallbackMsRef.current = performance.now();
              fallbackPeakerRef.current ??= new MainThreadPeaker();
              renderFallbackPeaking(canvas, source, thresholdRef.current, fallbackPeakerRef.current);
              canvas.dataset.renderer = "fallback";
            }
          } finally {
            mask?.close();
          }
        } catch {
          if (myGen === genRef.current) clearCanvas2d(canvasRef.current);
        }
      }
    } finally {
      busyRef.current = false;
      if (activeRef.current && queuedSourceRef.current) drainLoop(genRef.current);
    }
  }

  const hasFrame = Boolean(frameUrl || (videoTick > 0 && videoRef.current));

  useLayoutEffect(() => {
    thresholdRef.current = state.focusPeaking.threshold;
    if (!state.focusPeaking.enabled || !hasFrame || document.hidden) {
      resetPeaking();
      return;
    }
    if (frameUrl) {
      enqueuePeaking({ kind: "url", url: frameUrl });
    } else if (videoRef.current && videoRef.current.readyState >= 2) {
      enqueuePeaking({ kind: "video", video: videoRef.current });
    }
  }, [frameUrl, videoTick, state.focusPeaking.enabled, state.focusPeaking.threshold]);

  return h("canvas", {
    ref: canvasRef,
    class: "focus-peaking-canvas",
    "data-testid": "focus-peaking-canvas",
    hidden: !state.focusPeaking.enabled || !hasFrame,
    "aria-hidden": "true",
  });
}

function FocusPeakingControl({ state }) {
  return h("div", {
    class: "focus-peaking-control",
    "data-enabled": String(state.focusPeaking.enabled),
    children: [
      h("button", {
        type: "button",
        role: "switch",
        "aria-label": "峰值对焦",
        "aria-checked": state.focusPeaking.enabled,
        title: "峰值对焦",
        onClick: () => Store.dispatch({ type: "focus-peaking.toggled" }),
        children: "PEAK",
      }),
      h("input", {
        type: "range",
        min: "0",
        max: "255",
        step: "1",
        value: state.focusPeaking.threshold,
        disabled: !state.focusPeaking.enabled,
        "aria-label": "峰值对焦阈值",
        onInput: (ev) =>
          Store.dispatch({
            type: "focus-peaking.threshold",
            threshold: Number(ev.currentTarget.value),
          }),
      }),
      h("output", { "aria-hidden": "true", children: state.focusPeaking.threshold }),
    ],
  });
}

function PreviewTransportControl({ state }) {
  if (state.device?.capabilities.preview_h264 !== true) return null;
  const current = state.previewTransport;
  return h("div", {
    class: "preview-transport-control",
    "data-testid": "preview-transport",
    role: "group",
    "aria-label": "预览传输协议",
    children: [
      { id: "auto", label: "AUTO" },
      { id: "h264", label: "H.264" },
      { id: "mjpeg", label: "MJPEG" },
    ].map((opt) =>
      h(
        "button",
        {
          type: "button",
          "aria-pressed": current === opt.id,
          onClick: () => Store.dispatch({ type: "preview.transport", transport: opt.id }),
          children: opt.label,
        },
        opt.id
      )
    ),
  });
}

const INSPECT_MODES = [
  { mode: "both", label: "双目" },
  { mode: "left", label: "左眼" },
  { mode: "right", label: "右眼" },
];

const PREVIEW_STATE_MESSAGES = {
  live: "实时",
  waiting: "等待设备画面",
  unavailable: "画面暂不可用",
  disconnected: "相机未接入",
};

function PreviewCanvas({ frameUrl, videoTick, videoRef }) {
  const canvasRef = useRef(null);

  useLayoutEffect(() => {
    let cancelled = false;
    const canvas = canvasRef.current;
    if (!canvas) return;

    if (frameUrl) {
      decodeFrameUrl(frameUrl)
        .then((img) => {
          if (cancelled) return;
          const { width, height } = getSourceDimensions(img);
          if (width > 0 && height > 0) {
            if (canvas.width !== width) canvas.width = width;
            if (canvas.height !== height) canvas.height = height;
            const aspect = String(width / height);
            const parent = canvas.parentElement;
            if (parent?.style.getPropertyValue("--preview-aspect") !== aspect) {
              parent?.style.setProperty("--preview-aspect", aspect);
            }
            canvas.getContext("2d", { alpha: false })?.drawImage(img, 0, 0);
            canvas.dataset.frameUrl = frameUrl;
          }
        })
        .catch(() => {});
    } else if (videoTick > 0 && videoRef.current && videoRef.current.videoWidth > 0) {
      const vid = videoRef.current;
      const width = vid.videoWidth;
      const height = vid.videoHeight;
      if (canvas.width !== width) canvas.width = width;
      if (canvas.height !== height) canvas.height = height;
      const aspect = String(width / height);
      const parent = canvas.parentElement;
      if (parent?.style.getPropertyValue("--preview-aspect") !== aspect) {
        parent?.style.setProperty("--preview-aspect", aspect);
      }
      canvas.getContext("2d", { alpha: false })?.drawImage(vid, 0, 0);
      canvas.dataset.frameUrl = "mse://h264";
    } else {
      delete canvas.dataset.frameUrl;
    }

    return () => {
      cancelled = true;
    };
  }, [frameUrl, videoTick]);

  const visible = Boolean(frameUrl || (videoTick > 0 && videoRef.current));
  return h("canvas", {
    ref: canvasRef,
    "data-testid": "preview-image",
    role: "img",
    "aria-label": "设备实时预览",
    hidden: !visible,
  });
}

function LivePreviewStage({ state, active }) {
  const [frameUrl, setFrameUrl] = useState(null);
  const [videoTick, setVideoTick] = useState(0);
  const [previewState, setPreviewState] = useState("waiting");
  const mseVideoRef = useRef(null);

  const previewCap = state.device?.capabilities.preview !== false;
  const h264Cap = state.device?.capabilities.preview_h264 === true;
  const transportPref = state.previewTransport;

  useEffect(() => {
    if (!active || !previewCap) return;
    const controller = new AbortController();
    let mounted = true;

    setFrameUrl(null);
    setVideoTick(0);
    setPreviewState("waiting");

    (async () => {
      // Strictly check device.capabilities.preview_h264 BEFORE ever touching /preview/h264!
      const wantH264 = h264Cap && transportPref !== "mjpeg";
      if (wantH264 && mseVideoRef.current) {
        let retriesAfterLive = 0;
        while (!controller.signal.aborted && mounted) {
          try {
            const hadLiveFrames = await runH264MsePreviewLoop({
              signal: controller.signal,
              videoElement: mseVideoRef.current,
              onVideoTick: () => {
                if (mounted) setVideoTick((n) => (n + 1) & 0x3fffffff || 1);
              },
              onState: (st) => {
                if (mounted) setPreviewState(st);
              },
            });
            if (controller.signal.aborted || !mounted) return;
            if (mounted) setVideoTick(0);
            if (!hadLiveFrames) break;
            retriesAfterLive += 1;
            if (mounted) setPreviewState("unavailable");
            await abortableDelay(500, controller.signal);
            if (retriesAfterLive >= 3) break;
          } catch (err) {
            if (controller.signal.aborted || !mounted) return;
            if (mounted) setVideoTick(0);
            const camDisconnected =
              err instanceof DeviceApiError &&
              err.status === 503 &&
              err.code === "camera_not_connected";
            if (mounted) setPreviewState(camDisconnected ? "disconnected" : "unavailable");
            break;
          }
        }
      }
      if (controller.signal.aborted || !mounted) return;

      // Clean MJPEG/JPEG preview stream (primary when preview_h264=false, automatic fallback when true)
      await runMjpegPreviewLoop({
        signal: controller.signal,
        onFrame: (url) => {
          if (mounted) setFrameUrl(url);
        },
        onState: (st) => {
          if (mounted) setPreviewState(st);
        },
      });
    })();

    return () => {
      mounted = false;
      controller.abort();
    };
  }, [active, previewCap, h264Cap, transportPref]);

  const camDisconnected =
    (state.capture?.snapshot.runtime ?? state.device?.runtime ?? null)?.camera.state ===
      "disconnected" || previewState === "disconnected";
  const hasVisual = (Boolean(frameUrl) || videoTick > 0) && !camDisconnected;

  return h("div", {
    class: "frame",
    "data-inspect": state.inspect,
    "data-full": String(state.fullFrame),
    children: [
      h264Cap
        ? h("video", {
            ref: mseVideoRef,
            class: "preview-mse-sink",
            muted: true,
            playsInline: true,
            "aria-hidden": "true",
          })
        : null,
      h(PreviewCanvas, {
        frameUrl: hasVisual ? frameUrl : null,
        videoTick: hasVisual ? videoTick : 0,
        videoRef: mseVideoRef,
      }),
      h(FocusPeakingCanvas, {
        state,
        frameUrl: hasVisual ? frameUrl : null,
        videoTick: hasVisual ? videoTick : 0,
        videoRef: mseVideoRef,
      }),
      hasVisual
        ? h("div", {
            class: "vf-furniture",
            "aria-hidden": "true",
            children: [
              h("span", { class: "vf-tick", "data-edge": "top" }),
              h("span", { class: "vf-tick", "data-edge": "bottom" }),
              h("span", { class: "vf-tick", "data-edge": "left" }),
              h("span", { class: "vf-tick", "data-edge": "right" }),
              h("span", { class: "vf-reticle" }),
            ],
          })
        : h("p", {
            class: "frame-empty",
            children: [
              h("span", { class: "eyebrow", children: "PREVIEW" }),
              h("span", {
                children: camDisconnected
                  ? "相机未接入"
                  : previewCap
                    ? PREVIEW_STATE_MESSAGES[previewState]
                    : "本机不提供预览",
              }),
            ],
          }),
    ],
  });
}

function ViewfinderOverlayControls({ state }) {
  const [toolsOpen, setToolsOpen] = useState(false);
  const compact = useCompactViewport();
  const imu = state.capture?.snapshot.runtime.live_imu ?? null;
  const singleEye = state.inspect !== "both";

  const inspectSeg = h("div", {
    class: "inspect",
    "data-testid": "preview-inspect",
    role: "group",
    "aria-label": "预览取景",
    children: INSPECT_MODES.map(({ mode, label }) =>
      h(
        "button",
        {
          type: "button",
          "aria-pressed": state.inspect === mode,
          onClick: () => Store.dispatch({ type: "inspect.changed", mode }),
          children: label,
        },
        mode
      )
    ),
  });

  const imuCard = h("section", {
    class: "overlay-card",
    "aria-labelledby": "raw-imu-heading",
    children: [
      h("h2", { id: "raw-imu-heading", class: "eyebrow", children: "RAW IMU" }),
      h("dl", {
        class: "imu-list",
        children: [
          h("div", {
            class: "imu-row",
            children: [
              h("dt", { children: "a" }),
              h("dd", {
                "data-testid": "acceleration",
                "data-available": String(!!imu),
                children: imu ? formatImuVec(imu.raw.accelerometer, "raw") : "不可用",
              }),
            ],
          }),
          h("div", {
            class: "imu-row",
            children: [
              h("dt", { children: "w" }),
              h("dd", {
                "data-testid": "angular-velocity",
                "data-available": String(!!imu),
                children: imu ? formatImuVec(imu.raw.gyroscope, "raw") : "不可用",
              }),
            ],
          }),
          h("div", {
            class: "imu-row",
            children: [
              h("dt", { "aria-label": "同步质量", children: "~" }),
              h("dd", {
                "data-testid": "imu-sync",
                "data-available": String(!!imu),
                children: imu ? imu.sync.quality ?? "不可用" : "不可用",
              }),
            ],
          }),
        ],
      }),
    ],
  });

  const cropNote = singleEye
    ? h("button", {
        type: "button",
        class: "crop-note",
        onClick: () => Store.dispatch({ type: "full-frame.toggled" }),
        children: [
          h(FullscreenIcon, { size: 15 }),
          state.fullFrame ? "全画幅 - 回到铺满" : "已裁切取景 - 看全画幅",
        ],
      })
    : null;

  if (compact) {
    return h("div", {
      class: "stage-left",
      "data-tools-open": String(toolsOpen),
      children: [
        h("div", {
          class: "vf-tools",
          children: [
            inspectSeg,
            h("div", {
              class: "preview-actions",
              children: [
                h(FullscreenToggle, {}),
                h("button", {
                  type: "button",
                  class: "preview-tools-toggle icon-button",
                  "aria-label": "取景工具",
                  "aria-expanded": toolsOpen,
                  "aria-controls": "preview-settings",
                  onClick: () => setToolsOpen(!toolsOpen),
                  children: [h(DeviceIcon, { size: 18 }), h("span", { children: "工具" })],
                }),
              ],
            }),
          ],
        }),
        h("div", {
          class: "preview-settings",
          id: "preview-settings",
          children: [
            h(FocusPeakingControl, { state }),
            h(PreviewTransportControl, { state }),
            imuCard,
            cropNote,
          ],
        }),
      ],
    });
  }

  return h("div", {
    class: "stage-left",
    children: [
      h("div", {
        class: "vf-tools",
        children: [
          inspectSeg,
          h(FocusPeakingControl, { state }),
          h(PreviewTransportControl, { state }),
        ],
      }),
      imuCard,
      cropNote,
    ],
  });
}

// ---------------------------------------------------------------------------
// Top Bar & Firmware Update / Rollback Panel (/api/v1/firmware...)
// ---------------------------------------------------------------------------

function TopBar({ state, onVersion }) {
  const snapshot = state.capture?.snapshot;
  const deviceState = snapshot?.device_state ?? null;
  const storage = state.device?.storage;
  const runtime = snapshot?.runtime ?? state.device?.runtime ?? null;
  const connMethod = formatConnectionMethod(runtime?.connection_method);
  const logsSupported = state.device?.capabilities.diagnostic_logs !== false;

  return h("header", {
    class: "topbar",
    children: [
      h("div", {
        class: "brand",
        children: [
          h("h1", { class: "visually-hidden", children: "Miro-Ego" }),
          h("span", { class: "brand-mark", "aria-hidden": "true" }),
          h("span", {
            class: "brand-label",
            children: state.device?.device.device_label ?? "Miro-Ego",
          }),
          h("button", {
            type: "button",
            class: "version-badge",
            "aria-label": "版本与更新",
            onClick: onVersion,
            children: state.device?.build?.package_version
              ? `v${state.device.build.package_version}`
              : "版本",
          }),
        ],
      }),
      h("strong", {
        class: "state-chip",
        "data-testid": "capture-state",
        "data-state": deviceState ?? "unknown",
        children: [
          deviceState === "recording"
            ? h("span", { class: "rec-block", "aria-hidden": "true" })
            : null,
          formatDeviceState(deviceState),
        ],
      }),
      state.auditUnavailable
        ? h("span", {
            class: "audit-badge",
            role: "status",
            title: "Miro-Ego-Audit-Status: unavailable",
            children: "审计缓冲降级",
          })
        : null,
      h("div", {
        class: "topbar-facts",
        children: [
          h("span", {
            children: [
              h("span", {
                "data-testid": "storage-available",
                children: storage ? formatGiB(storage.available_bytes) : "--",
              }),
              " ",
              h("span", {
                "data-testid": "storage-writable",
                children: storage ? (storage.writable ? "可写" : "不可写") : "--",
              }),
            ],
          }),
          h("span", {
            "data-testid": "temperature",
            children: formatTempC(runtime?.temperature_celsius),
          }),
          h("button", {
            id: "network-panel-trigger",
            type: "button",
            class: "connection-method-trigger",
            "aria-pressed": state.panel === "network",
            "aria-controls": "network-panel",
            "aria-label": "网络设置",
            "aria-describedby": "connection-method-label",
            title: "网络设置",
            onClick: () => {
              const open = state.panel !== "network";
              Store.dispatch(open ? { type: "panel.opened", panel: "network" } : { type: "panel.closed" });
              if (open) Store.refreshNetwork();
            },
            children: h("span", {
              id: "connection-method-label",
              "data-testid": "connection-method",
              children: connMethod,
            }),
          }),
          h("output", {
            class: "connection link-dot",
            "data-state": state.connection,
            "aria-live": "polite",
            children: CONNECTION_LABELS[state.connection] ?? state.connection,
          }),
        ],
      }),
      h("button", {
        type: "button",
        class: "icon-button",
        "aria-pressed": state.panel === "sessions",
        "aria-label": "会话台账",
        title: "会话台账",
        onClick: () => {
          const open = state.panel !== "sessions";
          Store.dispatch(open ? { type: "panel.opened", panel: "sessions" } : { type: "panel.closed" });
          if (open) Store.refreshSessions();
        },
        children: h(ListIcon, {}),
      }),
      logsSupported
        ? h("button", {
            id: "logs-panel-trigger",
            type: "button",
            class: "icon-button",
            "aria-pressed": state.panel === "logs",
            "aria-controls": "logs-panel",
            "aria-label": "诊断日志",
            title: "诊断日志",
            onClick: () => {
              const open = state.panel !== "logs";
              Store.dispatch(open ? { type: "panel.opened", panel: "logs" } : { type: "panel.closed" });
              if (open) Store.refreshLogs();
            },
            children: h(LogsIcon, {}),
          })
        : null,
      h("button", {
        id: "device-panel-trigger",
        type: "button",
        class: "icon-button",
        "aria-pressed": state.panel === "device",
        "aria-controls": "device-panel",
        "aria-label": "设备与链路",
        title: "设备与链路",
        onClick: () => {
          const open = state.panel !== "device";
          Store.dispatch(open ? { type: "panel.opened", panel: "device" } : { type: "panel.closed" });
          if (open) {
            Store.refreshDevice();
            Store.refreshSafeSwap();
          }
        },
        children: h(DeviceIcon, {}),
      }),
    ],
  });
}

function isValidFirmwareRelease(v) {
  return (
    isPlainObject(v) &&
    isBoundedString(v.version, 1, 64) &&
    typeof v.commit === "string" &&
    /^[0-9a-f]{40}$/.test(v.commit)
  );
}

function isValidFirmwareTask(v) {
  return (
    isPlainObject(v) &&
    isBoundedString(v.id, 1, 128) &&
    (v.action === "update" || v.action === "rollback") &&
    typeof v.target_commit === "string" &&
    /^[0-9a-f]{40}$/.test(v.target_commit) &&
    ["accepted", "running", "succeeded", "failed"].includes(v.status) &&
    typeof v.message === "string" &&
    Number.isFinite(v.started_at) &&
    (v.finished_at === null || Number.isFinite(v.finished_at))
  );
}

async function firmwareApiFetch(subpath, init = {}) {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), 80000);
  try {
    const method = (init.method ?? "GET").toUpperCase();
    const response = await fetch(`${API_PREFIX}/firmware${subpath}`, {
      ...init,
      cache: "no-store",
      headers: buildHeaders("application/json", init.headers, method === "POST"),
      signal: controller.signal,
    });
    observeResponseAuditHeader(response);
    if (!response.ok) throw await parseErrorResponse(response);
    return await response.json();
  } finally {
    window.clearTimeout(timer);
  }
}

async function fetchFirmwareStatus(check = false, force = false) {
  const doc = await firmwareApiFetch(check ? `/check?force=${force}` : "");
  if (
    !doc ||
    doc.schema !== "miro-ego.firmware-status.v1" ||
    typeof doc.supported !== "boolean" ||
    typeof doc.has_update !== "boolean" ||
    !(doc.current === null || isValidFirmwareRelease(doc.current)) ||
    !(doc.previous === null || isValidFirmwareRelease(doc.previous)) ||
    !(
      doc.available === null ||
      (isValidFirmwareRelease(doc.available) &&
        (doc.available.release_notes === undefined || typeof doc.available.release_notes === "string"))
    ) ||
    !(doc.task === null || isValidFirmwareTask(doc.task)) ||
    !(doc.warning === null || typeof doc.warning === "string") ||
    !(doc.checked_at === null || Number.isFinite(doc.checked_at))
  ) {
    throw new DeviceApiError("设备更新状态无效", 502, "invalid_firmware_status");
  }
  return doc;
}

async function submitFirmwareAction(action, commit, key = newIdempotencyKey()) {
  const task = await firmwareApiFetch(`/${action}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": key,
    },
    body: JSON.stringify({ commit }),
  });
  if (!isValidFirmwareTask(task)) {
    throw new DeviceApiError("设备更新任务无效", 502, "invalid_firmware_task");
  }
  return task;
}

function FirmwarePanel({ state, onClose }) {
  const [fwStatus, setFwStatus] = useState(null);
  const [errMsg, setErrMsg] = useState(null);
  const [busy, setBusy] = useState(false);
  const [unsupported, setUnsupported] = useState(false);
  const [pendingConfirm, setPendingConfirm] = useState(null);
  const [reconnecting, setReconnecting] = useState(false);

  const mountedRef = useRef(true);
  const initialCommitRef = useRef(state.device?.build?.commit);
  const reqSeqRef = useRef(0);
  const mutatingRef = useRef(false);
  const foregroundBusyRef = useRef(false);
  const confirmBtnRef = useRef(null);
  const taskSectionRef = useRef(null);
  const [trackedTaskId, setTrackedTaskId] = useState(null);
  const [liveProbe, setLiveProbe] = useState(null);
  const idempotencySlotRef = useRef(null);

  const task = fwStatus?.task;
  const taskRunning = task?.status === "accepted" || task?.status === "running";
  const visibleTask =
    task &&
    (taskRunning ||
      task.id === trackedTaskId ||
      (task.status === "succeeded" &&
        task.target_commit === fwStatus?.current?.commit &&
        initialCommitRef.current &&
        task.target_commit !== initialCommitRef.current))
      ? task
      : null;

  const taskCompletedLive =
    visibleTask?.status === "succeeded" &&
    visibleTask.finished_at !== null &&
    visibleTask.target_commit === fwStatus?.current?.commit &&
    liveProbe?.commit === visibleTask.target_commit &&
    !reconnecting;
  const taskVerifying = visibleTask?.status === "succeeded" && !taskCompletedLive;
  const activeTransition = taskRunning || taskVerifying;
  const probeIdle = liveProbe?.idle === true;
  const currentVersion = fwStatus?.current?.version ?? state.device?.build?.package_version;

  async function pollFirmware(check = false, foreground = true) {
    if (mutatingRef.current || (!foreground && foregroundBusyRef.current)) return;
    const mySeq = ++reqSeqRef.current;
    if (foreground) {
      foregroundBusyRef.current = true;
      setBusy(true);
      setErrMsg(null);
    }
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), 8000);
    try {
      const [fwRes, devRes, capRes] = await Promise.allSettled([
        fetchFirmwareStatus(check, check),
        Api.getDevice(controller.signal),
        Api.getCaptureStatus(controller.signal),
      ]);
      if (!mountedRef.current || mySeq !== reqSeqRef.current) return;
      if (fwRes.status === "rejected") throw fwRes.reason;
      const doc = fwRes.value;
      setFwStatus(doc);
      setUnsupported(false);
      if (doc.task?.status === "accepted" || doc.task?.status === "running") {
        setTrackedTaskId(doc.task.id);
      }
      if (devRes.status === "fulfilled" && capRes.status === "fulfilled") {
        setLiveProbe({
          commit: devRes.value.build?.commit ?? null,
          idle: capRes.value.snapshot.device_state === "idle",
        });
        setReconnecting(false);
      } else {
        setLiveProbe(null);
        setReconnecting(true);
      }
    } catch (err) {
      if (mountedRef.current && mySeq === reqSeqRef.current) {
        setLiveProbe(null);
        setReconnecting(true);
        setUnsupported(err instanceof DeviceApiError && err.status === 404);
        if (foreground) setErrMsg(err instanceof Error ? err.message : "无法读取更新状态");
      }
    } finally {
      window.clearTimeout(timer);
      if (foreground) {
        foregroundBusyRef.current = false;
        if (mountedRef.current) setBusy(false);
      }
    }
  }

  useEffect(() => {
    mountedRef.current = true;
    pollFirmware();
    let inFlight = false;
    const interval = window.setInterval(async () => {
      if (!inFlight) {
        inFlight = true;
        try {
          await pollFirmware(false, false);
        } finally {
          inFlight = false;
        }
      }
    }, 2500);
    return () => {
      mountedRef.current = false;
      window.clearInterval(interval);
    };
  }, []);

  useEffect(() => {
    if (pendingConfirm) confirmBtnRef.current?.focus();
  }, [pendingConfirm]);

  useEffect(() => {
    if (visibleTask) taskSectionRef.current?.scrollIntoView({ block: "nearest" });
  }, [visibleTask?.id, taskCompletedLive, taskVerifying]);

  async function executeMutation() {
    if (!fwStatus || !pendingConfirm) return;
    const target = pendingConfirm;
    const existingSlot = idempotencySlotRef.current;
    const slot =
      existingSlot?.action === target.action && existingSlot.commit === target.commit
        ? existingSlot
        : { action: target.action, commit: target.commit, key: newIdempotencyKey() };
    idempotencySlotRef.current = slot;
    mutatingRef.current = true;
    reqSeqRef.current++;
    setBusy(true);
    setErrMsg(null);
    try {
      const acceptedTask = await submitFirmwareAction(slot.action, slot.commit, slot.key);
      if (mountedRef.current) {
        setTrackedTaskId(acceptedTask.id);
        setFwStatus({ ...fwStatus, task: acceptedTask });
        setPendingConfirm(null);
        idempotencySlotRef.current = null;
      }
    } catch (err) {
      if (mountedRef.current) {
        setErrMsg(err instanceof Error ? err.message : "请查询设备任务状态后重试");
      }
    } finally {
      mutatingRef.current = false;
      if (mountedRef.current) {
        setBusy(false);
        pollFirmware(false, false);
      }
    }
  }

  return h("aside", {
    class: "panel firmware-panel",
    "aria-label": "版本与更新",
    children: [
      h("div", {
        class: "panel-head",
        children: [
          h("span", { class: "panel-title", children: "版本与更新" }),
          h("button", {
            type: "button",
            class: "icon-button",
            "aria-label": "关闭版本面板",
            onClick: onClose,
            children: h(CloseIcon, {}),
          }),
        ],
      }),
      h("div", {
        class: "panel-body",
        children: [
          h("section", {
            class: "detail-section",
            children: [
              h("span", { class: "eyebrow", children: "当前版本" }),
              h("strong", {
                class: "firmware-version",
                children: currentVersion ? `v${currentVersion}` : "读取中…",
              }),
              h("p", {
                class: "panel-note",
                children: (fwStatus?.current?.commit ?? state.device?.build?.commit ?? "").slice(0, 12),
              }),
              h("button", {
                type: "button",
                class: "chip",
                disabled: busy || activeTransition || unsupported,
                onClick: () => pollFirmware(true),
                children: busy ? "正在处理…" : "检查更新",
              }),
            ],
          }),
          unsupported
            ? h("p", {
                class: "panel-note",
                children: "当前设备未启用网页固件更新控制器。",
              })
            : null,
          errMsg ? h("p", { role: "alert", class: "firmware-error", children: errMsg }) : null,
          fwStatus?.warning
            ? h("p", { role: "alert", class: "firmware-error", children: fwStatus.warning })
            : null,
          reconnecting
            ? h("p", {
                role: "status",
                children: "设备暂时断开，正在等待重新连接并查询任务结果…",
              })
            : null,
          visibleTask
            ? h("section", {
                ref: taskSectionRef,
                class: "detail-section",
                "aria-live": "polite",
                children: [
                  h("strong", {
                    children: taskCompletedLive
                      ? "操作完成"
                      : taskVerifying
                        ? "正在确认更新结果"
                        : visibleTask.status === "failed"
                          ? "操作失败"
                          : visibleTask.action === "rollback"
                            ? "正在回退"
                            : "正在更新",
                  }),
                  h("p", {
                    children: taskVerifying
                      ? "正在等待目标版本启动并恢复设备连接…"
                      : visibleTask.message,
                  }),
                  taskCompletedLive
                    ? h("button", {
                        type: "button",
                        class: "chip",
                        onClick: () => window.location.reload(),
                        children: "刷新页面",
                      })
                    : null,
                ],
              })
            : null,
          fwStatus?.has_update && fwStatus.available
            ? h("section", {
                class: "detail-section",
                children: [
                  h("strong", { children: ["可用版本 v", fwStatus.available.version] }),
                  h("pre", {
                    class: "firmware-notes",
                    children: fwStatus.available.release_notes || "此离线更新包未附带更新说明。",
                  }),
                  h("button", {
                    type: "button",
                    class: "chip",
                    disabled:
                      !probeIdle || activeTransition || busy || reconnecting || !!fwStatus.warning,
                    onClick: () => setPendingConfirm({ action: "update", ...fwStatus.available }),
                    children: "立即更新",
                  }),
                ],
              })
            : fwStatus?.checked_at && !fwStatus.warning && !activeTransition && !reconnecting
              ? h("p", { class: "detail-section", children: "已是最新版本" })
              : null,
          !probeIdle && !activeTransition
            ? h("p", {
                class: "panel-note detail-section",
                children: "设备连接正常且处于待机状态时才可执行更新或回退。",
              })
            : null,
          fwStatus?.previous
            ? h("section", {
                class: "detail-section",
                children: [
                  h("strong", { children: "版本回退" }),
                  h("p", {
                    children: [
                      "上一版本 v",
                      fwStatus.previous.version,
                      " - ",
                      fwStatus.previous.commit.slice(0, 12),
                    ],
                  }),
                  h("button", {
                    type: "button",
                    class: "chip",
                    disabled: !probeIdle || activeTransition || busy || reconnecting,
                    onClick: () => setPendingConfirm({ action: "rollback", ...fwStatus.previous }),
                    children: "回退上一版本",
                  }),
                ],
              })
            : null,
        ],
      }),
      pendingConfirm
        ? h("section", {
            class: "detail-section firmware-confirm",
            role: "group",
            "aria-label": "确认版本操作",
            children: [
              h("p", {
                children: [
                  pendingConfirm.action === "update" ? "确认更新到" : "确认回退到",
                  " v",
                  pendingConfirm.version,
                  "？服务将自动重启，已封存录制文件保留。",
                ],
              }),
              h("div", {
                class: "chips",
                children: [
                  h("button", {
                    ref: confirmBtnRef,
                    type: "button",
                    class: "chip",
                    disabled:
                      busy ||
                      !probeIdle ||
                      activeTransition ||
                      reconnecting ||
                      (pendingConfirm.action === "update" && !!fwStatus?.warning),
                    onClick: () => executeMutation(),
                    children: ["确认", pendingConfirm.action === "update" ? "更新" : "回退"],
                  }),
                  h("button", {
                    type: "button",
                    class: "chip",
                    disabled: busy,
                    onClick: () => setPendingConfirm(null),
                    children: "取消",
                  }),
                ],
              }),
            ],
          })
        : null,
    ],
  });
}

// ---------------------------------------------------------------------------
// Root Application Shell & Bootstrap
// ---------------------------------------------------------------------------

const CAPTURE_RECONCILE_INTERVAL_MS = 500;
const PANEL_REFRESH_INTERVAL_MS = 5000;

function resolveInitialInspectMode() {
  const saved = readPref(STORAGE_KEYS.inspectMode, "");
  if (["both", "left", "right"].includes(saved)) return saved;
  return window.matchMedia("(min-width: 860px)").matches ? "both" : "left";
}

function ConsoleApp() {
  const [firmwareOpen, setFirmwareOpen] = useState(false);
  const state = useConsoleState();
  const [previewActive, setPreviewActive] = useState(false);

  const startedRef = useRef(false);
  const captureSseRef = useRef(null);
  const networkSseRef = useRef(null);

  useEffect(() => {
    Store.dispatch({ type: "inspect.changed", mode: resolveInitialInspectMode() });
  }, []);

  useEffect(() => {
    if (startedRef.current) return;
    startedRef.current = true;
    let disposed = false;
    let reconcileTimer = null;
    let panelTimer = null;

    const tickCapture = () => {
      if (!document.hidden) Store.reconcileCapture();
    };
    const tickPanel = () => {
      if (!document.hidden) Store.refreshOpenPanel();
    };
    const onResume = () => {
      tickCapture();
      tickPanel();
    };
    const onVisibility = () => {
      if (!document.hidden) onResume();
    };

    const startTimers = () => {
      if (reconcileTimer === null) {
        reconcileTimer = window.setInterval(tickCapture, CAPTURE_RECONCILE_INTERVAL_MS);
        panelTimer = window.setInterval(tickPanel, PANEL_REFRESH_INTERVAL_MS);
        window.addEventListener("focus", onResume);
        window.addEventListener("online", onResume);
        document.addEventListener("visibilitychange", onVisibility);
      }
    };

    function startStreams() {
      const handleUnauthorized = (err) => {
        captureSseRef.current?.abort();
        captureSseRef.current = null;
        networkSseRef.current?.abort();
        networkSseRef.current = null;
        setPreviewActive(false);
        Store.dispatch({ type: "credentials.required" });
        Store.dispatch({ type: "connection.failed", error: toUiError(err) });
      };

      if (!captureSseRef.current) {
        const ctrl = new AbortController();
        captureSseRef.current = ctrl;
        streamCaptureEvents({
          signal: ctrl.signal,
          onEvent: Store.acceptCaptureEvent,
          onConnection: (conn) => Store.dispatch({ type: "connection.changed", connection: conn }),
          onUnauthorized: handleUnauthorized,
        });
      }

      if (!networkSseRef.current) {
        const ctrl = new AbortController();
        networkSseRef.current = ctrl;
        streamNetworkEvents({
          signal: ctrl.signal,
          onEvent: Store.acceptNetworkEvent,
          onUnauthorized: handleUnauthorized,
        });
      }

      if (Store.getState().device?.capabilities.preview) {
        setPreviewActive(true);
      }
    }

    Store.loadInitialState().then((ok) => {
      if (ok && !disposed) {
        startStreams();
        startTimers();
      }
    });

    const stopAll = () => {
      disposed = true;
      captureSseRef.current?.abort();
      networkSseRef.current?.abort();
      setPreviewActive(false);
    };

    window.addEventListener("pagehide", stopAll, { once: true });
    return () => {
      window.removeEventListener("pagehide", stopAll);
      window.removeEventListener("focus", onResume);
      window.removeEventListener("online", onResume);
      document.removeEventListener("visibilitychange", onVisibility);
      if (reconcileTimer !== null) window.clearInterval(reconcileTimer);
      if (panelTimer !== null) window.clearInterval(panelTimer);
      stopAll();
    };
  }, []);

  return h("div", {
    class: "console-shell",
    children: [
      h(LivePreviewStage, { state, active: previewActive }),
      h(TopBar, {
        state,
        onVersion: () => {
          Store.dispatch({ type: "panel.closed" });
          setFirmwareOpen((open) => !open);
        },
      }),
      h("div", {
        class: "stage-mid",
        children: [
          h(SafeSwapStageBanner, { state }),
          h(RetainedHazardBanner, { state }),
          h(AlertsBanner, { state }),
          h(ViewfinderOverlayControls, { state }),
        ],
      }),
      h(BottomCommandDeck, { state }),
      state.panel === "sessions" ? h(SessionsPanel, { state }) : null,
      state.panel === "device" ? h(DevicePanel, { state }) : null,
      state.panel === "logs" ? h(DiagnosticLogsPanel, { state }) : null,
      state.panel === "network" ? h(NetworkPanel, { state }) : null,
      firmwareOpen && state.panel === "none"
        ? h(FirmwarePanel, { state, onClose: () => setFirmwareOpen(false) })
        : null,
      state.needsCredentials
        ? h(CredentialDialog, {
            onConnected: () => {
              startedRef.current = false;
              window.location.reload();
            },
          })
        : null,
    ],
  });
}

const mountRoot = document.getElementById("miro-ego-root");
if (mountRoot) {
  renderRoot(h(ConsoleApp, {}), mountRoot);
}
