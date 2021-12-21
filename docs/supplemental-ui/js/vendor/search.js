/* eslint-env browser */
window.antoraLunr = (function (lunr) {
  var searchInput = document.getElementById('search-input')
  var searchResult = document.createElement('div')
  searchResult.classList.add('search-result-dropdown-menu')
  searchInput.parentNode.appendChild(searchResult)

  function highlightText (doc, position) {
    var hits = []
    var start = position[0]
    var length = position[1]

    var text = doc.text
    var highlightSpan = document.createElement('span')
    highlightSpan.classList.add('search-result-highlight')
    highlightSpan.innerText = text.substr(start, length)

    var end = start + length
    var textEnd = text.length - 1
    var contextOffset = 15
    var contextAfter = end + contextOffset > textEnd ? textEnd : end + contextOffset
    var contextBefore = start - contextOffset < 0 ? 0 : start - contextOffset
    if (start === 0 && end === textEnd) {
      hits.push(highlightSpan)
    } else if (start === 0) {
      hits.push(highlightSpan)
      hits.push(document.createTextNode(text.substr(end, contextAfter)))
    } else if (end === textEnd) {
      hits.push(document.createTextNode(text.substr(0, start)))
      hits.push(highlightSpan)
    } else {
      hits.push(document.createTextNode('...' + text.substr(contextBefore, start - contextBefore)))
      hits.push(highlightSpan)
      hits.push(document.createTextNode(text.substr(end, contextAfter - end) + '...'))
    }
    return hits
  }

  function highlightTitle (hash, doc, position) {
    var hits = []
    var start = position[0]
    var length = position[1]

    var highlightSpan = document.createElement('span')
    highlightSpan.classList.add('search-result-highlight')
    var title
    if (hash) {
      title = doc.titles.filter(function (item) {
        return item.id === hash
      })[0].text
    } else {
      title = doc.title
    }
    highlightSpan.innerText = title.substr(start, length)

    var end = start + length
    var titleEnd = title.length - 1
    if (start === 0 && end === titleEnd) {
      hits.push(highlightSpan)
    } else if (start === 0) {
      hits.push(highlightSpan)
      hits.push(document.createTextNode(title.substr(length, titleEnd)))
    } else if (end === titleEnd) {
      hits.push(document.createTextNode(title.substr(0, start)))
      hits.push(highlightSpan)
    } else {
      hits.push(document.createTextNode(title.substr(0, start)))
      hits.push(highlightSpan)
      hits.push(document.createTextNode(title.substr(end, titleEnd)))
    }
    return hits
  }

  function highlightHit (metadata, hash, doc) {
    var hits = []
    for (var token in metadata) {
      var fields = metadata[token]
      for (var field in fields) {
        var positions = fields[field]
        if (positions.position) {
          var position = positions.position[0] // only higlight the first match
          if (field === 'title') {
            hits = highlightTitle(hash, doc, position)
          } else if (field === 'text') {
            hits = highlightText(doc, position)
          }
        }
      }
    }
    return hits
  }

  function createSearchResult(result, store, searchResultDataset, query) {
    result.forEach(function (item) {
      var url = item.ref
      var hash
      if (url.includes('#')) {
        hash = url.substring(url.indexOf('#') + 1)
        url = url.replace('#' + hash, '')
      }
      var doc = store[url]
      var metadata = item.matchData.metadata
      var hits = highlightHit(metadata, hash, doc)
      searchResultDataset.appendChild(createSearchResultItem(doc, item, hits, query))
    })
  }

  function createSearchResultItem (doc, item, hits, query) {
    var documentTitle = document.createElement('div')
    documentTitle.classList.add('search-result-document-title')
    documentTitle.innerText = doc.title
    var documentHit = document.createElement('div')
    documentHit.classList.add('search-result-document-hit')
    var documentHitLink = document.createElement('a')
    var rootPath = window.antora.basePath

    var url = new URL(rootPath + item.ref, window.location.href)
    url.searchParams.set('q', query)

    documentHitLink.href = url.href
    documentHit.appendChild(documentHitLink)
    hits.forEach(function (hit) {
      documentHitLink.appendChild(hit)
    })
    var searchResultItem = document.createElement('div')
    searchResultItem.classList.add('search-result-item')
    searchResultItem.appendChild(documentTitle)
    searchResultItem.appendChild(documentHit)
    searchResultItem.addEventListener('mousedown', function (e) {
      e.preventDefault()
    })
    return searchResultItem
  }

  function createNoResult (text) {
    var searchResultItem = document.createElement('div')
    searchResultItem.classList.add('search-result-item')
    var documentHit = document.createElement('div')
    documentHit.classList.add('search-result-document-hit')
    var message = document.createElement('strong')
    message.innerText = 'No results found for query "' + text + '"'
    documentHit.appendChild(message)
    searchResultItem.appendChild(documentHit)
    return searchResultItem
  }

  function search (index, text) {
    // execute an exact match search
    var result = index.search(text)
    if (result.length > 0) {
      return result
    }
    // no result, use a begins with search
    result = index.search(text + '*')
    if (result.length > 0) {
      return result
    }
    // no result, use a contains search
    result = index.search('*' + text + '*')
    return result
  }

  function searchIndex (index, store, text) {
    // reset search result
    while (searchResult.firstChild) {
      searchResult.removeChild(searchResult.firstChild)
    }
    if (text.trim() === '') {
      return
    }
    var result = search(index, text)
    var searchResultDataset = document.createElement('div')
    searchResultDataset.classList.add('search-result-dataset')
    searchResult.appendChild(searchResultDataset)
    if (result.length > 0) {
      createSearchResult(result, store, searchResultDataset, text)
    } else {
      searchResultDataset.appendChild(createNoResult(text))
    }
  }

  function debounce (func, wait, immediate) {
    var timeout
    return function () {
      var context = this
      var args = arguments
      var later = function () {
        timeout = null
        if (!immediate) func.apply(context, args)
      }
      var callNow = immediate && !timeout
      clearTimeout(timeout)
      timeout = setTimeout(later, wait)
      if (callNow) func.apply(context, args)
    }
  }

  function init (data) {
    var index = Object.assign({index: lunr.Index.load(data.index), store: data.store})
    var searchFn = debounce(function () {
      searchIndex(index.index, index.store, searchInput.value)
    }, 100)
    searchInput.addEventListener('keydown', searchFn)

    // this is prevented in case of mousedown attached to SearchResultItem
    searchInput.addEventListener('blur', function (e) {
      while (searchResult.firstChild) {
        searchResult.removeChild(searchResult.firstChild)
      }
    })

    // Extract query from URL and highlite matches
    let params = new URLSearchParams(window.location.search.slice(1))
    let query = params.get('q')
    if (query !== null) {
      search(index.index, query)
        .filter(entry => entry.ref.split('#')[0] == window.location.pathname)
        .forEach(entry => {

          Object.keys(entry.matchData.metadata).forEach(function (term) {
            Object.keys(entry.matchData.metadata[term]).forEach(function (field) {
              let positions = entry.matchData.metadata[term][field].position
                .sort((a, b) => a[0] - b[0])
                .slice()
                
              let element = document.querySelector('article.doc')

              if (field == 'title') {
                let ref = entry.ref.split('#')
                
                let node = (ref[1] === undefined)
                  ? element.querySelector('h1') // Find the main title
                  : document.getElementById(ref[1]) // Find the sub-title

                if (node !== undefined) {
                  positions.forEach(match => {
                    // Define range of match in relation to current node
                    let range = document.createRange()
                    range.setStart(node.lastChild, match[0])
                    range.setEnd(node.lastChild, match[0] + match[1])

                    // Create marking element
                    let tag = document.createElement('mark')
                    tag.dataset.matchStart = match[0]
                    tag.dataset.matchLen = match[1]

                    // Insert marking element
                    range.surroundContents(tag)
                  })
                }

	            } else if (field == 'text') {
                // Walk the article but remove titles, navigation and toc
                walker = document.createTreeWalker(
                  element,
                  NodeFilter.SHOW_TEXT,
                  node => {
                    if (node.parentElement.matches('article.doc aside.toc')) return NodeFilter.FILTER_SKIP
                    if (node.parentElement.matches('article.doc aside.toc *')) return NodeFilter.FILTER_SKIP
                    if (node.parentElement.matches('article.doc nav.pagination')) return NodeFilter.FILTER_SKIP
                    if (node.parentElement.matches('article.doc nav.pagination *')) return NodeFilter.FILTER_SKIP
                    if (node.parentElement.matches('article.doc h1')) return NodeFilter.FILTER_SKIP
                    if (node.parentElement.matches('article.doc h1 *')) return NodeFilter.FILTER_SKIP
                    if (node.parentElement.matches('article.doc h2')) return NodeFilter.FILTER_SKIP
                    if (node.parentElement.matches('article.doc h2 *')) return NodeFilter.FILTER_SKIP
                    if (node.parentElement.matches('article.doc h3')) return NodeFilter.FILTER_SKIP
                    if (node.parentElement.matches('article.doc h3 *')) return NodeFilter.FILTER_SKIP
                    if (node.parentElement.matches('article.doc h4')) return NodeFilter.FILTER_SKIP
                    if (node.parentElement.matches('article.doc h4 *')) return NodeFilter.FILTER_SKIP
                    if (node.parentElement.matches('article.doc h5')) return NodeFilter.FILTER_SKIP
                    if (node.parentElement.matches('article.doc h5 *')) return NodeFilter.FILTER_SKIP
                    if (node.parentElement.matches('article.doc h6')) return NodeFilter.FILTER_SKIP
                    if (node.parentElement.matches('article.doc h6 *')) return NodeFilter.FILTER_SKIP
                    return NodeFilter.FILTER_ACCEPT
                  },
                )
                
                var index = -1 // Ignore first encountered blank
                var blank = true // Start in blank mode to trim of leading blank
                  
                var match = positions.shift()

                while (node = walker.nextNode()) {
                  if (match === undefined) break

                  var text = node.textContent

                  if (text == '') {
                    continue
                  }

                  // If node is blank, remeber and skip
                  if (/^[\t\n\r ]$/.test(text)) {
                    blank = true
                    continue
                  }

                  // If node does not starts with blank but a blank has been encountered before, insert it
                  if (blank && !/^[\t\n\r ]/.test(text)) {
                    index += 1
                  }

                  // Reset blank status
                  blank = false

                  // Check if match is part of this node
                  if (match[0] < index + text.length) {

                    // Define range of match in relation to current node
                    let range = document.createRange()
                    range.setStart(node, match[0] - index)
                    range.setEnd(node, match[0] + match[1] - index)
  
                    // Create marking element
                    let tag = document.createElement('mark')
                    tag.dataset.matchStart = match[0]
                    tag.dataset.matchLen = match[1]
  
                    // Insert marking element
                    range.surroundContents(tag)

                    // Move index to end of match, which is the same as the end of the marking element
                    index = match[0] + match[1] 
                    
                    // Pop next node from walker - which is the element we've just added
                    walker.nextNode()

                    // Move to next match
                    match = positions.shift()

                    continue
                  }

                  // Remember if node ends with blank
                  if (/[\t\n\r ]$/.test(text)) {
                    blank = true
                    text = text.trimEnd()
                  }

                  index += text.length
                }
	            }
            })
          })
        })
    }
  }

  return {
    init: init,
  }
})(window.lunr)
