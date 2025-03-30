/**
     * Reddit Post Viewer
     * 
     * A client-side script that handles dynamic loading and display of Reddit posts
     * with infinite scrolling and optimized DOM management.
     */
(function () {
    // Configuration
    const CONFIG = {
        chunkSize: 3,                 // Number of chunks to keep in view
        scrollThreshold: 300,         // Pixels from edge to trigger loading
        initialChunks: [1],           // Start with just chunk 1 to avoid ordering issues
        preloadThreshold: 0.7,        // Percentage of page scroll to trigger preloading
        fadeInDuration: 300,          // Duration of fade-in animation in ms
        maxRetries: 3,                // Maximum number of retries for failed requests
        retryDelay: 1000,             // Delay between retries in ms
        cacheExpiry: 1 * 60 * 1000    // Cache expiry time in ms (1 minutes)
    };

    // State management
    const state = {
        isLoading: false,
        loadedChunks: [],
        totalChunks: 0,
        nextChunkPromise: null,       // Store promise for next chunk preloading
        prevChunkPromise: null,       // Store promise for previous chunk preloading
        cache: new Map(),             // Cache for loaded chunks
        lastScrollPosition: 0,        // Track scroll direction
        scrollingDirection: null,     // 'up' or 'down'
        requestPool: new Set(),       // Track ongoing requests
        initialized: false            // Track whether initialization is complete
    };

    // DOM elements
    let gallery, loadingIndicator;

    /**
     * Initialize the application
     */
    function init() {
        gallery = document.getElementById("gallery");
        loadingIndicator = document.getElementById("loading");

        // Add initial skeleton loaders
        addSkeletonLoaders(3);

        // Fetch the total number of chunks
        fetchTotalChunks()
            .then(() => {
                // Load first chunk and wait for it to complete
                return loadChunk(CONFIG.initialChunks[0]);
            })
            .then(() => {
                // Remove skeleton loaders after content is loaded
                removeSkeletonLoaders();
                state.initialized = true;

                // Preload next chunk after initial load
                preloadNextChunk();
            })
            .catch(error => {
                console.error("Initialization error:", error);
                removeSkeletonLoaders();
                gallery.innerHTML = '<div class="post"><div class="post-text">Failed to load content. Please refresh the page.</div></div>';
            });

        // Set up scroll event listener with passive option for performance
        window.addEventListener("scroll", handleScroll, { passive: true });

        // Track scroll direction
        window.addEventListener("scroll", () => {
            const currentScrollPosition = window.scrollY;
            state.scrollingDirection = currentScrollPosition > state.lastScrollPosition ? 'down' : 'up';
            state.lastScrollPosition = currentScrollPosition;
        }, { passive: true });
    }

    /**
     * Add skeleton loaders to the gallery
     */
    function addSkeletonLoaders(count) {
        const skeletons = Array(count).fill(0).map(() =>
            `<div class="skeleton-post"></div>`
        ).join('');

        gallery.insertAdjacentHTML('beforeend', skeletons);
    }

    /**
     * Remove all skeleton loaders
     */
    function removeSkeletonLoaders() {
        document.querySelectorAll('.skeleton-post').forEach(el => el.remove());
    }

    /**
     * Handle scroll events to implement infinite scrolling with preloading
     */
    function handleScroll() {
        // Don't process scroll events until initialization is complete
        if (!state.initialized) return;

        // Get scroll position metrics
        const scrollTop = window.scrollY;
        const windowHeight = window.innerHeight;
        const documentHeight = document.documentElement.scrollHeight;
        const scrollPercentage = (scrollTop + windowHeight) / documentHeight;

        // Check if near the bottom or top
        const isNearBottom = scrollTop + windowHeight >= documentHeight - CONFIG.scrollThreshold;
        const isNearTop = scrollTop <= CONFIG.scrollThreshold;

        // Show loading indicator if actively loading
        if (state.isLoading) {
            showLoadingIndicator();
        }

        // Preload next chunk when 70% down the page
        if (scrollPercentage > CONFIG.preloadThreshold && state.scrollingDirection === 'down') {
            preloadNextChunk();
        }

        // Preload previous chunk when near the top 30% of the page
        if (scrollPercentage < (1 - CONFIG.preloadThreshold) && state.scrollingDirection === 'up') {
            preloadPrevChunk();
        }

        // Load and render when we're very close to edges
        if (isNearBottom) {
            handleBottomScroll();
        } else if (isNearTop) {
            handleTopScroll();
        } else {
            hideLoadingIndicator();
        }
    }

    /**
     * Preload the next chunk of posts without rendering
     */
    function preloadNextChunk() {
        if (state.nextChunkPromise || state.loadedChunks.length === 0) return;

        const nextChunk = state.loadedChunks[state.loadedChunks.length - 1] + 1;

        if (nextChunk <= state.totalChunks && !state.cache.has(`chunk-${nextChunk}`)) {
            state.nextChunkPromise = fetchChunk(nextChunk).then(data => {
                if (data && data.posts) {
                    // Cache the data
                    cacheChunkData(nextChunk, data);
                }
                state.nextChunkPromise = null;
                return data;
            }).catch(error => {
                console.error(`Error preloading chunk ${nextChunk}:`, error);
                state.nextChunkPromise = null;
            });
        }
    }

    /**
     * Preload the previous chunk of posts without rendering
     */
    function preloadPrevChunk() {
        if (state.prevChunkPromise || state.loadedChunks.length === 0) return;

        const prevChunk = state.loadedChunks[0] - 1;

        if (prevChunk >= 1 && !state.cache.has(`chunk-${prevChunk}`)) {
            state.prevChunkPromise = fetchChunk(prevChunk).then(data => {
                if (data && data.posts) {
                    // Cache the data
                    cacheChunkData(prevChunk, data);
                }
                state.prevChunkPromise = null;
                return data;
            }).catch(error => {
                console.error(`Error preloading chunk ${prevChunk}:`, error);
                state.prevChunkPromise = null;
            });
        }
    }

    /**
     * Cache chunk data with expiry
     */
    function cacheChunkData(chunk, data) {
        const now = Date.now();
        state.cache.set(`chunk-${chunk}`, {
            data: data,
            timestamp: now,
            expiry: now + CONFIG.cacheExpiry
        });

        // Clean old cache entries
        cleanCache();
    }

    /**
     * Clean expired cache entries
     */
    function cleanCache() {
        const now = Date.now();
        for (const [key, value] of state.cache.entries()) {
            if (value.expiry < now) {
                state.cache.delete(key);
            }
        }
    }

    /**
     * Get cached chunk data if available and not expired
     */
    function getCachedChunkData(chunk) {
        const cacheKey = `chunk-${chunk}`;
        const cachedItem = state.cache.get(cacheKey);

        if (cachedItem && cachedItem.expiry > Date.now()) {
            return cachedItem.data;
        }

        return null;
    }

    /**
     * Handle scrolling near the bottom of the page
     */
    function handleBottomScroll() {
        if (state.isLoading || state.loadedChunks.length === 0) return;

        const lastChunk = state.loadedChunks[state.loadedChunks.length - 1];
        const nextChunk = lastChunk + 1;

        if (nextChunk <= state.totalChunks) {
            loadChunk(nextChunk).then(() => {
                // Remove oldest chunks if we're keeping more than the limit
                if (state.loadedChunks.length > CONFIG.chunkSize) {
                    const removeCount = state.loadedChunks.length - CONFIG.chunkSize;
                    for (let i = 0; i < removeCount; i++) {
                        removeChunkFromDOM(state.loadedChunks[i]);
                    }
                    state.loadedChunks = state.loadedChunks.slice(removeCount);
                }

                hideLoadingIndicator();
            });
        }
    }

    /**
     * Handle scrolling near the top of the page
     */
    function handleTopScroll() {
        if (state.isLoading || state.loadedChunks.length === 0) return;

        const firstChunk = state.loadedChunks[0];
        const prevChunk = firstChunk - 1;

        if (prevChunk >= 1) {
            loadChunk(prevChunk, true).then(() => {
                // Remove newest chunks if we're keeping more than the limit
                if (state.loadedChunks.length > CONFIG.chunkSize) {
                    const removeCount = state.loadedChunks.length - CONFIG.chunkSize;
                    for (let i = 0; i < removeCount; i++) {
                        const chunkToRemove = state.loadedChunks[state.loadedChunks.length - 1 - i];
                        removeChunkFromDOM(chunkToRemove);
                    }
                    state.loadedChunks = state.loadedChunks.slice(0, CONFIG.chunkSize);
                }

                hideLoadingIndicator();
            });
        }
    }

    /**
     * Remove a chunk's posts from the DOM
     */
    function removeChunkFromDOM(chunkId) {
        document.querySelectorAll(`.post[data-chunk="${chunkId}"]`).forEach(post => {
            post.remove();
        });
    }

    /**
     * Load a specific chunk and append or prepend it to the gallery
     */
    function loadChunk(chunkId, prepend = false) {
        if (state.isLoading || state.loadedChunks.includes(chunkId)) {
            return Promise.resolve();
        }

        state.isLoading = true;
        showLoadingIndicator();

        return getChunkData(chunkId).then(data => {
            if (data && data.posts && data.posts.length > 0) {
                renderChunk(data, prepend);

                if (prepend) {
                    state.loadedChunks.unshift(chunkId);

                    // Maintain scroll position when prepending
                    const firstHeight = gallery.firstChild.offsetHeight;
                    window.scrollBy(0, firstHeight);
                } else {
                    state.loadedChunks.push(chunkId);
                }
            }

            state.isLoading = false;
            hideLoadingIndicator();
            return data;
        }).catch(error => {
            console.error(`Error loading chunk ${chunkId}:`, error);
            state.isLoading = false;
            hideLoadingIndicator();
            throw error;
        });
    }

    /**
     * Get chunk data either from cache or by fetching
     */
    function getChunkData(chunkId) {
        const cachedData = getCachedChunkData(chunkId);
        if (cachedData) {
            return Promise.resolve(cachedData);
        }

        return fetchChunk(chunkId);
    }

    /**
     * Fetch a chunk of posts from the server with retry logic
     */
    function fetchChunk(chunkId, retryCount = 0) {
        const requestId = `chunk-${chunkId}-${Date.now()}`;
        state.requestPool.add(requestId);

        return fetch(`/api/chunks/${chunkId}`)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return response.json();
            })
            .then(data => {
                state.requestPool.delete(requestId);
                // Make sure the chunk has the correct ID
                if (data && !data.id) {
                    data.id = chunkId;
                }
                cacheChunkData(chunkId, data);
                return data;
            })
            .catch(error => {
                state.requestPool.delete(requestId);
                console.error(`Error fetching chunk ${chunkId}:`, error);

                if (retryCount < CONFIG.maxRetries) {
                    console.log(`Retrying chunk ${chunkId} (attempt ${retryCount + 1}/${CONFIG.maxRetries})...`);
                    return new Promise(resolve => {
                        setTimeout(() => {
                            resolve(fetchChunk(chunkId, retryCount + 1));
                        }, CONFIG.retryDelay);
                    });
                }

                throw error;
            });
    }

    /**
     * Fetch the total number of available chunks
     */
    function fetchTotalChunks() {
        return fetch('/api/chunks/count')
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return response.json();
            })
            .then(data => {
                state.totalChunks = data.count;
                return data.count;
            })
            .catch(error => {
                console.error('Error fetching total chunks:', error);
                // Default to a reasonable number if we can't get the actual count
                state.totalChunks = 100;
                return 100;
            });
    }

    /**
     * Render a chunk of posts to the gallery
     */
    function renderChunk(chunkData, prepend = false) {
        if (!chunkData || !chunkData.posts || !chunkData.posts.length) {
            return;
        }

        const fragment = document.createDocumentFragment();
        const chunkId = chunkData.id;

        chunkData.posts.forEach(post => {
            const postElement = createPostElement(post, chunkId);
            fragment.appendChild(postElement);
        });

        if (prepend && gallery.firstChild) {
            gallery.insertBefore(fragment, gallery.firstChild);
        } else {
            gallery.appendChild(fragment);
        }

        // Apply fade-in animation to new posts
        setTimeout(() => {
            document.querySelectorAll('.post.fade-in').forEach(post => {
                post.classList.remove('fade-in');
            });
        }, 10);
    }

    /**
     * Create a post element
     */
    function createPostElement(post, chunkId) {
        const postDiv = document.createElement('div');
        postDiv.className = 'post fade-in';
        postDiv.dataset.chunk = chunkId;
        postDiv.dataset.postId = post.id;

        // Add post title
        const title = document.createElement('h2');
        title.className = 'post-title';
        title.textContent = post.title;
        postDiv.appendChild(title);

        // Add created time right after the title
        if (post.created_time) {
            const createdTimeDiv = document.createElement('div');
            createdTimeDiv.className = 'post-time';
            createdTimeDiv.textContent = post.created_time;
            postDiv.appendChild(createdTimeDiv);
        }

        // Add image if available
        if (post.image) {
            const img = document.createElement('img');
            img.className = 'post-image';
            img.src = `/images/${post.image}`;
            img.alt = post.title;
            img.loading = 'lazy'; // Use native lazy loading
            postDiv.appendChild(img);
        }

        // Add text content if available
        if (post.text) {
            const text = document.createElement('div');
            text.className = 'post-text';
            text.textContent = post.text;
            postDiv.appendChild(text);
        }

        // Add comments toggle
        const commentsToggle = document.createElement('div');
        commentsToggle.className = 'comments-toggle';
        commentsToggle.innerHTML = `
    <span class="arrow">▶</span>
    <span>Comments (${post.commentCount || 0})</span>
`;
        commentsToggle.addEventListener('click', () => toggleComments(postDiv, post.id));
        postDiv.appendChild(commentsToggle);

        // Add comments section (empty initially)
        const commentsSection = document.createElement('div');
        commentsSection.className = 'comments-section';
        commentsSection.dataset.loaded = 'false';
        postDiv.appendChild(commentsSection);

        return postDiv;
    }

    /**
     * Toggle comments visibility and load them if needed
     */
    function toggleComments(postElement, postId) {
        const commentsSection = postElement.querySelector('.comments-section');
        const arrowElement = postElement.querySelector('.arrow');

        // Toggle visibility
        if (commentsSection.style.display === 'block') {
            commentsSection.style.display = 'none';
            arrowElement.classList.remove('active');
            return;
        }

        commentsSection.style.display = 'block';
        arrowElement.classList.add('active');

        // Load comments if not already loaded
        if (commentsSection.dataset.loaded === 'false') {
            commentsSection.innerHTML = '<div class="comment"><p>Loading comments...</p></div>';

            fetchComments(postId)
                .then(comments => {
                    renderComments(commentsSection, comments);
                    commentsSection.dataset.loaded = 'true';
                })
                .catch(error => {
                    console.error(`Error loading comments for post ${postId}:`, error);
                    commentsSection.innerHTML = '<div class="comment"><p>Failed to load comments. Please try again.</p></div>';
                });
        }
    }

    /**
     * Fetch comments for a post
     */
    function fetchComments(postId) {
        return fetch(`/api/comments/${postId}`)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return response.json();
            })
            .then(data => data.comments || []);
    }

    /**
     * Render comments and their replies
     */
    function renderComments(container, comments) {
        container.innerHTML = '';

        if (!comments || comments.length === 0) {
            container.innerHTML = '<div class="comment"><p>No comments yet.</p></div>';
            return;
        }

        comments.forEach(comment => {
            const commentElement = createCommentElement(comment);
            container.appendChild(commentElement);
        });
    }

    /**
     * Create a comment element with nested replies
     */
    function createCommentElement(comment, depth = 1) {
        const commentDiv = document.createElement('div');
        commentDiv.className = 'comment';

        // Comment text with indents and -
        const commentText = document.createElement('p');
        const depthIndicator = '-'.repeat(depth) + ' ';
        commentText.textContent = depthIndicator + comment.text;
        commentDiv.appendChild(commentText);

        // Comment image if available
        if (comment.image) {
            const img = document.createElement('img');
            img.src = `/images/${comment.image}`;
            img.alt = 'Comment attachment';
            img.loading = 'lazy';
            commentDiv.appendChild(img);
        }

        // Add replies if any
        if (comment.replies && comment.replies.length > 0) {
            const repliesDiv = document.createElement('div');
            repliesDiv.className = 'replies';

            comment.replies.forEach(reply => {
                const replyElement = createCommentElement(reply, depth + 1); // Recursive
                repliesDiv.appendChild(replyElement);
            });

            commentDiv.appendChild(repliesDiv);
        }

        return commentDiv;
    }

    /**
     * Show loading indicator
     */
    function showLoadingIndicator() {
        loadingIndicator.classList.add('visible');
    }

    /**
     * Hide loading indicator
     */
    function hideLoadingIndicator() {
        loadingIndicator.classList.remove('visible');
    }

    // Initialize when the DOM is fully loaded
    document.addEventListener('DOMContentLoaded', init);
})();
