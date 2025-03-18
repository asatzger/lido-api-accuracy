// Function to check if element is in viewport
function isInViewport(element) {
    const rect = element.getBoundingClientRect();
    return (
        rect.top <= (window.innerHeight || document.documentElement.clientHeight) &&
        rect.bottom >= 0
    );
}

// Load chart when in viewport
function loadChartWhenVisible(elemId, chartPath) {
    const element = document.getElementById(elemId);
    
    if (isInViewport(element)) {
        // Show loading indicator
        element.innerHTML = '<div class="loading">Loading chart...</div>';
        
        // Add cache-busting parameter to avoid caching issues
        const cacheBuster = '?t=' + new Date().getTime();
        
        // Ensure correct content-type handling
        const options = {
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            },
            credentials: 'omit'  // Skip credentials for cross-origin requests
        };
        
        // Identify file extension to determine handling
        const isJavaScript = chartPath.endsWith('.js');
        
        if (isJavaScript) {
            // For JavaScript files, load via script tag
            const script = document.createElement('script');
            script.src = chartPath + cacheBuster;
            script.onload = () => {
                // The script will define its own loading logic
                console.log(`JavaScript chart loaded: ${chartPath}`);
            };
            script.onerror = (error) => {
                console.error(`Error loading JavaScript chart: ${chartPath}`, error);
                element.innerHTML = `<p style="color:red">Error loading chart script: ${chartPath}</p>`;
            };
            document.head.appendChild(script);
        } else {
            // For JSON files, fetch the data
            console.log('Loading chart from:', chartPath + cacheBuster);
            
            fetch(chartPath + cacheBuster, options)
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error ${response.status}: ${response.statusText}`);
                    }
                    const contentType = response.headers.get('content-type') || '';
                    if (!contentType.includes('application/json') && !contentType.includes('text/plain')) {
                        console.warn(`Unexpected content type: ${contentType}`);
                    }
                    return response.text();
                })
                .then(text => {
                    try {
                        // Try to parse JSON from the text response
                        const spec = JSON.parse(text);
                        vegaEmbed('#' + elemId, spec, {
                            mode: "vega-lite", 
                            actions: false,
                            renderer: "svg",
                            logLevel: 'info'
                        }).catch(error => {
                            console.error('Error rendering chart', elemId, error);
                            element.innerHTML = 
                                '<p style="color:red">Error rendering chart: ' + error.message + '</p>';
                        });
                    } catch (e) {
                        console.error('JSON parse error:', e, 'Response starts with:', text.substring(0, 50));
                        if (text.trim().startsWith('<')) {
                            // Received HTML instead of JSON - likely a 404 page
                            element.innerHTML = 
                                '<p style="color:red">Error: Received HTML instead of JSON. File may be missing or path may be incorrect.</p>';
                        } else {
                            element.innerHTML = 
                                '<p style="color:red">Error parsing chart data: ' + e.message + '</p>';
                        }
                    }
                })
                .catch(error => {
                    console.error('Network error loading chart', elemId, error);
                    element.innerHTML = 
                        '<p style="color:red">Error loading chart data: ' + error.message + '</p>';
                });
        }
        
        // Remove from charts to load array
        chartsToLoad = chartsToLoad.filter(chart => chart.id !== elemId);
        
        return true;
    }
    
    return false;
}

// Initialize array of charts to load
let chartsToLoad = [];

// Check visible charts on load and scroll
function checkVisibleCharts() {
    // Make a copy of the array since we'll be modifying it
    const currentCharts = [...chartsToLoad];
    
    // Try to load each chart if visible
    currentCharts.forEach(chart => {
        loadChartWhenVisible(chart.id, chart.path);
    });
    
    // If all charts loaded, remove scroll listener
    if (chartsToLoad.length === 0) {
        window.removeEventListener('scroll', checkVisibleCharts);
    }
}

// Initialize charts on page load
document.addEventListener('DOMContentLoaded', function() {
    // Find all chart containers
    document.querySelectorAll('[data-chart]').forEach(container => {
        // Add to charts to load
        chartsToLoad.push({
            id: container.id,
            path: container.getAttribute('data-chart')
        });
    });
    
    // Initial check for visible charts
    checkVisibleCharts();
    
    // Add scroll listener to check for visible charts
    window.addEventListener('scroll', checkVisibleCharts);
});