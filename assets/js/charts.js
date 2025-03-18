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
        
        // Fetch and render chart
        fetch(chartPath)
            .then(response => response.json())
            .then(spec => {
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
            })
            .catch(error => {
                console.error('Error loading chart', elemId, error);
                element.innerHTML = 
                    '<p style="color:red">Error loading chart data: ' + error.message + '</p>';
            });
        
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