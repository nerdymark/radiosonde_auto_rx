// Scan Result Chart Setup

var scan_chart_spectra;
var scan_chart_peaks;
var scan_chart_threshold;
var scan_chart_obj;
var scan_chart_latest_timestamp;
var scan_chart_last_drawn = "none";
// Current x-axis zoom domain ([min_mhz, max_mhz], null = unzoomed). Saved on
// every zoom/pan so a redraw with fresh scan data can re-apply it — otherwise
// each c3 .load() snaps the chart back to the full span.
var scan_chart_zoom_domain = null;

function setup_scan_chart(){
	scan_chart_spectra = {
	    xs: {
	        'Spectra': 'x_spectra'
	    },
	    columns: [
	        ['x_spectra',autorx_config.min_freq, autorx_config.max_freq],
	        ['Spectra',0,0]
	    ],
		colors: {
			Spectra: "#2f77b4"
		},
	    type:'line'
	};

	scan_chart_peaks = {
	    xs: {
	        'Peaks': 'x_peaks'
	    },
	    columns: [
	        ['x_peaks',0],
	        ['Peaks',0]
	    ],
		colors: {
			Peaks: "#ff7f0e"
		},
	    type:'scatter'
	};

	scan_chart_threshold = {
	    xs:{
	        'Threshold': 'x_thresh'
	    },
	    columns:[
	        ['x_thresh',autorx_config.min_freq, autorx_config.max_freq],
	        ['Threshold',autorx_config.snr_threshold,autorx_config.snr_threshold]
	    ],
		colors: {
			Threshold: "#2ca02c"
		},
	    type:'line'
	};

	scan_chart_obj = c3.generate({
	    bindto: '#scan_chart',
	    data: scan_chart_spectra,
        transition: {
            duration: 0
        },
        tooltip: {
            format: {
                title: function (d) { return (Math.round(d * 1000) / 1000) + " MHz"; },
                value: function (value) { return value + " dB"; }
            }
        },
        zoom: {
            enabled: true,      // mouse-wheel / pinch zoom + drag to pan
            rescale: true,      // rescale the y axis to the visible data
            onzoomend: function (domain) { scan_chart_zoom_domain = domain; }
        },
	    axis:{
	        x:{
	            tick:{
                    // Auto ticks (rather than a fixed value list) so labels
                    // stay useful when zoomed in to a slice of the band.
                    count: 13,
                    format: function (x) { return String(+x.toFixed(3)); },
                    outer: false
	            },
	            label:"Frequency (MHz)"
	        },
	        y:{
	            label:"Power (dB - Uncalibrated)"
	        }
	    },
		size: {
			height: 200
		},
		legend: {
			show: false
		},
	    point:{r:10}
	});

	// Double-click resets the zoom to the full scan span.
	document.getElementById('scan_chart').addEventListener('dblclick', function () {
		scan_chart_zoom_domain = null;
		scan_chart_obj.unzoom();
	});
}

function redraw_scan_chart(){
	// Plot the updated data.
	if(!scan_chart_latest_timestamp || scan_chart_last_drawn === scan_chart_latest_timestamp){
		// No need to re-draw.
		//console.log("No need to re-draw.");
		return;
	}
	scan_chart_obj.load(scan_chart_spectra);
	scan_chart_obj.load(scan_chart_peaks);
	scan_chart_obj.load(scan_chart_threshold);

	// Loading data resets the view; restore the user's zoom window.
	if (scan_chart_zoom_domain) {
		scan_chart_obj.zoom(scan_chart_zoom_domain);
	}

	scan_chart_last_drawn = scan_chart_latest_timestamp;

	//console.log("Scan plot redraw - " + scan_chart_latest_timestamp);

	// Run dark mode check again to solve render issues.
	var z = getCookie('dark');
		if (z == 'true') {
			changeTheme(true);
		} else if (z == 'false') {
			changeTheme(false);
		} else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
			changeTheme(true);
		} else {
			changeTheme(false);
		}

	// Show the latest scan time.
	if (getCookie('UTC') == 'false') {
		var date = new Date(scan_chart_latest_timestamp);
		var date_converted = date.toLocaleString(window.navigator.language,{hourCycle:'h23', year:"numeric", month:"2-digit", day:'2-digit', hour:'2-digit',minute:'2-digit', second:'2-digit'});
	} else {
		var date_converted = scan_chart_latest_timestamp.slice(0, 19).replace("T", " ") + ' UTC'
	}
	$('#scan_results').html('<b>Latest Scan:</b> ' + date_converted);
}
