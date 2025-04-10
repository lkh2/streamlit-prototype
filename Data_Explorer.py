import time
import streamlit as st
import streamlit.components.v1 as components
import tempfile, os
import json
import polars as pl
import datetime
import math
import html

# --- Constants ---
PAGE_SIZE = 10

st.set_page_config(
    layout="wide",
    page_icon="📊",
    page_title="Data Explorer",
    initial_sidebar_state="collapsed"
)

st.markdown(
    """
    <style>
        [data-testid="stAppViewContainer"] {
            background: linear-gradient(180deg, #2A5D4E 0%, #65897F 50%, #2A5D4E 100%);
        }
        [data-testid="stHeader"] {
            background: transparent;
        }
    </style>
    """,
    unsafe_allow_html=True
)


# --- Component Generation (Simplified) ---
# We will pass data directly to the component instance now
# No need for the complex closure structure if we aren't dynamically generating JS
# based on data *within* the component generation itself.
# Let's keep generate_component for now, but note its role changes.
def generate_component(name, template="", script=""):
    dir = f"{tempfile.gettempdir()}/{name}"
    if not os.path.isdir(dir): os.mkdir(dir)
    fname = f'{dir}/index.html'
    with open(fname, 'w') as f:
        f.write(f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <link href='https://fonts.googleapis.com/css?family=Poppins' rel='stylesheet'>
                <link href='https://fonts.googleapis.com/css?family=Playfair Display' rel='stylesheet'>
                <meta charset="UTF-8" />
                <title>{name}</title>
                <script>
                    function sendMessageToStreamlitClient(type, data) {{
                        const outData = Object.assign({{
                            isStreamlitMessage: true,
                            type: type,
                        }}, data);
                        window.parent.postMessage(outData, "*");
                    }}

                    const Streamlit = {{
                        setComponentReady: function() {{
                            sendMessageToStreamlitClient("streamlit:componentReady", {{apiVersion: 1}});
                        }},
                        setFrameHeight: function(height) {{
                            sendMessageToStreamlitClient("streamlit:setFrameHeight", {{height: height}});
                        }},
                        setComponentValue: function(value) {{
                            sendMessageToStreamlitClient("streamlit:setComponentValue", {{value: value}});
                        }},
                        RENDER_EVENT: "streamlit:render",
                        events: {{
                            addEventListener: function(type, callback) {{
                                window.addEventListener("message", function(event) {{
                                    if (event.data.type === type) {{
                                        event.detail = event.data
                                        callback(event);
                                    }}
                                }});
                            }}
                        }}
                    }}
                </script>
                {template}
            </head>
            <body>
                <div id="component-root"></div>
            </body>
            <script>
                {script}
            </script>
            </html>
        """)

    _component_func = components.declare_component(name, path=str(dir))

    def component_wrapper(component_data, key=None, default=None):
        component_value = _component_func(component_data=component_data, key=key, default=default)
        return component_value
    return component_wrapper


# --- Data Loading and Metadata ---
parquet_source_path = "data.parquet"
filter_metadata_path = "filter_metadata.json"

# Default structure (remains the same)
filter_options = {
    'categories': ['All Categories'],
    'countries': ['All Countries'],
    'states': ['All States'],
    'date_ranges': [
        'All Time', 'Last Month', 'Last 6 Months', 'Last Year',
        'Last 5 Years', 'Last 10 Years'
    ]
}
category_subcategory_map = {'All Categories': ['All Subcategories']}
min_max_values = {
    'pledged': {'min': 0, 'max': 1000},
    'goal': {'min': 0, 'max': 10000},
    'raised': {'min': 0, 'max': 500}
}

# Load metadata (remains largely the same, maybe add more robust defaults)
if not os.path.exists(filter_metadata_path):
    st.error(f"Filter metadata file not found at '{filter_metadata_path}'. Please run `database_download.py` first.")
    st.stop()
else:
    try:
        with open(filter_metadata_path, 'r', encoding='utf-8') as f:
            loaded_metadata = json.load(f)

        # Validate and load categories, countries, states
        filter_options['categories'] = loaded_metadata.get('categories') or ['All Categories']
        filter_options['countries'] = loaded_metadata.get('countries') or ['All Countries']
        filter_options['states'] = loaded_metadata.get('states') or ['All States']
        filter_options['date_ranges'] = loaded_metadata.get('date_ranges', filter_options['date_ranges']) # Load if present, else keep default

        # Load category-subcategory map
        category_subcategory_map = loaded_metadata.get('category_subcategory_map', {'All Categories': ['All Subcategories']})
        # Ensure 'All Categories' entry exists and has 'All Subcategories'
        if 'All Categories' not in category_subcategory_map:
            category_subcategory_map['All Categories'] = ['All Subcategories']
        if category_subcategory_map['All Categories'] and 'All Subcategories' not in category_subcategory_map['All Categories']:
             category_subcategory_map['All Categories'].insert(0, 'All Subcategories')

        # Add all unique subcategories to 'All Categories' list if not already present
        all_subs = set(loaded_metadata.get('subcategories', ['All Subcategories']))
        all_cats_subs = set(category_subcategory_map.get('All Categories', []))
        missing_subs = all_subs - all_cats_subs
        if missing_subs:
             category_subcategory_map['All Categories'].extend(sorted(list(missing_subs)))
             category_subcategory_map['All Categories'] = sorted(list(set(category_subcategory_map['All Categories'])), key=lambda x: (x != 'All Subcategories', x))


        # Load min/max values, keeping defaults if keys are missing
        loaded_min_max = loaded_metadata.get('min_max_values', {})
        min_max_values['pledged'] = loaded_min_max.get('pledged', min_max_values['pledged'])
        min_max_values['goal'] = loaded_min_max.get('goal', min_max_values['goal'])
        min_max_values['raised'] = loaded_min_max.get('raised', min_max_values['raised'])

        print("Filter metadata loaded successfully.")

    except json.JSONDecodeError:
        st.error(f"Error decoding JSON from '{filter_metadata_path}'. File might be corrupted. Using default filters.")
    except Exception as e:
        st.error(f"Error loading filter metadata from '{filter_metadata_path}': {e}. Using default filters.")

# Extract min/max for convenience
min_pledged = min_max_values['pledged']['min']
max_pledged = min_max_values['pledged']['max']
min_goal = min_max_values['goal']['min']
max_goal = min_max_values['goal']['max']
min_raised = min_max_values['raised']['min']
max_raised = min_max_values['raised']['max']

# --- Initialize Session State ---
if 'filters' not in st.session_state:
    st.session_state.filters = {
        'search': '',
        'categories': ['All Categories'],
        'subcategories': ['All Subcategories'],
        'countries': ['All Countries'],
        'states': ['All States'],
        'date': 'All Time',
        'ranges': {
            'pledged': {'min': min_pledged, 'max': max_pledged},
            'goal': {'min': min_goal, 'max': max_goal},
            'raised': {'min': min_raised, 'max': max_raised}
        }
    }
if 'sort_order' not in st.session_state:
    st.session_state.sort_order = 'popularity' # Default sort
if 'current_page' not in st.session_state:
    st.session_state.current_page = 1
if 'total_rows' not in st.session_state:
    st.session_state.total_rows = 0 # Will be calculated


# --- Base LazyFrame ---
if 'base_lf' not in st.session_state:
    if not os.path.exists(parquet_source_path):
        st.error(f"Parquet data source not found at '{parquet_source_path}'. Please ensure the file/directory exists.")
        st.stop()

    try:
        print(f"Scanning Parquet source: {parquet_source_path}")
        base_lf = pl.scan_parquet(parquet_source_path)

        print("Base LazyFrame created.")
        st.session_state.base_lf = base_lf
        # Initial schema check
        schema = st.session_state.base_lf.collect_schema()
        print("Schema:", schema)
        if len(schema) == 0: # Check if schema has columns
             st.error(f"Loaded data from '{parquet_source_path}' has no columns.")
             st.stop()
        # Check for duplicate column names explicitly
        if len(schema.names()) != len(set(schema.names())):
             st.error(f"Parquet source '{parquet_source_path}' contains duplicate column names. Please clean the source data.")
             # You might want to print the duplicate names here for debugging
             from collections import Counter
             counts = Counter(schema.names())
             duplicates = [name for name, count in counts.items() if count > 1]
             st.error(f"Duplicate columns found: {duplicates}")
             st.stop()


    except Exception as e:
        st.error(f"Error scanning Parquet or initial processing: {e}")
        # Provide more context if available
        if hasattr(e, 'context'):
            st.error(f"Context: {e.context()}")
        st.stop()

# --- Filtering and Sorting Logic ---
def apply_filters_and_sort(lf: pl.LazyFrame, filters: dict, sort_order: str) -> pl.LazyFrame:
    """Applies filters and sorting to a LazyFrame."""
    # Get column names once to avoid repeated schema resolution
    column_names = lf.collect_schema().names()

    # 1. Text Search (Apply across relevant text columns)
    search_term = filters.get('search', '')
    if search_term:
        # Add columns you want to search here
        search_cols = ['Project Name', 'Creator', 'Category', 'Subcategory']
        # Filter columns that actually exist in the frame
        valid_search_cols = [col for col in search_cols if col in column_names]
        if valid_search_cols:
            search_expr = None
            for col in valid_search_cols:
                 # Case-insensitive contains
                 # Ensure the column is Utf8 before applying string operations
                 current_expr = pl.col(col).cast(pl.Utf8).str.contains(f"(?i){search_term}")
                 if search_expr is None:
                     search_expr = current_expr
                 else:
                     search_expr = search_expr | current_expr # OR condition
            if search_expr is not None:
                 lf = lf.filter(search_expr)

    # 2. Categorical Filters
    if 'Category' in column_names and filters['categories'] != ['All Categories']:
        lf = lf.filter(pl.col('Category').is_in(filters['categories']))
    if 'Subcategory' in column_names and filters['subcategories'] != ['All Subcategories']:
        # Handle potential interaction with category filter if needed, assuming independent for now
        lf = lf.filter(pl.col('Subcategory').is_in(filters['subcategories']))
    if 'Country' in column_names and filters['countries'] != ['All Countries']:
        lf = lf.filter(pl.col('Country').is_in(filters['countries']))

    # State Filter - Now operates on the raw 'State' column
    if 'State' in column_names and filters['states'] != ['All States']:
        # Filter using the raw state values, case-insensitively
        lf = lf.filter(pl.col('State').cast(pl.Utf8).str.to_lowercase().is_in([s.lower() for s in filters['states']]))

    # 3. Range Filters
    ranges = filters.get('ranges', {})
    if 'Raw Pledged' in column_names and 'pledged' in ranges:
        min_p, max_p = ranges['pledged']['min'], ranges['pledged']['max']
        lf = lf.filter((pl.col('Raw Pledged') >= min_p) & (pl.col('Raw Pledged') <= max_p))
    if 'Raw Goal' in column_names and 'goal' in ranges:
        min_g, max_g = ranges['goal']['min'], ranges['goal']['max']
        lf = lf.filter((pl.col('Raw Goal') >= min_g) & (pl.col('Raw Goal') <= max_g))
    if 'Raw Raised' in column_names and 'raised' in ranges:
        min_r, max_r = ranges['raised']['min'], ranges['raised']['max']
        # Handle division by zero for goal if necessary for percentage calculation
        # Ensure 'Raw Raised' column exists and represents the percentage directly, or calculate it
        # Assuming 'Raw Raised' IS the percentage already calculated during data processing
        lf = lf.filter((pl.col('Raw Raised') >= min_r) & (pl.col('Raw Raised') <= max_r))


    # 4. Date Filter
    date_filter = filters.get('date', 'All Time')
    if date_filter != 'All Time' and 'Raw Date' in column_names:
        now = datetime.datetime.now()
        compare_date = None
        if date_filter == 'Last Month':
            compare_date = now - datetime.timedelta(days=30) # Approximation
        elif date_filter == 'Last 6 Months':
            compare_date = now - datetime.timedelta(days=182) # Approximation
        elif date_filter == 'Last Year':
            compare_date = now - datetime.timedelta(days=365)
        elif date_filter == 'Last 5 Years':
            compare_date = now - datetime.timedelta(days=5*365)
        elif date_filter == 'Last 10 Years':
            compare_date = now - datetime.timedelta(days=10*365)

        if compare_date:
             # Ensure Raw Date is datetime type before comparison
             # Use try_cast for robustness if dates might be invalid
             lf = lf.with_columns(pl.col("Raw Date").cast(pl.Datetime, strict=False).alias("Raw Date_dt"))
             lf = lf.filter(pl.col('Raw Date_dt') >= compare_date).drop("Raw Date_dt")


    # 5. Sorting
    sort_descending = True
    sort_col = 'Popularity Score' # Default for 'popularity'

    if sort_order == 'newest':
        sort_col = 'Raw Date'
        sort_descending = True
    elif sort_order == 'oldest':
        sort_col = 'Raw Date'
        sort_descending = False
    elif sort_order == 'mostfunded':
        sort_col = 'Raw Pledged'
        sort_descending = True
    elif sort_order == 'mostbacked':
        sort_col = 'Backer Count'
        sort_descending = True
    elif sort_order == 'enddate':
        sort_col = 'Raw Deadline'
        sort_descending = True # Assuming latest ending first

    if sort_col in column_names:
        lf = lf.sort(sort_col, descending=sort_descending, nulls_last=True)
    else:
        print(f"Warning: Sort column '{sort_col}' not found in LazyFrame.")


    return lf

# --- Generate HTML for the *current page* data ---
def generate_table_html_for_page(df_page: pl.DataFrame):
    visible_columns = ['Project Name', 'Creator', 'Pledged Amount', 'Link', 'Country', 'State']

    # Check required cols *exist* in the dataframe schema, not necessarily fetched for every row if null
    required_data_cols = [
        'Category', 'Subcategory', 'Raw Pledged', 'Raw Goal', 'Raw Raised',
        'Raw Date', 'Raw Deadline', 'Backer Count', 'Popularity Score'
    ]
    # Also need the visible columns and the raw 'State' column for styling
    all_needed_cols = list(set(visible_columns + required_data_cols + ['State']))

    missing_cols = [col for col in all_needed_cols if col not in df_page.columns]
    if missing_cols:
        st.error(f"FATAL: Missing required columns in fetched data page: {missing_cols}. Check base Parquet schema and processing.")
        # Return empty if critical columns are missing
        # Ensure visible_columns only contains existing columns before generating header
        visible_columns = [col for col in visible_columns if col in df_page.columns]
        header_html = ''.join(f'<th scope="col">{column}</th>' for column in visible_columns)
        return header_html, f'<tr><td colspan="{len(visible_columns) if visible_columns else 1}">Error: Missing critical data columns: {missing_cols}.</td></tr>'


    header_html = ''.join(f'<th scope="col">{column}</th>' for column in visible_columns)
    rows_html = ''

    if df_page.is_empty():
        return header_html, f'<tr><td colspan="{len(visible_columns)}">No projects match the current filters.</td></tr>'

    try:
        data_dicts = df_page.to_dicts()
    except Exception as e:
        st.error(f"Error converting page DataFrame to dictionaries: {e}")
        return header_html, f'<tr><td colspan="{len(visible_columns)}">Error rendering rows.</td></tr>'

    for row in data_dicts:
        # Apply State styling here using the raw 'State' value
        state_value = row.get('State') # Get original state value (might be None)
        state_value_str = str(state_value) if state_value is not None else 'unknown'
        # Create a CSS-friendly class name (lowercase, replace space with hyphen)
        state_class = state_value_str.lower().replace(' ', '-') if state_value_str != 'unknown' else 'unknown'
        # Ensure html escaping for the displayed text inside the div
        styled_state_html = f'<div class="state_cell state-{html.escape(state_class)}">{html.escape(state_value_str)}</div>' if state_value is not None else '<div class="state_cell state-unknown">unknown</div>'


        # Data attributes for potential client-side use (though filtering is server-side)
        # Ensure dates are formatted correctly if not None
        raw_date_str = row.get('Raw Date').strftime('%Y-%m-%d') if row.get('Raw Date') else 'N/A'
        raw_deadline_str = row.get('Raw Deadline').strftime('%Y-%m-%d') if row.get('Raw Deadline') else 'N/A'
        data_attrs = f'''
            data-category="{html.escape(str(row.get('Category', 'N/A')))}"
            data-subcategory="{html.escape(str(row.get('Subcategory', 'N/A')))}"
            data-pledged="{row.get('Raw Pledged', 0.0):.2f}"
            data-goal="{row.get('Raw Goal', 0.0):.2f}"
            data-raised="{row.get('Raw Raised', 0.0):.2f}"
            data-date="{raw_date_str}"
            data-deadline="{raw_deadline_str}"
            data-backers="{row.get('Backer Count', 0)}"
            data-popularity="{row.get('Popularity Score', 0.0):.6f}"
        '''
        visible_cells = ''
        for col in visible_columns:
            value = row.get(col) # Get value, might be None

            if col == 'Link':
                url = str(value) if value else '#'
                display_url = url if len(url) < 60 else url[:57] + '...'
                # Escape URL components for safety
                visible_cells += f'<td><a href="{html.escape(url)}" target="_blank" title="{html.escape(url)}">{html.escape(display_url)}</a></td>'
            elif col == 'Pledged Amount':
                 raw_pledged_val = row.get('Raw Pledged')
                 formatted_value = 'N/A'
                 if raw_pledged_val is not None:
                     try:
                         # Ensure it's treated as float first for consistency
                         amount = int(float(raw_pledged_val))
                         formatted_value = f"${amount:,}"
                     except (ValueError, TypeError):
                         pass # Keep N/A if conversion fails
                 visible_cells += f'<td>{html.escape(formatted_value)}</td>'
            elif col == 'State':
                 visible_cells += f'<td>{styled_state_html}</td>' # Use the styled HTML generated above
            else:
                # Handle potential None values before converting to string and escaping
                display_value = str(value) if value is not None else 'N/A'
                visible_cells += f'<td>{html.escape(display_value)}</td>'

        rows_html += f'<tr class="table-row" {data_attrs}>{visible_cells}</tr>'

    return header_html, rows_html


# --- Define Component ---
# Load CSS and JS script strings (Keep these as they are for now)
css = """
<style>
    .title-wrapper {
        width: 100%;
        text-align: center;    
        margin-bottom: 25px;
    }
    
    .title-wrapper span {
        color: white;
        font-family: 'Playfair Display';
        font-weight: 500;
        font-size: 70px;
    }
    
    .table-controls { 
        position: sticky; 
        top: 0; 
        background: #ffffff; 
        z-index: 2; 
        padding: 0 20px; 
        border-bottom: 1px solid #eee; 
        height: 60px; 
        display: flex; 
        align-items: center; 
        justify-content: space-between;
        margin-bottom: 1rem; 
        border-radius: 20px; 
    }
    
    .table-container { 
        position: relative;
        flex: 1;
        padding: 20px; 
        background: #ffffff; 
        overflow-y: auto;
        transition: height 0.3s ease;
        z-index: 3;
    }
    
    table { 
        border-collapse: collapse; 
        width: 100%; 
        background: #ffffff; 
        table-layout: fixed; 
    }

    /* Column width specifications */
    th[scope="col"]:nth-child(1) { width: 25%; } 
    th[scope="col"]:nth-child(2) { width: 12.5%; } 
    th[scope="col"]:nth-child(3) { width: 120px; } 
    th[scope="col"]:nth-child(4) { width: 25%; } 
    th[scope="col"]:nth-child(5) { width: 12.5%; } 
    th[scope="col"]:nth-child(6) { width: 120px; } 

    th { 
        background: #ffffff; 
        position: sticky; 
        top: 0; 
        z-index: 1; 
        padding: 12px 8px; 
        font-weight: 500; 
        font-family: 'Poppins'; 
        font-size: 14px; 
        color: #B5B7C0; 
        text-align: left; 
    }
    
    th:last-child { 
        text-align: center; 
    }

    td { 
        padding: 8px; 
        text-align: left; 
        border-bottom: 1px solid #ddd; 
        white-space: nowrap;
        font-family: 'Poppins';
        font-size: 14px;
        overflow-x: auto;
        -ms-overflow-style: none;
        overflow: -moz-scrollbars-none;
        scrollbar-width: none;
    }
    
    td::-webkit-scrollbar {
        display: none;
    }

    td:last-child { 
        width: 120px; 
        max-width: 120px; 
        text-align: center; 
    }

    .state_cell { 
        width: 100px; 
        max-width: 100px; 
        margin: 0 auto; 
        padding: 3px 5px; 
        text-align: center; 
        border-radius: 4px; 
        border: solid 1px; 
        display: inline-block; 
    }

    .state-canceled, .state-failed, .state-suspended { 
        background: #FFC5C5; 
        color: #DF0404; 
        border-color: #DF0404; 
    }
    
    .state-successful { 
        background: #16C09861; 
        color: #00B087; 
        border-color: #00B087; 
    }
    
    .state-live, .state-submitted, .state-started { 
        background: #E6F3FF; 
        color: #0066CC; 
        border-color: #0066CC; 
    }

    .table-wrapper { 
        position: relative;
        display: flex;
        flex-direction: column;
        max-width: 100%; 
        background: linear-gradient(180deg, #ffffff 15%, transparent 100%); 
        border-radius: 20px; 
        overflow: visible;
        transition: height 0.3s ease;
    }

    .search-input { 
        padding: 8px 12px; 
        border: 1px solid #ddd; 
        border-radius: 20px; 
        width: 200px; 
        font-size: 10px; 
        font-family: 'Poppins'; 
    }

    .search-input:focus { 
        outline: none; 
        border-color: #0066CC; 
        box-shadow: 0 0 0 2px rgba(0, 102, 204, 0.1); 
    }

    .pagination-controls {
        position: sticky;
        bottom: 0;
        background: #ffffff;
        z-index: 2;
        display: flex;
        justify-content: flex-end;
        align-items: center;
        padding: 1rem;
        gap: 0.5rem;
        border-top: 1px solid #eee;
        min-height: 60px;
        border-radius: 20px;
    }

    .page-numbers {
        display: flex;
        gap: 4px;
        align-items: center;
    }

    .page-number, .page-btn {
        min-width: 32px;
        height: 32px;
        padding: 0 6px;
        border: 1px solid #ddd;
        background: #fff;
        border-radius: 8px;
        cursor: pointer;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 12px;
        color: #333;
        font-family: 'Poppins';
    }

    .page-number:hover:not(:disabled),
    .page-btn:hover:not(:disabled) {
        background: #f0f0f0;
        border-color: #ccc;
    }

    .page-number.active {
        background: #5932EA;
        color: white;
        border-color: #5932EA;
    }

    .page-ellipsis {
        padding: 0 4px;
        color: #666;
    }

    .page-number:disabled,
    .page-btn:disabled {
        opacity: 0.5;
        cursor: not-allowed;
    }

    .hidden-cell {
        display: none;
    }

    .filter-wrapper {
        width: 100%;
        background: transparent;
        border-radius: 20px;
        margin-bottom: 20px;
        min-height: 120px;
        display: flex;
        flex-direction: row;
    }
    
    .reset-wrapper {
        width: auto;
        height: auto;
    }

    .filter-controls {
        padding: 15px;
        border-bottom: 1px solid #eee;
    }

    .filter-row {
        display: flex;
        align-items: center;
        gap: 10px;
        margin-bottom: 10px;
        margin-left: 5px;
        margin-right: 5px;
        width: 90%;
        justify-content: space-between;
    }

    .filter-label {
        font-family: 'Playfair Display';
        font-size: 24px;
        color: white;
        white-space: nowrap;
    }

    .filter-select {
        padding: 6px 12px;
        border: 1px solid #ddd;
        border-radius: 8px;
        font-family: 'Poppins';
        font-size: 12px;
        min-width: 120px;
        background: #fff;
    }

    .filter-select:focus {
        outline: none;
        border-color: #5932EA;
        box-shadow: 0 0 0 2px rgba(89, 50, 234, 0.1);
    }

    .reset-button {
        height: 100%;
        background: transparent;
        color: white;
        border: none;
        border-radius: 8px;
        cursor: pointer;
        padding: 0;
    }

    .reset-button span {
        transform: rotate(-90deg);
        white-space: nowrap;
        display: block;
        font-family: 'Playfair Display';
        font-size: 21px;
        letter-spacing: 1px;
    }

    .reset-button:hover {
        background: grey;
    }

    .filtered-text {
        font-family: 'Poppins';
        font-size: 22px;
        font-weight: 600;
        color: black;
    }

    td a {
        text-decoration: underline;
        overflow: hidden; 
        text-overflow: ellipsis; 
        white-space: nowrap; 
        font-family: 'Poppins'; 
        font-size: 14px; 
        color: black;
    }
    
    td a:hover {
        color: grey
    }

    /* Range Slider Styles */
    .range-dropdown {
        position: relative;
        display: inline-block;
    }

    .range-content {
        display: none;
        position: absolute;
        background-color: #fff;
        min-width: 300px;
        box-shadow: 0px 8px 16px 0px rgba(0,0,0,0.2);
        padding: 20px;
        border-radius: 8px;
        z-index: 1000;
    }

    .range-dropdown:hover .range-content {
        display: block;
    }

    .range-container {
        display: flex;
        flex-direction: column;
        width: 100%;
    }

    .sliders-control {
        position: relative;
        min-height: 50px;
    }

    .form-control {
        position: relative;
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-top: 10px;
        font-family: 'Poppins';
        column-gap: 10px;
    }

    .form-control-container {
        display: flex;
        align-items: center;
        gap: 5px;
    }

    .form-control-label {
        font-size: 12px;
        color: #666;
    }

    .form-control-input {
        width: 100px;
        padding: 4px 8px;
        border: 1px solid #ddd;
        border-radius: 4px;
        font-size: 12px;
        font-family: 'Poppins';
    }

    input[type="range"] {
        -webkit-appearance: none;
        appearance: none;
        height: 2px;
        width: 100%;
        position: absolute;
        background-color: #C6C6C6;
        pointer-events: none;
    }

    input[type="range"]::-webkit-slider-thumb {
        -webkit-appearance: none;
        pointer-events: all;
        width: 16px;
        height: 16px;
        background-color: #fff;
        border-radius: 50%;
        box-shadow: 0 0 0 1px #5932EA;
        cursor: pointer;
    }

    input[type="range"]::-moz-range-thumb {
        pointer-events: all;
        width: 16px;
        height: 16px;
        background-color: #fff;
        border-radius: 50%;
        box-shadow: 0 0 0 1px #5932EA;
        cursor: pointer;
    }

    #fromSlider, #goalFromSlider, #raisedFromSlider {
        height: 0;
        z-index: 1;
    }

    .multi-select-dropdown {
        position: relative;
        display: inline-block;
    }

    .multi-select-content {
        display: none;
        position: absolute;
        background-color: #fff;
        min-width: 200px;
        box-shadow: 0px 8px 16px 0px rgba(0,0,0,0.2);
        padding: 8px;
        border-radius: 8px;
        z-index: 1000;
        max-height: 300px;
        overflow-y: auto;
    }

    .multi-select-dropdown:hover .multi-select-content {
        display: block;
    }

    .multi-select-btn {
        min-width: 150px;
    }

    .category-option {
        padding: 8px 12px;
        cursor: pointer;
        border-radius: 4px;
        margin: 2px 0;
        font-family: 'Poppins';
        font-size: 12px;
        transition: all 0.2s ease;
    }

    .category-option:hover {
        background-color: #f0f0f0;
    }

    .category-option.selected {
        background-color: #5932EA;
        color: white;
    }

    .category-option[data-value="All Categories"] {
        border-bottom: 1px solid #eee;
        margin-bottom: 8px;
        padding-bottom: 12px;
    }

    .country-option {
        padding: 8px 12px;
        cursor: pointer;
        border-radius: 4px;
        margin: 2px 0;
        font-family: 'Poppins';
        font-size: 12px;
        transition: all 0.2s ease;
    }

    .country-option:hover {
        background-color: #f0f0f0;
    }

    .country-option.selected {
        background-color: #5932EA;
        color: white;
    }

    .country-option[data-value="All Countries"] {
        border-bottom: 1px solid #eee;
        margin-bottom: 8px;
        padding-bottom: 12px;
    }

    .state-option {
        padding: 8px 12px;
        cursor: pointer;
        border-radius: 4px;
        margin: 2px 0;
        font-family: 'Poppins';
        font-size: 12px;
        transition: all 0.2s ease;
    }

    .state-option:hover {
        background-color: #f0f0f0;
    }

    .state-option.selected {
        background-color: #5932EA;
        color: white;
    }

    .state-option[data-value="All States"] {
        border-bottom: 1px solid #eee;
        margin-bottom: 8px;
        padding-bottom: 12px;
    }

    .subcategory-option {
        padding: 8px 12px;
        cursor: pointer;
        border-radius: 4px;
        margin: 2px 0;
        font-family: 'Poppins';
        font-size: 12px;
        transition: all 0.2s ease;
    }

    .subcategory-option:hover {
        background-color: #f0f0f0;
    }

    .subcategory-option.selected {
        background-color: #5932EA;
        color: white;
    }

    .subcategory-option[data-value="All Subcategories"] {
        border-bottom: 1px solid #eee;
        margin-bottom: 8px;
        padding-bottom: 12px;
    }

    body { font-family: 'Poppins', sans-serif; margin: 0; padding: 20px; box-sizing: border-box; }
    #component-root { width: 100%; }
    /* Add a loading indicator style */
    .loading-overlay {
        position: absolute;
        top: 0; left: 0; right: 0; bottom: 0;
        background: rgba(255, 255, 255, 0.7);
        display: flex;
        justify-content: center;
        align-items: center;
        z-index: 100;
        font-size: 1.2em;
        color: #555;
    }
    .hidden { display: none; }
</style>
"""

script = """
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func.apply(this, args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

class TableManager {
    constructor(initialData) {
        console.log("TableManager initializing with:", initialData);
        this.componentRoot = document.getElementById('component-root');
        if (!this.componentRoot) {
            console.error("Component root element not found!");
            return;
        }

        // Initial state from Streamlit
        this.currentPage = initialData.current_page || 1;
        this.pageSize = initialData.page_size || 10;
        this.totalRows = initialData.total_rows || 0;
        this.currentFilters = initialData.filters || {};
        this.currentSort = initialData.sort_order || 'popularity';
        this.filterOptions = initialData.filter_options || {};
        this.categorySubcategoryMap = initialData.category_subcategory_map || {};
        this.minMaxValues = initialData.min_max_values || {};

        this.renderHTMLStructure(initialData.header_html);
        this.bindStaticElements(); // Bind elements that exist once after initial render
        this.updateUIState(initialData); // Set initial values for filters etc.
        this.updateTableContent(initialData.rows_html); // Populate initial table rows
        this.updatePagination(); // Create initial pagination
        this.adjustHeight(); // Adjust height after initial render

        console.log("TableManager initialized.");
    }

    renderHTMLStructure(headerHtml) {
        // Create the main structure once
        const minPledged = this.minMaxValues?.pledged?.min ?? 0;
        const maxPledged = this.minMaxValues?.pledged?.max ?? 1000;
        const minGoal = this.minMaxValues?.goal?.min ?? 0;
        const maxGoal = this.minMaxValues?.goal?.max ?? 10000;
        const minRaised = this.minMaxValues?.raised?.min ?? 0;
        const maxRaised = this.minMaxValues?.raised?.max ?? 500;

        this.componentRoot.innerHTML = `
            <div class="title-wrapper">
                <span>Explore Successful Projects</span>
            </div>
            <div class="filter-wrapper">
                 <div class="reset-wrapper">
                     <button class="reset-button" id="resetFilters">
                         <span>Default</span>
                     </button>
                 </div>
                 <div class="filter-controls">
                     <div class="filter-row">
                         <span class="filter-label">Explore</span>
                         <div class="multi-select-dropdown">
                             <button id="categoryFilterBtn" class="filter-select multi-select-btn">Categories</button>
                             <div class="multi-select-content" id="categoryOptionsContainer">
                                 ${(this.filterOptions.categories || []).map(opt => `<div class="category-option" data-value="${opt}">${opt}</div>`).join('')}
                             </div>
                         </div>
                         <span class="filter-label">&</span>
                         <div class="multi-select-dropdown">
                             <button id="subcategoryFilterBtn" class="filter-select multi-select-btn">Subcategories</button>
                             <div class="multi-select-content" id="subcategoryOptionsContainer">
                                 <!-- Populated dynamically -->
                             </div>
                         </div>
                         <span class="filter-label">Projects On</span>
                         <div class="multi-select-dropdown">
                             <button id="countryFilterBtn" class="filter-select multi-select-btn">Countries</button>
                             <div class="multi-select-content" id="countryOptionsContainer">
                                ${ (this.filterOptions.countries || []).map(opt => `<div class="country-option" data-value="${opt}">${opt}</div>`).join('')}
                             </div>
                         </div>
                         <span class="filter-label">Sorted By</span>
                         <select id="sortFilter" class="filter-select">
                             <option value="popularity">Most Popular</option>
                             <option value="newest">Newest First</option>
                             <option value="oldest">Oldest First</option>
                             <option value="mostfunded">Most Funded</option>
                             <option value="mostbacked">Most Backed</option>
                             <option value="enddate">End Date</option>
                         </select>
                     </div>
                     <div class="filter-row">
                        <span class="filter-label">More Flexible, Dynamic Search:</span>
                        <div class="multi-select-dropdown">
                            <button id="stateFilterBtn" class="filter-select multi-select-btn">States</button>
                            <div class="multi-select-content" id="stateOptionsContainer">
                                ${ (this.filterOptions.states || []).map(opt => `<div class="state-option" data-value="${opt}">${opt}</div>`).join('')}
                            </div>
                        </div>
                        <div class="range-dropdown">
                            <button class="filter-select">Pledged Amount Range</button>
                             <div class="range-content">
                                <div class="range-container">
                                    <div class="sliders-control">
                                        <input id="fromSlider" type="range" value="${minPledged}" min="${minPledged}" max="${maxPledged}"/>
                                        <input id="toSlider" type="range" value="${maxPledged}" min="${minPledged}" max="${maxPledged}"/>
                                    </div>
                                    <div class="form-control">
                                        <div class="form-control-container">
                                            <span class="form-control-label">Min $</span>
                                            <input class="form-control-input" type="number" id="fromInput" value="${minPledged}" min="${minPledged}" max="${maxPledged}"/>
                                        </div>
                                        <div class="form-control-container">
                                            <span class="form-control-label">Max $</span>
                                            <input class="form-control-input" type="number" id="toInput" value="${maxPledged}" min="${minPledged}" max="${maxPledged}"/>
                                        </div>
                                    </div>
                                </div>
                             </div>
                        </div>
                        <div class="range-dropdown">
                            <button class="filter-select">Goal Amount Range</button>
                             <div class="range-content">
                                <div class="range-container">
                                    <div class="sliders-control">
                                        <input id="goalFromSlider" type="range" value="${minGoal}" min="${minGoal}" max="${maxGoal}"/>
                                        <input id="goalToSlider" type="range" value="${maxGoal}" min="${minGoal}" max="${maxGoal}"/>
                                    </div>
                                    <div class="form-control">
                                        <div class="form-control-container">
                                            <span class="form-control-label">Min $</span>
                                            <input class="form-control-input" type="number" id="goalFromInput" value="${minGoal}" min="${minGoal}" max="${maxGoal}"/>
                                        </div>
                                        <div class="form-control-container">
                                            <span class="form-control-label">Max $</span>
                                            <input class="form-control-input" type="number" id="goalToInput" value="${maxGoal}" min="${minGoal}" max="${maxGoal}"/>
                                        </div>
                                    </div>
                                </div>
                             </div>
                        </div>
                        <div class="range-dropdown">
                            <button class="filter-select">Percentage Raised Range</button>
                            <div class="range-content">
                                <div class="range-container">
                                    <div class="sliders-control">
                                        <input id="raisedFromSlider" type="range" value="${minRaised}" min="${minRaised}" max="${maxRaised}"/>
                                        <input id="raisedToSlider" type="range" value="${maxRaised}" min="${minRaised}" max="${maxRaised}"/>
                                    </div>
                                    <div class="form-control">
                                        <div class="form-control-container">
                                            <span class="form-control-label">Min %</span>
                                            <input class="form-control-input" type="number" id="raisedFromInput" value="${minRaised}" min="${minRaised}" max="${maxRaised}"/>
                                        </div>
                                        <div class="form-control-container">
                                            <span class="form-control-label">Max %</span>
                                            <input class="form-control-input" type="number" id="raisedToInput" value="${maxRaised}" min="${minRaised}" max="${maxRaised}"/>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <select id="dateFilter" class="filter-select">
                            ${(this.filterOptions.date_ranges || []).map(opt => `<option value="${opt}">${opt}</option>`).join('')}
                        </select>
                     </div>
                 </div>
            </div>
            <div class="table-wrapper">
                <div class="table-controls">
                    <span class="filtered-text">Filtered Projects</span>
                    <input type="text" id="table-search" class="search-input" placeholder="Search table...">
                </div>
                <div class="table-container">
                    <table id="data-table">
                        <thead>
                            <tr>${headerHtml}</tr>
                        </thead>
                        <tbody id="table-body">
                            <!-- Rows will be inserted here -->
                        </tbody>
                    </table>
                    <div id="loading-indicator" class="loading-overlay hidden">Loading...</div>
                </div>
                <div class="pagination-controls">
                    <button id="prev-page" class="page-btn" aria-label="Previous page">&lt;</button>
                    <div id="page-numbers" class="page-numbers"></div>
                    <button id="next-page" class="page-btn" aria-label="Next page">&gt;</button>
                </div>
            </div>
        `;
    }

    bindStaticElements() {
        // --- Search ---
        this.searchInput = document.getElementById('table-search');
        this.searchInput.addEventListener('input', debounce((e) => {
            this.currentFilters.search = e.target.value.trim();
            this.currentPage = 1; // Reset to first page on new search
            this.requestUpdate();
        }, 500)); // Debounce search input

        // --- Pagination ---
        document.getElementById('prev-page').addEventListener('click', () => this.previousPage());
        document.getElementById('next-page').addEventListener('click', () => this.nextPage());
        // Page number clicks are handled dynamically in updatePagination

        // --- Reset ---
        document.getElementById('resetFilters').addEventListener('click', () => this.resetFilters());

        // --- Sort ---
        document.getElementById('sortFilter').addEventListener('change', (e) => {
            this.currentSort = e.target.value;
            this.currentPage = 1; // Reset to first page on sort change
            this.requestUpdate();
        });
        
        // --- Date Filter ---
        document.getElementById('dateFilter').addEventListener('change', (e) => {
             this.currentFilters.date = e.target.value;
             this.currentPage = 1;
             this.requestUpdate();
        });

        // --- Range Sliders ---
        this.setupRangeSlider(); // Sets up listeners for sliders/inputs

        // --- Multi-Select Dropdowns ---
        this.selectedCategories = new Set(this.currentFilters.categories || ['All Categories']);
        this.selectedSubcategories = new Set(this.currentFilters.subcategories || ['All Subcategories']);
        this.selectedCountries = new Set(this.currentFilters.countries || ['All Countries']);
        this.selectedStates = new Set(this.currentFilters.states || ['All States']);

        this.categoryBtn = document.getElementById('categoryFilterBtn');
        this.subcategoryBtn = document.getElementById('subcategoryFilterBtn');
        this.countryBtn = document.getElementById('countryFilterBtn');
        this.stateBtn = document.getElementById('stateFilterBtn');

        this.setupMultiSelect(
            document.querySelectorAll('#categoryOptionsContainer .category-option'),
            this.selectedCategories,
            'All Categories',
            this.categoryBtn,
            true // Trigger subcategory update
        );
        this.updateSubcategoryOptions(); // Initial population
        this.setupMultiSelect( // Setup for subcategories *after* initial population
             document.querySelectorAll('#subcategoryOptionsContainer .subcategory-option'),
             this.selectedSubcategories,
             'All Subcategories',
             this.subcategoryBtn
        );
        this.setupMultiSelect(
            document.querySelectorAll('#countryOptionsContainer .country-option'),
            this.selectedCountries,
            'All Countries',
            this.countryBtn
        );
         this.setupMultiSelect(
             document.querySelectorAll('#stateOptionsContainer .state-option'),
             this.selectedStates,
             'All States',
             this.stateBtn
         );
    }

    updateUIState(data) {
        // Update filter controls to reflect the current state received from Python
        this.currentPage = data.current_page;
        this.totalRows = data.total_rows;
        this.currentFilters = data.filters;
        this.currentSort = data.sort_order;

        // -- Update Search Input --
        if (this.searchInput) this.searchInput.value = this.currentFilters.search || '';

        // -- Update Sort Dropdown --
        const sortSelect = document.getElementById('sortFilter');
        if (sortSelect) sortSelect.value = this.currentSort;
        
         // -- Update Date Dropdown --
         const dateSelect = document.getElementById('dateFilter');
         if (dateSelect) dateSelect.value = this.currentFilters.date || 'All Time';

        // -- Update Multi-Selects --
        this.selectedCategories = new Set(this.currentFilters.categories || ['All Categories']);
        this.selectedSubcategories = new Set(this.currentFilters.subcategories || ['All Subcategories']);
        this.selectedCountries = new Set(this.currentFilters.countries || ['All Countries']);
        this.selectedStates = new Set(this.currentFilters.states || ['All States']);

        this.updateMultiSelectUI(document.querySelectorAll('#categoryOptionsContainer .category-option'), this.selectedCategories, this.categoryBtn, 'All Categories');
        this.updateSubcategoryOptions(); // Re-render subcategories based on selected categories
        this.updateMultiSelectUI(document.querySelectorAll('#subcategoryOptionsContainer .subcategory-option'), this.selectedSubcategories, this.subcategoryBtn, 'All Subcategories');
        this.updateMultiSelectUI(document.querySelectorAll('#countryOptionsContainer .country-option'), this.selectedCountries, this.countryBtn, 'All Countries');
        this.updateMultiSelectUI(document.querySelectorAll('#stateOptionsContainer .state-option'), this.selectedStates, this.stateBtn, 'All States');

        // -- Update Range Sliders --
        if (this.currentFilters.ranges && this.rangeSliderElements) {
             const { ranges } = this.currentFilters;
             const {
                 fromSlider, toSlider, fromInput, toInput,
                 goalFromSlider, goalToSlider, goalFromInput, goalToInput,
                 raisedFromSlider, raisedToSlider, raisedFromInput, raisedToInput,
                 fillSlider // Function to update slider background
             } = this.rangeSliderElements;

             // Pledged
             if (ranges.pledged) {
                 fromSlider.value = ranges.pledged.min;
                 toSlider.value = ranges.pledged.max;
                 fromInput.value = ranges.pledged.min;
                 toInput.value = ranges.pledged.max;
                 fillSlider(fromSlider, toSlider, '#C6C6C6', '#5932EA', toSlider);
             }
             // Goal
             if (ranges.goal) {
                 goalFromSlider.value = ranges.goal.min;
                 goalToSlider.value = ranges.goal.max;
                 goalFromInput.value = ranges.goal.min;
                 goalToInput.value = ranges.goal.max;
                 fillSlider(goalFromSlider, goalToSlider, '#C6C6C6', '#5932EA', goalToSlider);
             }
             // Raised
             if (ranges.raised) {
                  raisedFromSlider.value = ranges.raised.min;
                  raisedToSlider.value = ranges.raised.max;
                  raisedFromInput.value = ranges.raised.min;
                  raisedToInput.value = ranges.raised.max;
                  fillSlider(raisedFromSlider, raisedToSlider, '#C6C6C6', '#5932EA', raisedToSlider);
             }
        }
    }

    updateMultiSelectUI(options, selectedSet, buttonElement, allValue) {
         if (!options || options.length === 0) return; // Handle case where options not rendered yet
         options.forEach(option => {
            if (selectedSet.has(option.dataset.value)) {
                 option.classList.add('selected');
            }
         });
         this.updateButtonText(selectedSet, buttonElement, allValue);
    }


    requestUpdate() {
        // Show loading indicator
        this.showLoading(true);

        // Gather the current state from UI elements
        const state = {
            page: this.currentPage,
            filters: {
                search: this.searchInput.value.trim(),
                categories: Array.from(this.selectedCategories),
                subcategories: Array.from(this.selectedSubcategories),
                countries: Array.from(this.selectedCountries),
                states: Array.from(this.selectedStates),
                date: document.getElementById('dateFilter').value,
                ranges: {
                    pledged: { min: parseFloat(document.getElementById('fromInput').value), max: parseFloat(document.getElementById('toInput').value) },
                    goal: { min: parseFloat(document.getElementById('goalFromInput').value), max: parseFloat(document.getElementById('goalToInput').value) },
                    raised: { min: parseFloat(document.getElementById('raisedFromInput').value), max: parseFloat(document.getElementById('raisedToInput').value) }
                }
            },
            sort_order: this.currentSort
        };
        console.log("Requesting update with state:", state);
        Streamlit.setComponentValue(state);
    }

     showLoading(isLoading) {
         const indicator = document.getElementById('loading-indicator');
         if (indicator) {
             indicator.classList.toggle('hidden', !isLoading);
         }
         // Maybe also disable controls while loading
         const controls = this.componentRoot.querySelectorAll('button, input, select');
         controls.forEach(el => el.disabled = isLoading);
     }


    updateTableContent(rowsHtml) {
        const tbody = document.getElementById('table-body');
        if (tbody) {
            tbody.innerHTML = rowsHtml || '<tr><td colspan="6">Loading data...</td></tr>'; // Use colspan from header
        }
         // Hide loading indicator once table content is updated
         this.showLoading(false);
    }

    updatePagination() {
        const totalPages = Math.max(1, Math.ceil(this.totalRows / this.pageSize));
        const pageNumbers = this.generatePageNumbers(totalPages);
        const container = document.getElementById('page-numbers');
        if (!container) return;

        container.innerHTML = pageNumbers.map(page => {
            if (page === '...') {
                return '<span class="page-ellipsis">...</span>';
            }
            // Note: Direct onclick is simple here, but could use event listeners
            return `<button class="page-number ${page === this.currentPage ? 'active' : ''}"
                ${page === this.currentPage ? 'disabled' : ''}
                onclick="window.tableManagerInstance.goToPage(${page})">${page}</button>`;
        }).join('');

        document.getElementById('prev-page').disabled = this.currentPage <= 1;
        document.getElementById('next-page').disabled = this.currentPage >= totalPages;
    }

    generatePageNumbers(totalPages) {
        // Same logic as before
        let pages = [];
        if (totalPages <= 10) {
            pages = Array.from({length: totalPages}, (_, i) => i + 1);
        } else {
            if (this.currentPage <= 7) {
                pages = [...Array.from({length: 7}, (_, i) => i + 1), '...', totalPages - 1, totalPages];
            } else if (this.currentPage >= totalPages - 6) {
                pages = [1, 2, '...', ...Array.from({length: 7}, (_, i) => totalPages - 6 + i)];
            } else {
                pages = [1, 2, '...', this.currentPage - 1, this.currentPage, this.currentPage + 1, '...', totalPages - 1, totalPages];
            }
        }
        return pages;
    }

    previousPage() {
        if (this.currentPage > 1) {
            this.currentPage--;
            this.requestUpdate();
        }
    }

    nextPage() {
        const totalPages = Math.ceil(this.totalRows / this.pageSize);
        if (this.currentPage < totalPages) {
            this.currentPage++;
            this.requestUpdate();
        }
    }

    goToPage(page) {
        const totalPages = Math.ceil(this.totalRows / this.pageSize);
        if (page >= 1 && page <= totalPages && page !== this.currentPage) {
            this.currentPage = page;
            this.requestUpdate();
        }
    }

    resetFilters() {
        // Reset internal state representation
        this.selectedCategories = new Set(['All Categories']);
        this.selectedSubcategories = new Set(['All Subcategories']);
        this.selectedCountries = new Set(['All Countries']);
        this.selectedStates = new Set(['All States']);

        // Reset UI elements explicitly
        this.searchInput.value = '';
        document.getElementById('sortFilter').value = 'popularity';
        document.getElementById('dateFilter').value = 'All Time';

        // Reset range sliders (use min/max from metadata)
        const minPledged = this.minMaxValues?.pledged?.min ?? 0;
        const maxPledged = this.minMaxValues?.pledged?.max ?? 1000;
        const minGoal = this.minMaxValues?.goal?.min ?? 0;
        const maxGoal = this.minMaxValues?.goal?.max ?? 10000;
        const minRaised = this.minMaxValues?.raised?.min ?? 0;
        const maxRaised = this.minMaxValues?.raised?.max ?? 500;

        if (this.rangeSliderElements) {
            const { fromSlider, toSlider, fromInput, toInput, /* ... other sliders/inputs */ fillSlider } = this.rangeSliderElements;
             // Pledged
             fromSlider.value = minPledged; toSlider.value = maxPledged;
             fromInput.value = minPledged; toInput.value = maxPledged;
             fillSlider(fromSlider, toSlider, '#C6C6C6', '#5932EA', toSlider);
             // Goal
             const { goalFromSlider, goalToSlider, goalFromInput, goalToInput } = this.rangeSliderElements;
             goalFromSlider.value = minGoal; goalToSlider.value = maxGoal;
             goalFromInput.value = minGoal; goalToInput.value = maxGoal;
             fillSlider(goalFromSlider, goalToSlider, '#C6C6C6', '#5932EA', goalToSlider);
             // Raised
             const { raisedFromSlider, raisedToSlider, raisedFromInput, raisedToInput } = this.rangeSliderElements;
             raisedFromSlider.value = minRaised; raisedToSlider.value = maxRaised;
             raisedFromInput.value = minRaised; raisedToInput.value = maxRaised;
             fillSlider(raisedFromSlider, raisedToSlider, '#C6C6C6', '#5932EA', raisedToSlider);
        }

        // Reset multi-select UI
        this.updateMultiSelectUI(document.querySelectorAll('#categoryOptionsContainer .category-option'), this.selectedCategories, this.categoryBtn, 'All Categories');
        this.updateSubcategoryOptions(); // Regenerate subcategories for 'All Categories'
        this.updateMultiSelectUI(document.querySelectorAll('#subcategoryOptionsContainer .subcategory-option'), this.selectedSubcategories, this.subcategoryBtn, 'All Subcategories');
        this.updateMultiSelectUI(document.querySelectorAll('#countryOptionsContainer .country-option'), this.selectedCountries, this.countryBtn, 'All Countries');
        this.updateMultiSelectUI(document.querySelectorAll('#stateOptionsContainer .state-option'), this.selectedStates, this.stateBtn, 'All States');


        // Set state and request update
        this.currentPage = 1;
        this.currentSort = 'popularity';
        // No need to construct the full filter object here, just request the update
        this.requestUpdate(); // Will gather the reset state from UI
    }


    adjustHeight() {
         // Use requestAnimationFrame for smoother adjustments
         requestAnimationFrame(() => {
            const root = this.componentRoot;
            if (!root) return;
             // Calculate height based on rendered content
             const totalHeight = root.offsetHeight + 50; // Add some buffer

             // Check if height changed significantly to avoid rapid updates
             if (!this.lastHeight || Math.abs(this.lastHeight - totalHeight) > 10) {
                 this.lastHeight = totalHeight;
                 Streamlit.setFrameHeight(totalHeight);
                 console.log("Adjusting height to:", totalHeight);
             }
         });
    }

    // --- Multi-Select Logic ---
    updateButtonText(selectedItems, buttonElement, allValueLabel) {
         if (!buttonElement) return;
         const selectedArray = Array.from(selectedItems);
         const displayItems = selectedArray.filter(item => item !== allValueLabel);
         displayItems.sort((a, b) => a.localeCompare(b)); // Sort for consistent display

         if (selectedArray.length === 0 || (selectedArray.length === 1 && selectedArray[0] === allValueLabel) || displayItems.length === 0) {
             buttonElement.textContent = allValueLabel;
         } else if (displayItems.length > 2) {
             buttonElement.textContent = `${displayItems[0]}, ${displayItems[1]} +${displayItems.length - 2}`;
         } else {
             buttonElement.textContent = displayItems.join(', ');
         }
    }

    setupMultiSelect(options, selectedSet, allValue, buttonElement, triggerSubcategoryUpdate = false) {
        if (!options || options.length === 0) return; // Ensure options exist
        const allOption = Array.from(options).find(opt => opt.dataset.value === allValue);

        options.forEach(option => {
            // Clone and replace to remove previous listeners cleanly
            const newOption = option.cloneNode(true);
            option.parentNode.replaceChild(newOption, option);

             // Add selected class based on initial set
             if (selectedSet.has(newOption.dataset.value)) {
                 newOption.classList.add('selected');
             }

             newOption.addEventListener('click', (e) => {
                const clickedValue = e.target.dataset.value;
                const isCurrentlySelected = e.target.classList.contains('selected');
                const currentOptions = e.target.parentElement.querySelectorAll('[data-value]'); // Get current options in scope

                if (clickedValue === allValue) {
                    selectedSet.clear();
                    selectedSet.add(allValue);
                    currentOptions.forEach(opt => opt.classList.remove('selected'));
                    e.target.classList.add('selected'); // Select the 'All' option
                } else {
                    const currentAllOption = e.target.parentElement.querySelector(`[data-value="${allValue}"]`);
                    if (currentAllOption) {
                        selectedSet.delete(allValue);
                        currentAllOption.classList.remove('selected');
                    }

                    e.target.classList.toggle('selected');
                    if (e.target.classList.contains('selected')) {
                        selectedSet.add(clickedValue);
                    } else {
                        selectedSet.delete(clickedValue);
                    }

                    // If nothing selected, default back to 'All'
                    if (selectedSet.size === 0) {
                        selectedSet.add(allValue);
                         if (currentAllOption) currentAllOption.classList.add('selected');
                    }
                }

                this.updateButtonText(selectedSet, buttonElement, allValue);

                if (triggerSubcategoryUpdate) {
                    this.updateSubcategoryOptions();
                     // Important: Need to re-setup the subcategory multi-select listeners after updating options
                     this.setupMultiSelect(
                         document.querySelectorAll('#subcategoryOptionsContainer .subcategory-option'),
                         this.selectedSubcategories, // Use the current subcategory set
                         'All Subcategories',
                         this.subcategoryBtn // The subcategory button
                     );
                }

                this.currentPage = 1; // Reset page on filter change
                this.requestUpdate();
            });
        });

        // Initial button text update
        this.updateButtonText(selectedSet, buttonElement, allValue);
    }


    updateSubcategoryOptions() {
        const selectedCategories = this.selectedCategories || new Set(['All Categories']);
        const subcategoryMap = this.categorySubcategoryMap || {};
        const subcategoryOptionsContainer = document.getElementById('subcategoryOptionsContainer');
        const subcategoryBtn = document.getElementById('subcategoryFilterBtn');
        if (!subcategoryOptionsContainer || !subcategoryBtn) return;


        let availableSubcategories = new Set();
        let isAllCategoriesSelected = selectedCategories.has('All Categories');

        if (isAllCategoriesSelected || selectedCategories.size === 0) {
            // Add all known subcategories if 'All Categories' is selected
             (subcategoryMap['All Categories'] || []).forEach(subcat => availableSubcategories.add(subcat));
        } else {
             availableSubcategories.add('All Subcategories'); // Always include 'All Subcategories'
             selectedCategories.forEach(cat => {
                 (subcategoryMap[cat] || []).forEach(subcat => {
                      if (subcat !== 'All Subcategories') { // Don't add 'All Sub...' multiple times
                         availableSubcategories.add(subcat);
                      }
                 });
             });
        }

        const sortedSubcategories = Array.from(availableSubcategories);
        sortedSubcategories.sort((a, b) => {
            if (a === 'All Subcategories') return -1;
            if (b === 'All Subcategories') return 1;
            return a.localeCompare(b);
        });

        subcategoryOptionsContainer.innerHTML = sortedSubcategories.map(opt =>
            `<div class="subcategory-option" data-value="${opt}">${opt}</div>`
        ).join('');

        // --- Crucial: Reset subcategory selection logic when categories change ---
        // Determine if the *current* subcategory selection is still valid given the available options
        const availableSubSet = new Set(sortedSubcategories);
        let resetSubcategories = false;
        for (const subcat of this.selectedSubcategories) {
            if (subcat !== 'All Subcategories' && !availableSubSet.has(subcat)) {
                resetSubcategories = true;
                break;
            }
        }
        // If any selected subcategory (other than 'All') is no longer available, reset to 'All Subcategories'
        if (resetSubcategories || this.selectedSubcategories.size === 0) {
            this.selectedSubcategories.clear();
            this.selectedSubcategories.add('All Subcategories');
        }


        // Apply 'selected' class based on the (potentially reset) selectedSubcategories set
        const newSubOptions = subcategoryOptionsContainer.querySelectorAll('.subcategory-option');
        newSubOptions.forEach(opt => {
            if (this.selectedSubcategories.has(opt.dataset.value)) {
                opt.classList.add('selected');
            }
        });

        this.updateButtonText(this.selectedSubcategories, subcategoryBtn, 'All Subcategories');

        // Re-attach listeners for the new subcategory options
         // Note: We don't request an update here, that happens when a subcategory *itself* is clicked
         // Or when the category change triggers an update earlier.
         // We DO need to re-bind the listeners for the *newly created* subcategory options.
         this.setupMultiSelect(
             newSubOptions,
             this.selectedSubcategories,
             'All Subcategories',
             this.subcategoryBtn
         );
    }

    // --- Range Slider Logic (Mostly similar, ensure it calls requestUpdate) ---
    setupRangeSlider() {
        const fromSlider = document.getElementById('fromSlider');
        const toSlider = document.getElementById('toSlider');
        const fromInput = document.getElementById('fromInput');
        const toInput = document.getElementById('toInput');
        // ... (get goal and raised elements) ...
        const goalFromSlider = document.getElementById('goalFromSlider');
        const goalToSlider = document.getElementById('goalToSlider');
        const goalFromInput = document.getElementById('goalFromInput');
        const goalToInput = document.getElementById('goalToInput');

        const raisedFromSlider = document.getElementById('raisedFromSlider');
        const raisedToSlider = document.getElementById('raisedToSlider');
        const raisedFromInput = document.getElementById('raisedFromInput');
        const raisedToInput = document.getElementById('raisedToInput');

        if (!fromSlider) return; // Exit if elements aren't rendered yet

        const fillSlider = (from, to, sliderColor, rangeColor, controlSlider) => {
            const rangeDistance = controlSlider.max - controlSlider.min;
            const fromPosition = from.value - controlSlider.min;
            const toPosition = to.value - controlSlider.min;
            // Prevent division by zero or negative range distance
             const safeRangeDistance = (rangeDistance > 0) ? rangeDistance : 1;
            const fromPercent = (fromPosition / safeRangeDistance) * 100;
            const toPercent = (toPosition / safeRangeDistance) * 100;

            controlSlider.style.background = `linear-gradient(
                to right,
                ${sliderColor} 0%,
                ${sliderColor} ${fromPercent}%,
                ${rangeColor} ${fromPercent}%,
                ${rangeColor} ${toPercent}%,
                ${sliderColor} ${toPercent}%,
                ${sliderColor} 100%)`;
        };

        const debouncedRequestUpdate = debounce(() => {
            this.currentPage = 1; // Reset page on range change
            this.requestUpdate();
        }, 500); // Debounce slider/input changes

        const controlFromSlider = (fromSlider, toSlider, fromInput) => {
             const [from, to] = [parseInt(fromSlider.value), parseInt(toSlider.value)];
             fillSlider(fromSlider, toSlider, '#C6C6C6', '#5932EA', toSlider);
             if (from > to) {
                 fromSlider.value = to;
                 fromInput.value = to;
             } else {
                  fromInput.value = from;
             }
        };

        const controlToSlider = (fromSlider, toSlider, toInput) => {
             const [from, to] = [parseInt(fromSlider.value), parseInt(toSlider.value)];
             fillSlider(fromSlider, toSlider, '#C6C6C6', '#5932EA', toSlider);
             if (from <= to) {
                 toInput.value = to;
             } else {
                 toInput.value = from;
                 toSlider.value = from;
             }
        };
        
        const setupSliderListeners = (fSlider, tSlider, fInput, tInput) => {
            fSlider.addEventListener('input', () => { controlFromSlider(fSlider, tSlider, fInput); debouncedRequestUpdate(); });
            tSlider.addEventListener('input', () => { controlToSlider(fSlider, tSlider, tInput); debouncedRequestUpdate(); });
            fInput.addEventListener('input', () => { /* Basic sync, validation on blur/enter */ if (parseInt(fInput.value) >= parseInt(fInput.min) && parseInt(fInput.value) <= parseInt(tSlider.value)) fSlider.value = fInput.value; fillSlider(fSlider, tSlider, '#C6C6C6', '#5932EA', tSlider); debouncedRequestUpdate(); });
            tInput.addEventListener('input', () => { /* Basic sync, validation on blur/enter */ if (parseInt(tInput.value) <= parseInt(tInput.max) && parseInt(tInput.value) >= parseInt(fSlider.value)) tSlider.value = tInput.value; fillSlider(fSlider, tSlider, '#C6C6C6', '#5932EA', tSlider); debouncedRequestUpdate(); });
            
             // Add blur/enter listeners for final validation and update request
             const validateAndUpdate = (input, isMin) => {
                 let value = parseInt(input.value);
                 const minAllowed = parseInt(input.min);
                 const maxAllowed = parseInt(input.max);
                 const otherSlider = isMin ? tSlider : fSlider;
                 const otherValue = parseInt(otherSlider.value);

                 if (isNaN(value)) value = isMin ? minAllowed : maxAllowed; // Default if invalid input

                 if (isMin) {
                     value = Math.max(minAllowed, Math.min(otherValue, value)); // Clamp between min and other slider's value
                 } else {
                     value = Math.min(maxAllowed, Math.max(otherValue, value)); // Clamp between other slider's value and max
                 }
                 input.value = value; // Update input field
                 (isMin ? fSlider : tSlider).value = value; // Update corresponding slider
                 fillSlider(fSlider, tSlider, '#C6C6C6', '#5932EA', tSlider); // Update visual fill
                 debouncedRequestUpdate(); // Trigger update after validation
             };

             fInput.addEventListener('blur', () => validateAndUpdate(fInput, true));
             tInput.addEventListener('blur', () => validateAndUpdate(tInput, false));
             fInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); validateAndUpdate(fInput, true); } });
             tInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); validateAndUpdate(tInput, false); } });
        };

        setupSliderListeners(fromSlider, toSlider, fromInput, toInput);
        setupSliderListeners(goalFromSlider, goalToSlider, goalFromInput, goalToInput);
        setupSliderListeners(raisedFromSlider, raisedToSlider, raisedFromInput, raisedToInput);

        this.rangeSliderElements = {
            fromSlider, toSlider, fromInput, toInput,
            goalFromSlider, goalToSlider, goalFromInput, goalToInput,
            raisedFromSlider, raisedToSlider, raisedFromInput, raisedToInput,
            fillSlider // Store the function for reset/update
        };

        // Initial fill
        fillSlider(fromSlider, toSlider, '#C6C6C6', '#5932EA', toSlider);
        fillSlider(goalFromSlider, goalToSlider, '#C6C6C6', '#5932EA', goalToSlider);
        fillSlider(raisedFromSlider, raisedToSlider, '#C6C6C6', '#5932EA', raisedToSlider);
    }

} // End of TableManager class

// --- Streamlit Communication ---
let tableManagerInstance = null;

function onRender(event) {
    const data = event.detail.args.component_data;
    console.log("Streamlit RENDER event received:", data);

    if (!window.tableManagerInstance) {
        // First render: Create the manager instance
        window.tableManagerInstance = new TableManager(data);
        // Expose goToPage globally for pagination buttons
        window.goToPage = window.tableManagerInstance.goToPage.bind(window.tableManagerInstance);
    } else {
        // Subsequent renders: Update the existing manager instance
        window.tableManagerInstance.updateUIState(data); // Update filter controls, ranges, etc.
        window.tableManagerInstance.updateTableContent(data.rows_html); // Update table rows
        window.tableManagerInstance.updatePagination(); // Update pagination based on new totalRows/currentPage
        window.tableManagerInstance.adjustHeight(); // Adjust height after content update
    }

    // Add ResizeObserver after the first render
    if (!window.resizeObserver) {
        window.resizeObserver = new ResizeObserver(debounce(() => {
            if (window.tableManagerInstance) {
                window.tableManagerInstance.adjustHeight();
            }
        }, 100)); // Debounce resize events

        const rootElement = document.getElementById('component-root');
        if (rootElement) {
            window.resizeObserver.observe(rootElement);
        }
    }
}

Streamlit.events.addEventListener(Streamlit.RENDER_EVENT, onRender);
Streamlit.setComponentReady();
"""

# --- Create Component Instance ---
table_component = generate_component('kickstarter_table', template=css, script=script)


# --- Main App Logic ---

# 1. Prepare default state and potentially retrieve the last state sent from the component
# Use a default dictionary structure matching what JS sends
default_state = {
    "page": st.session_state.current_page,
    "filters": st.session_state.filters,
    "sort_order": st.session_state.sort_order
}
# This variable will hold the state *sent from* the component on the *previous* run,
# or default_state on the first run or if the component didn't send anything.
component_value = st.session_state.get("kickstarter_state_value", default_state)

# Store previous state for comparison
previous_page = st.session_state.current_page
previous_filters = st.session_state.filters
previous_sort = st.session_state.sort_order

# 2. Update Streamlit session state based on the component value *from the previous run*
#    Always try to update, as the component is the source of truth for interaction changes.
st.session_state.current_page = component_value.get("page", st.session_state.current_page)
st.session_state.filters = component_value.get("filters", st.session_state.filters)
st.session_state.sort_order = component_value.get("sort_order", st.session_state.sort_order)

# Check if the state *actually* changed due to the component value
# Use json.dumps for reliable comparison of potentially nested dicts in filters
state_changed = (
    st.session_state.current_page != previous_page or
    json.dumps(st.session_state.filters, sort_keys=True) != json.dumps(previous_filters, sort_keys=True) or
    st.session_state.sort_order != previous_sort
)

# 3. If the state relevant for fetching data changed based on the component interaction, rerun immediately.
#    The *next* run will then use the correct, updated session state from the start.
if state_changed:
    print("State changed based on component value, triggering rerun.")
    # No need to explicitly store component_value back here, session state is already updated
    st.rerun()

# --- Code from here only executes if state didn't change OR after the st.rerun ---
print("Proceeding with script execution (either initial load or after rerun)")


# 4. Apply filters and sorting to the base LazyFrame (using potentially updated session state)
print(f"Applying filters: {st.session_state.filters}")
print(f"Applying sort: {st.session_state.sort_order}")
filtered_lf = apply_filters_and_sort(
    st.session_state.base_lf,
    st.session_state.filters,
    st.session_state.sort_order
)

# 5. Calculate total rows for pagination *after* filtering
try:
    print("Calculating total rows...")
    start_count_time = time.time()
    # Use pl.len() instead of pl.count()
    total_rows_result = filtered_lf.select(pl.len()).collect()
    st.session_state.total_rows = total_rows_result.item() if total_rows_result is not None and total_rows_result.height > 0 else 0
    count_duration = time.time() - start_count_time
    print(f"Total rows calculated: {st.session_state.total_rows} (took {count_duration:.2f}s)")
except Exception as e:
    st.error(f"Error calculating total rows: {e}")
    st.session_state.total_rows = 0 # Set to 0 on error

# 6. Calculate pagination details
total_pages = math.ceil(st.session_state.total_rows / PAGE_SIZE) if PAGE_SIZE > 0 else 1
# Clamp page number *after* potential update and *before* fetching data
st.session_state.current_page = max(1, min(st.session_state.current_page, total_pages if total_pages > 0 else 1))
offset = (st.session_state.current_page - 1) * PAGE_SIZE

# 7. Fetch *only* the data for the current page
df_page = pl.DataFrame() # Default to empty DF
if st.session_state.total_rows > 0 and offset < st.session_state.total_rows : # Add check offset is valid
    try:
        print(f"Fetching page {st.session_state.current_page} (offset: {offset}, limit: {PAGE_SIZE})...")
        start_fetch_time = time.time()
        df_page = filtered_lf.slice(offset, PAGE_SIZE).collect(engine="streaming")
        fetch_duration = time.time() - start_fetch_time
        print(f"Page data fetched: {len(df_page)} rows (took {fetch_duration:.2f}s)")
    except Exception as e:
        st.error(f"Error fetching data for page {st.session_state.current_page}: {e}")
        # Keep df_page empty
else:
    if st.session_state.total_rows == 0:
         print("No rows match filters, skipping page fetch.")
    else:
         print(f"Calculated offset {offset} is out of bounds for total rows {st.session_state.total_rows}. Resetting page?")
         # This case might happen if filters change drastically. Resetting to page 1 might be desired.
         # For now, we just fetch nothing by keeping df_page empty.


# 8. Generate HTML for the fetched page
header_html, rows_html = generate_table_html_for_page(df_page)

# 9. Prepare data payload for the component *for this run*
#    This payload reflects the state processed in *this* run.
component_data_payload = {
    "current_page": st.session_state.current_page,
    "page_size": PAGE_SIZE,
    "total_rows": st.session_state.total_rows,
    "filters": st.session_state.filters, # Send the current filters
    "sort_order": st.session_state.sort_order, # Send the current sort order
    "header_html": header_html,
    "rows_html": rows_html,
    # Pass necessary options/metadata for UI rendering
    "filter_options": filter_options,
    "category_subcategory_map": category_subcategory_map,
    "min_max_values": min_max_values,
}

# 10. Render the component ONCE, sending the payload and potentially getting the next state request
print("Rendering component...")
# This single call sends the 'component_data_payload' to the frontend for the *current* render.
# It returns the value that was set by 'Streamlit.setComponentValue' on the *previous* interaction in the frontend.
# We store this returned value in session state to process it at the *start* of the *next* script run.
component_return_value = table_component(
    component_data=component_data_payload,
    key="kickstarter_state", # The single key for this component instance
    default=default_state # Default value if component hasn't sent anything yet
)
st.session_state.kickstarter_state_value = component_return_value # Store the received value for the next run