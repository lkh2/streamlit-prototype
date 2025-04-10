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

# --- Component Generation ---
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

if not os.path.exists(filter_metadata_path):
    st.error(f"Filter metadata file not found at '{filter_metadata_path}'. Please run `database_download.py` first.")
    st.stop()
else:
    try:
        with open(filter_metadata_path, 'r', encoding='utf-8') as f:
            loaded_metadata = json.load(f)

        filter_options['categories'] = loaded_metadata.get('categories') or ['All Categories']
        filter_options['countries'] = loaded_metadata.get('countries') or ['All Countries']
        filter_options['states'] = loaded_metadata.get('states') or ['All States']
        filter_options['date_ranges'] = loaded_metadata.get('date_ranges', filter_options['date_ranges'])

        category_subcategory_map = loaded_metadata.get('category_subcategory_map', {'All Categories': ['All Subcategories']})
        if 'All Categories' not in category_subcategory_map:
            category_subcategory_map['All Categories'] = ['All Subcategories']
        if category_subcategory_map['All Categories'] and 'All Subcategories' not in category_subcategory_map['All Categories']:
             category_subcategory_map['All Categories'].insert(0, 'All Subcategories')

        all_subs = set(loaded_metadata.get('subcategories', ['All Subcategories']))
        all_cats_subs = set(category_subcategory_map.get('All Categories', []))
        missing_subs = all_subs - all_cats_subs
        if missing_subs:
             category_subcategory_map['All Categories'].extend(sorted(list(missing_subs)))
             category_subcategory_map['All Categories'] = sorted(list(set(category_subcategory_map['All Categories'])), key=lambda x: (x != 'All Subcategories', x))

        loaded_min_max = loaded_metadata.get('min_max_values', {})
        min_max_values['pledged'] = loaded_min_max.get('pledged', min_max_values['pledged'])
        min_max_values['goal'] = loaded_min_max.get('goal', min_max_values['goal'])
        min_max_values['raised'] = loaded_min_max.get('raised', min_max_values['raised'])

    except json.JSONDecodeError:
        st.error(f"Error decoding JSON from '{filter_metadata_path}'. File might be corrupted. Using default filters.")
    except Exception as e:
        st.error(f"Error loading filter metadata from '{filter_metadata_path}': {e}. Using default filters.")

min_pledged = min_max_values['pledged']['min']
max_pledged = min_max_values['pledged']['max']
min_goal = min_max_values['goal']['min']
max_goal = min_max_values['goal']['max']
min_raised = min_max_values['raised']['min']
max_raised = min_max_values['raised']['max']

# --- Define default state structure ONCE ---
DEFAULT_FILTERS = {
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
DEFAULT_COMPONENT_STATE = {
    "page": 1,
    "filters": DEFAULT_FILTERS,
    "sort_order": 'popularity'
}


# --- Initialize Session State ---
if 'filters' not in st.session_state:
    st.session_state.filters = DEFAULT_FILTERS.copy()
if 'sort_order' not in st.session_state:
    st.session_state.sort_order = DEFAULT_COMPONENT_STATE['sort_order']
if 'current_page' not in st.session_state:
    st.session_state.current_page = DEFAULT_COMPONENT_STATE['page']
if 'total_rows' not in st.session_state:
    st.session_state.total_rows = 0
if 'kickstarter_state_value' not in st.session_state:
    st.session_state.kickstarter_state_value = None
if 'state_sent_to_component' not in st.session_state:
    st.session_state.state_sent_to_component = DEFAULT_COMPONENT_STATE.copy()


# --- Base LazyFrame ---
if 'base_lf' not in st.session_state:
    if not os.path.exists(parquet_source_path):
        st.error(f"Parquet data source not found at '{parquet_source_path}'. Please ensure the file/directory exists.")
        st.stop()

    try:
        base_lf = pl.scan_parquet(parquet_source_path)
        st.session_state.base_lf = base_lf
        schema = st.session_state.base_lf.collect_schema()
        if len(schema) == 0:
             st.error(f"Loaded data from '{parquet_source_path}' has no columns.")
             st.stop()
        if len(schema.names()) != len(set(schema.names())):
             st.error(f"Parquet source '{parquet_source_path}' contains duplicate column names. Please clean the source data.")
             from collections import Counter
             counts = Counter(schema.names())
             duplicates = [name for name, count in counts.items() if count > 1]
             st.error(f"Duplicate columns found: {duplicates}")
             st.stop()

    except Exception as e:
        st.error(f"Error scanning Parquet or initial processing: {e}")
        if hasattr(e, 'context'):
            st.error(f"Context: {e.context()}")
        st.stop()

# --- Filtering and Sorting Logic ---
def apply_filters_and_sort(lf: pl.LazyFrame, filters: dict, sort_order: str) -> pl.LazyFrame:
    column_names = lf.collect_schema().names()

    search_term = filters.get('search', '')
    if search_term:
        search_cols = ['Project Name', 'Creator', 'Category', 'Subcategory']
        valid_search_cols = [col for col in search_cols if col in column_names]
        if valid_search_cols:
            search_expr = None
            for col in valid_search_cols:
                 current_expr = pl.col(col).cast(pl.Utf8).str.contains(f"(?i){search_term}")
                 if search_expr is None:
                     search_expr = current_expr
                 else:
                     search_expr = search_expr | current_expr
            if search_expr is not None:
                 lf = lf.filter(search_expr)

    if 'Category' in column_names and filters['categories'] != ['All Categories']:
        lf = lf.filter(pl.col('Category').is_in(filters['categories']))
    if 'Subcategory' in column_names and filters['subcategories'] != ['All Subcategories']:
        lf = lf.filter(pl.col('Subcategory').is_in(filters['subcategories']))
    if 'Country' in column_names and filters['countries'] != ['All Countries']:
        lf = lf.filter(pl.col('Country').is_in(filters['countries']))

    if 'State' in column_names and filters['states'] != ['All States']:
        lf = lf.filter(pl.col('State').cast(pl.Utf8).str.to_lowercase().is_in([s.lower() for s in filters['states']]))

    ranges = filters.get('ranges', {})
    if 'Raw Pledged' in column_names and 'pledged' in ranges:
        min_p, max_p = ranges['pledged']['min'], ranges['pledged']['max']
        lf = lf.filter((pl.col('Raw Pledged') >= min_p) & (pl.col('Raw Pledged') <= max_p))
    if 'Raw Goal' in column_names and 'goal' in ranges:
        min_g, max_g = ranges['goal']['min'], ranges['goal']['max']
        lf = lf.filter((pl.col('Raw Goal') >= min_g) & (pl.col('Raw Goal') <= max_g))
    if 'Raw Raised' in column_names and 'raised' in ranges:
        min_r, max_r = ranges['raised']['min'], ranges['raised']['max']
        lf = lf.filter((pl.col('Raw Raised') >= min_r) & (pl.col('Raw Raised') <= max_r))


    date_filter = filters.get('date', 'All Time')
    if date_filter != 'All Time' and 'Raw Date' in column_names:
        now = datetime.datetime.now()
        compare_date = None
        if date_filter == 'Last Month':
            compare_date = now - datetime.timedelta(days=30)
        elif date_filter == 'Last 6 Months':
            compare_date = now - datetime.timedelta(days=182)
        elif date_filter == 'Last Year':
            compare_date = now - datetime.timedelta(days=365)
        elif date_filter == 'Last 5 Years':
            compare_date = now - datetime.timedelta(days=5*365)
        elif date_filter == 'Last 10 Years':
            compare_date = now - datetime.timedelta(days=10*365)

        if compare_date:
             lf = lf.with_columns(pl.col("Raw Date").cast(pl.Datetime, strict=False).alias("Raw Date_dt"))
             lf = lf.filter(pl.col('Raw Date_dt') >= compare_date).drop("Raw Date_dt")

    sort_descending = True
    sort_col = 'Popularity Score'

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
        sort_descending = True

    if sort_col in column_names:
        lf = lf.sort(sort_col, descending=sort_descending, nulls_last=True)
    else:
        print(f"Warning: Sort column '{sort_col}' not found in LazyFrame.")

    return lf

# --- Generate HTML for the *current page* data ---
def generate_table_html_for_page(df_page: pl.DataFrame):
    visible_columns = ['Project Name', 'Creator', 'Pledged Amount', 'Link', 'Country', 'State']
    header_html = ''.join(f'<th scope="col">{column}</th>' for column in visible_columns)

    if df_page.is_empty():
        colspan = len(visible_columns) if visible_columns else 1
        return header_html, f'<tr><td colspan="{colspan}">No projects match the current filters.</td></tr>'

    required_data_cols = [
        'Category', 'Subcategory', 'Raw Pledged', 'Raw Goal', 'Raw Raised',
        'Raw Date', 'Raw Deadline', 'Backer Count', 'Popularity Score'
    ]
    all_needed_cols = list(set(visible_columns + required_data_cols + ['State']))

    missing_cols = [col for col in all_needed_cols if col not in df_page.columns]
    if missing_cols:
        st.error(f"FATAL: Missing required columns in fetched data page: {missing_cols}. Check base Parquet schema and processing.")
        colspan = len(visible_columns) if visible_columns else 1
        header_html_error = ''.join(f'<th scope="col">{col}</th>' for col in visible_columns if col in df_page.columns)
        return header_html_error, f'<tr><td colspan="{colspan}">Error: Missing critical data columns: {missing_cols}.</td></tr>'

    rows_html = ''

    try:
        data_dicts = df_page.to_dicts()
    except Exception as e:
        st.error(f"Error converting page DataFrame to dictionaries: {e}")
        return header_html, f'<tr><td colspan="{len(visible_columns)}">Error rendering rows.</td></tr>'

    for row in data_dicts:
        state_value = row.get('State')
        state_value_str = str(state_value) if state_value is not None else 'unknown'
        state_class = state_value_str.lower().replace(' ', '-') if state_value_str != 'unknown' else 'unknown'
        styled_state_html = f'<div class="state_cell state-{html.escape(state_class)}">{html.escape(state_value_str)}</div>' if state_value is not None else '<div class="state_cell state-unknown">unknown</div>'

        raw_date = row.get('Raw Date')
        raw_deadline = row.get('Raw Deadline')
        raw_date_str = raw_date.strftime('%Y-%m-%d') if raw_date else 'N/A'
        raw_deadline_str = raw_deadline.strftime('%Y-%m-%d') if raw_deadline else 'N/A'
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
            value = row.get(col)

            if col == 'Link':
                url = str(value) if value else '#'
                display_url = url if len(url) < 60 else url[:57] + '...'
                visible_cells += f'<td><a href="{html.escape(url)}" target="_blank" title="{html.escape(url)}">{html.escape(display_url)}</a></td>'
            elif col == 'Pledged Amount':
                 raw_pledged_val = row.get('Raw Pledged')
                 formatted_value = 'N/A'
                 if raw_pledged_val is not None:
                     try:
                         amount = int(float(raw_pledged_val))
                         formatted_value = f"${amount:,}"
                     except (ValueError, TypeError):
                         pass
                 visible_cells += f'<td>{html.escape(formatted_value)}</td>'
            elif col == 'State':
                 visible_cells += f'<td>{styled_state_html}</td>'
            else:
                display_value = str(value) if value is not None else 'N/A'
                visible_cells += f'<td>{html.escape(display_value)}</td>'

        rows_html += f'<tr class="table-row" {data_attrs}>{visible_cells}</tr>'

    return header_html, rows_html


# --- Define Component ---
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
        border-radius: 0 0 20px 20px;
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
    
    .filter-flex-wrapper {
        width: 100%;
        justify-content: center;
        display: flex;
    }

    .filter-wrapper {
        max-width: 100%;
        width: fit-content;
        background: transparent;
        border-radius: 20px;
        margin-bottom: 20px;
        min-height: 120px;
        display: flex;    
        flex-direction: row;
        justify-content: space-around;
        overflow-x: auto;
    }
    
    .filter-wrapper::-webkit-scrollbar-track {
        -webkit-box-shadow: inset 0 0 6px rgba(0,0,0,0.05);
        border-radius: 10px;
        background-color: white;
    }

    .filter-wrapper::-webkit-scrollbar {
        width: 8px;
        background-color: transparent;
    }

    .filter-wrapper::-webkit-scrollbar-thumb {
        border-radius: 10px;
        -webkit-box-shadow: inset 0 0 6px rgba(0,0,0,0.05);
        background-color: lightgrey;
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

    body { 
        font-family: 'Poppins', 
        sans-serif; margin: 0; 
        padding: 20px; 
        box-sizing: border-box; 
    }

    #component-root { 
        width: 100%; 
    }

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
        this.componentRoot = document.getElementById('component-root');
        if (!this.componentRoot) {
            console.error("Component root element not found!");
            return;
        }

        this.currentPage = initialData.current_page || 1;
        this.pageSize = initialData.page_size || 10;
        this.totalRows = initialData.total_rows || 0;
        this.currentFilters = initialData.filters || {};
        this.currentSort = initialData.sort_order || 'popularity';
        this.filterOptions = initialData.filter_options || {};
        this.categorySubcategoryMap = initialData.category_subcategory_map || {};
        this.minMaxValues = initialData.min_max_values || {};

        this.subcategoryParentMap = {};
        for (const category in this.categorySubcategoryMap) {
            if (category !== 'All Categories' && Array.isArray(this.categorySubcategoryMap[category])) {
                this.categorySubcategoryMap[category].forEach(subcategory => {
                    if (subcategory !== 'All Subcategories') {
                        // Allow overriding if a subcategory appears under multiple parents?
                        // For now, the last one wins. Consider if this needs refinement.
                        this.subcategoryParentMap[subcategory] = category;
                    }
                });
            }
        }

        this.renderHTMLStructure(initialData.header_html);
        this.bindStaticElements();
        this.updateUIState(initialData);
        this.updateTableContent(initialData.rows_html);
        this.updatePagination();
        this.adjustHeight();
    }

    renderHTMLStructure(headerHtml) {
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
            <div class="filter-flex-wrapper">
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
        this.searchInput = document.getElementById('table-search');
        this.searchInput.addEventListener('input', debounce((e) => {
            this.currentFilters.search = e.target.value.trim();
            this.currentPage = 1;
            this.requestUpdate();
        }, 500));

        document.getElementById('prev-page').addEventListener('click', () => this.previousPage());
        document.getElementById('next-page').addEventListener('click', () => this.nextPage());

        document.getElementById('resetFilters').addEventListener('click', () => this.resetFilters());

        document.getElementById('sortFilter').addEventListener('change', (e) => {
            this.currentSort = e.target.value;
            this.currentPage = 1;
            this.requestUpdate();
        });

        document.getElementById('dateFilter').addEventListener('change', (e) => {
             this.currentFilters.date = e.target.value;
             this.currentPage = 1;
             this.requestUpdate();
        });

        this.setupRangeSlider();

        this.selectedCategories = new Set(this.currentFilters.categories || ['All Categories']);
        this.selectedSubcategories = new Set(this.currentFilters.subcategories || ['All Subcategories']);
        this.selectedCountries = new Set(this.currentFilters.countries || ['All Countries']);
        this.selectedStates = new Set(this.currentFilters.states || ['All States']);

        this.categoryBtn = document.getElementById('categoryFilterBtn');
        this.subcategoryBtn = document.getElementById('subcategoryFilterBtn');
        this.countryBtn = document.getElementById('countryFilterBtn');
        this.stateBtn = document.getElementById('stateFilterBtn');

        this.setupMultiSelect(
            'category',
            document.querySelectorAll('#categoryOptionsContainer .category-option'),
            this.selectedCategories,
            'All Categories',
            this.categoryBtn
        );
        this.updateSubcategoryOptions();
        this.setupMultiSelect(
            'country',
            document.querySelectorAll('#countryOptionsContainer .country-option'),
            this.selectedCountries,
            'All Countries',
            this.countryBtn
        );
         this.setupMultiSelect(
             'state',
             document.querySelectorAll('#stateOptionsContainer .state-option'),
             this.selectedStates,
             'All States',
             this.stateBtn
         );
    }

    updateUIState(data) {
        this.currentPage = data.current_page;
        this.totalRows = data.total_rows;
        this.currentFilters = data.filters;
        this.currentSort = data.sort_order;

        if (this.searchInput) this.searchInput.value = this.currentFilters.search || '';

        const sortSelect = document.getElementById('sortFilter');
        if (sortSelect) sortSelect.value = this.currentSort;

        const dateSelect = document.getElementById('dateFilter');
        if (dateSelect) dateSelect.value = this.currentFilters.date || 'All Time';

        this.selectedCategories = new Set(this.currentFilters.categories || ['All Categories']);
        this.selectedSubcategories = new Set(this.currentFilters.subcategories || ['All Subcategories']);
        this.selectedCountries = new Set(this.currentFilters.countries || ['All Countries']);
        this.selectedStates = new Set(this.currentFilters.states || ['All States']);

        this.updateMultiSelectUI(document.querySelectorAll('#categoryOptionsContainer .category-option'), this.selectedCategories, this.categoryBtn, 'All Categories');
        this.updateMultiSelectUI(document.querySelectorAll('#countryOptionsContainer .country-option'), this.selectedCountries, this.countryBtn, 'All Countries');
        this.updateMultiSelectUI(document.querySelectorAll('#stateOptionsContainer .state-option'), this.selectedStates, this.stateBtn, 'All States');

        this.setupMultiSelect(
            'category',
            document.querySelectorAll('#categoryOptionsContainer .category-option'),
            this.selectedCategories,
            'All Categories',
            this.categoryBtn
        );

        this.updateSubcategoryOptions();

        this.setupMultiSelect(
            'country',
            document.querySelectorAll('#countryOptionsContainer .country-option'),
            this.selectedCountries,
            'All Countries',
            this.countryBtn
        );
         this.setupMultiSelect(
             'state',
             document.querySelectorAll('#stateOptionsContainer .state-option'),
             this.selectedStates,
             'All States',
             this.stateBtn
         );

        if (this.currentFilters.ranges && this.rangeSliderElements) {
             const { ranges } = this.currentFilters;
             const {
                 fromSlider, toSlider, fromInput, toInput,
                 goalFromSlider, goalToSlider, goalFromInput, goalToInput,
                 raisedFromSlider, raisedToSlider, raisedFromInput, raisedToInput,
                 fillSlider
             } = this.rangeSliderElements;

             if (ranges.pledged && fromSlider && toSlider && fromInput && toInput && fillSlider) {
                 fromSlider.value = ranges.pledged.min;
                 toSlider.value = ranges.pledged.max;
                 fromInput.value = ranges.pledged.min;
                 toInput.value = ranges.pledged.max;
                 fillSlider(fromSlider, toSlider, '#C6C6C6', '#5932EA', toSlider);
             }
             if (ranges.goal && goalFromSlider && goalToSlider && goalFromInput && goalToInput && fillSlider) {
                 goalFromSlider.value = ranges.goal.min;
                 goalToSlider.value = ranges.goal.max;
                 goalFromInput.value = ranges.goal.min;
                 goalToInput.value = ranges.goal.max;
                 fillSlider(goalFromSlider, goalToSlider, '#C6C6C6', '#5932EA', goalToSlider);
             }
             if (ranges.raised && raisedFromSlider && raisedToSlider && raisedFromInput && raisedToInput && fillSlider) {
                  raisedFromSlider.value = ranges.raised.min;
                  raisedToSlider.value = ranges.raised.max;
                  raisedFromInput.value = ranges.raised.min;
                  raisedToInput.value = ranges.raised.max;
                  fillSlider(raisedFromSlider, raisedToSlider, '#C6C6C6', '#5932EA', raisedToSlider);
             }
        }
    }

    updateMultiSelectUI(options, selectedSet, buttonElement, allValue) {
         if (!options || options.length === 0 || !selectedSet) return;
         options.forEach(option => {
            const isSelected = selectedSet.has(option.dataset.value);
            option.classList.toggle('selected', isSelected);
         });
         this.updateButtonText(selectedSet, buttonElement, allValue);
    }


    requestUpdate() {
        this.showLoading(true);

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
        Streamlit.setComponentValue(state);
    }

     showLoading(isLoading) {
         const indicator = document.getElementById('loading-indicator');
         if (indicator) {
             indicator.classList.toggle('hidden', !isLoading);
         }
     }


    updateTableContent(rowsHtml) {
        const tbody = document.getElementById('table-body');
        if (tbody) {
            tbody.innerHTML = rowsHtml || '<tr><td colspan="6">Loading data or no results...</td></tr>';
        }
         this.showLoading(false);
    }

    updatePagination() {
        const currentTotalRows = parseInt(this.totalRows || 0, 10);
        const currentPageSize = parseInt(this.pageSize || 10, 10);

        let calculatedPages = 1;
        if (currentPageSize > 0) {
             calculatedPages = Math.ceil(currentTotalRows / currentPageSize);
        }

        const totalPages = Math.max(1, calculatedPages);

        const pageNumbers = this.generatePageNumbers(totalPages);
        const container = document.getElementById('page-numbers');
        if (!container) {
            console.error("Pagination container 'page-numbers' not found!");
             return;
         }

        container.innerHTML = pageNumbers.map(page => {
            if (page === '...') {
                return '<span class="page-ellipsis">...</span>';
            }
            const button = document.createElement('button');
            button.className = `page-number ${page === this.currentPage ? 'active' : ''}`;
            button.textContent = page;
            button.disabled = page === this.currentPage;
            button.dataset.page = page;
            return button.outerHTML;
        }).join('');

        if (!this.handlePageClick) {
             this.handlePageClick = (event) => {
                 if (event.target.classList.contains('page-number') && !event.target.disabled) {
                     this.goToPage(parseInt(event.target.dataset.page));
                 }
             };
        }
        container.removeEventListener('click', this.handlePageClick);
        container.addEventListener('click', this.handlePageClick);

        const prevButton = document.getElementById('prev-page');
        const nextButton = document.getElementById('next-page');

        if (prevButton) {
            prevButton.disabled = this.currentPage <= 1;
        } else {
            console.error("Previous page button not found!");
        }
        if (nextButton) {
            nextButton.disabled = this.currentPage >= totalPages;
        } else {
            console.error("Next page button not found!");
        }
    }

    generatePageNumbers(totalPages) {
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
        const defaultFilters = window.tableManagerInstance?.minMaxValues
            ? {
                search: '',
                categories: ['All Categories'],
                subcategories: ['All Subcategories'],
                countries: ['All Countries'],
                states: ['All States'],
                date: 'All Time',
                ranges: {
                    pledged: { min: this.minMaxValues.pledged.min, max: this.minMaxValues.pledged.max },
                    goal: { min: this.minMaxValues.goal.min, max: this.minMaxValues.goal.max },
                    raised: { min: this.minMaxValues.raised.min, max: this.minMaxValues.raised.max }
                }
            }
            : JSON.parse(JSON.stringify(this.currentFilters)); 

        const defaultSort = 'popularity';
        const defaultPage = 1;

        this.showLoading(true);

        const resetStatePayload = {
            page: defaultPage,
            filters: JSON.parse(JSON.stringify(defaultFilters)), 
            sort_order: defaultSort
        };

        Streamlit.setComponentValue(resetStatePayload);

        try {
            this.currentPage = defaultPage;
            this.currentSort = defaultSort;
            this.currentFilters = JSON.parse(JSON.stringify(defaultFilters));
            this.selectedCategories = new Set(defaultFilters.categories);
            this.selectedSubcategories = new Set(defaultFilters.subcategories);
            this.selectedCountries = new Set(defaultFilters.countries);
            this.selectedStates = new Set(defaultFilters.states);

            if (this.searchInput) this.searchInput.value = defaultFilters.search;
            const sortSelect = document.getElementById('sortFilter');
            if (sortSelect) sortSelect.value = defaultSort;
            const dateSelect = document.getElementById('dateFilter');
            if (dateSelect) dateSelect.value = defaultFilters.date;

            this.updateMultiSelectUI(document.querySelectorAll('#categoryOptionsContainer .category-option'), this.selectedCategories, this.categoryBtn, 'All Categories');
            this.updateMultiSelectUI(document.querySelectorAll('#countryOptionsContainer .country-option'), this.selectedCountries, this.countryBtn, 'All Countries');
            this.updateMultiSelectUI(document.querySelectorAll('#stateOptionsContainer .state-option'), this.selectedStates, this.stateBtn, 'All States');

            this.setupMultiSelect('category', document.querySelectorAll('#categoryOptionsContainer .category-option'), this.selectedCategories, 'All Categories', this.categoryBtn);
            this.updateSubcategoryOptions(); 
            this.setupMultiSelect('country', document.querySelectorAll('#countryOptionsContainer .country-option'), this.selectedCountries, 'All Countries', this.countryBtn);
            this.setupMultiSelect('state', document.querySelectorAll('#stateOptionsContainer .state-option'), this.selectedStates, 'All States', this.stateBtn);

            if (defaultFilters.ranges && this.rangeSliderElements) {
                 const { ranges } = defaultFilters;
                 const { fromSlider, toSlider, fromInput, toInput,
                         goalFromSlider, goalToSlider, goalFromInput, goalToInput,
                         raisedFromSlider, raisedToSlider, raisedFromInput, raisedToInput,
                         fillSlider } = this.rangeSliderElements;

                 if (ranges.pledged && fromSlider && toSlider && fromInput && toInput && fillSlider) {
                     fromSlider.value = ranges.pledged.min; toSlider.value = ranges.pledged.max;
                     fromInput.value = ranges.pledged.min; toInput.value = ranges.pledged.max;
                     fillSlider(fromSlider, toSlider, '#C6C6C6', '#5932EA', toSlider);
                 }
                 if (ranges.goal && goalFromSlider && goalToSlider && goalFromInput && goalToInput && fillSlider) {
                     goalFromSlider.value = ranges.goal.min; goalToSlider.value = ranges.goal.max;
                     goalFromInput.value = ranges.goal.min; goalToInput.value = ranges.goal.max;
                     fillSlider(goalFromSlider, goalToSlider, '#C6C6C6', '#5932EA', goalToSlider);
                 }
                 if (ranges.raised && raisedFromSlider && raisedToSlider && raisedFromInput && raisedToInput && fillSlider) {
                      raisedFromSlider.value = ranges.raised.min; raisedToSlider.value = ranges.raised.max;
                      raisedFromInput.value = ranges.raised.min; raisedToInput.value = ranges.raised.max;
                      fillSlider(raisedFromSlider, raisedToSlider, '#C6C6C6', '#5932EA', raisedToSlider);
                 }
            }

            this.updatePagination();
            this.showLoading(false);
        } catch (error) {
             console.error("Error during optimistic UI reset in resetFilters:", error);
             this.showLoading(false);
        }
    }

    adjustHeight() {
         requestAnimationFrame(() => {
            const root = this.componentRoot;
            if (!root) return;
             const totalHeight = root.scrollHeight + 50;

             if (!this.lastHeight || Math.abs(this.lastHeight - totalHeight) > 10) {
                 this.lastHeight = totalHeight;
                 Streamlit.setFrameHeight(totalHeight);
             }
         });
    }

    updateButtonText(selectedItems, buttonElement, allValueLabel) {
         if (!buttonElement || !selectedItems) return;
         const selectedArray = Array.from(selectedItems);
         const displayItems = selectedArray.filter(item => item !== allValueLabel);
         displayItems.sort((a, b) => a.localeCompare(b));

         if (displayItems.length === 0) {
             buttonElement.textContent = allValueLabel;
         } else if (displayItems.length > 2) {
             buttonElement.textContent = `${displayItems[0]}, ${displayItems[1]} +${displayItems.length - 2}`;
         } else {
             buttonElement.textContent = displayItems.join(', ');
         }
    }

    setupMultiSelect(type, options, selectedSet, allValue, buttonElement) {
        if (!options || options.length === 0 || !selectedSet || !buttonElement) {
             return;
        }

        options.forEach(option => {
            const newOption = option.cloneNode(true);
            option.parentNode.replaceChild(newOption, option);

            if (selectedSet.has(newOption.dataset.value)) {
                 newOption.classList.add('selected');
             } else {
                 newOption.classList.remove('selected');
             }

             newOption.addEventListener('click', (e) => {
                const clickedValue = e.target.dataset.value;
                const isCurrentlySelected = e.target.classList.contains('selected');
                const currentOptions = Array.from(e.target.parentElement.querySelectorAll('[data-value]'));

                if (clickedValue === allValue) {
                    selectedSet.clear();
                    selectedSet.add(allValue);
                    currentOptions.forEach(opt => opt.classList.remove('selected'));
                    e.target.classList.add('selected');
                } else {
                    const allOptionElement = e.target.parentElement.querySelector(`[data-value="${allValue}"]`);
                    if (allOptionElement && selectedSet.has(allValue)) {
                        selectedSet.delete(allValue);
                        if (allOptionElement) allOptionElement.classList.remove('selected');
                    }

                    if (isCurrentlySelected) {
                        selectedSet.delete(clickedValue);
                        e.target.classList.remove('selected');
                    } else {
                        selectedSet.add(clickedValue);
                        e.target.classList.add('selected');
                    }

                    const hasSpecificSelection = Array.from(selectedSet).some(item => item !== allValue);
                    if (!hasSpecificSelection) {
                         selectedSet.clear();
                         selectedSet.add(allValue);
                         if (allOptionElement) allOptionElement.classList.add('selected');
                         currentOptions.forEach(opt => {
                              if (opt.dataset.value !== allValue) opt.classList.remove('selected');
                         });
                    }
                }

                if (type === 'category') {
                    this.updateSubcategoryOptions(); 
                } else if (type === 'subcategory') {
                    if (clickedValue !== allValue && !isCurrentlySelected) {
                        const parentCategory = this.subcategoryParentMap[clickedValue];
                        if (parentCategory && !this.selectedCategories.has(parentCategory)) {
                             if (this.selectedCategories.has('All Categories')) {
                                 this.selectedCategories.delete('All Categories');
                             }
                             this.selectedCategories.add(parentCategory);
                             this.updateMultiSelectUI(document.querySelectorAll('#categoryOptionsContainer .category-option'), this.selectedCategories, this.categoryBtn, 'All Categories');
                             this.setupMultiSelect('category', document.querySelectorAll('#categoryOptionsContainer .category-option'), this.selectedCategories, 'All Categories', this.categoryBtn);
                        }
                    }
                }

                this.updateButtonText(selectedSet, buttonElement, allValue);
                this.currentPage = 1;
                this.requestUpdate();
            });
        });

        this.updateButtonText(selectedSet, buttonElement, allValue);
    }


    updateSubcategoryOptions() {
        const subcategoryOptionsContainer = document.getElementById('subcategoryOptionsContainer');
        const subcategoryBtn = document.getElementById('subcategoryFilterBtn'); 
        if (!subcategoryOptionsContainer || !subcategoryBtn || !this.selectedSubcategories || !this.categorySubcategoryMap || !this.selectedCategories) {
            console.warn("Cannot update subcategory options - missing elements or data.");
            return;
        }

        const isAllCategoriesSelected = this.selectedCategories.has('All Categories');
        let availableSubcategories = new Set(['All Subcategories']); 

        if (isAllCategoriesSelected || this.selectedCategories.size === 0) {
             (this.categorySubcategoryMap['All Categories'] || []).forEach(subcat => {
                 availableSubcategories.add(subcat); 
            });
        } else {
            this.selectedCategories.forEach(cat => {
                (this.categorySubcategoryMap[cat] || []).forEach(subcat => {
                    availableSubcategories.add(subcat); 
                });
            });
        }

        let changedSelection = false;
        const currentSelectedSubs = Array.from(this.selectedSubcategories);
        currentSelectedSubs.forEach(subcat => {
            if (subcat !== 'All Subcategories' && !availableSubcategories.has(subcat)) {
                this.selectedSubcategories.delete(subcat);
                changedSelection = true;
            }
        });

        const hasSpecificSelection = Array.from(this.selectedSubcategories).some(s => s !== 'All Subcategories');
        if (!hasSpecificSelection && !this.selectedSubcategories.has('All Subcategories')) {
            this.selectedSubcategories.clear();
            this.selectedSubcategories.add('All Subcategories');
            changedSelection = true;
        } else if (this.selectedSubcategories.size === 0) {
             this.selectedSubcategories.add('All Subcategories');
             changedSelection = true;
        }

        const sortedSubcategories = Array.from(availableSubcategories);
        sortedSubcategories.sort((a, b) => {
            if (a === 'All Subcategories') return -1;
            if (b === 'All Subcategories') return 1;
            return a.localeCompare(b);
        });

        subcategoryOptionsContainer.innerHTML = sortedSubcategories.map(opt =>
            `<div class="subcategory-option ${this.selectedSubcategories.has(opt) ? 'selected' : ''}" data-value="${opt}">${opt}</div>`
        ).join('');

        this.updateButtonText(this.selectedSubcategories, subcategoryBtn, 'All Subcategories');

        this.setupMultiSelect(
            'subcategory', 
             subcategoryOptionsContainer.querySelectorAll('.subcategory-option'),
             this.selectedSubcategories,
             'All Subcategories',
             subcategoryBtn 
         );
         return changedSelection;
    }

    setupRangeSlider() {
        const fromSlider = document.getElementById('fromSlider');
        const toSlider = document.getElementById('toSlider');
        const fromInput = document.getElementById('fromInput');
        const toInput = document.getElementById('toInput');
        const goalFromSlider = document.getElementById('goalFromSlider');
        const goalToSlider = document.getElementById('goalToSlider');
        const goalFromInput = document.getElementById('goalFromInput');
        const goalToInput = document.getElementById('goalToInput');
        const raisedFromSlider = document.getElementById('raisedFromSlider');
        const raisedToSlider = document.getElementById('raisedToSlider');
        const raisedFromInput = document.getElementById('raisedFromInput');
        const raisedToInput = document.getElementById('raisedToInput');

        if (!fromSlider || !toSlider || !fromInput || !toInput ||
            !goalFromSlider || !goalToSlider || !goalFromInput || !goalToInput ||
            !raisedFromSlider || !raisedToSlider || !raisedFromInput || !raisedToInput) {
             console.error("One or more range slider elements not found. Aborting setup.");
             this.rangeSliderElements = null;
             return;
        }

        this.rangeSliderElements = {
            fromSlider, toSlider, fromInput, toInput,
            goalFromSlider, goalToSlider, goalFromInput, goalToInput,
            raisedFromSlider, raisedToSlider, raisedFromInput, raisedToInput,
            fillSlider: null
        };


        const fillSlider = (from, to, sliderColor, rangeColor, controlSlider) => {
             if (!from || !to || !controlSlider) return;

            const min = parseFloat(controlSlider.min);
            const max = parseFloat(controlSlider.max);
            const validMin = isNaN(min) ? 0 : min;
            const validMax = isNaN(max) ? (validMin + 1) : max; 

            const fromVal = parseFloat(from.value);
            const toVal = parseFloat(to.value);
            const validFromVal = isNaN(fromVal) ? validMin : Math.max(validMin, Math.min(validMax, fromVal));
            const validToVal = isNaN(toVal) ? validMax : Math.max(validMin, Math.min(validMax, toVal));


            const rangeDistance = validMax - validMin;
            const safeRangeDistance = (rangeDistance > 0) ? rangeDistance : 1;
            const displayFromVal = Math.min(validFromVal, validToVal);
            const displayToVal = Math.max(validFromVal, validToVal);
            const fromPosition = displayFromVal - validMin;
            const toPosition = displayToVal - validMin;
            const fromPercent = Math.min(100, Math.max(0, (fromPosition / safeRangeDistance) * 100));
            const toPercent = Math.min(100, Math.max(0, (toPosition / safeRangeDistance) * 100));

            controlSlider.style.background = `linear-gradient(
                to right,
                ${sliderColor} 0%,
                ${sliderColor} ${fromPercent}%,
                ${rangeColor} ${fromPercent}%,
                ${rangeColor} ${toPercent}%,
                ${sliderColor} ${toPercent}%,
                ${sliderColor} 100%)`;
        };

        this.rangeSliderElements.fillSlider = fillSlider;

        const debouncedRangeUpdate = debounce(() => {
            this.currentPage = 1;
            this.requestUpdate();
        }, 400);


        const controlFromInput = (fromSlider, toSlider, fromInput, fillFn) => {
             const minVal = parseFloat(fromSlider.min);
             const maxVal = parseFloat(toSlider.value); 
             let fromVal = parseFloat(fromInput.value);

             if (isNaN(fromVal) || fromVal < minVal) fromVal = minVal;
             if (fromVal > maxVal) fromVal = maxVal;

             fromInput.value = fromVal; 
             fromSlider.value = fromVal; 
             fillFn(fromSlider, toSlider, '#C6C6C6', '#5932EA', toSlider); 
        };

        const controlToInput = (fromSlider, toSlider, toInput, fillFn) => {
             const minVal = parseFloat(fromSlider.value); 
             const maxVal = parseFloat(toSlider.max);
             let toVal = parseFloat(toInput.value);

             if (isNaN(toVal) || toVal > maxVal) toVal = maxVal;
             if (toVal < minVal) toVal = minVal;

             toInput.value = toVal;
             toSlider.value = toVal;
             fillFn(fromSlider, toSlider, '#C6C6C6', '#5932EA', toSlider); 
        };

        const controlFromSlider = (fromSlider, toSlider, fromInput, fillFn) => {
             const fromVal = parseFloat(fromSlider.value);
             const toVal = parseFloat(toSlider.value);
             if (fromVal > toVal) {
                 toSlider.value = fromVal;
                 const toInputId = toSlider.id.replace('Slider', 'Input');
                 const toInput = document.getElementById(toInputId);
                 if(toInput) toInput.value = fromVal;
             }
             fromInput.value = fromVal;
             fillFn(fromSlider, toSlider, '#C6C6C6', '#5932EA', toSlider);
        };

        const controlToSlider = (fromSlider, toSlider, toInput, fillFn) => {
             const fromVal = parseFloat(fromSlider.value);
             const toVal = parseFloat(toSlider.value);
             if (fromVal > toVal) {
                 fromSlider.value = toVal;
                 const fromInputId = fromSlider.id.replace('Slider', 'Input');
                 const fromInput = document.getElementById(fromInputId);
                 if(fromInput) fromInput.value = toVal;
             }
             toInput.value = toVal;
             fillFn(fromSlider, toSlider, '#C6C6C6', '#5932EA', toSlider);
        };

        const makeControlFn = (controlFn, fillFn) => {
            return (slider1, slider2, input) => controlFn(slider1, slider2, input, fillFn);
        };

        const controlFromInputFilled = makeControlFn(controlFromInput, fillSlider);
        const controlToInputFilled = makeControlFn(controlToInput, fillSlider);
        const controlFromSliderFilled = makeControlFn(controlFromSlider, fillSlider);
        const controlToSliderFilled = makeControlFn(controlToSlider, fillSlider);


        const setupSliderListeners = (fSlider, tSlider, fInput, tInput) => {
            fSlider.addEventListener('input', () => { controlFromSliderFilled(fSlider, tSlider, fInput); debouncedRangeUpdate(); });
            tSlider.addEventListener('input', () => { controlToSliderFilled(fSlider, tSlider, tInput); debouncedRangeUpdate(); });
            fInput.addEventListener('input', () => { controlFromInputFilled(fSlider, tSlider, fInput); debouncedRangeUpdate(); });
            tInput.addEventListener('input', () => { controlToInputFilled(fSlider, tSlider, tInput); debouncedRangeUpdate(); });
        };

        setupSliderListeners(fromSlider, toSlider, fromInput, toInput);
        setupSliderListeners(goalFromSlider, goalToSlider, goalFromInput, goalToInput);
        setupSliderListeners(raisedFromSlider, raisedToSlider, raisedFromInput, raisedToInput);

        fillSlider(fromSlider, toSlider, '#C6C6C6', '#5932EA', toSlider);
        fillSlider(goalFromSlider, goalToSlider, '#C6C6C6', '#5932EA', goalToSlider);
        fillSlider(raisedFromSlider, raisedToSlider, '#C6C6C6', '#5932EA', raisedToSlider);
    }

}

let tableManagerInstance = null;

function onRender(event) {
    try {
        const data = event.detail.args.component_data;

        if (!data) {
             console.warn("onRender called with no data. Skipping update.");
             return;
        }

        if (!window.tableManagerInstance) {
            window.tableManagerInstance = new TableManager(data);
        } else {
            window.tableManagerInstance.updateUIState(data);
            window.tableManagerInstance.updateTableContent(data.rows_html);
            window.tableManagerInstance.updatePagination();
            window.tableManagerInstance.adjustHeight();
        }

        if (!window.resizeObserver && document.getElementById('component-root')) {
            window.resizeObserver = new ResizeObserver(debounce(() => {
                if (window.tableManagerInstance) {
                    window.tableManagerInstance.adjustHeight();
                }
            }, 150));

            window.resizeObserver.observe(document.getElementById('component-root'));
        }

    } catch (error) {
        console.error("Error during onRender:", error);
        if (window.tableManagerInstance && typeof window.tableManagerInstance.showLoading === 'function') {
             window.tableManagerInstance.showLoading(false);
        }
    }
}

Streamlit.events.addEventListener(Streamlit.RENDER_EVENT, onRender);
Streamlit.setComponentReady();
"""

# --- Create Component Instance ---
table_component = generate_component('kickstarter_table', template=css, script=script)


# --- Main App Logic ---

component_state_from_last_run = st.session_state.get("kickstarter_state_value", None)
state_sent_last_run = st.session_state.get('state_sent_to_component', DEFAULT_COMPONENT_STATE)

component_sent_new_state = False
if component_state_from_last_run is not None:
    try:
        last_run_str = json.dumps(component_state_from_last_run, sort_keys=True)
        sent_last_run_str = json.dumps(state_sent_last_run, sort_keys=True)
        if last_run_str != sent_last_run_str:
            component_sent_new_state = True
    except TypeError as e:
         print(f"Error comparing states using JSON: {e}. Assuming state is new for safety.")
         component_sent_new_state = True

if component_sent_new_state:
    if (isinstance(component_state_from_last_run, dict) and
            "page" in component_state_from_last_run and
            "sort_order" in component_state_from_last_run and
            "filters" in component_state_from_last_run and
            isinstance(component_state_from_last_run.get("filters"), dict)):

        st.session_state.current_page = component_state_from_last_run["page"]
        st.session_state.sort_order = component_state_from_last_run["sort_order"]

        new_filters = component_state_from_last_run["filters"]
        validated_filters = DEFAULT_FILTERS.copy()
        for key, default_value in DEFAULT_FILTERS.items():
            if key in new_filters:
                 if key == 'ranges':
                      if isinstance(new_filters[key], dict):
                          validated_range = default_value.copy()
                          for r_key, r_default in default_value.items():
                              if r_key in new_filters[key] and isinstance(new_filters[key].get(r_key), dict) and all(k in new_filters[key][r_key] for k in ['min', 'max']):
                                  try:
                                      min_val = float(new_filters[key][r_key]['min'])
                                      max_val = float(new_filters[key][r_key]['max'])
                                      validated_range[r_key] = {'min': min_val, 'max': max_val}
                                  except (ValueError, TypeError):
                                      print(f"Warning: Invalid min/max type for range '{r_key}'. Using default.")
                              else:
                                   print(f"Warning: Invalid/missing structure for range '{r_key}'. Using default.")
                          validated_filters[key] = validated_range
                      else:
                          print(f"Warning: Invalid type for 'ranges'. Using default.")
                 elif isinstance(new_filters.get(key), type(default_value)):
                      validated_filters[key] = new_filters[key]
                 else:
                      print(f"Warning: Type mismatch for filter '{key}'. Using default.")

        st.session_state.filters = validated_filters
    else:
        print(f"Warning: Invalid structure in new component state: {component_state_from_last_run}. NOT updating session state.")

if 'base_lf' not in st.session_state:
     st.error("Base LazyFrame not found. Please reload.")
     st.stop()

filtered_lf = apply_filters_and_sort(
    st.session_state.base_lf,
    st.session_state.filters,
    st.session_state.sort_order
)

try:
    total_rows_result_df = filtered_lf.select(pl.len()).collect()
    st.session_state.total_rows = total_rows_result_df.item() if total_rows_result_df is not None and not total_rows_result_df.is_empty() else 0
except Exception as e:
    st.error(f"Error calculating total rows: {e}")
    st.session_state.total_rows = 0

total_pages = math.ceil(st.session_state.total_rows / PAGE_SIZE) if PAGE_SIZE > 0 and st.session_state.total_rows > 0 else 1
st.session_state.current_page = max(1, min(st.session_state.current_page, total_pages))
offset = (st.session_state.current_page - 1) * PAGE_SIZE

df_page = pl.DataFrame()

if st.session_state.total_rows > 0 and offset < st.session_state.total_rows:
    try:
        df_page = filtered_lf.slice(offset, PAGE_SIZE).collect()
    except Exception as e:
        st.error(f"Error fetching data for page {st.session_state.current_page}: {e}")
        df_page = pl.DataFrame()


header_html, rows_html = generate_table_html_for_page(df_page)

component_data_payload = {
    "current_page": st.session_state.current_page,
    "page_size": PAGE_SIZE,
    "total_rows": st.session_state.total_rows,
    "filters": st.session_state.filters,
    "sort_order": st.session_state.sort_order,
    "header_html": header_html,
    "rows_html": rows_html,
    "filter_options": filter_options,
    "category_subcategory_map": category_subcategory_map,
    "min_max_values": min_max_values,
}

state_being_sent_this_run = {
    "page": st.session_state.current_page,
    "filters": st.session_state.filters,
    "sort_order": st.session_state.sort_order,
}
st.session_state.state_sent_to_component = json.loads(json.dumps(state_being_sent_this_run))


component_return_value = table_component(
    component_data=component_data_payload,
    key="kickstarter_state",
    default=None
)

needs_rerun = False
if component_return_value is not None:
    if (isinstance(component_return_value, dict) and
            "page" in component_return_value and
            "sort_order" in component_return_value and
            "filters" in component_return_value and
            isinstance(component_return_value.get("filters"), dict)):

        try:
            received_state_str = json.dumps(component_return_value, sort_keys=True)
            sent_state_str = json.dumps(state_being_sent_this_run, sort_keys=True)

            if received_state_str != sent_state_str:
                st.session_state.current_page = component_return_value["page"]
                st.session_state.sort_order = component_return_value["sort_order"]

                new_filters = component_return_value["filters"]
                validated_filters = DEFAULT_FILTERS.copy()
                if isinstance(new_filters, dict):
                     validated_filters.update(new_filters)
                st.session_state.filters = validated_filters
                needs_rerun = True
        except Exception as e:
            print(f"Error during state comparison or update for rerun: {e}")
    else:
        print("Warning: Invalid structure received from component at end of run. Skipping comparison/update.")


st.session_state.kickstarter_state_value = component_return_value

if needs_rerun:
    st.rerun()